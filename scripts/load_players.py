import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone

class LoadPlayers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
        # Initialize the save lock if it doesn't exist
        if not hasattr(self.bot, 'save_lock'):
            self.bot.save_lock = asyncio.Lock()
            
        # GLOBAL SWARM LOCK: Ensures only one swarm runs on the entire bot at once
        if not hasattr(self.bot, 'is_swarm_active'):
            self.bot.is_swarm_active = False

    @app_commands.command(name="load_players", description="Automatically queue and load all history using a worker swarm.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)", workers="Concurrent API workers (Max 10 to avoid IP bans).")
    async def load_players(self, interaction: discord.Interaction, clan_tag: str, workers: int = 5):
        # 1. Check the Global Lock BEFORE doing anything else
        if getattr(self.bot, 'is_swarm_active', False):
            await interaction.response.send_message(
                "**Global Swarm Lock Active!**\nAnother clan's history is currently being processed. To protect the bot from API bans, please wait for the current swarm to finish.", 
                ephemeral=True
            )
            return

        tag_upper = clan_tag.upper()
        
        # Hard limit to protect the user from getting API-banned
        if workers > 10:
            workers = 10
            msg = f"Capped workers at **10** to prevent API bans.\nScanning history for **[{tag_upper}]** to queue unprocessed games..."
        else:
            msg = f"Scanning history for **[{tag_upper}]**...\nI will spawn a swarm of **{workers} concurrent workers** to process missing games."

        await interaction.response.send_message(msg)
        
        # 2. Engage the lock and dispatch the master controller
        self.bot.is_swarm_active = True
        self.bot.loop.create_task(self.master_process(tag_upper, workers, interaction.channel))

    async def master_process(self, tag_upper, workers, channel):
        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions"

        if tag_upper not in self.bot.player_data:
            self.bot.player_data[tag_upper] = {"total_games": 0, "players": {}}
        if tag_upper not in self.bot.processed_games:
            self.bot.processed_games[tag_upper] = []

        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        two_hours_ago_ms = int(two_hours_ago.timestamp() * 1000)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        await channel.send(f"Failed to load API for **[{tag_upper}]** (Status {resp.status}).")
                        return
                        
                    sessions = await resp.json()
                    if not sessions:
                        await channel.send(f"No sessions found for **[{tag_upper}]**.")
                        return

                # --- 1. FILTERING PHASE ---
                games_to_process = []
                for game in sessions:
                    game_id = game.get("gameId")
                    if not game_id: continue
                    
                    if game_id in self.bot.processed_games[tag_upper]:
                        continue
                        
                    if hasattr(self.bot, 'recent_games') and tag_upper in getattr(self.bot, 'recent_games', {}):
                        if game_id in self.bot.recent_games[tag_upper]:
                            continue
                            
                    games_to_process.append(game)

                total_games = len(games_to_process)
                if total_games == 0:
                    await channel.send(f"**[{tag_upper}]** is fully up to date! No historical games left to process.")
                    return

                await channel.send(f"🚀 Found **{total_games}** unprocessed games! Unleashing the swarm...")
                print(f"[{tag_upper}] STARTING SWARM: {total_games} games distributed across {workers} workers.")

                # --- 2. BUILD THE QUEUE ---
                queue = asyncio.Queue()
                for game in games_to_process:
                    queue.put_nowait(game)

                # Use lists as mutable references to pass counters into the worker scopes
                games_added = [0] 
                new_players_count = [0]
                games_skipped_recent = [0]

                # --- 3. THE WORKER DEFINITION ---
                async def worker_task(worker_id):
                    while True:
                        try:
                            game = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                            
                        game_id = game.get("gameId")
                        is_win = game.get("hasWon", False)
                        
                        game_url = f"https://api.openfront.io/public/game/{game_id}?turns=false"
                        try:
                            async with session.get(game_url, timeout=15) as game_resp:
                                if game_resp.status == 200:
                                    game_data = await game_resp.json()
                                    info = game_data.get("info", {})
                                    
                                    start_time_ms = info.get("start")
                                    if start_time_ms and int(start_time_ms) >= two_hours_ago_ms:
                                        games_skipped_recent[0] += 1
                                        queue.task_done()
                                        continue

                                    player_list = info.get("players", [])
                                    seen_players_in_game = set()

                                    for p in player_list:
                                        if p.get("clanTag", "").upper() == tag_upper:
                                            p_name = p.get("username", "Unknown")

                                            if p_name in seen_players_in_game:
                                                continue  
                                            seen_players_in_game.add(p_name)

                                            # Direct Memory Edit
                                            if p_name not in self.bot.player_data[tag_upper]["players"]:
                                                self.bot.player_data[tag_upper]["players"][p_name] = {"name": [p_name], "games_played": 0, "wins": 0}
                                                new_players_count[0] += 1
                                            
                                            p_stats = self.bot.player_data[tag_upper]["players"][p_name]
                                            
                                            if not isinstance(p_stats.get("name"), list):
                                                p_stats["name"] = [p_stats.get("name", p_name)]
                                            if p_name not in p_stats["name"]:
                                                p_stats["name"].append(p_name)
                                                
                                            p_stats["games_played"] += 1
                                            if is_win:
                                                p_stats["wins"] += 1
                                                
                                            if p_stats["games_played"] > 0:
                                                p_stats["winrate"] = round((p_stats["wins"] / p_stats["games_played"]) * 100, 2)

                                    games_added[0] += 1
                                    self.bot.player_data[tag_upper]["total_games"] += 1
                                    self.bot.processed_games[tag_upper].append(game_id)
                                    
                                    # Output progress to the terminal
                                    if games_added[0] % 100 == 0:
                                        print(f"[{tag_upper}] Progress: {games_added[0]}/{total_games} games processed...")

                        except Exception as e:
                            print(f"[Worker {worker_id}] Error on {game_id}: {e}")
                            
                        # Tell the queue this specific game is finished
                        queue.task_done()
                        
                        # Small delay to prevent the API from dropping connections
                        await asyncio.sleep(0.1) 

                # --- 4. THE AUTO-SAVER DEFINITION ---
                async def auto_saver():
                    while not queue.empty():
                        await asyncio.sleep(60) # Wake up every 60 seconds
                        async with self.bot.save_lock:
                            self.bot.save_data()
                        print(f"[{tag_upper}] Auto-saved progress to disk.")

                # --- 5. UNLEASH THE SWARM ---
                saver = asyncio.create_task(auto_saver())
                worker_tasks = [asyncio.create_task(worker_task(i)) for i in range(workers)]
                
                # Halt the master script here until the workers empty the entire queue
                await queue.join()
                
                # Cleanup
                saver.cancel()
                
                # Final safe save
                async with self.bot.save_lock:
                    self.bot.save_data()
                
                print(f"[{tag_upper}] SWARM COMPLETE! Final save successful.")

                embed = discord.Embed(
                    title=f"Swarm Load Complete: [{tag_upper}]", 
                    description=f"Successfully chewed through and tallied **{games_added[0]}** historical matches using {workers} concurrent workers.\n\n"
                                f"Found **{new_players_count[0]}** new unique players.\n"
                                f"Skipped **{games_skipped_recent[0]}** recent games (left for live tracker).",
                    color=discord.Color.green()
                )
                await channel.send(embed=embed)

        except Exception as e:
            await channel.send(f"An error occurred during the swarm load for **[{tag_upper}]**: {e}")
            print(f"Error in master_process for {tag_upper}: {e}")
            
        finally:
            # 3. Disengage the lock when finished (or if it crashes!) so the bot is ready again
            self.bot.is_swarm_active = False

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))