import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone

class LoadPlayers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
        # Initialize the locks if they don't exist yet
        if not hasattr(self.bot, 'locked_games'):
            self.bot.locked_games = set()
        if not hasattr(self.bot, 'save_lock'):
            self.bot.save_lock = asyncio.Lock()

    @app_commands.command(name="load_players", description="Load historical player data in batches to allow concurrent tasks.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)", batch_size="How many games to process in this specific task.")
    async def load_players(self, interaction: discord.Interaction, clan_tag: str, batch_size: int = 1000):
        tag_upper = clan_tag.upper()
        
        await interaction.response.send_message(
            f"Started a background task to process **{batch_size}** historical games for **[{tag_upper}]**.\n"
            f"Progress will be auto-saved every 50 games to prevent data loss! (Check terminal for progress)."
        )
        
        self.bot.loop.create_task(self.background_process_games(tag_upper, batch_size, interaction.channel))

    async def background_process_games(self, tag_upper, batch_size, channel):
        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions"

        # Ensure the databases exist before we start directly modifying them
        if tag_upper not in self.bot.player_data:
            self.bot.player_data[tag_upper] = {"total_games": 0, "players": {}}
        if tag_upper not in self.bot.processed_games:
            self.bot.processed_games[tag_upper] = []

        # Calculate exactly 2 hours ago to filter out recent live games
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        two_hours_ago_ms = int(two_hours_ago.timestamp() * 1000)

        games_to_process = []
        games_added = 0
        games_skipped_recent = 0
        new_players_count = 0

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

                    # --- CONCURRENCY LOCKING PHASE ---
                    # Skim the massive list and lock down the next available batch
                    for game in sessions:
                        game_id = game.get("gameId")
                        
                        # 1. Skip if ALREADY in the permanent file
                        if game_id in self.bot.processed_games[tag_upper]:
                            continue
                            
                        # 2. Skip if in the live tracker's 2-hour window
                        if hasattr(self.bot, 'recent_games') and tag_upper in self.bot.recent_games:
                            if game_id in self.bot.recent_games[tag_upper]:
                                continue
                                
                        # 3. Skip if ANOTHER concurrent /load_players task is currently working on it
                        if game_id in self.bot.locked_games:
                            continue
                            
                        # Lock it down for this specific task
                        games_to_process.append(game)
                        self.bot.locked_games.add(game_id)
                        
                        # Stop gathering once we hit the requested batch size limit
                        if len(games_to_process) >= batch_size:
                            break

                    if not games_to_process:
                        await channel.send(f"[{tag_upper}] No more unprocessed games found! The history is fully loaded.")
                        return

                    print(f"[{tag_upper}] TASK STARTED: Reserved a batch of {len(games_to_process)} unprocessed games.")

                    # --- PROCESSING PHASE ---
                    for index, game in enumerate(games_to_process):
                        game_id = game.get("gameId")
                        is_win = game.get("hasWon", False)

                        game_url = f"https://api.openfront.io/public/game/{game_id}?turns=false"
                        async with session.get(game_url) as game_resp:
                            if game_resp.status == 200:
                                game_data = await game_resp.json()
                                info = game_data.get("info", {})
                                
                                # Check if the game started within the last 2 hours.
                                start_time_ms = info.get("start")
                                if start_time_ms and int(start_time_ms) >= two_hours_ago_ms:
                                    games_skipped_recent += 1
                                    continue

                                player_list = info.get("players", [])
                                seen_players_in_game = set()

                                for p in player_list:
                                    if p.get("clanTag", "").upper() == tag_upper:
                                        p_name = p.get("username", "Unknown")

                                        if p_name in seen_players_in_game:
                                            continue  
                                        seen_players_in_game.add(p_name)

                                        # ---> DIRECT LIVE MEMORY UPDATE <---
                                        if p_name not in self.bot.player_data[tag_upper]["players"]:
                                            self.bot.player_data[tag_upper]["players"][p_name] = {"name": [p_name], "games_played": 0, "wins": 0}
                                            new_players_count += 1
                                        
                                        p_stats = self.bot.player_data[tag_upper]["players"][p_name]
                                        
                                        # Data migration safety for names
                                        if not isinstance(p_stats.get("name"), list):
                                            p_stats["name"] = [p_stats.get("name", p_name)]
                                        if p_name not in p_stats["name"]:
                                            p_stats["name"].append(p_name)
                                            
                                        p_stats["games_played"] += 1
                                        if is_win:
                                            p_stats["wins"] += 1
                                            
                                        # Recalculate winrate instantly
                                        if p_stats["games_played"] > 0:
                                            p_stats["winrate"] = round((p_stats["wins"] / p_stats["games_played"]) * 100, 2)

                                games_added += 1
                                self.bot.player_data[tag_upper]["total_games"] += 1
                                
                                # Append directly to the live processed file cache
                                self.bot.processed_games[tag_upper].append(game_id)

                        # --- PERIODIC HARD DRIVE SAVE ---
                        # Every 50 iterations, briefly lock the files and save progress
                        if (index + 1) % 50 == 0:
                            print(f"[{tag_upper} TASK] Auto-saving... {index + 1}/{len(games_to_process)} games processed. ({games_added} matches tallied)")
                            async with self.bot.save_lock:
                                self.bot.save_data()

                        # Small delay to prevent API ratelimiting
                        await asyncio.sleep(0.3)

                    # --- FINAL SAFE SAVE PHASE ---
                    # Ensure the final batch of games is saved when the task completes
                    async with self.bot.save_lock:
                        self.bot.save_data()
                        print(f"[{tag_upper}] Task complete! Final database save successful.")

                    embed = discord.Embed(
                        title=f"Task Complete: [{tag_upper}]", 
                        description=f"Successfully processed and auto-saved **{games_added}** historical matches into the live database.\n\n"
                                    f"Found **{new_players_count}** new unique players.\n"
                                    f"Skipped **{games_skipped_recent}** games from the last 2 hours (handled by live tracker).",
                        color=discord.Color.green()
                    )
                    await channel.send(embed=embed)

        except Exception as e:
            await channel.send(f"An error occurred while processing background data for **[{tag_upper}]**: {e}")
            print(f"Error in load_players for {tag_upper}: {e}")
            
        finally:
            # IMPORTANT: Once the task is fully complete (or if it crashes), 
            # we unlock the game IDs. Since they are now physically saved in processed_games.json, 
            # they won't be double-counted anyway. If the bot crashed before an auto-save, unlocking them 
            # allows a future task to safely retry the lost ones.
            for game in games_to_process:
                self.bot.locked_games.discard(game.get("gameId"))

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))