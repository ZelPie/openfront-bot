import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import asyncio
import random
from datetime import datetime, timedelta, timezone

class LoadPlayers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
        if not hasattr(self.bot, 'save_lock'):
            self.bot.save_lock = asyncio.Lock()
            
        if not hasattr(self.bot, 'is_swarm_active'):
            self.bot.is_swarm_active = False

    @app_commands.command(name="load_players", description="Automatically queue and load all history using a batched worker swarm.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)", workers="Concurrent API workers (Max 10 to avoid IP bans).")
    async def load_players(self, interaction: discord.Interaction, clan_tag: str, workers: int = 10):
        if getattr(self.bot, 'is_swarm_active', False):
            await interaction.response.send_message(
                "Global Swarm Lock Active! Another clan's history is currently being processed. Please wait.", 
                ephemeral=True
            )
            return

        tag_upper = clan_tag.upper()
        workers = min(workers, 10) 

        await interaction.response.send_message(f"Paging backward through history for [{tag_upper}] to build batches. This might take a minute...")
        
        self.bot.is_swarm_active = True
        self.bot.loop.create_task(self.master_process(tag_upper, workers, interaction.channel))

    async def master_process(self, tag_upper, workers, channel):
        base_url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions"

        if tag_upper not in self.bot.player_data:
            self.bot.player_data[tag_upper] = {"total_games": 0, "players": {}}
        elif "total_games" not in self.bot.player_data[tag_upper]:
            self.bot.player_data[tag_upper]["total_games"] = 0
            
        if tag_upper not in self.bot.processed_games:
            self.bot.processed_games[tag_upper] = []

        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        current_end_iso = one_hour_ago.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        try:
            async with aiohttp.ClientSession() as session:
                games_to_process = []
                seen_game_ids = set()
                consecutive_processed_count = 0
                page_count = 0

                # --- 1. GATHER ALL UNPROCESSED GAMES (From > 1 Hour Ago) ---
                while True:
                    page_url = f"{base_url}?end={current_end_iso}"
                    
                    async with session.get(page_url) as resp:
                        if resp.status != 200:
                            print(f"[{tag_upper}] API Error {resp.status}")
                            break
                            
                        page_data = await resp.json()
                        page_count += 1
                        
                        if not page_data or not isinstance(page_data, list) or len(page_data) == 0:
                            break 
                            
                        oldest_ms_in_page = float('inf')
                        new_found_this_page = 0
                        
                        for game in page_data:
                            game_id = game.get("gameId")
                            if not game_id or game_id in seen_game_ids:
                                continue
                            
                            seen_game_ids.add(game_id)
                            
                            g_start = game.get("start")
                            if g_start and g_start < oldest_ms_in_page:
                                oldest_ms_in_page = g_start
                                
                            if game_id in self.bot.processed_games[tag_upper]:
                                consecutive_processed_count += 1
                                continue
                                    
                            games_to_process.append(game)
                            new_found_this_page += 1
                            consecutive_processed_count = 0 
                            
                        if consecutive_processed_count >= 2000:
                            print(f"[{tag_upper}] Found 2000+ already processed games. Stopping scan.")
                            break
                            
                        if oldest_ms_in_page != float('inf'):
                            next_dt = datetime.fromtimestamp((oldest_ms_in_page - 1) / 1000.0, tz=timezone.utc)
                            current_end_iso = next_dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                        else:
                            break
                        
                        print(f"[{tag_upper}] Paging... Found {len(games_to_process)} games. (Page {page_count})")
                        await asyncio.sleep(0.3) 

                total_to_do = len(games_to_process)
                if total_to_do == 0:
                    await channel.send(f"[{tag_upper}] history is already fully processed.")
                    return

                # --- 2. SLICE INTO BATCHES OF 25 ---
                chunk_size = 25
                batches = [games_to_process[i:i + chunk_size] for i in range(0, total_to_do, chunk_size)]
                
                await channel.send(f"Scanning complete. Sliced **{total_to_do}** games into **{len(batches)}** batches of 25. Spawning swarm...")
                print(f"[{tag_upper}] STARTING SWARM: {len(batches)} batches distributed across {workers} workers.")

                queue = asyncio.Queue()
                for batch in batches:
                    queue.put_nowait(batch)

                processed_count = [0]
                new_players = [0]
                batches_finished = [0]

                # --- 3. BATCH WORKER LOGIC ---
                async def worker(wid):
                    while not queue.empty():
                        try:
                            batch = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                            
                        for game in batch:
                            gid = game.get("gameId")
                            is_win = game.get("hasWon", False)
                            
                            retries = 0
                            
                            # INFINITE RETRY LOOP: Try again until it successfully gets the data
                            while True:
                                try:
                                    async with session.get(f"https://api.openfront.io/public/game/{gid}?turns=false", timeout=15) as g_resp:
                                        if g_resp.status == 200:
                                            g_data = await g_resp.json()
                                            info = g_data.get("info", {})

                                            players = info.get("players", [])
                                            counted_here = set()

                                            for p in players:
                                                if p.get("clanTag", "").upper() == tag_upper:
                                                    name = p.get("username", "Unknown")
                                                    if name in counted_here: continue
                                                    counted_here.add(name)

                                                    if name not in self.bot.player_data[tag_upper]["players"]:
                                                        self.bot.player_data[tag_upper]["players"][name] = {"name": [name], "games_played": 0, "wins": 0}
                                                        new_players[0] += 1
                                                    
                                                    stats = self.bot.player_data[tag_upper]["players"][name]
                                                    
                                                    if not isinstance(stats.get("name"), list):
                                                        stats["name"] = [stats.get("name", name)]
                                                    if name not in stats["name"]:
                                                        stats["name"].append(name)
                                                        
                                                    stats["games_played"] += 1
                                                    if is_win: stats["wins"] += 1
                                                    stats["winrate"] = round((stats["wins"] / stats["games_played"]) * 100, 2)

                                            processed_count[0] += 1
                                            self.bot.player_data[tag_upper]["total_games"] += 1
                                            self.bot.processed_games[tag_upper].append(gid)
                                            
                                            # Standard delay after a successful fetch to pace the workers
                                            await asyncio.sleep(0.3)
                                            break # SUCCESS! Break out of the infinite retry loop and move to the next game.
                                            
                                        else:
                                            # API threw an error (429 Rate Limit, 500 Server Error, etc.)
                                            base_wait = min(60.0, 2 ** retries)
                                            jitter = random.uniform(0.1, 1.5)
                                            wait_time = base_wait + jitter
                                            
                                            print(f"[Worker {wid}] API Error {g_resp.status} on {gid}. Retrying in {wait_time:.1f}s...")
                                            await asyncio.sleep(wait_time)
                                            retries += 1

                                except Exception as e:
                                    # Network timeout, disconnect, or JSON parse error
                                    base_wait = min(60.0, 2 ** retries)
                                    jitter = random.uniform(0.1, 1.5)
                                    wait_time = base_wait + jitter
                                    
                                    print(f"[Worker {wid}] Network Error on {gid}: {e}. Retrying in {wait_time:.1f}s...")
                                    await asyncio.sleep(wait_time)
                                    retries += 1
                                
                        # The worker has finished its batch of 25
                        batches_finished[0] += 1
                        if batches_finished[0] % 10 == 0:
                            print(f"[{tag_upper}] Finished {batches_finished[0]}/{len(batches)} batches. ({processed_count[0]} games added)")
                        
                        queue.task_done()

                # --- 4. AUTO-SAVER ---
                async def auto_saver():
                    while not queue.empty():
                        await asyncio.sleep(60)
                        async with self.bot.save_lock:
                            self.bot.save_data()
                        print(f"[{tag_upper}] Progress saved.")

                # --- 5. EXECUTE ---
                saver_task = asyncio.create_task(auto_saver())
                workers_list = [asyncio.create_task(worker(i)) for i in range(workers)]
                
                await queue.join()
                saver_task.cancel()
                
                async with self.bot.save_lock:
                    self.bot.save_data()
                
                await channel.send(f"[{tag_upper}] load complete. Added {processed_count[0]} games and {new_players[0]} new players.")

        except Exception as e:
            await channel.send(f"Error: {e}")
        finally:
            self.bot.is_swarm_active = False

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))