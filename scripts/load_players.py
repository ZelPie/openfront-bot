import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
import re
from dotenv import load_dotenv
import os

load_dotenv()

dev_server_id = int(os.getenv('DEV_SERVER_ID', '0'))  # Default to 0 if not set

class LoadPlayers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
        if not hasattr(self.bot, 'save_lock'):
            self.bot.save_lock = asyncio.Lock()
            
        if not hasattr(self.bot, 'is_swarm_active'):
            self.bot.is_swarm_active = False

        # --- Cancellation State ---
        self.cancel_event = asyncio.Event()
        self.current_queue = None

    @app_commands.command(name="cancel-load", description="Cancels the currently running background load and saves progress.")
    async def cancel_load(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You don't have permission to cancel background loads. Only administrators can do this.", ephemeral=True)
            return

        if interaction.guild_id != dev_server_id:
            await interaction.response.send_message("This command is currently restricted to the developer's server for performance reasons.", ephemeral=True)
            return

        if not getattr(self.bot, 'is_swarm_active', False):
            await interaction.response.send_message("There is no background load currently running.", ephemeral=True)
            return

        self.cancel_event.set()
        await interaction.response.send_message("**Cancellation requested!** The bot will finish its current active games, save progress, and stop.")

        # Instantly empty the queue so workers stop grabbing new games
        if self.current_queue:
            while not self.current_queue.empty():
                try:
                    self.current_queue.get_nowait()
                    self.current_queue.task_done()
                except asyncio.QueueEmpty:
                    break

    @app_commands.command(name="load-clan-data", description="Persistent backfill that continuously retries games until clan and player data loads.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)")
    async def load_players(self, interaction: discord.Interaction, clan_tag: str):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You don't have permission to start background loads. Only administrators can do this.", ephemeral=True)
            return

        if interaction.guild_id != dev_server_id:
            await interaction.response.send_message("This command is currently restricted to the developer's server for performance reasons.", ephemeral=True)
            return

        if getattr(self.bot, 'is_swarm_active', False):
            await interaction.response.send_message(
                "A background load is already running for another clan. Please wait for it to finish.", 
                ephemeral=True
            )
            return

        tag_upper = clan_tag.upper()
        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)  # Sanitize input to prevent issues

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return

        await interaction.response.send_message(f"Paging backward through history for [{tag_upper}] to build the queue...")
        
        self.bot.is_swarm_active = True
        self.bot.loop.create_task(self.background_loader(tag_upper, interaction.channel))

    async def background_loader(self, tag_upper, channel):
        # Reset cancellation state for the new load
        self.cancel_event.clear()
        self.current_queue = None
        
        base_url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions"

        # --- DATA INITIALIZATION ---
        if tag_upper not in self.bot.player_data:
            self.bot.player_data[tag_upper] = {"total_games": 0, "players": {}}
        elif "total_games" not in self.bot.player_data[tag_upper]:
            self.bot.player_data[tag_upper]["total_games"] = 0
            
        if "load_time_seconds" not in self.bot.player_data[tag_upper]:
            self.bot.player_data[tag_upper]["load_time_seconds"] = 0
            
        if tag_upper not in self.bot.processed_games:
            self.bot.processed_games[tag_upper] = []

        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        one_hour_ago_ms = int(one_hour_ago.timestamp() * 1000)
        current_end_iso = one_hour_ago.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        try:
            async with aiohttp.ClientSession() as session:
                games_to_process = []
                seen_game_ids = set()
                consecutive_processed_count = 0

                # GATHER ALL UNPROCESSED GAMES
                while True:
                    # Break out early if the user cancelled during the paging phase
                    if self.cancel_event.is_set():
                        await channel.send(f"Scan for **[{tag_upper}]** cancelled by user during the paging phase. Aborting.")
                        return
                        
                    page_url = f"{base_url}?end={current_end_iso}"
                    
                    async with session.get(page_url) as resp:
                        if resp.status != 200:
                            print(f"[{tag_upper}] API Error {resp.status} while paging.")
                            break
                            
                        page_data = await resp.json()
                        
                        if not page_data or not isinstance(page_data, list) or len(page_data) == 0:
                            break 
                            
                        oldest_ms_in_page = float('inf')
                        
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
                            consecutive_processed_count = 0 
                            
                        if consecutive_processed_count >= 2000:
                            break
                            
                        if oldest_ms_in_page != float('inf'):
                            next_dt = datetime.fromtimestamp((oldest_ms_in_page - 1) / 1000.0, tz=timezone.utc)
                            current_end_iso = next_dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                        else:
                            break
                        
                        await asyncio.sleep(0.5) 

                total_to_do = len(games_to_process)
                if total_to_do == 0:
                    await channel.send(f"[{tag_upper}] history is already fully processed.")
                    return

                await channel.send(f"Found **{total_to_do}** missing games for clan **[{tag_upper}]**. Starting persistent queue...")
                print(f"[{tag_upper}] STARTING PERSISTENT QUEUE for {total_to_do} games for clan {tag_upper}...")

                # PERSISTENT TIMER
                async def timer():
                    try:
                        while True:
                            await asyncio.sleep(1)
                            self.bot.player_data[tag_upper]["load_time_seconds"] += 1
                    except asyncio.CancelledError:
                        pass
                
                timer_task = asyncio.create_task(timer())

                processed_count = [0]
                new_players = [0]

                # SETUP QUEUE
                self.current_queue = asyncio.Queue()
                for game in games_to_process:
                    self.current_queue.put_nowait(game)

                # QUEUE WORKER LOGIC
                async def worker(wid):
                    while True:
                        try:
                            game = await self.current_queue.get()
                        except asyncio.CancelledError:
                            break
                            
                        gid = game.get("gameId") if isinstance(game, dict) else game
                        fallback_win = game.get("hasWon", False) if isinstance(game, dict) else False
                        
                        await asyncio.sleep(1)  

                        success = False
                        try:
                            async with session.get(f"https://api.openfront.io/public/game/{gid}?turns=false", timeout=15) as g_resp:
                                if g_resp.status == 200:
                                    g_data = await g_resp.json()
                                    info = g_data.get("info", {})
                                    
                                    # CHECK IF EMPTY: Re-queue if missing info or players
                                    if not g_data or not info or not info.get("players"):
                                        print(f"[Worker {wid}] Game {gid} returned empty. Re-queueing...")
                                        # Only re-queue if we haven't been cancelled!
                                        if not self.cancel_event.is_set():
                                            self.current_queue.put_nowait(game)
                                    else:
                                        g_start = info.get("start")
                                        if g_start and int(g_start) >= one_hour_ago_ms:
                                            success = True 
                                        else:
                                            is_win = g_data.get("hasWon", fallback_win)
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
                                            success = True
                                            
                                elif g_resp.status == 429:
                                    print(f"[Worker {wid}] 429 Rate Limit. Pausing for 5s...")
                                    await asyncio.sleep(5)
                                    if not self.cancel_event.is_set():
                                        self.current_queue.put_nowait(game)
                                else:
                                    print(f"[Worker {wid}] API Error {g_resp.status} on {gid}. Re-queueing...")
                                    if not self.cancel_event.is_set():
                                        self.current_queue.put_nowait(game)

                        except Exception as e:
                            if not self.cancel_event.is_set():
                                self.current_queue.put_nowait(game)
                            
                        self.current_queue.task_done()
                        
                        if success and processed_count[0] % 50 == 0 and processed_count[0] > 0:
                            print(f"[{tag_upper}] Backfill progress: {processed_count[0]} / {total_to_do}...")
                            
                        await asyncio.sleep(0.9)

                # Auto saver task
                async def auto_saver():
                    try:
                        while True:
                            await asyncio.sleep(60)
                            async with self.bot.save_lock:
                                self.bot.save_data()
                    except asyncio.CancelledError:
                        pass

                # Start tasks
                saver_task = asyncio.create_task(auto_saver())
                workers_list = [asyncio.create_task(worker(i)) for i in range(3)]
                
                # Wait until the queue is completely empty (or emptied by the cancel command)
                await self.current_queue.join()
                
                # Cleanup tasks
                saver_task.cancel()
                timer_task.cancel()
                for w in workers_list:
                    w.cancel()
                
                # FORMAT FINAL TIME & SAVE
                total_secs = self.bot.player_data[tag_upper].get("load_time_seconds", 0)
                m, s = divmod(total_secs, 60)
                h, m = divmod(m, 60)
                formatted_time = f"{h:03d}:{m:02d}:{s:02d}"

                async with self.bot.save_lock:
                    self.bot.save_data()
                
                if self.cancel_event.is_set():
                    await channel.send(
                        f"**[{tag_upper}]** Background load CANCELLED!\n"
                        f"Saved **{processed_count[0]}** new games and **{new_players[0]}** new players.\n"
                        f"⏱ **Time Spent:** `{formatted_time}`"
                    )

                    print(f"[{tag_upper}] BACKGROUND LOAD CANCELLED by user. Saved {processed_count[0]} games and {new_players[0]} new players. Time spent: {formatted_time}")
                else:
                    await channel.send(
                        f"**[{tag_upper}]** Background load complete! Every game was successfully found and processed.\n"
                        f"Added **{processed_count[0]}** games and **{new_players[0]}** new players.\n"
                        f"⏱ **Total Time Taken:** `{formatted_time}`"
                    )
                    print(f"[{tag_upper}] BACKGROUND LOAD COMPLETE! Added {processed_count[0]} games and {new_players[0]} new players. Total time: {formatted_time}")

        except Exception as e:
            await channel.send(f"An error occurred during backfill: {e}")
        finally:
            self.bot.is_swarm_active = False

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))