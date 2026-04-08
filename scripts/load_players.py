import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
import re
from dotenv import load_dotenv
import os

from .fetch_worker import fetch_game_worker

load_dotenv()

dev_server_id = int(os.getenv('DEV_SERVER_ID', '0'))

class LoadPlayers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
        if not hasattr(self.bot, 'save_lock'):
            self.bot.save_lock = asyncio.Lock()
            
        if not hasattr(self.bot, 'is_swarm_active'):
            self.bot.is_swarm_active = False

        self.cancel_event = asyncio.Event()
        self.current_queue = None

    @app_commands.command(name="cancel-load", description="Cancels the currently running background load and saves progress.")
    async def cancel_load(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You don't have permission to cancel.", ephemeral=True)
            return

        if interaction.guild_id != dev_server_id:
            await interaction.response.send_message("Restricted to developer server.", ephemeral=True)
            return

        if not getattr(self.bot, 'is_swarm_active', False):
            await interaction.response.send_message("No background load currently running.", ephemeral=True)
            return

        self.cancel_event.set()
        await interaction.response.send_message("**Cancellation requested!** The bot will finish its current active games, save progress, and stop.")

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
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        if interaction.guild_id != dev_server_id:
            await interaction.response.send_message("Restricted to developer server.", ephemeral=True)
            return

        if getattr(self.bot, 'is_swarm_active', False):
            await interaction.response.send_message("A background load is already running.", ephemeral=True)
            return

        tag_upper = clan_tag.upper()
        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag.", ephemeral=True)
            return

        await interaction.response.send_message(f"Paging backward through history for [{tag_upper}] to build the queue...")
        
        self.bot.is_swarm_active = True
        self.bot.loop.create_task(self.background_loader(tag_upper, interaction.channel))

    async def background_loader(self, tag_upper, channel):
        self.cancel_event.clear()
        self.current_queue = None
        
        base_url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions"

        if tag_upper not in self.bot.player_data:
            self.bot.player_data[tag_upper] = {"total_games": 0, "players": {}}
        elif "total_games" not in self.bot.player_data[tag_upper]:
            self.bot.player_data[tag_upper]["total_games"] = 0
            
        if "load_time_seconds" not in self.bot.player_data[tag_upper]:
            self.bot.player_data[tag_upper]["load_time_seconds"] = 0
            
        if tag_upper not in self.bot.processed_games:
            self.bot.processed_games[tag_upper] = []

        try:
            async with aiohttp.ClientSession() as session:
                games_to_process = []
                seen_game_ids = set()
                consecutive_processed_count = 0

                async with session.get(f"{base_url}?limit=1") as resp:
                    if resp.status != 200:
                        await channel.send(f"Failed to access API for **[{tag_upper}]**. Status code: {resp.status}")
                        self.bot.is_swarm_active = False
                        return
                    data = await resp.json()
                    total_games = int(data.get("total", 0))
                    print(f"[{tag_upper}] Total games according to API: {total_games}")

                # --- PAGING PHASE ---
                if total_games <= 10000:
                    # STRATEGY A: Fast Standard Pagination (No Time Chunking)
                    page = 1
                    while len(seen_game_ids) + len(self.bot.processed_games[tag_upper]) < total_games:
                        if self.cancel_event.is_set():
                            await channel.send(f"Scan for **[{tag_upper}]** cancelled by user. Aborting.")
                            return
                            
                        async with session.get(f"{base_url}?page={page}&limit=50") as resp:
                            if resp.status == 429:
                                await asyncio.sleep(1)
                                continue
                            if resp.status != 200:
                                break
                                
                            page_data = await resp.json()
                            results = page_data.get("results", [])
                            
                            if not results:
                                break
                                
                            for game in results:
                                gid = game.get("gameId")
                                if not gid or gid in seen_game_ids:
                                    continue
                                    
                                seen_game_ids.add(gid)
                                if gid in self.bot.processed_games[tag_upper]:
                                    consecutive_processed_count += 1
                                    continue
                                        
                                games_to_process.append(game)
                                consecutive_processed_count = 0
                                
                        if consecutive_processed_count >= 2000:
                            break
                            
                        page += 1
                        await asyncio.sleep(0.3)
                else:
                    # STRATEGY B: Time-Chunked Pagination for > 10,000 Games
                    cutoff_date = datetime(2025, 11, 10, tzinfo=timezone.utc)
                    current_end = datetime.now(timezone.utc)
                    current_start = current_end - timedelta(days=3)
                    
                    while len(seen_game_ids) + len(self.bot.processed_games[tag_upper]) < total_games:
                        if self.cancel_event.is_set():
                            await channel.send(f"Scan for **[{tag_upper}]** cancelled by user. Aborting.")
                            return
                        if current_end < cutoff_date:
                            break
                            
                        start_iso = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
                        end_iso = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                        
                        page = 1
                        while True:
                            page_url = f"{base_url}?start={start_iso}&end={end_iso}&page={page}&limit=50"
                            async with session.get(page_url) as resp:
                                if resp.status == 429:
                                    await asyncio.sleep(1)
                                    continue
                                if resp.status != 200:
                                    break
                                    
                                page_data = await resp.json()
                                results = page_data.get("results", [])
                                
                                if not results:
                                    break
                                    
                                for game in results:
                                    gid = game.get("gameId")
                                    if not gid or gid in seen_game_ids:
                                        continue
                                        
                                    seen_game_ids.add(gid)
                                    if gid in self.bot.processed_games[tag_upper]:
                                        consecutive_processed_count += 1
                                        continue
                                        
                                    games_to_process.append(game)
                                    consecutive_processed_count = 0
                                    
                            page += 1
                            await asyncio.sleep(0.3)
                            
                        if consecutive_processed_count >= 2000:
                            break
                            
                        # Slide window back 3 days
                        current_end = current_start
                        current_start = current_start - timedelta(days=3)

                total_to_do = len(games_to_process)
                if total_to_do == 0:
                    await channel.send(f"[{tag_upper}] history is already fully processed.")
                    return

                games_to_process.sort(key=lambda x: x.get("gameStart", ""))

                await channel.send(f"Found **{total_to_do}** missing games for clan **[{tag_upper}]**. Starting persistent chronological queue...")
                print(f"[{tag_upper}] STARTING PERSISTENT QUEUE for {total_to_do} games...")

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

                if "winstreak" not in self.bot.player_data[tag_upper]:
                    self.bot.player_data[tag_upper]["winstreak"] = 0
                if "highest_winstreak" not in self.bot.player_data[tag_upper]:
                    self.bot.player_data[tag_upper]["highest_winstreak"] = 0

                self.current_queue = asyncio.Queue()
                for game in games_to_process:
                    self.current_queue.put_nowait(game)

                downloaded_games = {}

                # Create tasks using the imported shared worker
                workers_list = [
                    asyncio.create_task(fetch_game_worker(i, session, self.current_queue, self.cancel_event, downloaded_games)) 
                    for i in range(3)
                ]
                
                async def auto_saver():
                    try:
                        while True:
                            await asyncio.sleep(60)
                            async with self.bot.save_lock:
                                self.bot.save_data()
                    except asyncio.CancelledError:
                        pass

                saver_task = asyncio.create_task(auto_saver())
                
                for game in games_to_process:
                    if self.cancel_event.is_set():
                        break
                        
                    gid = game.get("gameId")
                    fallback_win = game.get("hasWon", False)
                    
                    while gid not in downloaded_games:
                        if self.cancel_event.is_set():
                            break
                        await asyncio.sleep(0.1)
                        
                    if self.cancel_event.is_set():
                        break
                        
                    g_data = downloaded_games.pop(gid)
                    
                    if g_data:
                        info = g_data.get("info", {})
                        is_win = g_data.get("hasWon", fallback_win)
                        
                        if is_win:
                            self.bot.player_data[tag_upper]["winstreak"] += 1
                            if self.bot.player_data[tag_upper]["winstreak"] > self.bot.player_data[tag_upper]["highest_winstreak"]:
                                self.bot.player_data[tag_upper]["highest_winstreak"] = self.bot.player_data[tag_upper]["winstreak"]
                        else:
                            self.bot.player_data[tag_upper]["winstreak"] = 0

                        players = info.get("players", [])
                        counted_here = set()
                        for p in players:
                            if p.get("clanTag", "").upper() == tag_upper:
                                name = p.get("username", "Unknown")
                                if name in counted_here: continue
                                counted_here.add(name)

                                if name not in self.bot.player_data[tag_upper]["players"]:
                                    self.bot.player_data[tag_upper]["players"][name] = {
                                        "name": [name], "games_played": 0, "wins": 0, 
                                        "winstreak": 0, "highest_winstreak": 0
                                    }
                                    new_players[0] += 1
                                
                                stats = self.bot.player_data[tag_upper]["players"][name]
                                
                                if "winstreak" not in stats: stats["winstreak"] = 0
                                if "highest_winstreak" not in stats: stats["highest_winstreak"] = 0
                                    
                                stats["games_played"] += 1
                                if is_win: 
                                    stats["wins"] += 1
                                    stats["winstreak"] += 1
                                    if stats["winstreak"] > stats["highest_winstreak"]:
                                        stats["highest_winstreak"] = stats["winstreak"]
                                else:
                                    stats["winstreak"] = 0
                                    
                                stats["winrate"] = round((stats["wins"] / stats["games_played"]) * 100, 2)

                        processed_count[0] += 1
                        self.bot.player_data[tag_upper]["total_games"] += 1
                        self.bot.processed_games[tag_upper].append(gid)
                        
                    if processed_count[0] % 50 == 0 and processed_count[0] > 0:
                        print(f"[{tag_upper}] Backfill progress: {processed_count[0]} / {total_to_do}...")
                            
                        await asyncio.sleep(0.9)

                await self.current_queue.join()
                
                saver_task.cancel()
                timer_task.cancel()
                for w in workers_list:
                    w.cancel()
                
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
                else:
                    await channel.send(
                        f"**[{tag_upper}]** Background load complete! Every game was successfully found and processed.\n"
                        f"Added **{processed_count[0]}** games and **{new_players[0]}** new players.\n"
                        f"⏱ **Total Time Taken:** `{formatted_time}`"
                    )

        except Exception as e:
            await channel.send(f"An error occurred during backfill: {e}")
        finally:
            self.bot.is_swarm_active = False

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))