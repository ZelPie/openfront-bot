import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timezone
import re
from dotenv import load_dotenv
import os
import time

from .fetch_worker import fetch_game_worker

load_dotenv()
dev_server_id = int(os.getenv('DEV_SERVER_ID', '0'))

class LoadPlayers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
            
        if not hasattr(self.bot, 'is_swarm_active'):
            self.bot.is_swarm_active = False

        self.cancel_event = asyncio.Event()
        self.current_queue = None
        self.start_time = None

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

        time_spent_str = "000:00:00"
        if self.start_time:
            current_duration = int(time.time() - self.start_time)
            m, s = divmod(current_duration, 60)
            h, m = divmod(m, 60)
            time_spent_str = f"{h:03d}:{m:02d}:{s:02d}"

        await interaction.response.send_message(
            f"**Cancellation requested!** The bot ran for `{time_spent_str}`. "
            f"It will finish its current active games, save progress, and stop."
        )

        if self.current_queue:
            while not self.current_queue.empty():
                try:
                    self.current_queue.get_nowait()
                    self.current_queue.task_done()
                except asyncio.QueueEmpty:
                    break

    @app_commands.command(name="load-clan-data", description="Persistent backfill that continuously retries games until clan and player data loads.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)", num="Max number of games to go through (Default: 10000)")
    async def load_players(self, interaction: discord.Interaction, clan_tag: str, num: int = 10000):
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

        if num <= 0:
            await interaction.response.send_message("Please provide a positive number.", ephemeral=True)
            return

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag.", ephemeral=True)
            return

        await interaction.response.send_message(f"Starting database sync for [{tag_upper}]...")
        
        self.bot.is_swarm_active = True
        self.bot.loop.create_task(self.background_loader(tag_upper, interaction.channel, num))

    async def background_loader(self, tag_upper, channel, num):
        self.cancel_event.clear()
        self.current_queue = None
        self.start_time = time.time()

        stats = await self.bot.clan_manager.get_clan_stats(tag_upper)
        base_url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions"

        try:
            async with aiohttp.ClientSession() as session:
                games_to_process = []
                seen_game_ids = set()

                async with session.get(f"{base_url}?limit=1") as resp:
                    if resp.status != 200:
                        await channel.send(f"Failed to access API for **[{tag_upper}]**. Status code: {resp.status}")
                        self.bot.is_swarm_active = False
                        return
                    data = await resp.json()
                    total_games = int(data.get("total", 0))

                processed_count_db = await self.bot.clan_manager.get_processed_count(tag_upper)
                LIMIT = 50
                total_processed_count = 0

                if processed_count_db >= total_games:
                    await channel.send(f"[{tag_upper}] history is already fully processed.")
                    return

                # 1. State-Saved Latest Cursor (Protects against the "Gap Problem")
                latest_cursor = stats.get("latest_cursor")
                new_latest_cursor = latest_cursor

                # 2. Dynamic Historical Cursor (The past is solid, so just read the oldest game!)
                historical_cursor = None
                saved_matches = self.bot.clan_manager.clans[tag_upper].get("matches", [])
                
                if saved_matches:
                    raw_start = saved_matches[0].get("start")
                    if raw_start:
                        # Safely convert milliseconds to an ISO string for the OpenFront API
                        try:
                            if isinstance(raw_start, (int, float)) or (isinstance(raw_start, str) and raw_start.isdigit()):
                                dt = datetime.fromtimestamp(int(raw_start) / 1000, tz=timezone.utc)
                                # The API requires the 'Z' suffix for UTC time
                                historical_cursor = dt.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                            else:
                                # Fallback if it is somehow already an ISO string
                                historical_cursor = raw_start
                        except Exception as e:
                            print(f"[{tag_upper}] Time conversion error for historical cursor: {e}")
                            historical_cursor = raw_start
                    
                new_historical_cursor = historical_cursor

                # PHASE 1: Catch up on missed games
                if latest_cursor:
                    try:
                        dt = datetime.fromisoformat(latest_cursor.replace('Z', '+00:00'))
                        cursor_sec = int(dt.timestamp())
                        display_time = f"<t:{cursor_sec}:f>"
                        await channel.send(f"Fetching missed games since {display_time}...")
                    except Exception as e:
                        await channel.send(f"Error occurred while processing latest_cursor: {e}")
                    page = 1
                    while total_processed_count < num:
                        if self.cancel_event.is_set():
                            break
                            
                        page_url = f"{base_url}?start={latest_cursor}&page={page}&limit={LIMIT}"
                        async with session.get(page_url) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(1)
                                continue
                            if resp.status != 200:
                                break
                                
                            page_data = await resp.json()
                            results = page_data.get("results", [])
                            
                            if not results:
                                break # Got all the new games!
                                
                            for game in results:
                                game_start = game.get("gameStart")
                                if game_start and (not new_latest_cursor or game_start > new_latest_cursor):
                                    new_latest_cursor = game_start
                                    
                                gid = game.get("gameId")
                                if not gid or gid in seen_game_ids:
                                    continue
                                    
                                seen_game_ids.add(gid)
                                if await self.bot.clan_manager.is_processed(tag_upper, gid):
                                    continue
                                    
                                games_to_process.append(game)
                                total_processed_count += 1
                                
                                if total_processed_count >= num:
                                    break
                                    
                        page += 1
                        await asyncio.sleep(0.2)

                # --- PHASE 2: Deep History Scan ---
                if total_processed_count < num:
                    if historical_cursor:
                        try:
                            dt = datetime.fromisoformat(historical_cursor.replace('Z', '+00:00'))
                            cursor_sec = int(dt.timestamp())
                            display_time = f"<t:{cursor_sec}:f>"
                            await channel.send(f"Resuming history scan from {display_time}...")
                        except Exception as e:
                            await channel.send(f"Error occurred while processing historical_cursor: {e}")
                    else:
                        await channel.send("No historical cursor found. Starting deep scan from the beginning...")
                        
                    page = 1
                    while total_processed_count < num:
                        if self.cancel_event.is_set():
                            break
                            
                        if historical_cursor:
                            page_url = f"{base_url}?end={historical_cursor}&page={page}&limit={LIMIT}"
                        else:
                            page_url = f"{base_url}?page={page}&limit={LIMIT}"
                            
                        async with session.get(page_url) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(1)
                                continue
                            if resp.status != 200:
                                break
                                
                            page_data = await resp.json()
                            results = page_data.get("results", [])
                            
                            if not results:
                                break # Reached the absolute end of the clan's history!
                                
                            for game in results:
                                game_start = game.get("gameStart")
                                
                                # Capture the very first game's time as our latest cursor if this is a fresh start
                                if game_start and not new_latest_cursor:
                                    new_latest_cursor = game_start
                                    
                                # Push the historical cursor backward
                                if game_start and (not new_historical_cursor or game_start < new_historical_cursor):
                                    new_historical_cursor = game_start

                                gid = game.get("gameId")
                                if not gid or gid in seen_game_ids:
                                    continue
                                    
                                seen_game_ids.add(gid)
                                if await self.bot.clan_manager.is_processed(tag_upper, gid):
                                    continue
                                    
                                games_to_process.append(game)
                                total_processed_count += 1

                                if total_processed_count % 250 == 0 and total_processed_count > 0:
                                    print(f"[{tag_upper}] Historical scan progress: {total_processed_count} / {num} found so far...")
                                
                                if total_processed_count >= num:
                                    break
                                    
                        page += 1
                        await asyncio.sleep(0.2)
                
                total_secs = int(time.time() - self.start_time) if self.start_time else 0
                m, s = divmod(total_secs, 60)
                h, m = divmod(m, 60)
                formatted_time = f"{h:03d}:{m:02d}:{s:02d}"

                await channel.send(f"Data fetch complete! Found **{total_processed_count}** total games to process. ⏱ Time taken: `{formatted_time}`")

                total_to_do = len(games_to_process)
                if total_to_do == 0:
                    await channel.send(f"[{tag_upper}] history is already fully processed.")
                    return

                games_to_process.sort(key=lambda x: x.get("gameStart", ""))

                await channel.send(f"Found **{total_to_do}** missing games for clan **[{tag_upper}]**. Starting processing queue...")
                print(f"[{tag_upper}] STARTING PERSISTENT QUEUE for {total_to_do} games...")

                processed_count = [0]
                self.current_queue = asyncio.Queue()
                for game in games_to_process:
                    self.current_queue.put_nowait(game)

                downloaded_games = {}

                self.start_workers = time.time()

                workers_list = [
                    asyncio.create_task(fetch_game_worker(i, session, self.current_queue, self.cancel_event, downloaded_games)) 
                    for i in range(3)
                ]
                
                for game in games_to_process:
                    if self.cancel_event.is_set():
                        break
                        
                    gid = game.get("gameId")
                    
                    while gid not in downloaded_games:
                        if self.cancel_event.is_set():
                            break
                        await asyncio.sleep(0.1)
                        
                    if self.cancel_event.is_set():
                        break
                        
                    g_data = downloaded_games.pop(gid)
                    
                    if g_data:
                        info = g_data.get("info", {})
                        success = await self.bot.clan_manager.process_game(tag_upper, game, info, mode="backfill")
                        
                        if success:
                            processed_count[0] += 1
                            
                        if processed_count[0] % 50 == 0 and processed_count[0] > 0:
                            print(f"[{tag_upper}] Backfill progress: {processed_count[0]} / {total_to_do}...")

                            await self.bot.clan_manager.save_clan(tag_upper)

                            await asyncio.sleep(0.6)

                if not self.cancel_event.is_set():
                    await self.current_queue.join()
                
                for w in workers_list:
                    w.cancel()

                worker_sec = int(time.time() - self.start_workers) if self.start_workers else 0
                m, s = divmod(worker_sec, 60)
                h, m = divmod(m, 60)
                formatted_worker_time = f"{h:03d}:{m:02d}:{s:02d}"

                total_time = worker_sec + total_secs
                m, s = divmod(total_time, 60)
                h, m = divmod(m, 60)
                formatted_total_time = f"{h:03d}:{m:02d}:{s:02d}"

                if self.cancel_event.is_set():
                    await channel.send(
                        f"**[{tag_upper}]** Background load CANCELLED!\n"
                        f"Saved **{processed_count[0]}** new games.\n"
                        f"⏱ **Time Spent:** `{formatted_total_time}` (Worker Time: `{formatted_worker_time}`, Fetch Time: `{formatted_time}`)"
                    )
                else:
                    await channel.send(
                        f"**[{tag_upper}]** Background load complete! Every game was successfully found and processed.\n"
                        f"Added **{processed_count[0]}** games.\n"
                        f"⏱ **Total Time Taken:** `{formatted_total_time}` (Worker Time: `{formatted_worker_time}`, Fetch Time: `{formatted_time}`)"
                    )
                
                # ... 
                if not self.cancel_event.is_set():
                    active_stats = await self.bot.clan_manager.get_clan_stats(tag_upper)
                    
                    # We ONLY need to save the latest_cursor now!
                    active_stats["latest_cursor"] = new_latest_cursor
                    print(f"Queue done. Saved latest_cursor: {new_latest_cursor}")
                
                print(f"[{tag_upper}] Finalizing batch update and saving to disk...")
                await self.bot.clan_manager.finalize_batch_update(tag_upper)

        except Exception as e:
            await channel.send(f"An error occurred during backfill: {e}")
        finally:
            self.bot.is_swarm_active = False
            self.start_time = None

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))