import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
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

        await interaction.response.send_message(f"Paging backward through history for [{tag_upper}] to build the queue...")
        
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
                    print(f"[{tag_upper}] Total games according to API: {total_games}")

                processed_count_db = await self.bot.clan_manager.get_processed_count(tag_upper)
                LIMIT = 50
                total_processed_count = 0

                if processed_count_db >= total_games:
                    await channel.send(f"[{tag_upper}] history is already fully processed.")
                    return

                # Retrieve saved cursor to skip already processed periods
                historical_cursor_str = stats.get("historical_cursor")
                if historical_cursor_str:
                    try:
                        historical_cursor = datetime.fromisoformat(historical_cursor_str).replace(tzinfo=timezone.utc)
                    except ValueError:
                        historical_cursor = datetime.now(timezone.utc)
                else:
                    historical_cursor = datetime.now(timezone.utc)
                    
                cutoff_date = datetime(2025, 11, 10, tzinfo=timezone.utc)
                limit_reached = False

                # PHASE 1: Catch up on recent games
                page = 1
                consecutive_processed = 0
                OVERLAP_THRESHOLD = 50
                
                while total_processed_count < num and not limit_reached:
                    if self.cancel_event.is_set():
                        await channel.send(f"Scan for **[{tag_upper}]** cancelled by user. Aborting.")
                        return
                        
                    async with session.get(f"{base_url}?page={page}&limit={LIMIT}") as resp:
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
                            if total_processed_count >= num:
                                limit_reached = True
                                break

                            gid = game.get("gameId")
                            if not gid or gid in seen_game_ids:
                                continue
                                
                            seen_game_ids.add(gid)
                            is_processed = await self.bot.clan_manager.is_processed(tag_upper, gid)
                            
                            if is_processed:
                                consecutive_processed += 1
                                continue
                                
                            # If we find a new game, reset the consecutive counter
                            consecutive_processed = 0
                            games_to_process.append(game)
                            total_processed_count += 1
                            
                        if consecutive_processed >= OVERLAP_THRESHOLD:
                            print(f"[{tag_upper}] Found {OVERLAP_THRESHOLD} processed games in a row. Gap bridged. Moving to Phase 2.")
                            break 
                            
                    # Break the outer while loop as well if the threshold was met inside the for loop
                    if consecutive_processed >= OVERLAP_THRESHOLD:
                        break
                            
                    page += 1
                    await asyncio.sleep(0.25)

                # PHASE 2: Deep History Scan (historical_cursor down to 2025-11-10)
                current_end = historical_cursor
                current_start = current_end - timedelta(days=3)
                new_historical_cursor = historical_cursor
                
                if not limit_reached and current_end > cutoff_date:
                    await channel.send(f"Resuming historical scan from `{current_end.strftime('%b %d, %Y')}` to skip already processed periods...")
                
                while total_processed_count < num and not limit_reached:
                    if self.cancel_event.is_set():
                        await channel.send(f"Scan for **[{tag_upper}]** cancelled by user. Aborting.")
                        return
                        
                    if current_end < cutoff_date:
                        new_historical_cursor = cutoff_date # We hit the bottom!
                        break
                        
                    start_iso = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
                    end_iso = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                    page = 1
                    chunk_success = True
                    
                    while total_processed_count < num:
                        page_url = f"{base_url}?start={start_iso}&end={end_iso}&page={page}&limit={LIMIT}"
                        async with session.get(page_url) as resp:
                            if resp.status == 429:
                                await asyncio.sleep(1)
                                continue
                            if resp.status != 200:
                                chunk_success = False
                                break
                                
                            page_data = await resp.json()
                            results = page_data.get("results", [])
                            
                            if not results:
                                break # Chunk is successfully empty
                                
                            for game in results:
                                if total_processed_count >= num:
                                    limit_reached = True
                                    break
                                    
                                gid = game.get("gameId")
                                if not gid or gid in seen_game_ids:
                                    continue
                                    
                                seen_game_ids.add(gid)
                                is_processed = await self.bot.clan_manager.is_processed(tag_upper, gid)
                                if is_processed:
                                    continue
                                    
                                games_to_process.append(game)
                                total_processed_count += 1
                                
                        if limit_reached or not chunk_success:
                            break
                            
                        page += 1
                        await asyncio.sleep(0.25)
                        
                    # Only save the cursor forward if we successfully cleared this chunk without hitting limits or API errors
                    if not limit_reached and chunk_success:
                        new_historical_cursor = current_start
                    elif not chunk_success:
                        print("API error during chunk. Stopping queue build to prevent permanent data gaps.")
                        break
                        
                    current_end = current_start
                    current_start = current_start - timedelta(days=3)

                total_to_do = len(games_to_process)
                if total_to_do == 0:
                    await channel.send(f"[{tag_upper}] history is already fully processed.")
                    return

                games_to_process.sort(key=lambda x: x.get("gameStart", ""))

                await channel.send(f"Found **{total_to_do}** missing games for clan **[{tag_upper}]**. Starting persistent chronological queue...")
                print(f"[{tag_upper}] STARTING PERSISTENT QUEUE for {total_to_do} games...")

                processed_count = [0]
                self.current_queue = asyncio.Queue()
                for game in games_to_process:
                    self.current_queue.put_nowait(game)

                downloaded_games = {}

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
                            await asyncio.sleep(0.9)

                await self.current_queue.join()
                
                for w in workers_list:
                    w.cancel()
                
                total_secs = int(time.time() - self.start_time) if self.start_time else 0
                m, s = divmod(total_secs, 60)
                h, m = divmod(m, 60)
                formatted_time = f"{h:03d}:{m:02d}:{s:02d}"

                if self.cancel_event.is_set():
                    await channel.send(
                        f"**[{tag_upper}]** Background load CANCELLED!\n"
                        f"Saved **{processed_count[0]}** new games.\n"
                        f"⏱ **Time Spent:** `{formatted_time}`"
                    )
                else:
                    await channel.send(
                        f"**[{tag_upper}]** Background load complete! Every game was successfully found and processed.\n"
                        f"Added **{processed_count[0]}** games.\n"
                        f"⏱ **Total Time Taken:** `{formatted_time}`"
                    )
                
                if self.current_queue.empty() and not self.cancel_event.is_set():
                    # Only save the new historical cursor if everything processed smoothly!
                    stats["historical_cursor"] = new_historical_cursor.isoformat()
                    print(f"Queue done. Saved historical cursor: {new_historical_cursor.isoformat()}")
                    await self.bot.clan_manager.finalize_batch_update(tag_upper)

        except Exception as e:
            await channel.send(f"An error occurred during backfill: {e}")
        finally:
            self.bot.is_swarm_active = False
            self.start_time = None

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))