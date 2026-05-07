import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio
import re
from dotenv import load_dotenv
import os
import time
import json

from .fetch_worker import fetch_game_worker

load_dotenv()
dev_server_id = int(os.getenv('DEV_SERVER_ID', '0'))

class RecheckCmds(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
            
        if not hasattr(self.bot, 'is_recheck_active'):
            self.bot.is_recheck_active = False

        self.cancel_event = asyncio.Event()
        self.current_queue = None
        self.start_time = None

    def get_progress_path(self, clan_tag):
        """Gets the path to the temporary file storing reprocessed game IDs."""
        base_dir = os.path.join(self.bot.clan_manager.base_dir, clan_tag.upper())
        os.makedirs(base_dir, exist_ok=True)
        return os.path.join(base_dir, "recheck_progress.tmp")

    def load_progress(self, clan_tag):
        """Loads the list of already reprocessed game IDs."""
        path = self.get_progress_path(clan_tag)
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    return set(json.load(f))
            except Exception:
                return set()
        return set()

    def save_progress(self, clan_tag, processed_set):
        path = self.get_progress_path(clan_tag)
        temp_path = f"{path}.write"
        
        # Write to a safe dummy file first
        with open(temp_path, "w") as f:
            json.dump(list(processed_set), f)
            
        retries = 10
        for i in range(retries):
            try:
                os.replace(temp_path, path)
                break
            except (PermissionError, OSError) as e:
                if i == retries - 1:
                    print(f"Failed to atomically save progress for {clan_tag}: {e}")
                time.sleep(0.5)

    def clear_progress(self, clan_tag):
        """Deletes the temporary progress file once fully complete."""
        path = self.get_progress_path(clan_tag)
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass

    @app_commands.command(name="cancel-recheck", description="Cancels the currently running background recheck safely.")
    async def cancel_recheck(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You don't have permission to cancel.", ephemeral=True)
            return

        if interaction.guild_id != dev_server_id:
            await interaction.response.send_message("Restricted to developer server.", ephemeral=True)
            return

        if not getattr(self.bot, 'is_recheck_active', False):
            await interaction.response.send_message("No background recheck currently running.", ephemeral=True)
            return

        self.cancel_event.set()

        time_spent_str = "000:00:00"
        if self.start_time:
            current_duration = int(time.time() - self.start_time)
            m, s = divmod(current_duration, 60)
            h, m = divmod(m, 60)
            time_spent_str = f"{h:03d}:{m:02d}:{s:02d}"

        await interaction.response.send_message(
            f"**Cancellation requested!** The recheck ran for `{time_spent_str}`.\n"
            f"It will finish its active games, save its progress, and stop. You can resume at any time."
        )

        if self.current_queue:
            while not self.current_queue.empty():
                try:
                    self.current_queue.get_nowait()
                    self.current_queue.task_done()
                except asyncio.QueueEmpty:
                    break

    @app_commands.command(name="recheck-clan-data", description="Scans and live-updates existing matches, tracking progress to allow pausing/resuming.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)")
    async def recheck_players(self, interaction: discord.Interaction, clan_tag: str):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("You don't have permission.", ephemeral=True)
            return

        if interaction.guild_id != dev_server_id:
            await interaction.response.send_message("Restricted to developer server.", ephemeral=True)
            return

        if getattr(self.bot, 'is_recheck_active', False):
            await interaction.response.send_message("A background recheck is already running.", ephemeral=True)
            return

        tag_upper = clan_tag.upper()
        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag.", ephemeral=True)
            return

        await self.bot.clan_manager.load_clan(tag_upper)
        clan_data = self.bot.clan_manager.clans.get(tag_upper)
        
        if not clan_data or not clan_data.get("matches"):
            await interaction.response.send_message(f"No match data found for **[{tag_upper}]** to recheck.", ephemeral=True)
            return

        await interaction.response.send_message(f"Starting live database recheck for [{tag_upper}]...")
        
        self.bot.is_recheck_active = True
        self.bot.loop.create_task(self.background_recheck(tag_upper, interaction.channel))

    async def background_recheck(self, tag_upper, channel):
        self.cancel_event.clear()
        self.current_queue = None
        self.start_time = time.time()

        try:
            async with aiohttp.ClientSession() as session:
                clan_data = self.bot.clan_manager.clans[tag_upper]
                
                # ONLY USE ALREADY SAVED MATCHES
                all_matches = clan_data.get("matches", [])
                
                # Load previously finished gameIds so we can resume
                processed_ids = self.load_progress(tag_upper)

                # Filter out the games we've already rechecked
                games_to_process = [m for m in all_matches if m.get("gameId") not in processed_ids]
                
                total_to_do = len(games_to_process)
                if total_to_do == 0:
                    await channel.send(f"[{tag_upper}] is fully up to date! All games have already been rechecked.")
                    self.clear_progress(tag_upper)
                    return

                await channel.send(f"Found **{total_to_do}** existing games for clan **[{tag_upper}]** that need rechecking. Starting worker queue...")
                print(f"[{tag_upper}] STARTING RECHECK QUEUE for {total_to_do} games...")

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
                        if self.cancel_event.is_set(): break
                        await asyncio.sleep(0.1)
                        
                    if self.cancel_event.is_set(): break
                        
                    g_data = downloaded_games.pop(gid)
                    
                    if g_data:
                        info = g_data.get("info", {})
                        
                        new_match_data = self.bot.clan_manager.extract_match_record(tag_upper, game, info)
                        
                        # Apply live update directly to the clan manager memory
                        async with self.bot.clan_manager.lock:
                            for i, m in enumerate(self.bot.clan_manager.clans[tag_upper]["matches"]):
                                if m.get("gameId") == gid:
                                    self.bot.clan_manager.clans[tag_upper]["matches"][i] = new_match_data
                                    break
                        
                        processed_ids.add(gid)
                        processed_count[0] += 1
                            
                        if processed_count[0] % 50 == 0 and processed_count[0] > 0:
                            print(f"[{tag_upper}] Recheck progress: {processed_count[0]} / {total_to_do}...")
                            await self.bot.clan_manager.save_clan(tag_upper)
                            
                            self.save_progress(tag_upper, processed_ids)
                            
                            await asyncio.sleep(0.6)

                if not self.cancel_event.is_set():
                    await self.current_queue.join()
                
                for w in workers_list:
                    w.cancel()

                worker_sec = int(time.time() - self.start_workers)

                m, s = divmod(worker_sec, 60)
                h, m = divmod(m, 60)
                formatted_worker_time = f"{h:03d}:{m:02d}:{s:02d}"

                if self.cancel_event.is_set():
                    # Same order for the cancel hook: Save main DB, then progress
                    await self.bot.clan_manager.save_clan(tag_upper)
                    self.save_progress(tag_upper, processed_ids)
                    
                    await channel.send(
                        f"**[{tag_upper}]** Recheck CANCELLED!\n"
                        f"Live match data updated for **{processed_count[0]}** games. You can resume at any time.\n"
                        f"⏱ **Time Spent:** `{formatted_worker_time}`"
                    )
                else:
                    # Save DB one last time
                    await self.bot.clan_manager.save_clan(tag_upper)
                    
                    # Clear out the .tmp file entirely now that 100% of the games have been checked
                    self.clear_progress(tag_upper)
                    
                    # Finalize batch update to trigger a recalculation of winstreaks/stats using the new data
                    print(f"[{tag_upper}] Finalizing batch update and calculating stats...")
                    await self.bot.clan_manager.finalize_batch_update(tag_upper)

                    await channel.send(
                        f"**[{tag_upper}]** Recheck finished! All old match data has been successfully formatted and re-processed live.\n"
                        f"Processed **{processed_count[0]}** games.\n"
                        f"⏱ **Total Time Taken:** `{formatted_worker_time}`"
                    )

        except Exception as e:
            await channel.send(f"An error occurred during recheck: {e}")
        finally:
            self.bot.is_recheck_active = False
            self.start_time = None

async def setup(bot):
    await bot.add_cog(RecheckCmds(bot))