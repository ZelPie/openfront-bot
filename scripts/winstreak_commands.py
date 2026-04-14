import discord
from discord.ext import commands
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

class WinstreakCmds(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
            
        if not hasattr(self.bot, 'is_swarm_active'):
            self.bot.is_swarm_active = False

        self.cancel_event = asyncio.Event()
        self.current_queue = None

    @app_commands.command(name="cancel-recheck", description="Cancels the currently running winstreak recheck.")
    async def cancel_recheck(self, interaction: discord.Interaction):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Only administrators can do this.", ephemeral=True)
            return

        if interaction.guild_id != dev_server_id:
            await interaction.response.send_message("Restricted to developer server.", ephemeral=True)
            return

        if not getattr(self.bot, 'is_swarm_active', False):
            await interaction.response.send_message("No background recheck currently running.", ephemeral=True)
            return

        self.cancel_event.set()
        await interaction.response.send_message("**Cancellation requested!** The bot will stop scanning and revert changes safely.")

        if self.current_queue:
            while not self.current_queue.empty():
                try:
                    self.current_queue.get_nowait()
                    self.current_queue.task_done()
                except asyncio.QueueEmpty:
                    break

    @app_commands.command(name="recheck-clan-data", description="Rescans all games to get all player data again.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)")
    async def recheck_winstreaks(self, interaction: discord.Interaction, clan_tag: str):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Only administrators can do this.", ephemeral=True)
            return

        if interaction.guild_id != dev_server_id:
            await interaction.response.send_message("Restricted to developer server.", ephemeral=True)
            return

        if getattr(self.bot, 'is_swarm_active', False):
            await interaction.response.send_message("A background task is already running. Please wait.", ephemeral=True)
            return

        tag_upper = clan_tag.upper()
        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Invalid clan tag.", ephemeral=True)
            return

        await interaction.response.send_message(f"Initiating full winstreak recheck for **[{tag_upper}]**...")
        
        self.bot.is_swarm_active = True
        self.bot.loop.create_task(self.background_recheck(tag_upper, interaction.channel))

    async def background_recheck(self, tag_upper, channel):
        self.cancel_event.clear()
        self.current_queue = None
        timer_task = None 
        
        all_games = []
        seen_game_ids = set()

        try:
            async with aiohttp.ClientSession() as session:
                total_games = 0
                url_total = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions?limit=1"
                async with session.get(url_total, timeout=10) as resp:
                    if resp.status == 200:
                        dat = await resp.json()
                        total_games = int(dat.get("total", 0))

                await channel.send(f"Paging backward to collect all `{total_games}` games...")
                
                if total_games <= 10000:
                    page = 1
                    while len(seen_game_ids) < total_games:
                        if self.cancel_event.is_set():
                            await channel.send("Cancelled during paging.")
                            return

                        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions?page={page}&limit=50"
                        async with session.get(url, timeout=15) as response:
                            if response.status == 429:
                                await asyncio.sleep(1)
                                continue 
                            if response.status != 200:
                                break 
                            data = await response.json()
                            results = data.get("results", [])
                            if not results:
                                break 
                                
                            for game in results:
                                gid = game.get("gameId")
                                if gid and gid not in seen_game_ids:
                                    seen_game_ids.add(gid)
                                    all_games.append(game)
                                        
                        page += 1
                        await asyncio.sleep(0.2) 
                else:
                    cutoff_date = datetime(2025, 11, 10, tzinfo=timezone.utc)
                    current_end = datetime.now(timezone.utc)
                    current_start = current_end - timedelta(days=3)
                    
                    while len(seen_game_ids) < total_games:
                        if self.cancel_event.is_set():
                            await channel.send("Cancelled during paging.")
                            return
                        if current_end < cutoff_date:
                            break

                        start_iso = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
                        end_iso = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                        
                        page = 1
                        while True:
                            url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions?start={start_iso}&end={end_iso}&page={page}&limit=50"
                            async with session.get(url, timeout=15) as response:
                                if response.status == 429:
                                    await asyncio.sleep(1)
                                    continue 
                                if response.status != 200:
                                    break 
                                data = await response.json()
                                results = data.get("results", [])
                                if not results:
                                    break 
                                    
                                for game in results:
                                    gid = game.get("gameId")
                                    if gid and gid not in seen_game_ids:
                                        seen_game_ids.add(gid)
                                        all_games.append(game)
                                            
                            page += 1
                            await asyncio.sleep(0.2) 
                            
                        current_end = current_start
                        current_start = current_start - timedelta(days=3)

                if not all_games:
                    await channel.send(f"No games found for **[{tag_upper}]**.")
                    return

                all_games.sort(key=lambda x: x.get("gameStart", ""))
                total_to_do = len(all_games)
                
                await channel.send(f"Found **{total_to_do}** total games. Starting deep scan to safely recalculate winstreaks...")

                self.current_queue = asyncio.Queue()
                for game in all_games:
                    self.current_queue.put_nowait(game)

                downloaded_games = {}

                async def timer():
                    try:
                        while True:
                            await asyncio.sleep(1)
                            stats = await self.bot.clan_manager.get_clan_stats(tag_upper)
                            stats["load_time_seconds"] = stats.get("load_time_seconds", 0) + 1
                    except asyncio.CancelledError:
                        pass
                
                timer_task = asyncio.create_task(timer())

                workers_list = [
                    asyncio.create_task(fetch_game_worker(i, session, self.current_queue, self.cancel_event, downloaded_games)) 
                    for i in range(3)
                ]
                
                # WIPE OLD DATA
                await self.bot.clan_manager.reset_clan_stats(tag_upper)

                processed_count = 0
                for game in all_games:
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
                        
                        await self.bot.clan_manager.process_game(tag_upper, game, info, mode="recheck")
                        processed_count += 1
                        
                    if processed_count % 50 == 0 and processed_count > 0:
                        print(f"[{tag_upper}] Recheck progress: {processed_count} / {total_to_do}...")
                            
                await self.current_queue.join()
                for w in workers_list:
                    w.cancel()
                    
                if timer_task:
                    timer_task.cancel()

                final_stats = await self.bot.clan_manager.get_clan_stats(tag_upper)
                player_count = len(final_stats.get("players", {}))

                if self.cancel_event.is_set():
                    await channel.send(f"**[{tag_upper}]** Winstreak recheck CANCELLED at {processed_count}/{total_to_do} games.")
                else:
                    total_time = final_stats.get("load_time_seconds", 0)
                    m, s = divmod(total_time, 60)
                    h, m = divmod(m, 60)
                    if h > 0:
                        time_str = f"{h}h {m}m {s}s"
                    elif m > 0:
                        time_str = f"{m}m {s}s"
                    else:
                        time_str = f"{s}s"

                    await channel.send(f"**[{tag_upper}]** Winstreak recheck complete in **{time_str}**! Successfully re-evaluated and updated winstreaks for **{player_count}** players over **{processed_count}** games.")

        except Exception as e:
            await channel.send(f"An error occurred during recheck: {e}")
        finally:
            self.bot.is_swarm_active = False
            if timer_task and not timer_task.done():
                timer_task.cancel()

async def setup(bot):
    await bot.add_cog(WinstreakCmds(bot))