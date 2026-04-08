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
        
        if not hasattr(self.bot, 'save_lock'):
            self.bot.save_lock = asyncio.Lock()
            
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

        if tag_upper not in self.bot.player_data:
            self.bot.player_data[tag_upper] = {"total_games": 0, "winstreak": 0, "highest_winstreak": 0, "players": {}, "load_time_seconds": 0}
        if "load_time_seconds" not in self.bot.player_data[tag_upper]:
            self.bot.player_data[tag_upper]["load_time_seconds"] = 0
            
        if not hasattr(self.bot, 'processed_games'):
            self.bot.processed_games = {}
        if tag_upper not in self.bot.processed_games:
            self.bot.processed_games[tag_upper] = []

        try:
            async with aiohttp.ClientSession() as session:
                total_games = 0
                url_total = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions?limit=1"
                async with session.get(url_total, timeout=10) as resp:
                    if resp.status == 200:
                        dat = await resp.json()
                        total_games = int(dat.get("total", 0))

                await channel.send(f"Paging backward to collect all `{total_games}` games...")
                
                # --- MATCHING PAGING LOGIC ---
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
                                    
                                    if gid not in self.bot.processed_games[tag_upper]:
                                        self.bot.processed_games[tag_upper].append(gid)
                                        
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
                                        
                                        if gid not in self.bot.processed_games[tag_upper]:
                                            self.bot.processed_games[tag_upper].append(gid)
                                            
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
                            self.bot.player_data[tag_upper]["load_time_seconds"] += 1
                    except asyncio.CancelledError:
                        pass
                
                timer_task = asyncio.create_task(timer())

                # Use the imported shared worker
                workers_list = [
                    asyncio.create_task(fetch_game_worker(i, session, self.current_queue, self.cancel_event, downloaded_games)) 
                    for i in range(3)
                ]
                
                temp_clan_winstreak = 0
                temp_clan_highest = 0
                temp_players = {}

                processed_count = 0
                for game in all_games:
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
                            temp_clan_winstreak += 1
                            if temp_clan_winstreak > temp_clan_highest:
                                temp_clan_highest = temp_clan_winstreak
                        else:
                            temp_clan_winstreak = 0

                        players = info.get("players", [])
                        counted_here = set()
                        for p in players:
                            if p.get("clanTag", "").upper() == tag_upper:
                                
                                # FIX: Do not strip out the clan tags. Keep them exactly as load_players does.
                                db_username = p.get("username", "Unknown")
                                
                                if db_username in counted_here: continue
                                counted_here.add(db_username)

                                if db_username not in temp_players:
                                    temp_players[db_username] = {"games_played": 0, "wins": 0, "winstreak": 0, "highest_winstreak": 0}
                                
                                stats = temp_players[db_username]
                                
                                stats["games_played"] += 1
                                if is_win: 
                                    stats["wins"] += 1
                                    stats["winstreak"] += 1
                                    if stats["winstreak"] > stats["highest_winstreak"]:
                                        stats["highest_winstreak"] = stats["winstreak"]
                                else:
                                    stats["winstreak"] = 0
                                    
                        processed_count += 1
                        
                    if processed_count % 50 == 0 and processed_count > 0:
                        print(f"[{tag_upper}] Recheck progress: {processed_count} / {total_to_do}...")
                            
                await self.current_queue.join()
                for w in workers_list:
                    w.cancel()
                    
                if timer_task:
                    timer_task.cancel()

                if self.cancel_event.is_set():
                    await channel.send(f"**[{tag_upper}]** Winstreak recheck CANCELLED at {processed_count}/{total_to_do} games. No data was modified.")
                else:
                    if tag_upper not in self.bot.player_data:
                        self.bot.player_data[tag_upper] = {"total_games": 0, "players": {}}
                    
                    self.bot.player_data[tag_upper]["winstreak"] = temp_clan_winstreak
                    self.bot.player_data[tag_upper]["highest_winstreak"] = temp_clan_highest
                    
                    for p_name, p_stats in temp_players.items():
                        if p_name not in self.bot.player_data[tag_upper]["players"]:
                            self.bot.player_data[tag_upper]["players"][p_name] = {"name": [p_name], "games_played": 0, "wins": 0, "winstreak": 0, "highest_winstreak": 0}
                            
                        self.bot.player_data[tag_upper]["players"][p_name]["games_played"] = p_stats["games_played"]
                        self.bot.player_data[tag_upper]["players"][p_name]["wins"] = p_stats["wins"]
                        self.bot.player_data[tag_upper]["players"][p_name]["winstreak"] = p_stats["winstreak"]
                        self.bot.player_data[tag_upper]["players"][p_name]["highest_winstreak"] = p_stats["highest_winstreak"]
                        self.bot.player_data[tag_upper]["players"][p_name]["winrate"] = round((p_stats["wins"] / p_stats["games_played"]) * 100, 2) if p_stats["games_played"] > 0 else 0.0

                    async with self.bot.save_lock:
                        self.bot.save_data()

                    total_time = self.bot.player_data[tag_upper].get("load_time_seconds", 0)
                    m, s = divmod(total_time, 60)
                    h, m = divmod(m, 60)
                    if h > 0:
                        time_str = f"{h}h {m}m {s}s"
                    elif m > 0:
                        time_str = f"{m}m {s}s"
                    else:
                        time_str = f"{s}s"

                    await channel.send(f"**[{tag_upper}]** Winstreak recheck complete in **{time_str}**! Successfully re-evaluated and updated winstreaks for **{len(temp_players)}** players over **{processed_count}** games.")

        except Exception as e:
            await channel.send(f"An error occurred during recheck: {e}")
        finally:
            self.bot.is_swarm_active = False
            if timer_task and not timer_task.done():
                timer_task.cancel()

    # NOTE: The rest of the WinstreakCmds class (e.g. alltime_winstreak) goes here...
    # (Left unchanged for brevity, make sure to keep your existing alltime_winstreak method)

async def setup(bot):
    await bot.add_cog(WinstreakCmds(bot))