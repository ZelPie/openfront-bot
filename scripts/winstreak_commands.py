import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone
import re
from dotenv import load_dotenv
import os

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
            await interaction.response.send_message("This command is currently restricted to the developer's server.", ephemeral=True)
            return

        if not getattr(self.bot, 'is_swarm_active', False):
            await interaction.response.send_message("There is no background recheck currently running.", ephemeral=True)
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

    @app_commands.command(name="recheck-clan-data", description="Rescans all games to get all player data again. Use this if you think the winstreaks are wrong.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)")
    async def recheck_winstreaks(self, interaction: discord.Interaction, clan_tag: str):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Only administrators can do this.", ephemeral=True)
            return

        if interaction.guild_id != dev_server_id:
            await interaction.response.send_message("This command is currently restricted to the developer's server.", ephemeral=True)
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

                cutoff_date = datetime(2025, 11, 10, tzinfo=timezone.utc)
                current_end = datetime.now(timezone.utc)
                current_start = current_end - timedelta(days=3) # 3 Day block
                
                await channel.send(f"Paging backward to collect all `{total_games}` games...")
                
                while True:
                    if self.cancel_event.is_set():
                        await channel.send("Cancelled during paging.")
                        return

                    # Stop if we hit Nov 10th 2025
                    if current_end < cutoff_date:
                        break

                    start_iso = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
                    end_iso = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                    page = 1
                    day_results_count = 0
                    
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
                                    day_results_count += 1
                                    
                                    # Add to processed games if missing
                                    if gid not in self.bot.processed_games[tag_upper]:
                                        self.bot.processed_games[tag_upper].append(gid)
                                        
                            page += 1
                            await asyncio.sleep(0.2) 
                            
                    if total_games > 0 and len(seen_game_ids) >= total_games:
                        break
                        
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

                async def worker(wid):
                    while True:
                        try:
                            game = await self.current_queue.get()
                        except asyncio.CancelledError:
                            break
                        gid = game.get("gameId")
                        while True:
                            if self.cancel_event.is_set():
                                break
                            try:
                                async with session.get(f"https://api.openfront.io/public/game/{gid}?turns=false", timeout=15) as g_resp:
                                    if g_resp.status == 200:
                                        g_data = await g_resp.json()
                                        info = g_data.get("info", {})
                                        if not g_data or not info or not info.get("players"):
                                            await asyncio.sleep(1)
                                            continue
                                        downloaded_games[gid] = g_data
                                        break
                                    elif g_resp.status == 429:
                                        await asyncio.sleep(5)
                                    else:
                                        downloaded_games[gid] = None
                                        break
                            except Exception:
                                await asyncio.sleep(2)
                        self.current_queue.task_done()
                        await asyncio.sleep(0.5)

                workers_list = [asyncio.create_task(worker(i)) for i in range(3)]
                
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
                                api_name = p.get("username", "")
                                clean_api_name = api_name.replace(f"[{tag_upper.upper()}]", "").replace(f"[{tag_upper.lower()}]", "").strip()
                                
                                if clean_api_name.lower() in counted_here: continue
                                counted_here.add(clean_api_name.lower())

                                db_username = clean_api_name
                                for existing_name in temp_players.keys():
                                    if existing_name.lower() == clean_api_name.lower():
                                        db_username = existing_name
                                        break

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
                            self.bot.player_data[tag_upper]["players"][p_name] = {"games_played": 0, "wins": 0, "winstreak": 0, "highest_winstreak": 0}
                            
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

                    await channel.send(f"✅ **[{tag_upper}]** Winstreak recheck complete in **{time_str}**! Successfully re-evaluated and updated winstreaks for **{len(temp_players)}** players over **{processed_count}** games.")
                    print(f"[{tag_upper}] Winstreak recheck complete in {time_str}. Updated {len(temp_players)} players over {processed_count} games.")

        except Exception as e:
            await channel.send(f"An error occurred during recheck: {e}")
        finally:
            self.bot.is_swarm_active = False
            if timer_task and not timer_task.done():
                timer_task.cancel()

    @app_commands.command(name="alltime-winstreak", description="Calculates the highest all-time winstreak for a clan or a specific player.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)", username="Optional: Check a specific player's streak instead")
    async def alltime_winstreak(self, interaction: discord.Interaction, clan_tag: str, username: str = None):
        await interaction.response.defer()
        
        tag_upper = clan_tag.upper()
        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)  

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.followup.send("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return
        
        await interaction.followup.send(f"Scanning all games for **[{tag_upper}]** to calculate all-time winstreaks...")

        all_games = []
        seen_game_ids = set()

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

                cutoff_date = datetime(2025, 11, 10, tzinfo=timezone.utc)
                current_end = datetime.now(timezone.utc)
                current_start = current_end - timedelta(days=3)
                empty_days = 0
                
                while True:
                    if current_end < cutoff_date:
                        break

                    start_iso = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
                    end_iso = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                    page = 1
                    day_results_count = 0
                    
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
                            
                            if not results or not isinstance(results, list) or len(results) == 0:
                                break 
                                
                            for game in results:
                                gid = game.get("gameId")
                                if gid and gid not in seen_game_ids:
                                    seen_game_ids.add(gid)
                                    all_games.append(game)
                                    day_results_count += 1
                                    
                                    # Append to processed games cache if it's missing
                                    if gid not in self.bot.processed_games[tag_upper]:
                                        self.bot.processed_games[tag_upper].append(gid)
                                    
                            page += 1
                            await asyncio.sleep(0.1) 
                            
                    if total_games > 0 and len(seen_game_ids) >= total_games:
                        break
                        
                    current_end = current_start
                    current_start = current_start - timedelta(days=3)
                        
        except Exception as e:
            await interaction.followup.send(f"An error occurred while fetching clan history: {e}")
            return

        if not all_games:
            await interaction.followup.send(f"No games found for **[{tag_upper}]**.")
            return

        all_games.sort(key=lambda x: x.get("gameStart", ""))

        # PLAYER SPECIFIC SCAN
        if username:
            search_name = username.strip().lower()
            status_msg = await interaction.followup.send(f"Found **{len(all_games)}** total games for **[{tag_upper}]**.\n\nNow deep-scanning every game chronologically to find matches for **{username}**...")
            
            p_highest_streak = 0
            p_current_streak = 0
            p_games_played = 0
            exact_username = username 
            
            async with aiohttp.ClientSession() as session:
                for game in all_games:
                    gid = game.get("gameId")
                    is_win = game.get("hasWon", False)
                    
                    while True:
                        try:
                            game_url = f"https://api.openfront.io/public/game/{gid}?turns=false"
                            async with session.get(game_url, timeout=10) as g_resp:
                                if g_resp.status == 429:
                                    await asyncio.sleep(1)
                                    continue
                                
                                if g_resp.status == 200:
                                    g_data = await g_resp.json()
                                    info = g_data.get("info", {})
                                    players = info.get("players", [])
                                    
                                    player_in_game = False
                                    for p in players:
                                        api_name = p.get("username", "")
                                        clean_api_name = api_name.lower().replace(f"[{tag_upper.lower()}]", "").strip()
                                        
                                        if p.get("clanTag", "").upper() == tag_upper and clean_api_name == search_name:
                                            player_in_game = True
                                            exact_username = api_name.replace(f"[{tag_upper.upper()}]", "").replace(f"[{tag_upper.lower()}]", "").strip()
                                            break
                                    
                                    if player_in_game:
                                        p_games_played += 1
                                        if is_win:
                                            p_current_streak += 1
                                            if p_current_streak > p_highest_streak:
                                                p_highest_streak = p_current_streak
                                        else:
                                            p_current_streak = 0
                                break
                        except Exception:
                            await asyncio.sleep(1) 
                            continue
                            
                    await asyncio.sleep(0.15) 

            if p_games_played == 0:
                await status_msg.edit(content=f"Player **{username}** was not found in any of the {len(all_games)} games played by **[{tag_upper}]**.")
                return
                
            data_changed = False
            if tag_upper not in self.bot.player_data:
                self.bot.player_data[tag_upper] = {"total_games": 0, "winstreak": 0, "highest_winstreak": 0, "players": {}}
                
            db_username = exact_username
            for existing_name in self.bot.player_data[tag_upper]["players"].keys():
                if existing_name.lower() == exact_username.lower():
                    db_username = existing_name
                    break
                    
            if db_username not in self.bot.player_data[tag_upper]["players"]:
                self.bot.player_data[tag_upper]["players"][db_username] = {"games_played": 0, "wins": 0, "winstreak": 0, "highest_winstreak": 0}
                
            p_stats = self.bot.player_data[tag_upper]["players"][db_username]
            
            if p_highest_streak > p_stats.get("highest_winstreak", 0):
                p_stats["highest_winstreak"] = p_highest_streak
                data_changed = True
            
            p_stats["winstreak"] = p_current_streak
            p_stats["games_played"] = p_games_played
            data_changed = True

            if data_changed and hasattr(self.bot, 'save_lock'):
                async with self.bot.save_lock:
                    self.bot.save_data()

            embed = discord.Embed(title=f"All-Time Winstreak for {db_username} [{tag_upper}]", color=discord.Color.blue())
            embed.add_field(name="Highest Winstreak", value=f"**{p_highest_streak}**", inline=False)
            embed.add_field(name="Current Winstreak", value=f"**{p_current_streak}**", inline=False)
            embed.add_field(name="Games Played", value=f"``{p_games_played}``", inline=False)
            
            await status_msg.edit(content=None, embed=embed)
            return

        # NORMAL CLAN ROUTE
        current_streak = 0
        highest_streak = 0

        for game in all_games:
            is_win = game.get("hasWon", False)
            if is_win:
                current_streak += 1
                if current_streak > highest_streak:
                    highest_streak = current_streak
            else:
                current_streak = 0

        data_changed = False
        if tag_upper in self.bot.player_data:
            stored_highest = self.bot.player_data[tag_upper].get("highest_winstreak", 0)
            if highest_streak > stored_highest:
                self.bot.player_data[tag_upper]["highest_winstreak"] = highest_streak
                data_changed = True
        else:
            self.bot.player_data[tag_upper] = {"total_games": len(all_games), "winstreak": current_streak, "highest_winstreak": highest_streak, "players": {}}
            data_changed = True
                
        if data_changed and hasattr(self.bot, 'save_lock'):
            async with self.bot.save_lock:
                self.bot.save_data()

        embed = discord.Embed(title=f"All-Time Winstreak for [{tag_upper}]", color=discord.Color.gold())
        embed.add_field(name="Highest Winstreak", value=f"**{highest_streak}**", inline=False)
        embed.add_field(name="Total Games Analyzed", value=f"``{len(all_games)}``", inline=False)
        
        if data_changed:
            embed.set_footer(text="New highest winstreak saved to database!")

        await interaction.followup.send(content=f"<@{interaction.user.id}>", embed=embed)


async def setup(bot):
    await bot.add_cog(WinstreakCmds(bot))