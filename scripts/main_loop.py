import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone

import re
import urllib.parse

from math import ceil

class BackgroundLoop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
        self.live_queue = asyncio.Queue()
        self.queued_games = set() 
        self.match_details_cache = {}
        
        self.worker_task = self.bot.loop.create_task(self.live_worker())
        self.check_clan_stats.start()

    def script_unload(self):
        self.check_clan_stats.cancel()
        if hasattr(self, 'worker_task'):
            self.worker_task.cancel()

    def get_map_thumbnail(self, map_name: str, commit_sha: str = "main") -> str:
        github_raw_base = "https://raw.githubusercontent.com/openfrontio/OpenFrontIO" 

        if not map_name:
            return f"{github_raw_base}/{commit_sha}/resources/images/GameplayScreenshot.png"
        
        normalized_map = re.sub(r'[\s.()]+', '', map_name.lower())

        if normalized_map:
            return f"{github_raw_base}/{commit_sha}/resources/maps/{normalized_map}/thumbnail.webp"

        return f"{github_raw_base}/{commit_sha}/resources/images/GameplayScreenshot.png"

    async def create_match_embed(self, http_session, clan_tag: str, session: dict, clan_data: dict, match_cache: dict = None):
        if match_cache is None: match_cache = {}

        session_id = session.get("gameId", "Unknown")
        is_win = session.get("hasWon", False)
        gamemode = session.get("playerTeams", "Unknown Mode")
        num_teams = session.get("numTeams", "?")
        player_count = session.get("clanPlayerCount", 1)
        score = session.get("score", 0)
        total_players = session.get("totalPlayerCount", "?")

        raw_start = session.get("start")
        raw_end = session.get("end")
        all_players = []
        max_players = 0
        player_teams = 0
        commit_sha = "main"

        map_name = "Unknown Map"
        
        if session_id in match_cache:
            cache_data = match_cache[session_id]
            all_players = cache_data.get("players", [])
            if not raw_start: raw_start = cache_data.get("start")
            if not raw_end: raw_end = cache_data.get("end")

            max_players = cache_data.get("maxPlayers", 0)
            player_teams = cache_data.get("playerTeams", 0)
            commit_sha = cache_data.get("gitCommit", "main")
            map_name = cache_data.get("gameMap", "Unknown Map")
        else:
            all_data = False
            game_url = f"https://api.openfront.io/public/game/{session_id}?turns=false"
            while not all_data:
                try:
                    async with http_session.get(game_url, timeout=10) as game_response:
                        if game_response.status == 200:
                            game_data = await game_response.json()
                            info = game_data.get("info", {})
                            all_players = info.get("players", [])
                            config = info.get("config", {})
                            
                            if not raw_start: raw_start = info.get("start")
                            if not raw_end: raw_end = info.get("end")

                            map_name = config.get("gameMap", "Unknown Map")
                            
                            max_players = config.get("maxPlayers")
                            player_teams = config.get("playerTeams")

                            match_cache[session_id] = {
                                "players": all_players, 
                                "start": raw_start, 
                                "end": raw_end, 
                                "maxPlayers": max_players, 
                                "playerTeams": player_teams,
                                "mapName": map_name,
                                "commitSHA": commit_sha
                            }

                            if all_players and game_data and config:
                                all_data = True
                except Exception as e:
                    print(f"Failed to fetch player details for session {session_id}: {e}")

        start_display = "Unknown"
        end_display = "Unknown"
        duration_display = "Unknown"

        if raw_start and raw_end:
            try:
                start_sec = int(int(raw_start) / 1000)
                end_sec = int(int(raw_end) / 1000)
                start_display = f"<t:{start_sec}:t>" 
                end_display = f"<t:{end_sec}:t>"
                
                duration_sec = end_sec - start_sec
                m, s = divmod(duration_sec, 60)
                h, m = divmod(m, 60)
                if h > 0:
                    duration_display = f"**{h}h {m}m {s}s**"
                else:
                    duration_display = f"**{m}m {s}s**"
            except Exception as e:
                print(f"Could not parse time for {session_id}: {e}")

        clan_players = [
            f"``{p.get('username', 'Unknown')}``" for p in all_players 
            if (p.get("clanTag") or "").upper() == clan_tag.upper()
        ]
        player_names = ", ".join(clan_players) if clan_players else "Unknown Players"

        c_stats = clan_data.get("clan", {})
        overall_wins = c_stats.get("wins", 0)
        overall_games = c_stats.get("games", 0)
        overall_losses = overall_games - overall_wins
        overall_wl = c_stats.get("weightedWLRatio", 0.0)
        
        local_stats = await self.bot.clan_manager.get_clan_stats(clan_tag)
        winstreak = local_stats.get("winstreak", 0)
        highest_winstreak = local_stats.get("highest_winstreak", 0)
        
        if is_win:
            title = f"Clan [{clan_tag}] Victory!"
            color = discord.Color.green()
            rating_text = f"**+{score}** Weighted Wins"
        else:
            title = f"Clan [{clan_tag}] Defeat..."
            color = discord.Color.red()
            rating_text = f"**{score}** Weighted Wins"
        
        display_gamemode = ""

        if gamemode.lower() in ["trios", "quads", "duos"]:
            display_gamemode = f"{gamemode} ({num_teams} Teams)"
        else:
            display_gamemode = f"{num_teams} teams of {max_players // player_teams}" if max_players and player_teams else "Unknown Mode"

        embed = discord.Embed(title=title, color=color)

        thumbnail_url = self.get_map_thumbnail(map_name, commit_sha)

        replay_url = f"https://openfront.io/game/{session_id}?live"

        embed.set_image(url=thumbnail_url)

        embed.add_field(name="Map", value=f"[{map_name}]({replay_url})", inline=False)
        embed.add_field(name="Started", value=start_display, inline=True)
        embed.add_field(name="Ended", value=end_display, inline=True)
        embed.add_field(name="Duration", value=duration_display, inline=True)
        embed.add_field(name="Rating Change", value=rating_text, inline=False)
        embed.add_field(name="Winstreak", value=f"Current: **{winstreak}** | Highest: **{highest_winstreak}**", inline=False)
        embed.add_field(name="Clan Players", value = f"``{player_count}`` / ``{total_players}``", inline=True)
        embed.add_field(name="Gamemode", value=f"{display_gamemode}", inline=True)
        embed.add_field(name="Clan Players in Match", value=f"{player_names}", inline=False)
        embed.add_field(name="New Overall Clan Stats", value=f"Total: **{overall_wins}W** - **{overall_losses}L** (W/L: **{overall_wl:.2f}**)", inline=False)
        
        if raw_end:
            embed.set_footer(text=f"Match ID: {session_id} • Ended")
        else:
            embed.set_footer(text=f"Match ID: {session_id}")

        return embed

    @app_commands.command(name="test", description="Test the embed output using the latest game from clan UN.")
    @app_commands.describe(clan_tag="The clan tag to test with (default: UN)")
    async def test_embed(self, interaction: discord.Interaction, clan_tag: str = "UN"):
        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message("You don't have permission to manage channels, which is required to run tests.", ephemeral=True)
            return

        await interaction.response.defer()
        api_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}/sessions?limit=1"
        try:
            async with aiohttp.ClientSession() as http_session:
                async with http_session.get(api_url, timeout=10) as response:
                    if response.status == 200:
                        api_data = await response.json()
                        sessions = api_data.get("results", [])
                        
                        if not sessions or not sessions[0].get("gameId"):
                            await interaction.followup.send(f"Could not find any recent valid games for [{clan_tag}].")
                            return
                            
                        latest_session = sessions[-1] 

                        # Fetch clan stats once for the test
                        stats_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}"
                        clan_data = {}
                        try:
                            async with http_session.get(stats_url, timeout=5) as stat_resp:
                                if stat_resp.status == 200:
                                    clan_data = await stat_resp.json()
                        except Exception:
                            pass
                            
                        embed = await self.create_match_embed(http_session, clan_tag.upper(), latest_session, clan_data)
                        if embed:
                            await interaction.followup.send(content=f"**TEST MODE:** Latest match for [{clan_tag.upper()}]", embed=embed)
                        else:
                            await interaction.followup.send("Failed to build embed.")
                    else:
                        await interaction.followup.send(f"API Error: {response.status}")
        except Exception as e:
            await interaction.followup.send(f"An error occurred during test: {e}")

    @tasks.loop(seconds=30) 
    async def check_clan_stats(self):
        unique_clans = set()
        for data in list(self.bot.server_data.values()):
            for tracker in data.get("trackers", []):
                if tracker.get("clan_tag"):
                    unique_clans.add(tracker["clan_tag"])
        
        # Pull strictly 1 hour of history for every clan, regardless of channel trackers
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        iso_timestamp = one_hour_ago.strftime('%Y-%m-%dT%H:%M:%SZ')
        LIMIT = 50

        print(f"Checking for new games for {len(unique_clans) if unique_clans else 'no'} clans. . .")
        
        async with aiohttp.ClientSession() as http_session:
            for clan_tag in unique_clans:
                sessions = []
                page = 1
                try:
                    while True:
                        api_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}/sessions?start={iso_timestamp}&page={page}&limit={LIMIT}"
                        async with http_session.get(api_url, timeout=10) as response:
                            if response.status != 200:
                                break
                            
                            api_data = await response.json()
                            results = api_data.get("results", [])
                            
                            if not results or results == []:
                                break

                            sessions.extend(results)
                            page += 1
                except Exception as e:
                    print(f"Error fetching data for {clan_tag}: {e}")
                    continue

                if not sessions:
                    continue

                sessions.sort(key=lambda x: x.get("gameStart", ""))

                if not isinstance(sessions[0], dict) or not sessions[0].get("gameId"):
                    continue

                new_sessions = []
                for session in sessions:
                    try:
                        session_id = session.get("gameId")
                        if not session_id:
                            continue
                        
                        is_processed = await self.bot.clan_manager.is_processed(clan_tag, session_id)
                        if not is_processed and session_id not in self.queued_games:
                            new_sessions.append(session)
                    except Exception as e:
                        print(f"Error: {e}")

                if new_sessions:
                    for session in new_sessions:
                        # Only put the clan_tag and session in the queue
                        self.live_queue.put_nowait((clan_tag, session))
                        self.queued_games.add(session.get("gameId"))

                    print(f"Queued {len(new_sessions)} new games for clan [{clan_tag}].")

    async def live_worker(self):
        await self.bot.wait_until_ready()
        async with aiohttp.ClientSession() as http_session:
            while True:
                try:
                    clan_tag, session = await self.live_queue.get()
                    session_id = session.get("gameId")
                    is_win = session.get("hasWon", False)
                    game_url = f"https://api.openfront.io/public/game/{session_id}?turns=false"
                    
                    while True:
                        try:
                            async with http_session.get(game_url, timeout=10) as game_resp:
                                if game_resp.status == 200:
                                    game_data = await game_resp.json()
                                    info = game_data.get("info", {})
                                    
                                    if not game_data or not info or not info.get("players"):
                                        print(f"Data for {session_id} is still empty. Retrying in 2s...")
                                        await asyncio.sleep(2) 
                                        continue 
                                        
                                    all_players = info.get("players", [])
                                    config = info.get("config", {})

                                    self.match_details_cache[session_id] = {
                                        "players": all_players,
                                        "start": info.get("start"),
                                        "end": info.get("end"),
                                        "maxPlayers": config.get("maxPlayers", 0),
                                        "playerTeams": config.get("playerTeams", 0),
                                        "gameMap": config.get("gameMap", "Unknown Map")
                                    }

                                    await self.bot.clan_manager.process_game(clan_tag, session, info, mode="live")
                                    game_end_ms = int(info.get("end", 0)) if info.get("end") else 0

                                    stats_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}"
                                    clan_data = {}
                                    retries = 0
                                    while not clan_data and retries < 3:
                                        try:
                                            async with http_session.get(stats_url, timeout=5) as stat_resp:
                                                if stat_resp.status == 200:
                                                    clan_data = await stat_resp.json()
                                                else:
                                                    retries += 1
                                        except Exception:
                                            retries += 1

                                    embed = await self.create_match_embed(
                                        http_session, clan_tag, session, clan_data, self.match_details_cache
                                    )

                                    # DISTRIBUTE THE PRE-BUILT EMBED TO ALL TRACKING CHANNELS
                                    for guild_id, data in list(self.bot.server_data.items()):
                                        for tracker in data.get("trackers", []):
                                            if tracker.get("clan_tag") == clan_tag and tracker.get("channel_id"):
                                                
                                                channel_scan_time = tracker.get("initial_scan_time", 0)
                                                if game_end_ms >= channel_scan_time:
                                                    # Delegate 'track_losses' logic here, rather than inside embed generation
                                                    if not is_win and not tracker.get("track_losses", False):
                                                        continue 
                                                        
                                                    channel = self.bot.get_channel(tracker["channel_id"])
                                                    if channel and embed:
                                                        await channel.send(embed=embed)
                                    
                                    print(f"Successfully processed game {session_id} for clan [{clan_tag}]. Win: {is_win}. Games left in queue: {len(self.queued_games) - 1}")

                                    # CLEAR CACHE TO PREVENT MEMORY LEAK
                                    self.match_details_cache.pop(session_id, None)
                                    self.queued_games.discard(session_id)
                                    stats = await self.bot.clan_manager.get_clan_stats(clan_tag)
                                    
                                    break 

                                elif game_resp.status == 429:
                                    print(f"429 Rate Limit. Pausing 5s before retrying {session_id}...")
                                    await asyncio.sleep(5)
                                else:
                                    print(f"Error {game_resp.status}. Retrying {session_id} in 3s...")
                                    await asyncio.sleep(3)

                        except Exception as e:
                            print(f"Network Hiccup on {session_id}. Retrying in 3s... ({e})")
                            await asyncio.sleep(3)
                            
                    self.live_queue.task_done()
                
                    if self.live_queue.empty():
                        await self.bot.clan_manager.finalize_batch_update(clan_tag)

                    await asyncio.sleep(0.2) 
                    
                except Exception as e:
                    print(f"Live Queue Critical Error: {e}")
                    await asyncio.sleep(0.3)

    @check_clan_stats.before_loop
    async def before_check_clan_stats(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(BackgroundLoop(bot))