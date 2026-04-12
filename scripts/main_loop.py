import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone

class BackgroundLoop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        if not hasattr(self.bot, 'save_lock'):
            self.bot.save_lock = asyncio.Lock()
        
        # Live Tracking Queue System
        self.live_queue = asyncio.Queue()
        self.queued_games = set() # Keeps track of what's already in line
        self.match_details_cache = {}
        
        # Start the background worker and the 15-second scout
        self.worker_task = self.bot.loop.create_task(self.live_worker())
        self.check_clan_stats.start()

    def script_unload(self):
        self.check_clan_stats.cancel()
        if hasattr(self, 'worker_task'):
            self.worker_task.cancel()

    async def create_match_embed(self, http_session, clan_tag, session, track_losses=True, match_cache=None):
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
        
        if session_id in match_cache:
            cache_data = match_cache[session_id]
            all_players = cache_data.get("players", [])
            if not raw_start: raw_start = cache_data.get("start")
            if not raw_end: raw_end = cache_data.get("end")

            max_players = cache_data.get("maxPlayers", 0)
            player_teams = cache_data.get("playerTeams", 0)
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
                            
                            max_players = config.get("maxPlayers")
                            player_teams = config.get("playerTeams")

                            match_cache[session_id] = {"players": all_players, "start": raw_start, "end": raw_end, "maxPlayers": max_players, "playerTeams": player_teams}

                            if all_players and game_data and config:
                                all_data = True
                except Exception as e:
                    print(f"Failed to fetch player details for session {session_id}: {e}")
        start_display = "Unknown"
        end_display = "Unknown"
        duration_display = "Unknown"
        print(max_players)
        print(player_teams)

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
            if p.get("clanTag", "").upper() == clan_tag.upper()
        ]
        player_names = ", ".join(clan_players) if clan_players else "Unknown Players"

        stats_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}"

        all_data = False
        retries = 0
        while not all_data:
            try:
                async with http_session.get(stats_url, timeout=5) as stat_resp:
                    if stat_resp.status == 200:
                        clan_data = await stat_resp.json()
                    else:
                        clan_data = {}
                    
                    if clan_data and clan_data.get("clan", {}).get("games", 0) is not None or retries >= 3:
                        all_data = True
                    retries += 1
            except Exception:
                clan_data = {}
                retries += 1
                
        c_stats = clan_data.get("clan", {})
        overall_wins = c_stats.get("wins", 0)
        overall_games = c_stats.get("games", 0)
        overall_losses = overall_games - overall_wins
        overall_wl = c_stats.get("weightedWLRatio", 0.0)
        winstreak = self.bot.player_data.get(clan_tag, {}).get("winstreak", 0)
        highest_winstreak = self.bot.player_data.get(clan_tag, {}).get("highest_winstreak", 0)
        
        if is_win:
            title = f"Clan [{clan_tag}] Victory!"
            color = discord.Color.green()
            rating_text = f"**+{score}** Weighted Wins"
        else:
            if not track_losses:
                return None 
            title = f"Clan [{clan_tag}] Defeat..."
            color = discord.Color.red()
            rating_text = f"**{score}** Weighted Wins"
        
        display_gamemode = ""

        if gamemode.lower() == "trios" or gamemode.lower() == "quads" or gamemode.lower() == "duos":
            display_gamemode = f"{gamemode} ({num_teams} Teams)"
        else:
            display_gamemode = f"{num_teams} teams of {max_players // player_teams}" if max_players and player_teams else "Unknown Mode 1"

        embed = discord.Embed(title=title, color=color)
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
        # Update URL for the new format
        api_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}/sessions?limit=1"
        try:
            async with aiohttp.ClientSession() as http_session:
                async with http_session.get(api_url, timeout=10) as response:
                    if response.status == 200:
                        api_data = await response.json()
                        # Extract the 'results' list
                        sessions = api_data.get("results", [])
                        
                        if not sessions or not sessions[0].get("gameId"):
                            await interaction.followup.send(f"Could not find any recent valid games for [{clan_tag}].")
                            return
                            
                        latest_session = sessions[-1] 
                        embed = await self.create_match_embed(http_session, clan_tag, latest_session, track_losses=True)
                        if embed:
                            await interaction.followup.send(content=f"**TEST MODE:** Latest match for [{clan_tag.upper()}]", embed=embed)
                        else:
                            await interaction.followup.send("Failed to build embed.")
                    else:
                        await interaction.followup.send(f"API Error: {response.status}")
        except Exception as e:
            await interaction.followup.send(f"An error occurred during test: {e}")

    # BACKGROUND LOOP CODE
    @tasks.loop(seconds=30) 
    async def check_clan_stats(self):
        unique_clans = set()
        for data in list(self.bot.server_data.values()):
            for tracker in data.get("trackers", []):
                if tracker.get("clan_tag"):
                    unique_clans.add(tracker["clan_tag"])
        
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        iso_timestamp = two_hours_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

        print(f"Checking for new games for {len(unique_clans) if unique_clans else 'no'} clans. . .")
        
        async with aiohttp.ClientSession() as http_session:
            for clan_tag in unique_clans:
                

                # Setup clan data
                if clan_tag not in self.bot.player_data:
                    self.bot.player_data[clan_tag] = {"total_games": 0, "winstreak": 0, "highest_winstreak": 0, "wins": 0, "players": {}}
                else:
                    if "total_games" not in self.bot.player_data[clan_tag]:
                        self.bot.player_data[clan_tag]["total_games"] = 0
                    if "winstreak" not in self.bot.player_data[clan_tag]:
                        self.bot.player_data[clan_tag]["winstreak"] = 0
                    if "highest_winstreak" not in self.bot.player_data[clan_tag]:
                        self.bot.player_data[clan_tag]["highest_winstreak"] = 0
                    if "players" not in self.bot.player_data[clan_tag]:
                        self.bot.player_data[clan_tag]["players"] = {}

                if clan_tag not in self.bot.processed_games:
                    self.bot.processed_games[clan_tag] = []
                
                if "wins" not in self.bot.player_data[clan_tag]:
                    self.bot.player_data[clan_tag]["wins"] = 0

                if "initial_scan_time" not in self.bot.player_data[clan_tag]:
                    now = datetime.now(timezone.utc) - timedelta(hours=1)
                    self.bot.player_data[clan_tag]["initial_scan_time"] = int(now.timestamp() * 1000)

                initial_scan_time = self.bot.player_data[clan_tag].get("initial_scan_time")

                async with self.bot.save_lock:
                    self.bot.save_data()

                sessions = []
                page = 1
                try:
                    # Loop through pages until empty
                    while True:
                        api_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}/sessions?start={iso_timestamp}&page={page}&limit=50"
                        async with http_session.get(api_url, timeout=10) as response:
                            if response.status != 200:
                                break
                            
                            api_data = await response.json()
                            results = api_data.get("results", [])
                            
                            if not results: # Results empty, stop paging
                                break
                                
                            sessions.extend(results)
                            page += 1
                except Exception as e:
                    print(f"Error fetching data for {clan_tag}: {e}")
                    continue

                if not sessions:
                    continue

                # Ensure oldest games are first!
                sessions.sort(key=lambda x: x.get("gameStart", ""))

                if not isinstance(sessions[0], dict) or not sessions[0].get("gameId"):
                    continue

                new_sessions = []
                for session in sessions:
                    try:
                        session_id = session.get("gameId")
                        iso = session.get("gameStart")
                        if not iso:
                            continue
                        game_start_ms = datetime.fromisoformat(iso.replace('Z', '+00:00')).timestamp() * 1000
                        if session_id and session_id not in self.bot.processed_games[clan_tag] and session_id not in self.queued_games:
                            if initial_scan_time < game_start_ms:
                                new_sessions.append(session)
                    except Exception as e:
                        print(f"Error: {e}")

                if new_sessions:
                    # Feed into the queue strictly oldest-first
                    for session in new_sessions:
                        self.live_queue.put_nowait((clan_tag, session, initial_scan_time))
                        self.queued_games.add(session.get("gameId"))

                    print(f"Queued {len(new_sessions)} new games for clan [{clan_tag}].")
                else:
                    print("0 found")

    # LIVE WORKER CODE
    async def live_worker(self):
        await self.bot.wait_until_ready()
        async with aiohttp.ClientSession() as http_session:
            while True:
                try:
                    # Block until a game is added to the line
                    clan_tag, session, initial_scan_time = await self.live_queue.get()
                    session_id = session.get("gameId")
                    is_win = session.get("hasWon", False)
                    game_url = f"https://api.openfront.io/public/game/{session_id}?turns=false"
                    
                    # STUBBORN RETRY LOOP: Forces the worker to process this exact game before moving on
                    while True:
                        try:
                            async with http_session.get(game_url, timeout=10) as game_resp:
                                if game_resp.status == 200:
                                    game_data = await game_resp.json()
                                    info = game_data.get("info", {})
                                    
                                    # CHECK IF DATA IS READY YET.
                                    if not game_data or not info or not info.get("players"):
                                        if not initial_scan_time:
                                            print(f"Data for {session_id} is still empty. Retrying in 2s...")
                                        await asyncio.sleep(2) 
                                        continue # Retry this same game
                                        
                                    all_players = info.get("players", [])
                                    config = info.get("config", {})

                                    self.match_details_cache[session_id] = {
                                        "players": all_players,
                                        "start": info.get("start"),
                                        "end": info.get("end"),
                                        "maxPlayers": config.get("maxPlayers", 0),
                                        "playerTeams": config.get("playerTeams", 0)
                                    }

                                    # UPDATE GLOBAL PLAYER STATS
                                    self.bot.player_data[clan_tag]["total_games"] += 1
                                    self.bot.processed_games[clan_tag].append(session_id)

                                    if self.bot.player_data[clan_tag]["winstreak"] is None:
                                        self.bot.player_data[clan_tag]["winstreak"] = 0
                                        self.bot.player_data[clan_tag]["highest_winstreak"] = self.bot.player_data[clan_tag]["winstreak"]
                                    else:
                                        if is_win:
                                            self.bot.player_data[clan_tag]["winstreak"] += 1
                                            if self.bot.player_data[clan_tag]["winstreak"] > self.bot.player_data[clan_tag]["highest_winstreak"]:
                                                self.bot.player_data[clan_tag]["highest_winstreak"] = self.bot.player_data[clan_tag]["winstreak"]
                                        else:
                                            self.bot.player_data[clan_tag]["winstreak"] = 0

                                    # ANNOUNCE TO DISCORD
                                    # Safely fetch the raw milliseconds timestamp of the game and the timestamp 1 hour ago
                                    game_start_ms = int(info.get("start", 0)) if info.get("start") else 0
                                    two_hours_ago_ms = int((datetime.now(timezone.utc) - timedelta(hours=2)).timestamp() * 1000)

                                    if game_start_ms >= two_hours_ago_ms: 
                                        for guild_id, data in list(self.bot.server_data.items()):
                                            for tracker in data.get("trackers", []):
                                                if tracker.get("clan_tag") == clan_tag and tracker.get("channel_id"):
                                                    channel = self.bot.get_channel(tracker["channel_id"])
                                                    if channel:
                                                        embed = await self.create_match_embed(
                                                            http_session, clan_tag, session, 
                                                            tracker.get("track_losses", False), 
                                                            self.match_details_cache
                                                        )
                                                        if embed:
                                                            await channel.send(embed=embed)

                                    already_counted_players = set()
                                    for p in all_players:
                                        if p.get("clanTag", "").upper() == clan_tag.upper():
                                            p_name = p.get("username", "Unknown")
                                            if p_name in already_counted_players: continue

                                            already_counted_players.add(p_name)
                                            
                                            if p_name not in self.bot.player_data[clan_tag]["players"]:
                                                self.bot.player_data[clan_tag]["players"][p_name] = {"games_played": 0, "wins": 0, "winstreak": 0, "highest_winstreak": 0}
                                                
                                            p_stats = self.bot.player_data[clan_tag]["players"][p_name]
                                            
                                            if "winstreak" not in p_stats:
                                                p_stats["winstreak"] = 0
                                            if "highest_winstreak" not in p_stats:
                                                p_stats["highest_winstreak"] = 0

                                            p_stats["games_played"] += 1
                                            if is_win:
                                                p_stats["wins"] += 1
                                                p_stats["winstreak"] += 1
                                                if p_stats["winstreak"] > p_stats["highest_winstreak"]:
                                                    p_stats["highest_winstreak"] = p_stats["winstreak"]
                                            else:
                                                p_stats["winstreak"] = 0

                                    # Success! Clean up, Save, and break the inner loop to move on to the next game
                                    self.queued_games.discard(session_id)

                                    async with self.bot.save_lock:
                                        self.bot.save_data()
                                    
                                    print(f"Successfully processed & announced {session_id} for clan [{clan_tag}]. Win: {is_win}. Current Winstreak: {self.bot.player_data[clan_tag]['winstreak']}. Games left in queue: {len(self.queued_games)}")
                                    
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
                    await asyncio.sleep(0.3) 
                    
                except Exception as e:
                    print(f"Live Queue Critical Error: {e}")
                    await asyncio.sleep(0.3)

    @check_clan_stats.before_loop
    async def before_check_clan_stats(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(BackgroundLoop(bot))