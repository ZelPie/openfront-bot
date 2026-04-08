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
        
        if session_id in match_cache:
            cache_data = match_cache[session_id]
            all_players = cache_data.get("players", [])
            if not raw_start: raw_start = cache_data.get("start")
            if not raw_end: raw_end = cache_data.get("end")
        else:
            game_url = f"https://api.openfront.io/public/game/{session_id}?turns=false"
            try:
                async with http_session.get(game_url, timeout=10) as game_response:
                    if game_response.status == 200:
                        game_data = await game_response.json()
                        info = game_data.get("info", {})
                        all_players = info.get("players", [])
                        
                        if not raw_start: raw_start = info.get("start")
                        if not raw_end: raw_end = info.get("end")
                        
                        match_cache[session_id] = {"players": all_players, "start": raw_start, "end": raw_end}
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
            if p.get("clanTag", "").upper() == clan_tag.upper()
        ]
        player_names = ", ".join(clan_players) if clan_players else "Unknown Players"

        stats_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}"
        try:
            async with http_session.get(stats_url, timeout=5) as stat_resp:
                if stat_resp.status == 200:
                    clan_data = await stat_resp.json()
                else:
                    clan_data = {}
        except Exception:
            clan_data = {}
                
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

        embed = discord.Embed(title=title, color=color)
        embed.add_field(name="Started", value=start_display, inline=True)
        embed.add_field(name="Ended", value=end_display, inline=True)
        embed.add_field(name="Duration", value=duration_display, inline=True)
        embed.add_field(name="Rating Change", value=rating_text, inline=False)
        embed.add_field(name="Winstreak", value=f"Current: **{winstreak}** | Highest: **{highest_winstreak}**", inline=False)
        embed.add_field(name="Clan Players", value = f"``{player_count}`` / ``{total_players}``", inline=True)
        embed.add_field(name="Gamemode", value=f"{gamemode} ({num_teams} Teams)", inline=True)
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
                            await interaction.followup.send(content=f"**TEST MODE:** Latest match for [{clan_tag}]", embed=embed)
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
        
        twelve_hours_ago = datetime.now(timezone.utc) - timedelta(hours=12)
        iso_timestamp = twelve_hours_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

        print(f"Checking for new games for {len(unique_clans) if unique_clans else 'no'} clans. . .")
        
        async with aiohttp.ClientSession() as http_session:
            for clan_tag in unique_clans:
                # Setup clan data (Keep your existing dict setups here)
                if clan_tag not in self.bot.player_data:
                    self.bot.player_data[clan_tag] = {"total_games": 0, "winstreak": 0, "highest_winstreak": 0, "players": {}}
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

                is_initial_scan = len(self.bot.processed_games[clan_tag]) == 0

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

                sessions.sort(key=lambda x: x.get("gameStart", ""))

                if not isinstance(sessions[0], dict) or not sessions[0].get("gameId"):
                    continue

                # Add unseen games to the queue (Keep your existing queue logic here)
                new_sessions = []
                for session in sessions:
                    session_id = session.get("gameId")
                    if session_id and session_id not in self.bot.processed_games[clan_tag] and session_id not in self.queued_games:
                        new_sessions.append(session)

                if new_sessions:
                    new_sessions.reverse()
                    for session in new_sessions:
                        self.live_queue.put_nowait((clan_tag, session, is_initial_scan))
                        self.queued_games.add(session.get("gameId"))
                    
                    if not is_initial_scan:
                        print(f"Queued {len(new_sessions)} new games for clan [{clan_tag}].")

    # LIVE WORKER CODE
    # Continuously grabs games from the queue. If they are empty/loading, puts them back in line.
    async def live_worker(self):
        await self.bot.wait_until_ready()
        async with aiohttp.ClientSession() as http_session:
            while True:
                try:
                    # Block until a game is added to the line
                    clan_tag, session, is_initial_scan = await self.live_queue.get()
                    session_id = session.get("gameId")
                    is_win = session.get("hasWon", False)
                    game_url = f"https://api.openfront.io/public/game/{session_id}?turns=false"
                    
                    try:
                        async with http_session.get(game_url, timeout=10) as game_resp:
                            if game_resp.status == 200:
                                game_data = await game_resp.json()
                                info = game_data.get("info", {})
                                
                                # CHECK IF DATA IS READY YET. If not, re-queue!
                                if not game_data or not info or not info.get("players"):
                                    if not is_initial_scan:
                                        print(f"Data for {session_id} is still empty. Re-queueing...")
                                    self.live_queue.put_nowait((clan_tag, session, is_initial_scan))
                                    await asyncio.sleep(0.3) 
                                    self.live_queue.task_done()
                                    continue # Skip the rest of the loop
                                    
                                all_players = info.get("players", [])
                                self.match_details_cache[session_id] = {
                                    "players": all_players,
                                    "start": info.get("start"),
                                    "end": info.get("end")
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

                                # ANNOUNCE TO DISCORD (Skipped if this is the first scan)
                                if not is_initial_scan and (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp() < int(session.get("gameStart", 0)) / 1000: # Only announce games that started within the last hour during the initial scan to avoid spam
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

                                # Success! Clean up and Save.
                                self.queued_games.discard(session_id)

                                async with self.bot.save_lock:
                                    self.bot.save_data()
                                
                                if not is_initial_scan:
                                    print(f"Successfully processed & announced {session_id} for clan [{clan_tag}]. Win: {is_win}. Current Winstreak: {self.bot.player_data[clan_tag]['winstreak']}. Games left in queue: {len(self.queued_games)}")

                            elif game_resp.status == 429:
                                print(f"429 Rate Limit. Re-queueing {session_id}...")
                                self.live_queue.put_nowait((clan_tag, session, is_initial_scan))
                                await asyncio.sleep(0.3)
                            else:
                                print(f"Error {game_resp.status}. Re-queueing {session_id}...")
                                self.live_queue.put_nowait((clan_tag, session, is_initial_scan))

                    except Exception as e:
                        print(f"Network Hiccup on {session_id}. Re-queueing... ({e})")
                        self.live_queue.put_nowait((clan_tag, session, is_initial_scan))
                        
                    self.live_queue.task_done()
                    await asyncio.sleep(0.3) # Pace the live tracker so it doesn't fight the backfill
                    
                except Exception as e:
                    print(f"Live Queue Critical Error: {e}")
                    await asyncio.sleep(0.3)

    @check_clan_stats.before_loop
    async def before_check_clan_stats(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(BackgroundLoop(bot))