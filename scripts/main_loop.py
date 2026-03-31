import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone

class BackgroundLoop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.check_clan_stats.start()

    def script_unload(self):
        self.check_clan_stats.cancel()

    # The shared embed builder method
    async def create_match_embed(self, http_session, clan_tag, session, track_losses=True, match_cache=None, stats_cache=None):
        if match_cache is None: match_cache = {}
        if stats_cache is None: stats_cache = {}

        session_id = session.get("gameId", "Unknown")
        is_win = session.get("hasWon", False)
        gamemode = session.get("playerTeams", "Unknown Mode")
        num_teams = session.get("numTeams", "?")
        player_count = session.get("clanPlayerCount", 1)
        score = session.get("score", 0)
        total_players = session.get("totalPlayerCount", "?")

        all_players = []
        if session_id in match_cache:
            all_players = match_cache[session_id]
        else:
            game_url = f"https://api.openfront.io/public/game/{session_id}?turns=false"
            try:
                async with http_session.get(game_url, timeout=10) as game_response:
                    if game_response.status == 200:
                        game_data = await game_response.json()
                        all_players = game_data.get("info", {}).get("players", [])
                match_cache[session_id] = all_players
            except Exception as e:
                print(f"Failed to fetch player details for session {session_id}: {e}")

        clan_players = [
            f"``{p.get('username', 'Unknown')}``" for p in all_players 
            if p.get("clanTag", "").upper() == clan_tag.upper()
        ]
        player_names = ", ".join(clan_players) if clan_players else "Unknown Players"

        if clan_tag not in stats_cache:
            stats_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}"
            try:
                async with http_session.get(stats_url, timeout=5) as stat_resp:
                    if stat_resp.status == 200:
                        stats_cache[clan_tag] = await stat_resp.json()
                    else:
                        stats_cache[clan_tag] = {}
            except Exception:
                stats_cache[clan_tag] = {}
                
        c_stats = stats_cache.get(clan_tag, {}).get("clan", {})
        overall_wins = c_stats.get("wins", 0)
        overall_games = c_stats.get("games", 0)
        overall_losses = overall_games - overall_wins
        overall_wl = c_stats.get("weightedWLRatio", 0.0)
        
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
        embed.add_field(name="Rating Change", value=rating_text, inline=False)
        embed.add_field(name="Clan Players", value = f"``{player_count}`` / ``{total_players}``", inline=True)
        embed.add_field(name="Gamemode", value=f"{gamemode} ({num_teams} Teams)", inline=True)
        embed.add_field(name="Clan Players in Match", value=f"{player_names}", inline=False)
        embed.add_field(name="New Overall Clan Stats", value=f"Total: **{overall_wins}W** - **{overall_losses}L** (W/L: **{overall_wl:.2f}**)", inline=False)
        embed.set_footer(text=f"Match ID: {session_id}")

        return embed

    @tasks.loop(seconds=15) 
    async def check_clan_stats(self):
        print("Checking for clan updates...")

        unique_clans = set()
        for data in list(self.bot.server_data.values()):
            for tracker in data.get("trackers", []):
                if tracker.get("clan_tag"):
                    unique_clans.add(tracker["clan_tag"])
        
        # Calculate ISO 8601 Timestamp for exactly 2 hours ago
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        iso_timestamp = two_hours_ago.strftime('%Y-%m-%dT%H:%M:%S.000Z')
        
        async with aiohttp.ClientSession() as http_session:
            match_details_cache = {}
            clan_overall_stats_cache = {}
            data_changed = False
            
            for clan_tag in unique_clans:
                api_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}/sessions?start={iso_timestamp}"
                
                # Ensure all dictionaries exist for this clan
                if clan_tag not in self.bot.player_data:
                    self.bot.player_data[clan_tag] = {"total_games": 0, "players": {}}
                if clan_tag not in self.bot.processed_games:
                    self.bot.processed_games[clan_tag] = []

                try:
                    async with http_session.get(api_url, timeout=10) as response:
                        if response.status != 200:
                            continue
                        api_data = await response.json()
                        sessions = list(api_data) if isinstance(api_data, list) else [api_data]
                except Exception as e:
                    print(f"Error fetching data for {clan_tag}: {e}")
                    continue

                if not sessions or not isinstance(sessions[0], dict) or not sessions[0].get("gameId"):
                    continue

                # Extract a pure list of IDs currently in the 2-hour window
                fetched_ids = [s.get("gameId") for s in sessions if s.get("gameId")]

                # FIRST TIME CLAN SETUP: Prevent 2-hour backlog spam on fresh trackers
                if clan_tag not in self.bot.recent_games:
                    print(f"Initializing rolling 2-hour window for [{clan_tag}]. Skipping backlog.")
                    self.bot.recent_games[clan_tag] = fetched_ids
                    self.bot.save_data()
                    continue

                new_sessions = []
                for session in sessions:
                    session_id = session.get("gameId")
                    if session_id and session_id not in self.bot.recent_games[clan_tag]:
                        new_sessions.append(session)

                if not new_sessions:
                    # Maintain the rolling window by overwriting it with current IDs
                    self.bot.recent_games[clan_tag] = fetched_ids
                    continue

                # Reverse to process games chronologically (Oldest -> Newest)
                new_sessions.reverse()
                print(f"Found {len(new_sessions)} new games for clan [{clan_tag}] in the last 2 hours.")

                # Process the brand new games
                for session in new_sessions:
                    session_id = session.get("gameId")
                    is_win = session.get("hasWon", False)
                    game_url = f"https://api.openfront.io/public/game/{session_id}?turns=false"
                    
                    try:
                        async with http_session.get(game_url, timeout=10) as game_resp:
                            if game_resp.status == 200:
                                game_data = await game_resp.json()
                                all_players = game_data.get("info", {}).get("players", [])
                                match_details_cache[session_id] = all_players 
                                
                                # -------------------------------------------------------------
                                # 1. ANNOUNCE TO DISCORD TRACKERS
                                # -------------------------------------------------------------
                                for guild_id, data in list(self.bot.server_data.items()):
                                    for tracker in data.get("trackers", []):
                                        if tracker.get("clan_tag") == clan_tag and tracker.get("channel_id"):
                                            channel = self.bot.get_channel(tracker["channel_id"])
                                            if channel:
                                                embed = await self.create_match_embed(
                                                    http_session, clan_tag, session, 
                                                    tracker.get("track_losses", False), 
                                                    match_details_cache, clan_overall_stats_cache
                                                )
                                                if embed:
                                                    await channel.send(embed=embed)

                                # -------------------------------------------------------------
                                # 2. UPDATE GLOBAL PLAYER STATS (If not already processed)
                                # -------------------------------------------------------------
                                if session_id not in self.bot.processed_games[clan_tag]:
                                    self.bot.player_data[clan_tag]["total_games"] += 1
                                    self.bot.processed_games[clan_tag].append(session_id)
                                    
                                    already_counted_players = set()
                                    for p in all_players:
                                        if p.get("clanTag", "").upper() == clan_tag.upper():
                                            p_name = p.get("username", "Unknown")
                                            if p_name in already_counted_players:
                                                continue

                                            already_counted_players.add(p_name)
                                            
                                            if p_name not in self.bot.player_data[clan_tag]["players"]:
                                                self.bot.player_data[clan_tag]["players"][p_name] = {"name": [p_name], "games_played": 0, "wins": 0}
                                                
                                            p_stats = self.bot.player_data[clan_tag]["players"][p_name]
                                            
                                            if not isinstance(p_stats["name"], list):
                                                p_stats["name"] = [p_stats["name"]]
                                                
                                            if p_name not in p_stats["name"]:
                                                p_stats["name"].append(p_name)
                                                
                                            p_stats["games_played"] += 1
                                            if is_win:
                                                p_stats["wins"] += 1

                    except Exception as e:
                        print(f"Failed to process data for session {session_id}: {e}")

                # After all new sessions are processed, update the rolling window 
                # so we don't announce these games again on the next loop.
                self.bot.recent_games[clan_tag] = fetched_ids
                data_changed = True

            # Save once at the end of the loop if anything updated
            if data_changed:
                self.bot.save_data()

    @check_clan_stats.before_loop
    async def before_check_clan_stats(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(BackgroundLoop(bot))