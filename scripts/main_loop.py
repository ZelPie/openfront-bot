import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timezone

class BackgroundLoop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # In-memory set to prevent Discord embed spam when the bot restarts or tracks a new clan
        self.first_run = set() 
        self.check_clan_stats.start()

    def script_unload(self):
        self.check_clan_stats.cancel()

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
        
        embed.add_field(name="Started", value=start_display, inline=True)
        embed.add_field(name="Ended", value=end_display, inline=True)
        embed.add_field(name="Duration", value=duration_display, inline=True)
        
        embed.add_field(name="Rating Change", value=rating_text, inline=False)
        embed.add_field(name="Clan Players", value = f"``{player_count}`` / ``{total_players}``", inline=True)
        embed.add_field(name="Gamemode", value=f"{gamemode} ({num_teams} Teams)", inline=True)
        embed.add_field(name="Clan Players in Match", value=f"{player_names}", inline=False)
        embed.add_field(name="New Overall Clan Stats", value=f"Total: **{overall_wins}W** - **{overall_losses}L** (W/L: **{overall_wl:.2f}**)", inline=False)
        
        if raw_end:
            embed.timestamp = datetime.fromtimestamp(int(raw_end) / 1000, tz=timezone.utc)
            embed.set_footer(text=f"Match ID: {session_id} • Ended")
        else:
            embed.set_footer(text=f"Match ID: {session_id}")

        return embed

    @app_commands.command(name="test", description="Test the embed output using the latest game from clan UN.")
    async def test_embed(self, interaction: discord.Interaction):
        await interaction.response.defer()
        clan_tag = "UN" 
        api_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}/sessions"
        
        try:
            async with aiohttp.ClientSession() as http_session:
                async with http_session.get(api_url, timeout=10) as response:
                    if response.status == 200:
                        api_data = await response.json()
                        sessions = list(api_data) if isinstance(api_data, list) else [api_data]
                        
                        if not sessions or not sessions[0].get("gameId"):
                            await interaction.followup.send(f"Could not find any recent valid games for [{clan_tag}].")
                            return
                        
                        latest_session = sessions[0] 
                        
                        embed = await self.create_match_embed(http_session, clan_tag, latest_session, track_losses=True)
                        if embed:
                            await interaction.followup.send(content=f"**TEST MODE:** Latest match for [{clan_tag}]", embed=embed)
                        else:
                            await interaction.followup.send("Failed to build embed.")
                    else:
                        await interaction.followup.send(f"API Error: {response.status}")
        except Exception as e:
            await interaction.followup.send(f"An error occurred during test: {e}")

    @tasks.loop(seconds=15) 
    async def check_clan_stats(self):
        unique_clans = set()
        for data in list(self.bot.server_data.values()):
            for tracker in data.get("trackers", []):
                if tracker.get("clan_tag"):
                    unique_clans.add(tracker["clan_tag"])
        
        async with aiohttp.ClientSession() as http_session:
            match_details_cache = {}
            clan_overall_stats_cache = {}
            data_changed = False
            
            for clan_tag in unique_clans:
                # We just ask for the most recent default page of games
                api_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}/sessions"
                
                if clan_tag not in self.bot.player_data:
                    self.bot.player_data[clan_tag] = {"total_games": 0, "players": {}}
                if clan_tag not in self.bot.processed_games:
                    self.bot.processed_games[clan_tag] = []

                # Is this the first time checking this clan since bot boot?
                is_first_scan = clan_tag not in self.first_run

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

                # Filter out games we already have in the permanent vault
                new_sessions = [s for s in sessions if s.get("gameId") and s.get("gameId") not in self.bot.processed_games[clan_tag]]

                if not new_sessions:
                    # If there's nothing new, mark as successfully scanned and skip
                    self.first_run.add(clan_tag)
                    continue

                # Process chronologically (Oldest to Newest)
                new_sessions.reverse()
                
                if not is_first_scan:
                    print(f"Found {len(new_sessions)} new games for clan [{clan_tag}].")

                for session in new_sessions:
                    session_id = session.get("gameId")
                    is_win = session.get("hasWon", False)
                    game_url = f"https://api.openfront.io/public/game/{session_id}?turns=false"
                    
                    try:
                        async with http_session.get(game_url, timeout=10) as game_resp:
                            if game_resp.status == 200:
                                game_data = await game_resp.json()
                                info = game_data.get("info", {})
                                all_players = info.get("players", [])
                                
                                match_details_cache[session_id] = {
                                    "players": all_players,
                                    "start": info.get("start"),
                                    "end": info.get("end")
                                }
                                
                                # 1. UPDATE GLOBAL PLAYER STATS
                                self.bot.player_data[clan_tag]["total_games"] = self.bot.player_data[clan_tag].get("total_games", 0) + 1
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
                                            
                                        if p_stats["games_played"] > 0:
                                            p_stats["winrate"] = round((p_stats["wins"] / p_stats["games_played"]) * 100, 2)

                                # 2. ANNOUNCE TO DISCORD (Only if it's not the initial silent bootup scan)
                                if not is_first_scan:
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

                    except Exception as e:
                        print(f"Failed to process data for session {session_id}: {e}")

                # Mark as scanned so future games trigger Discord embeds
                self.first_run.add(clan_tag)
                data_changed = True

            if data_changed:
                self.bot.save_data()

    @check_clan_stats.before_loop
    async def before_check_clan_stats(self):
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(BackgroundLoop(bot))