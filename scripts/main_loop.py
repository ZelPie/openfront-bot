import discord
from discord.ext import tasks, commands
from discord import app_commands
import aiohttp
import asyncio

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
                        
                        if not sessions:
                            await interaction.followup.send(f"Could not find any recent games for [{clan_tag}].")
                            return
                        
                        latest_session = sessions[-1] 
                        
                        # Use the helper method defined above!
                        embed = await self.create_match_embed(http_session, clan_tag, latest_session, track_losses=True)
                        if embed:
                            await interaction.followup.send(content=f"**TEST MODE:** Latest match for [{clan_tag}]", embed=embed)
                        else:
                            await interaction.followup.send("Failed to build embed.")
                    else:
                        await interaction.followup.send(f"API Error: {response.status}")
        except Exception as e:
            await interaction.followup.send(f"An error occurred during test: {e}")

@tasks.loop(seconds=30) 
    async def check_clan_stats(self):
        print("Checking for clan updates...")

        unique_clans = set()
        for data in list(self.bot.server_data.values()):
            for tracker in data.get("trackers", []):
                if tracker.get("clan_tag"):
                    unique_clans.add(tracker["clan_tag"])
        
        async with aiohttp.ClientSession() as http_session:
            clan_api_data = {}
            match_details_cache = {}
            
            # Fetch recent sessions for each unique clan
            for clan_tag in unique_clans:
                api_url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}/sessions"
                try:
                    async with http_session.get(api_url, timeout=10) as response:
                        if response.status == 200:
                            api_data = await response.json()
                            sessions = list(api_data) if isinstance(api_data, list) else [api_data]
                            if sessions:
                                clan_api_data[clan_tag] = sessions
                except asyncio.TimeoutError:
                    pass
                except Exception as e:
                    print(f"Error fetching data for {clan_tag}: {e}")

            # -----------------------------------------------------------------
            # 1. GLOBAL PLAYER STATS (Uses processed_games to avoid double counting)
            # -----------------------------------------------------------------
            stats_updated = False
            for clan_tag, sessions in clan_api_data.items():
                if clan_tag not in self.bot.player_data:
                    self.bot.player_data[clan_tag] = {"wins": 0, "losses": 0, "winrate": 0.0, "total_games": 0, "players": {}}

                for session in sessions:
                    session_id = session.get("gameId")
                    
                    # If we have already counted this game globally, skip it
                    if session_id in self.bot.processed_games:
                        continue
                        
                    is_win = session.get("hasWon", False)
                    game_url = f"https://api.openfront.io/public/game/{session_id}?turns=false"
                    
                    try:
                        async with http_session.get(game_url, timeout=10) as game_resp:
                            if game_resp.status == 200:
                                game_data = await game_resp.json()
                                all_players = game_data.get("info", {}).get("players", [])
                                
                                match_details_cache[session_id] = all_players 
                                self.bot.player_data[clan_tag]["total_games"] += 1
                                
                                # Add the game to the global list so it's never checked again
                                self.bot.processed_games.append(session_id)
                                stats_updated = True
                                
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
                                        
                                        # Update the name aliases if somehow changed
                                        if p_name not in p_stats["name"]:
                                            p_stats["name"].append(p_name)
                                            
                                        p_stats["games_played"] += 1
                                        if is_win:
                                            p_stats["wins"] += 1
                                            
                    except Exception as e:
                        print(f"Failed to process global player stats for {session_id}: {e}")

            if stats_updated:
                self.bot.save_data()

            # -----------------------------------------------------------------
            # 2. PER-SERVER EMBED NOTIFICATIONS (Uses channel-specific last_session_id)
            # -----------------------------------------------------------------
            clan_overall_stats_cache = {}

            for guild_id, data in list(self.bot.server_data.items()):
                trackers = data.get("trackers", [])
                
                for tracker in list(trackers):
                    channel_id = tracker.get("channel_id")
                    clan_tag = tracker.get("clan_tag")
                    track_losses = tracker.get("track_losses", False)
                    
                    if not channel_id or not clan_tag:
                        continue
                        
                    channel = self.bot.get_channel(channel_id)
                    if not channel:
                        continue

                    sessions = clan_api_data.get(clan_tag)
                    if not sessions:
                        continue 
                    
                    last_session_id = tracker.get("last_session_id")
                    
                    if not last_session_id:
                        tracker["last_session_id"] = sessions[-1].get("gameId")
                        self.bot.save_data()
                        continue
                    
                    new_sessions = []
                    found_anchor = False
                    
                    for session in sessions:
                        if not found_anchor:
                            if session.get("gameId") == last_session_id:
                                found_anchor = True
                            continue 
                        new_sessions.append(session)

                    if not found_anchor and last_session_id:
                        tracker["last_session_id"] = sessions[-1].get("gameId")
                        self.bot.save_data()
                        continue

                    if not new_sessions:
                        continue
                        
                    for session in new_sessions: 
                        embed = await self.create_match_embed(
                            http_session, 
                            clan_tag, 
                            session, 
                            track_losses, 
                            match_details_cache, 
                            clan_overall_stats_cache
                        )
                        
                        if embed:
                            await channel.send(embed=embed)
                        
                    tracker["last_session_id"] = sessions[-1].get("gameId")
                    self.bot.save_data()

    @check_clan_stats.before_loop
    async def before_check_clan_stats(self):
        # Wait for the bot to be fully ready before starting the loop
        await self.bot.wait_until_ready()

async def setup(bot):
    await bot.add_cog(BackgroundLoop(bot))