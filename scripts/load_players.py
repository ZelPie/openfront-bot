import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import asyncio
from datetime import datetime, timedelta, timezone

class LoadPlayers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="load_players", description="Load ALL historical player data for a specific clan.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., CAF)")
    async def load_players(self, interaction: discord.Interaction, clan_tag: str):
        tag_upper = clan_tag.upper()
        
        await interaction.response.send_message(
            f"Started a background task to fetch all historical data for **[{tag_upper}]**.\n"
            f"I will send a message in this channel when it's finished and merged! (Check your bot terminal for live progress)."
        )
        
        self.bot.loop.create_task(self.background_process_games(tag_upper, interaction.channel))

    async def background_process_games(self, tag_upper, channel):
        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions"

        # Temporary DB to hold findings before we merge
        clan_db = {"total_games": 0, "players": {}}

        # Ensure the clan exists in the dictionary
        if tag_upper not in self.bot.processed_games:
            self.bot.processed_games[tag_upper] = []

        # Calculate exactly 2 hours ago to filter out recent games
        two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)
        two_hours_ago_ms = int(two_hours_ago.timestamp() * 1000)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        await channel.send(f"Failed to load API for **[{tag_upper}]** (Status {resp.status}).")
                        return
                        
                    sessions = await resp.json()
                    if not sessions:
                        await channel.send(f"No sessions found for **[{tag_upper}]**.")
                        return

                    games_added = 0
                    games_skipped_recent = 0
                    games_skipped_processed = 0

                    print(f"[{tag_upper}] HISTORICAL LOAD INITIATED: Found {len(sessions)} total API games.")

                    # Loop through all games
                    for index, game in enumerate(sessions):
                        game_id = game.get("gameId")
                        is_win = game.get("hasWon", False)

                        # Skip if game is ALREADY in this clan's processed list
                        if game_id in self.bot.processed_games[tag_upper]:
                            games_skipped_processed += 1
                            continue
                            
                        # Double-check the live tracker's 2-hour window list just in case
                        if hasattr(self.bot, 'recent_games') and tag_upper in self.bot.recent_games:
                            if game_id in self.bot.recent_games[tag_upper]:
                                games_skipped_recent += 1
                                continue

                        game_url = f"https://api.openfront.io/public/game/{game_id}?turns=false"
                        async with session.get(game_url) as game_resp:
                            if game_resp.status == 200:
                                game_data = await game_resp.json()
                                info = game_data.get("info", {})
                                
                                # --- TIME FILTER ---
                                # Check if the game started within the last 2 hours.
                                # If it did, SKIP IT so the live main_loop.py tracker can handle it normally!
                                start_time_ms = info.get("start")
                                if start_time_ms and int(start_time_ms) >= two_hours_ago_ms:
                                    games_skipped_recent += 1
                                    continue

                                player_list = info.get("players", [])
                                seen_players_in_game = set()

                                for p in player_list:
                                    if p.get("clanTag", "").upper() == tag_upper:
                                        p_name = p.get("username", "Unknown")

                                        if p_name in seen_players_in_game:
                                            continue  
                                        seen_players_in_game.add(p_name)

                                        if p_name not in clan_db["players"]:
                                            clan_db["players"][p_name] = {"games_played": 0, "wins": 0, "winrate": 0.0}
                                        
                                        p_stats = clan_db["players"][p_name]
                                        p_stats["games_played"] += 1
                                        if is_win:
                                            p_stats["wins"] += 1

                                games_added += 1
                                clan_db["total_games"] += 1
                                
                                # Append to the clan's permanent file
                                self.bot.processed_games[tag_upper].append(game_id)

                        # --- TERMINAL PROGRESS OUTPUT ---
                        # Print an update to the terminal every 25 iterations
                        if (index + 1) % 25 == 0:
                            print(f"[{tag_upper}] Still processing... Checked {index + 1}/{len(sessions)} games. Added {games_added} new stats so far.")

                        # Small delay to prevent API ratelimiting
                        await asyncio.sleep(0.3)

                    # --- MERGE INTO LIVE TRACKING DATA (player_data.json) ---
                    print(f"[{tag_upper}] Processing complete! Merging {games_added} matches into database...")
                    
                    if tag_upper not in self.bot.player_data:
                        self.bot.player_data[tag_upper] = {"total_games": 0, "players": {}}
                    
                    main_db = self.bot.player_data[tag_upper]
                    main_db["total_games"] = main_db.get("total_games", 0) + clan_db["total_games"]
                    
                    new_players_count = 0
                    for p_name, stats in clan_db["players"].items():
                        if p_name not in main_db["players"]:
                            main_db["players"][p_name] = {"name": [p_name], "games_played": 0, "wins": 0}
                            new_players_count += 1
                        
                        main_p_stats = main_db["players"][p_name]
                        
                        # Data migration safeguard for names
                        if not isinstance(main_p_stats.get("name"), list):
                            main_p_stats["name"] = [main_p_stats.get("name", p_name)]
                        if p_name not in main_p_stats["name"]:
                            main_p_stats["name"].append(p_name)
                            
                        main_p_stats["games_played"] += stats["games_played"]
                        main_p_stats["wins"] += stats["wins"]
                        
                        # Recalculate Winrates
                        if main_p_stats["games_played"] > 0:
                            main_p_stats["winrate"] = round((main_p_stats["wins"] / main_p_stats["games_played"]) * 100, 2)

                    # Save player_data.json and processed_games.json
                    self.bot.save_data()
                    print(f"[{tag_upper}] Database save complete.")

                    embed = discord.Embed(
                        title=f"Historical Load Complete: [{tag_upper}]", 
                        description=f"Successfully processed and merged **{games_added}** historical matches into the live tracking database.\n\n"
                                    f"Found **{new_players_count}** new unique players.\n"
                                    f"Skipped **{games_skipped_recent}** games from the last 2 hours (handled by live tracker).\n"
                                    f"Skipped **{games_skipped_processed}** already processed games.",
                        color=discord.Color.green()
                    )
                    await channel.send(embed=embed)

        except Exception as e:
            await channel.send(f"❌ An error occurred while processing background data for **[{tag_upper}]**: {e}")
            print(f"Error in load_players for {tag_upper}: {e}")

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))