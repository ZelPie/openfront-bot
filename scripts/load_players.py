import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import asyncio

class LoadPlayers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="load_players", description="Load ALL player data for a specific clan in the background.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., CAF)")
    async def load_players(self, interaction: discord.Interaction, clan_tag: str):
        tag_upper = clan_tag.upper()
        
        await interaction.response.send_message(
            f"Started a background task to fetch all historical data for **[{tag_upper}]**.\n"
            f"I will send a message in this channel when it's finished and merged!"
        )
        
        self.bot.loop.create_task(self.background_process_games(tag_upper, interaction.channel))

    async def background_process_games(self, tag_upper, channel):
        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions"

        # Temporary DB to hold findings before we merge
        clan_db = {"total_games": 0, "players": {}}

        if tag_upper in self.bot.processed_games:
            self.bot.processed_games[tag_upper] = []  # Reset the list for this clan to avoid duplicates in future runs

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
                    new_players_added = 0

                    for game in sessions:
                        game_id = game.get("gameId")
                        is_win = game.get("hasWon", False)

                        # Skip if the main loop has already actively tracked this game
                        if game_id in self.bot.processed_games:
                            continue

                        game_url = f"https://api.openfront.io/public/game/{game_id}"
                        async with session.get(game_url) as game_resp:
                            if game_resp.status == 200:
                                game_data = await game_resp.json()
                                player_list = game_data.get("info", {}).get("players", [])

                                seen_players_in_game = set()

                                for p in player_list:
                                    if p.get("clanTag", "").upper() == tag_upper:
                                        p_name = p.get("username", "Unknown")

                                        if p_name in seen_players_in_game:
                                            continue  
                                        seen_players_in_game.add(p_name)

                                        if p_name not in clan_db["players"]:
                                            clan_db["players"][p_name] = {"games_played": 0, "wins": 0, "winrate": 0.0}
                                            new_players_added += 1
                                        
                                        p_stats = clan_db["players"][p_name]
                                        p_stats["games_played"] += 1
                                        if is_win:
                                            p_stats["wins"] += 1

                                games_added += 1
                                clan_db["total_games"] += 1
                                
                                # Add to the recent games tracking set to prevent double counting
                                self.bot.processed_games.append(game_id)

                        # Small delay to prevent API ratelimiting
                        await asyncio.sleep(0.3)

                    # --- MERGE INTO LIVE TRACKING DATA (player_data.json) ---
                    if tag_upper not in self.bot.player_data:
                        self.bot.player_data[tag_upper] = {"wins": 0, "losses": 0, "winrate": 0.0, "total_games": 0, "players": {}}
                    
                    main_db = self.bot.player_data[tag_upper]
                    
                    for p_name, stats in clan_db["players"].items():
                        if p_name not in main_db["players"]:
                            main_db["players"][p_name] = {"name": [p_name], "games_played": 0, "wins": 0}
                        
                        main_p_stats = main_db["players"][p_name]
                        main_p_stats["games_played"] += stats["games_played"]
                        main_p_stats["wins"] += stats["wins"]
                        
                        if main_p_stats["games_played"] > 0:
                            main_p_stats["winrate"] = round((main_p_stats["wins"] / main_p_stats["games_played"]) * 100, 2)

                    self.bot.save_data()

                    embed = discord.Embed(
                        title=f"Background Task Complete: [{tag_upper}]", 
                        description=f"Successfully processed and merged **{games_added}** historical matches into the live tracking data.\nFound **{new_players_added}** unique players.",
                        color=discord.Color.green()
                    )
                    await channel.send(embed=embed)

        except Exception as e:
            await channel.send(f"❌ An error occurred while processing background data for **[{tag_upper}]**: {e}")

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))