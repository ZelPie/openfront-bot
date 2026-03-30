import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import asyncio

class LoadPlayers(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="load_players", description="Load player data for a specific clan.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., CAF)")
    async def load_players(self, interaction: discord.Interaction, clan_tag: str):
        await interaction.response.defer()
        tag_upper = clan_tag.upper()
        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions"

        if tag_upper not in self.bot.loaded_player_data:
            self.bot.loaded_player_data[tag_upper] = {
                    "total_games": 0,
                    "anchor_game_id": None,
                    "players": {}
                }
        
        clan_db = self.bot.loaded_player_data[tag_upper]
        anchor_game_id = clan_db.get("anchor_game_id")

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        
                        sessions = await resp.json()
                        if not sessions:
                            await interaction.followup.send(f"No sessions found for **[{tag_upper}]**.")
                            return
                        
                        new_sessions = []

                        if not anchor_game_id:
                            new_sessions = sessions
                        else:
                            found_anchor = False
                            for game_session in sessions:
                                if not found_anchor:
                                    if game_session.get("gameId") == anchor_game_id:
                                        found_anchor = True
                                    continue
                                new_sessions.append(game_session)
                            
                            if not found_anchor:
                                new_sessions = sessions
                        
                        if not new_sessions:
                            await interaction.followup.send(f"No new sessions to load for **[{tag_upper}]** since last anchor game.")
                            return
                        
                        games_added = 0
                        new_players_added = 0

                        for game in new_sessions:
                            game_id = game.get("gameId")
                            is_win = game.get("hasWon", False)

                            game_url = f"https://api.openfront.io/public/game/{game_id}"
                            async with session.get(game_url) as game_resp:
                                if game_resp.status == 200:
                                    print(f"Processing game {game_id} for clan [{tag_upper}] - Win: {is_win}")
                                    game_data = await game_resp.json()
                                    player_list = game_data.get("info", {}).get("players", [])

                                    for p in player_list:
                                        if p.get("clanTag", "").upper() == tag_upper:
                                            p_name = p.get("username")

                                            if p_name not in clan_db["players"]:
                                                clan_db["players"][p_name] = {
                                                    "games_played": 0,
                                                    "wins": 0,
                                                    "winrate": 0.0
                                                }
                                                new_players_added += 1
                                            
                                            p_stats = clan_db["players"][p_name]
                                            p_stats["games_played"] += 1
                                            if is_win:
                                                p_stats["wins"] += 1

                                    games_added += 1
                                    clan_db["total_games"] += 1

                            await asyncio.sleep(0.2)

                        await asyncio.sleep(1)

                        # Update winrates
                        for p_name, stats in clan_db["players"].items():
                            if stats["games_played"] > 0:
                                g = stats["games_played"]
                                w = stats["wins"]
                                stats["winrate"] = round(w / g, 2)
                        
                        clan_db["anchor_game_id"] = new_sessions[-1].get("gameId")
                        
                        print(f"Attempting to save {games_added} matches to JSON...")
                        self.bot.save_data()
                        print("Save successful.")

                        embed = discord.Embed(
                            title=f"Database Updated For [{tag_upper}]", 
                            description=f"Successfully processed **{games_added}** new matches.\nFound **{new_players_added}** new players.",
                            color=discord.Color.blue()
                        )
                        embed.set_footer(text=f"Total tracked games: {clan_db['total_games']} | New Anchor: {clan_db['anchor_game_id']}")
                        await interaction.followup.send(embed=embed)
                    else:
                        await interaction.followup.send(f"Failed to load player data for **[{tag_upper}]**. API returned status code {resp.status}.")
                    
        except Exception as e:
            await interaction.followup.send(f"An error occurred while loading player data for **[{tag_upper}]**: {e}")
        except aiohttp.ClientError as e:
            await interaction.followup.send(f"Network error while loading player data for **[{tag_upper}]**: {e}")

async def setup(bot):
    await bot.add_cog(LoadPlayers(bot))