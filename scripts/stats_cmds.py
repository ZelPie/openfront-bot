import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import re
from .pages import LbDisplay
import asyncio
from datetime import datetime, timedelta, timezone

class StatsCmds(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="clan-stats", description="Get overall statistics for a specific clan.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., CAF)")
    async def clan_info(self, interaction: discord.Interaction, clan_tag: str):
        tag_upper = clan_tag.upper()
        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}"

        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper) 

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return
        
        await interaction.response.defer() 
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        dat = await response.json()
                        data = dat.get("clan", {})
                        wins = data.get("wins", 0)
                        games = data.get("games", 0)
                        losses = games - wins
                        wl_ratio = data.get("weightedWLRatio", 0)
                        weighted_wins = data.get("weightedWins", 0)
                        
                        clan_stats = await self.bot.clan_manager.get_clan_stats(tag_upper)
                        winstreak = clan_stats.get("winstreak", 0)
                        highest_winstreak = clan_stats.get("highest_winstreak", 0)
                        
                        embed = discord.Embed(title=f"Clan [{tag_upper}] Statistics", color=discord.Color.blurple())
                        embed.add_field(name="Total Matches", value=f"**{games}**", inline=True)
                        embed.add_field(name="Wins / Losses", value=f"**{wins}** / **{losses}**", inline=True)
                        embed.add_field(name="Win/Loss Ratio", value=f"**{wl_ratio:.2f}**", inline=True)
                        embed.add_field(name="Weighted Wins", value=f"**{weighted_wins}**", inline=True)
                        embed.add_field(name="Winstreak", value=f"Current: **{winstreak}** | Highest: **{highest_winstreak}**", inline=True)

                        await interaction.followup.send(embed=embed)
                    else:
                        await interaction.followup.send(f"Could not find stats for **[{tag_upper}]**. (API returned status {response.status})")
        except Exception as e:
            await interaction.followup.send(f"An error occurred while fetching clan info: {e}")

    @app_commands.command(name="clan-player-stats", description="Get tracked internal statistics for a specific player.")
    @app_commands.describe(clan_tag="The clan they play for (e.g., CAF)", username="The player's username")
    async def player_info(self, interaction: discord.Interaction, clan_tag: str, username: str):
        tag_upper = clan_tag.upper()
        search_name = username.lower()

        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper) 

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return
        
        search_name = re.sub(r'[^A-Za-z0-9_ ]', '', search_name) 

        if len(search_name) == 0 or len(search_name) > 25:
            await interaction.response.send_message("Please provide a valid username (1-25 alphanumeric characters).", ephemeral=True)
            return
        
        clan_stats = await self.bot.clan_manager.get_clan_stats(tag_upper)
        if not clan_stats or "players" not in clan_stats or not clan_stats["players"]:
            await interaction.response.send_message(f"We don't have any tracked data for clan **[{tag_upper}]** yet.", ephemeral=True)
            return
        
        await self.bot.clan_manager.finalize_batch_update(clan_tag)
            
        found_player_id = None
        players = clan_stats.get("players", {})
        player_list = []

        for player in players.keys():
            if search_name == player.strip('[' + tag_upper + ']').strip().lower():
                player_list.append(player)
                found_player_id = player
        
        if not found_player_id:
            await interaction.response.send_message(f"Could not find any tracked games for player **{username}** in clan **[{tag_upper}]**.")
            return
        
        multiple = False
        current_p_num = 0

        if player_list and len(player_list) > 1:
            embed = discord.Embed(title=f"Multiple players found matching '{username}' in [{tag_upper}]", description="Here are all the players we found that match that name:", color=discord.Color.blue())
            multiple = True
        else:
            embed = discord.Embed(title=f"Player Stats: {found_player_id}", color=discord.Color.blue())

        for p in player_list:
            current_p_num += 1
            stats = players[p]
            games_played = stats.get("games_played", 0)
            wins = stats.get("wins", 0)
            losses = games_played - wins
            total_clan_games = clan_stats.get("total_games", 0)
            winstreak = stats.get("winstreak", 0)
            highest_winstreak = stats.get("highest_winstreak", 0)
            
            winrate = (wins / games_played) * 100 if games_played > 0 else 0.0
            participation = (games_played / total_clan_games) * 100 if total_clan_games > 0 else 0.0
            
            embed.add_field(name="Player Name", value=f"**{p}**", inline=False)
            embed.add_field(name="Personal Win/Loss", value=f"**{wins}W** - **{losses}L**", inline=True)
            embed.add_field(name="Personal Win Rate", value=f"**{winrate:.1f}%**", inline=True)
            embed.add_field(name="Personal Winstreak", value=f"Current: **{winstreak}** | Highest: **{highest_winstreak}**", inline=False)
            embed.add_field(name="Clan Participation", value=f"Played in ``{games_played}`` / ``{total_clan_games}`` tracked matches (``{participation:.1f}%`` of clan activity)", inline=False)
            
            if multiple and current_p_num < len(player_list):
                embed.add_field(name="", value="----------------------------------------------------------------", inline=False)
            
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="leaderboard", description="Displays the top OpenFront clans.")
    @app_commands.describe(sort_by="Choose how to rank the clans", num="How many clans per page (Default: 5)", lower_num="What place to start the list from (Default: 1)", reverse_sort="Whether to reverse the sort order (Default: False)")
    @app_commands.choices(sort_by=[
        app_commands.Choice(name="Highest Total Wins", value="wins"),
        app_commands.Choice(name="Highest Win/Loss Ratio", value="winrate"),
        app_commands.Choice(name='Weighted Wins', value="weighted_wins"),
    ])
    async def show_leaderboard(self, interaction: discord.Interaction, sort_by: app_commands.Choice[str] = None, num: int = 5, lower_num: int = 1, reverse_sort: bool = False):
        await interaction.response.defer()

        if num < 1: num = 5
        if lower_num < 1: lower_num = 1
        
        try:
            url = "https://api.openfront.io/public/clans/leaderboard"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        clans_list = data.get("clans", [])
                        
                        sort_choice = sort_by.value if sort_by else "default"
                        embed_title = f"🏆 Top OpenFront Clans" 
                        
                        if sort_choice == "wins":
                            clans_list.sort(key=lambda c: c.get("wins", 0), reverse=not reverse_sort)
                            embed_title = f"🏆 Top Clans by Total Wins"
                        elif sort_choice == "winrate":
                            clans_list.sort(key=lambda c: c.get("weightedWLRatio", 0), reverse=not reverse_sort)
                            embed_title = f"🏆 Top Clans by W/L Ratio"
                        elif sort_choice == "weighted_wins" or sort_choice == "default":
                            clans_list.sort(key=lambda c: c.get("weightedWins", 0), reverse=not reverse_sort)
                            embed_title = f"🏆 Top Clans by Weighted Wins"

                        top_clans = clans_list[lower_num - 1:]

                        def format_clan(rank, clan):
                            tag = clan.get("clanTag", "UNK")
                            wins = clan.get("wins", 0)
                            weighted_wins = clan.get("weightedWins", 0)
                            games = clan.get("games", 0)
                            wl_ratio = clan.get("weightedWLRatio", 0)
                            
                            wins_str = f"``{wins}``" if sort_choice in ["wins", "default"] else f"{wins}"
                            wl_str = f"``{wl_ratio:.2f}``" if sort_choice == "winrate" else f"{wl_ratio:.2f}"
                            weighted_wins_str = f"``{weighted_wins}``" if sort_choice == "weighted_wins" else f"{weighted_wins}"

                            stat_string = f"Wins: {wins_str} (Weighted Wins: {weighted_wins_str}) \n Games: {games} \n W/L: {wl_str}"
                            return f"**#{rank}. [{tag}]**\n{stat_string}\n\n"
                            
                        view = LbDisplay(
                            data=top_clans, 
                            formatter_func=format_clan, 
                            title=embed_title,
                            items_per_page=num if num < 10 else 10,
                        )
                        
                        await interaction.followup.send(embed=view.format_page(), view=view)
                    else:
                        await interaction.followup.send(f"Failed to fetch leaderboard. Status Code: {response.status}")
        except Exception as e:
            await interaction.followup.send(f"An error occurred while loading the leaderboard: {e}")

    @app_commands.command(name="clan-players", description="List all tracked players for a specific clan.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., CAF)", num="Number of players per page", min_games="Minimum games played to be included in the list (Default: 5)", sort_by="Choose how to sort the players", reverse_sort="Whether to reverse the sort order (Default: False)")
    @app_commands.choices(sort_by=[
        app_commands.Choice(name="Win Rate", value="winrate"),
        app_commands.Choice(name="Games Played", value="games"),
        app_commands.Choice(name='Total Wins', value="wins"),
        app_commands.Choice(name='Highest Winstreak', value="highest_winstreak"),
    ])
    async def clan_players(self, interaction: discord.Interaction, clan_tag: str, num: int = 5, min_games: int = 5, sort_by: str = "default", reverse_sort: bool = False):
        tag_upper = clan_tag.upper()
        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper) 

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return
        
        clan_stats = await self.bot.clan_manager.get_clan_stats(tag_upper)
        if not clan_stats or "players" not in clan_stats or not clan_stats["players"]:
            await interaction.response.send_message(f"No tracked player data found for clan **[{tag_upper}]**.", ephemeral=True)
            return
            
        players = clan_stats.get("players", {})

        if sort_by == "winrate" or sort_by == "default":
            sorted_players = sorted(
                [x for x in players.items() if x[1].get("games_played", 0) >= min_games],
                key=lambda x: ((x[1].get("wins", 0) / x[1].get("games_played", 0) if x[1].get("games_played", 0) > 0 else 0), x[1].get("games_played", 0)),
                reverse = not reverse_sort
            )
        elif sort_by == "games":
            sorted_players = sorted(
                [x for x in players.items() if x[1].get("games_played", 0) >= min_games],
                key=lambda x: (x[1].get("games_played", 0), x[1].get("wins", 0)),
                reverse = not reverse_sort
            )
        elif sort_by == "wins":
            sorted_players = sorted(
                [x for x in players.items() if x[1].get("games_played", 0) >= min_games],
                key=lambda x: (x[1].get("wins", 0), x[1].get("games_played", 0)),
                reverse = not reverse_sort
            )
        elif sort_by == "highest_winstreak":
            sorted_players = sorted(
                [x for x in players.items() if x[1].get("games_played", 0) >= min_games],
                key=lambda x: (x[1].get("highest_winstreak", 0), x[1].get("winrate", 0), x[1].get("games_played", 0)),
                reverse = not reverse_sort
            )

        if not sorted_players:
            await interaction.response.send_message(f"No players are currently being tracked for clan **[{tag_upper}]**.", ephemeral=True)
            return
        
        await self.bot.clan_manager.finalize_batch_update(clan_tag)
            
        total_clan_games = clan_stats.get('total_games', 0)

        def format_player(rank, item):
            p_id, stats = item
            games_played = stats.get("games_played", 0)
            wins = stats.get("wins", 0)
            losses = games_played - wins
            winrate = (wins / games_played) * 100 if games_played > 0 else 0.0
            percent_of_clan = (games_played / total_clan_games * 100) if total_clan_games > 0 else 0
            winstreak = stats.get("winstreak", 0)
            highest_winstreak = stats.get("highest_winstreak", 0)
            
            return f"**#{rank}. {p_id}**\n- Games: {games_played}\n- Percent of Clan Games: {percent_of_clan:.1f}%\n- Win Rate: {winrate:.1f}%\n- W/L: {wins}/{losses}\n- Current Winstreak: {winstreak}\n- Highest Winstreak: {highest_winstreak}\n\n"

        view = LbDisplay(
            data=sorted_players,
            formatter_func=format_player,
            title=f"Tracked Players for [{tag_upper}]",
            items_per_page=num if num < 10 else 10,
        )
        
        await interaction.response.send_message(embed=view.format_page(), view=view)
    
    @app_commands.command(name="outstanding-games", description="Display the number of games that have not been processed for a specific clan.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., CAF)")
    async def outstanding_games(self, interaction: discord.Interaction, clan_tag: str):
        await interaction.response.defer()
        tag_upper = clan_tag.upper()

        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return
        
        processed_games = await self.bot.clan_manager.get_processed_count(tag_upper)
        
        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions?limit=1"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        dat = await response.json()
                        total_games = int(dat.get("total", 0))
                        outstanding = total_games - processed_games
                        
                        await interaction.followup.send(f"**[{tag_upper}]** has **{outstanding}** outstanding games that have not been processed yet. (Total: {total_games}, Processed: {processed_games})")
                    else:
                        await interaction.followup.send(f"Could not fetch total games for **[{tag_upper}]** from the API.")
        except Exception as e:
            await interaction.followup.send(f"An error occurred while fetching clan info: {e}")

    @app_commands.command(name="match-history", description="View the saved match history for a specific clan or player.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)", username="Player's username in clan (Optional)", num="Matches per page (Default: 5, Max: 10)")
    async def match_history(self, interaction: discord.Interaction, clan_tag: str, username: str = None, num: int = 5):
        tag_upper = clan_tag.upper()
        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper) 

        search_name = username.lower() if username else None
        if search_name:
            search_name = re.sub(r'[^A-Za-z0-9_ ]', '', search_name) 

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag.", ephemeral=True)
            return
        
        # Only validate search_name length if it was actually provided
        if search_name and (len(search_name) == 0 or len(search_name) > 25):
            await interaction.response.send_message("Please provide a valid username (1-25 alphanumeric characters).", ephemeral=True)
            return
            
        await interaction.response.defer()

        await self.bot.clan_manager.finalize_batch_update(clan_tag)
        
        # Load the clan data into memory
        await self.bot.clan_manager.load_clan(tag_upper)
        clan_data = self.bot.clan_manager.clans.get(tag_upper)
        
        # Set up shared variables for the view at the end
        display_matches = []
        display_title = f"Match History for [{tag_upper}]"

        if username:
            if not clan_data or not clan_data.get("stats", {}).get("players"):
                await interaction.followup.send(f"No match history found for **[{tag_upper}]**. Load some games first!", ephemeral=True)
                return
            
            clan_stats = clan_data.get("stats", {})
        
            found_player_id = None
            players = clan_stats.get("players", {})
            player_list = []

            for player in players.keys():
                if search_name == player.strip('[' + tag_upper + ']').strip().lower():
                    player_list.append(player)
                    found_player_id = player
                    
            if not found_player_id:
                await interaction.followup.send(f"Could not find any tracked games for player **{username}** in clan **[{tag_upper}]**.")
                return
                
            # Get all matches, reverse them, and filter for games where this player participated
            all_matches = clan_data.get("matches", [])[::-1]
            display_matches = [
                match for match in all_matches 
                if any(p in match.get("clanPlayers", []) for p in player_list)
            ]
            
            if not display_matches:
                await interaction.followup.send(f"No match history found for **{username}** in clan **[{tag_upper}]**.")
                return
                
            # Make the embed title specific to the player
            display_name = player_list[0] if len(player_list) == 1 else username
            display_title = f"Match History: {display_name} [{tag_upper}]"
            
        else:
            if not clan_data or not clan_data.get("matches"):
                await interaction.followup.send(f"No match history found for **[{tag_upper}]**. Try loading some games first!", ephemeral=True)
                return
            
            # Get matches and reverse them so the latest game is index 1
            display_matches = clan_data["matches"][::-1]
        
        def format_match(index, match):
            is_win = match.get("hasWon", False)
            result = "🟢 VICTORY" if is_win else "🔴 DEFEAT"
            score = match.get("score", 0)
            gamemode = match.get("gamemode", "Unknown")
            game_id = match.get("gameId", "Unknown")
            players_count = len(match.get("clanPlayers", []))
            total_players = match.get("totalPlayersInMatch", "?")
            
            raw_start = match.get("start")
            time_str = "Unknown Time"
            if raw_start:
                # Convert ms timestamp to seconds for Discord time formatting
                time_str = f"<t:{int(raw_start / 1000)}:R>"
                
            sign = "+" if is_win else ""

            clan_players = match.get("clanPlayers", [])
            clan_players_str = ", ".join([f"`{p}`" for p in clan_players]) if clan_players else "Unknown"
            
            return (
                f"**{index}. {result}** | {time_str}\n"
                f"> **Mode:** {gamemode}\n"
                f"> **Rating:** {sign}{score} Weighted Wins\n"
                f"> **Clan Players:** {players_count} / {total_players}\n"
                f"> **Clan Players:** {clan_players_str}\n"
                f"> **Match ID:** ``{game_id}``\n\n"
            )
            
        # Ensure items_per_page is within a reasonable limit to prevent Discord embed limits
        valid_num = num if 0 < num <= 10 else 5
            
        view = LbDisplay(
            data=display_matches,
            formatter_func=format_match,
            title=display_title,
            items_per_page=valid_num
        )
        
        await interaction.followup.send(embed=view.format_page(), view=view)

async def setup(bot):
    await bot.add_cog(StatsCmds(bot))