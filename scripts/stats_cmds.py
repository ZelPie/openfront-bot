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

        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)  # Sanitize input to prevent issues

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return
        
        await interaction.response.defer()  # Defer the response as we might take a moment to fetch data from the API
        
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
                        winstreak = self.bot.player_data.get(tag_upper, {}).get("winstreak", 0)
                        highest_winstreak = self.bot.player_data.get(tag_upper, {}).get("highest_winstreak", 0)
                        
                        embed = discord.Embed(title=f"Clan [{tag_upper}] Statistics", color=discord.Color.blurple())
                        embed.add_field(name="Total Matches", value=f"**{games}**", inline=True)
                        embed.add_field(name="Wins / Losses", value=f"**{wins}** / **{losses}**", inline=True)
                        embed.add_field(name="Win/Loss Ratio", value=f"**{wl_ratio:.2f}**", inline=True)
                        # embed.add_field(name="", value="\u200b", inline=True)  # Empty field for spacing
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

        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)  # Sanitize input to prevent issues

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return
        
        search_name = re.sub(r'[^A-Za-z0-9_ ]', '', search_name)  # Sanitize input to prevent issues

        if len(search_name) == 0 or len(search_name) > 25:
            await interaction.response.send_message("Please provide a valid username (1-25 alphanumeric characters).", ephemeral=True)
            return
        
        if tag_upper not in self.bot.player_data:
            await interaction.response.send_message(f"We don't have any tracked data for clan **[{tag_upper}]** yet.", ephemeral=True)
            return
            
        clan_db = self.bot.player_data[tag_upper]
        found_player_id = None

        players = clan_db.get("players", {})
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

            # might not use below
            # embed.add_field(name="Player List", value="\n".join(player_list), inline=False)
        else:
            embed = discord.Embed(title=f"Player Stats: {found_player_id}", color=discord.Color.blue())

        for p in player_list:
            current_p_num += 1
            stats = players[p]
            games_played = stats.get("games_played", 0)
            wins = stats.get("wins", 0)
            losses = games_played - wins
            total_clan_games = clan_db.get("total_games", 0)
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
                        embed_title = f"🏆 Top OpenFront Clans" # In case
                        
                        if sort_choice == "wins":
                            clans_list.sort(key=lambda c: c.get("wins", 0), reverse=not reverse_sort)
                            embed_title = f"🏆 Top Clans by Total Wins"
                        elif sort_choice == "winrate":
                            clans_list.sort(key=lambda c: c.get("weightedWLRatio", 0), reverse=not reverse_sort)
                            embed_title = f"🏆 Top Clans by W/L Ratio"
                        elif sort_choice == "weighted_wins" or sort_choice == "default":
                            clans_list.sort(key=lambda c: c.get("weightedWins", 0), reverse=not reverse_sort)
                            embed_title = f"🏆 Top Clans by Weighted Wins"

                        # Slice to the user's requested range
                        top_clans = clans_list[lower_num - 1:]

                        # 1. Define the formatting rule
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
                            
                        # 2. Create the Paginator
                        view = LbDisplay(
                            data=top_clans, 
                            formatter_func=format_clan, 
                            title=embed_title,
                            items_per_page=num if num < 10 else 10,  # Show all on one page if 10 or fewer, otherwise paginate with 10 per page
                        )
                        
                        # 3. Send it!
                        await interaction.followup.send(embed=view.format_page(), view=view)
                    else:
                        await interaction.followup.send(f"Failed to fetch leaderboard. Status Code: {response.status}")
        except Exception as e:
            await interaction.followup.send(f"An error occurred while loading the leaderboard: {e}")

    @app_commands.command(name="clan-players", description="List all tracked players for a specific clan.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., CAF)", num="Number of top players to fetch (Default: 50)", min_games="Minimum games played to be included in the list (Default: 5)", sort_by="Choose how to sort the players", reverse_sort="Whether to reverse the sort order (Default: False)")
    @app_commands.choices(sort_by=[
        app_commands.Choice(name="Win Rate", value="winrate"),
        app_commands.Choice(name="Games Played", value="games"),
        app_commands.Choice(name='Total Wins', value="wins"),
    ])
    async def clan_players(self, interaction: discord.Interaction, clan_tag: str, num: int = 50, min_games: int = 5, sort_by: str = "default", reverse_sort: bool = False):
        tag_upper = clan_tag.upper()
        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper) 

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return
        
        if tag_upper not in self.bot.player_data:
            await interaction.response.send_message(f"No tracked player data found for clan **[{tag_upper}]**.", ephemeral=True)
            return
            
        clan_db = self.bot.player_data[tag_upper]
        players = clan_db.get("players", {})

        if sort_by == "winrate" or sort_by == "default":
            sorted_players = sorted(
                [x for x in players.items() if x[1].get("games_played", 0) >= min_games],
                key=lambda x: ((x[1].get("wins", 0) / x[1].get("games_played", 0) if x[1].get("games_played", 0) > 0 else 0), x[1].get("games_played", 0)),
                reverse = not reverse_sort
            )[:num]
        elif sort_by == "games":
            sorted_players = sorted(
                [x for x in players.items() if x[1].get("games_played", 0) >= min_games],
                key=lambda x: (x[1].get("games_played", 0), x[1].get("wins", 0)),
                reverse = not reverse_sort
            )[:num]
        elif sort_by == "wins":
            sorted_players = sorted(
                [x for x in players.items() if x[1].get("games_played", 0) >= min_games],
                key=lambda x: (x[1].get("wins", 0), x[1].get("games_played", 0)),
                reverse = not reverse_sort
            )[:num]

        if not sorted_players:
            await interaction.response.send_message(f"No players are currently being tracked for clan **[{tag_upper}]**.", ephemeral=True)
            return
            
        total_clan_games = clan_db.get('total_games', 0)

        # 1. Define the formatting rule
        def format_player(rank, item):
            p_id, stats = item
            games_played = stats.get("games_played", 0)
            wins = stats.get("wins", 0)
            losses = games_played - wins
            winrate = (wins / games_played) * 100 if games_played > 0 else 0.0
            percent_of_clan = (games_played / total_clan_games * 100) if total_clan_games > 0 else 0
            
            return f"**#{rank}. {p_id}**\n- Games: {games_played}\n- Percent of Clan Games: {percent_of_clan:.1f}%\n- Win Rate: {winrate:.1f}%\n- W/L: {wins}/{losses}\n\n"

        # 2. Create the Paginator
        view = LbDisplay(
            data=sorted_players,
            formatter_func=format_player,
            title=f"Tracked Players for [{tag_upper}]",
            items_per_page=5,
        )
        
        # 3. Send it!
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
        
        processed_games = len(self.bot.processed_games.get(tag_upper, []))
        
        # Hit the sessions API to parse the "total" key attached to the games index
        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions?limit=1"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        dat = await response.json()
                        # Extract total based on new dictionary structure
                        total_games = int(dat.get("total", 0))
                        
                        outstanding = total_games - processed_games
                        
                        await interaction.followup.send(f"**[{tag_upper}]** has **{outstanding}** outstanding games that have not been processed yet. (Total: {total_games}, Processed: {processed_games})")
                    else:
                        await interaction.followup.send(f"Could not fetch total games for **[{tag_upper}]** from the API.")
        except Exception as e:
            await interaction.followup.send(f"An error occurred while fetching clan info: {e}")

    @app_commands.command(name="alltime-winstreak", description="Calculates the highest all-time winstreak for a clan or a specific player.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)", username="Optional: Check a specific player's streak instead")
    async def alltime_winstreak(self, interaction: discord.Interaction, clan_tag: str, username: str = None):
        await interaction.response.defer()
        
        tag_upper = clan_tag.upper()
        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)  

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.followup.send("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return

        # PLAYER CHECK ROUTE (API)
        if username:
            if tag_upper not in self.bot.player_data:
                await interaction.followup.send(f"No tracked data found for clan **[{tag_upper}]** yet. Please run a background load first.")
                return
                
            clan_db = self.bot.player_data.get(tag_upper, {})
            players = clan_db.get("players", {})
            search_name = username.lower()
            
            found_player = None
            for player in players.keys():
                if search_name == player.strip('[' + tag_upper + ']').strip().lower():
                    found_player = player
                    break
            
            if found_player:
                p_stats = players[found_player]
                highest = p_stats.get("highest_winstreak", 0)
                current = p_stats.get("winstreak", 0)
                games = p_stats.get("games_played", 0)
                
                embed = discord.Embed(title=f"All-Time Winstreak for {found_player}", color=discord.Color.blue())
                embed.add_field(name="Highest Winstreak", value=f"**{highest}**", inline=False)
                embed.add_field(name="Current Winstreak", value=f"**{current}**", inline=False)
                embed.add_field(name="Tracked Games", value=f"``{games}``", inline=False)
                await interaction.followup.send(embed=embed)
            else:
                await interaction.followup.send(f"Player **{username}** not found in the processed database for **[{tag_upper}]**.")
            return

        # CLAN CHECK ROUTE (API)
        all_games = []
        seen_game_ids = set()
        
        try:
            async with aiohttp.ClientSession() as session:
                total_games = 0
                url_total = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions?limit=1"
                async with session.get(url_total, timeout=10) as resp:
                    if resp.status == 200:
                        dat = await resp.json()
                        total_games = int(dat.get("total", 0))

                current_end = datetime.now(timezone.utc)
                current_start = current_end - timedelta(days=1)
                empty_days = 0
                
                while True:
                    start_iso = current_start.strftime('%Y-%m-%dT%H:%M:%SZ')
                    end_iso = current_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                    page = 1
                    day_results_count = 0
                    
                    while True:
                        url = f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions?start={start_iso}&end={end_iso}&page={page}&limit=50"
                        
                        async with session.get(url, timeout=15) as response:
                            if response.status == 429:
                                await asyncio.sleep(2)
                                continue 
                                
                            if response.status != 200:
                                break 
                                
                            data = await response.json()
                            results = data.get("results", [])
                            
                            if not results or not isinstance(results, list) or len(results) == 0:
                                break 
                                
                            for game in results:
                                gid = game.get("gameId")
                                if gid and gid not in seen_game_ids:
                                    seen_game_ids.add(gid)
                                    all_games.append(game)
                                    day_results_count += 1
                                    
                            page += 1
                            await asyncio.sleep(0.2) 
                            
                    if total_games > 0 and len(seen_game_ids) >= total_games:
                        break

                    if day_results_count == 0:
                        empty_days += 1
                        if empty_days >= 3: 
                            break
                    else:
                        empty_days = 0
                        
                    current_end = current_start
                    current_start = current_start - timedelta(days=1)
                        
        except Exception as e:
            await interaction.followup.send(f"An error occurred while fetching clan history: {e}")
            return

        if not all_games:
            await interaction.followup.send(f"No games found for **[{tag_upper}]**.")
            return

        all_games.sort(key=lambda x: x.get("gameStart", ""))

        current_streak = 0
        highest_streak = 0

        for game in all_games:
            is_win = game.get("hasWon", False)
            if is_win:
                current_streak += 1
                if current_streak > highest_streak:
                    highest_streak = current_streak
            else:
                current_streak = 0

        data_changed = False
        if tag_upper in self.bot.player_data:
            stored_highest = self.bot.player_data[tag_upper].get("highest_winstreak", 0)
            if highest_streak > stored_highest:
                self.bot.player_data[tag_upper]["highest_winstreak"] = highest_streak
                data_changed = True
        else:
            self.bot.player_data[tag_upper] = {"total_games": len(all_games), "winstreak": current_streak, "highest_winstreak": highest_streak, "players": {}}
            data_changed = True
                
        if data_changed and hasattr(self.bot, 'save_lock'):
            async with self.bot.save_lock:
                self.bot.save_data()

        embed = discord.Embed(title=f"All-Time Winstreak for [{tag_upper}]", color=discord.Color.gold())
        embed.add_field(name="Highest Winstreak", value=f"**{highest_streak}**", inline=False)
        embed.add_field(name="Total Games Analyzed", value=f"``{len(all_games)}``", inline=False)
        
        if data_changed:
            embed.set_footer(text="New highest winstreak saved to database!")
        
        await interaction.followup.send(embed=embed)


async def setup(bot):
    await bot.add_cog(StatsCmds(bot))