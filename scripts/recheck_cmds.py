import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import asyncio
import os

dev_server_id = int(os.getenv('DEV_SERVER_ID', '0'))

class RecheckCmds(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="rebuild-match-data", description="Backfills missing map and player stats into already saved matches.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., UN)")
    async def rebuild_match_data(self, interaction: discord.Interaction, clan_tag: str):
        if not interaction.user.guild_permissions.administrator or interaction.guild_id != dev_server_id:
            await interaction.response.send_message("Unauthorized.", ephemeral=True)
            return

        tag_upper = clan_tag.upper().strip()
        await interaction.response.defer()

        await self.bot.clan_manager.load_clan(tag_upper)
        clan_data = self.bot.clan_manager.clans.get(tag_upper)

        if not clan_data or not clan_data.get("matches"):
            await interaction.followup.send(f"No match data found for **[{tag_upper}]**.")
            return

        matches = clan_data["matches"]
        total = len(matches)
        await interaction.followup.send(f"Starting data backfill for **{total}** saved matches in **[{tag_upper}]**... This may take a while.")

        updated_count = 0
        async with aiohttp.ClientSession() as session:
            for match in matches:
                game_id = match.get("gameId")
                if not game_id: continue

                # Skip if it already has the new data format to save API calls
                if "mapName" in match and "playerStats" in match:
                    continue

                game_url = f"https://api.openfront.io/public/game/{game_id}?turns=false"
                try:
                    async with session.get(game_url, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            info = data.get("info", {})
                            config = info.get("config", {})
                            all_players = info.get("players", [])

                            # Inject Map Name
                            match["mapName"] = config.get("mapName", config.get("map", "Unknown"))

                            # Inject Player Stats
                            player_match_stats = {}
                            for p in all_players:
                                if p.get("clanTag", "").upper() == tag_upper:
                                    p_name = p.get("username", "Unknown")
                                    player_match_stats[p_name] = {
                                        "gold": p.get("gold", 0),
                                        "nukes": p.get("nukesLaunched", 0)
                                    }
                            
                            match["playerStats"] = player_match_stats
                            updated_count += 1
                        elif resp.status == 429:
                            await asyncio.sleep(2)  # Respect rate limits
                except Exception as e:
                    print(f"Failed to fetch {game_id}: {e}")
                
                await asyncio.sleep(0.3)  # Small delay to prevent API bans

        # Save the mutated dictionaries back to disk
        await self.bot.clan_manager.save_clan(tag_upper)
        await interaction.channel.send(f"Successfully rebuilt data for **{updated_count}** out of {total} matches for **[{tag_upper}]**.")

async def setup(bot):
    await bot.add_cog(RecheckCmds(bot))