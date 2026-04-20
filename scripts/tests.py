import discord
from discord.ext import commands
from discord import app_commands
import asyncio
import os

class Tests(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.dev_server_id = int(os.getenv('DEV_SERVER_ID', '0'))

    async def run_test(self, name, coro):
        """Helper to run a test and return a status message."""
        try:
            await coro
            return f"**{name}**: Passed"
        except Exception as e:
            return f"**{name}**: Failed ({e})"

    @app_commands.command(name="run-all-tests", description="Runs a full diagnostic of the bot's systems.")
    async def run_diagnostics(self, interaction: discord.Interaction):
        # Restricted to Developer Server and Admins
        if interaction.guild_id != self.dev_server_id or not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Unauthorized.", ephemeral=True)
            return

        await interaction.response.defer()
        results = []

        # 1. Test Data Management (ClanDataManager)
        async def test_clan_manager():
            test_tag = "TEST"
            await self.bot.clan_manager.load_clan(test_tag)
            stats = await self.bot.clan_manager.get_clan_stats(test_tag)
            if stats is None: raise Exception("Could not load clan stats")
        
        results.append(await self.run_test("Clan Data Manager", test_clan_manager()))

        # 2. Test File Persistence
        async def test_persistence():
            self.bot.save_data()
            if not os.path.exists(os.path.join(os.path.dirname(self.bot.clan_manager.base_dir), "tracking_data.json")):
                raise Exception("Tracking data file missing after save")

        results.append(await self.run_test("File Persistence", test_persistence()))

        # 3. Test API Connectivity (BackgroundLoop Logic)
        async def test_api():
            # Borrow the logic from main_loop to check API reachability
            cog = self.bot.get_cog("BackgroundLoop")
            if not cog: raise Exception("BackgroundLoop cog not loaded")
            # We just check if we can reach the public leaderboard as a heartbeat
            async with self.bot.clan_manager.lock: # Reuse manager's session or logic
                 import aiohttp
                 async with aiohttp.ClientSession() as session:
                     async with session.get("https://api.openfront.io/public/clans/leaderboard?limit=1") as r:
                         if r.status != 200: raise Exception(f"API returned {r.status}")

        results.append(await self.run_test("OpenFront API Reachability", test_api()))

        # 4. Test if the match data in the clans are in chronological order
        async def test_matches_order():

            manager = self.bot.clan_manager

            for clan_tag, clan_data in manager.clans.items():
                previous_game_start = 0
                for game in clan_data["matches"]:
                    current_start = game.get("start", 0)

                    if current_start < previous_game_start:
                        raise Exception(f"Order error in [{clan_tag}]: {current_start} ordered before {previous_game_start}")
                    
                    previous_game_start = current_start
        
        results.append(await self.run_test("Match Order:", test_matches_order()))

        # 5. Test Cog Integrity
        required_cogs = ["BackgroundLoop", "StatsCmds", "TrackingCmds", "LoadPlayers"]
        missing_cogs = [c for c in required_cogs if self.bot.get_cog(c) is None]
        if not missing_cogs:
            results.append("**Cog Loading**: All extensions active")
        else:
            results.append(f"**Cog Loading**: Missing {', '.join(missing_cogs)}")


        # Create Summary Embed
        embed = discord.Embed(title="Bot Diagnostic Report", color=discord.Color.blue())
        embed.description = "\n".join(results)
        embed.timestamp = discord.utils.utcnow()
        
        await interaction.followup.send(embed=embed)

        

async def setup(bot):
    await bot.add_cog(Tests(bot))