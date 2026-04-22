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

        bg_loop = self.bot.get_cog("BackgroundLoop")
        
        if getattr(self.bot, 'is_swarm_active', False) or (bg_loop and not bg_loop.live_queue.empty()):
            await interaction.followup.send("Waiting for active background loads and live queues to finish before running tests...")

        # 1. Wait for backfill (LoadPlayers) to finish
        while getattr(self.bot, 'is_swarm_active', False):
            await asyncio.sleep(1)

        # 2. Wait for the main loop's live queue to fully process
        if bg_loop:
            print("Waiting for BackgroundLoop live queue to finish...")
            await bg_loop.live_queue.join()

        # 3. Explicitly finalize match data for all loaded clans
        for clan_tag in list(self.bot.clan_manager.clans.keys()):
            await self.bot.clan_manager.finalize_batch_update(clan_tag)

        results = []

        print("Starting bot diagnostics...")

        # 1. Test Data Management (ClanDataManager)
        async def test_clan_manager():
            print("Testing ClanDataManager...")

            test_tag = "TESTING_TAG"
            await self.bot.clan_manager.load_clan(test_tag)
            stats = await self.bot.clan_manager.get_clan_stats(test_tag)
            if stats is None: raise Exception("Could not load clan stats")
        
        results.append(await self.run_test("Clan Data Manager", test_clan_manager()))

        # 2. Test File Persistence
        async def test_persistence():
            print("Testing file persistence...")

            self.bot.save_data()
            if not os.path.exists(os.path.join(os.path.dirname(self.bot.clan_manager.base_dir), "tracking_data.json")):
                raise Exception("Tracking data file missing after save")

        results.append(await self.run_test("File Persistence", test_persistence()))

        # 3. Test API Connectivity (BackgroundLoop Logic)
        async def test_api():
            print("Testing API connectivity...")

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
            print("Testing match data order...")

            manager = self.bot.clan_manager

            for clan_tag, clan_data in manager.clans.items():
                previous_game_start = 0
                for game in clan_data["matches"]:
                    current_start = game.get("start", 0)

                    if current_start < previous_game_start:
                        raise Exception(f"Order error in [{clan_tag}]: {current_start} ordered before {previous_game_start}")
                    
                    previous_game_start = current_start
        
        results.append(await self.run_test("Match Order:", test_matches_order()))

        # 5. Test Data Integrity (Missing or Duplicate Games)
        async def test_data_integrity():
            print("Testing data integrity...")

            import aiohttp
            manager = self.bot.clan_manager

            async with aiohttp.ClientSession() as session:
                for clan_tag, clan_data in manager.clans.items():
                    # 1. Check for duplicates in the saved matches list
                    saved_matches = clan_data.get("matches", [])
                    saved_ids = [m.get("gameId") for m in saved_matches]
                    if len(saved_ids) != len(set(saved_ids)):
                        raise Exception(f"[{clan_tag}] contains duplicate games in matches.json!")
                        
                    # 2. Check if the 'processed' list length matches the 'matches' list length
                    processed_set = clan_data.get("processed", set())
                    if len(saved_matches) != len(processed_set):
                        raise Exception(f"[{clan_tag}] mismatch: {len(saved_matches)} matches saved, but {len(processed_set)} marked as processed!")

                    # 3. Test the API
                    url = f"https://api.openfront.io/public/clan/{clan_tag.lower()}/sessions?limit=1"
                    async with session.get(url, timeout=10) as response:
                        if response.status != 200:
                            raise Exception(f"API returned {response.status} for clan {clan_tag}!")
                    
                    # 4. Check if any saved game has a start time of 0 (indicating a failed load)
                    for game in saved_matches:
                        if game.get("start", 0) == 0:
                            raise Exception(f"[{clan_tag}] contains a game with start time 0, indicating a failed load!")
                        
                    # 5. Check if any match id is missing from the processed set (indicating a potential processing failure)
                    for game in saved_matches:
                        game_id = game.get("gameId")
                        if game_id not in processed_set:
                            raise Exception(f"[{clan_tag}] contains unprocessed game ID {game_id}!")
                    
                    # 6. Check if any game appears more than once in the processed set (indicating a potential duplicate processing)
                    processed_ids = [game_id for game_id in processed_set]
                    if len(processed_ids) != len(set(processed_ids)):
                        raise Exception(f"[{clan_tag}] contains duplicate game IDs in processed set!")
                    
                    # 7. Check if the latest game in matches.json is also in the processed set (indicating that the most recent game was fully processed)
                    if saved_matches:
                        latest_game_id = saved_matches[-1].get("gameId")
                        if latest_game_id not in processed_set:
                            raise Exception(f"[{clan_tag}] latest game ID {latest_game_id} is not marked as processed!")

        results.append(await self.run_test("Data Integrity (Duplicates & Counts)", test_data_integrity()))

        # 6. Test Cog Integrity
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
        
        print("Diagnostics complete. Sending report...")
        await interaction.followup.send(embed=embed)

        

async def setup(bot):
    await bot.add_cog(Tests(bot))