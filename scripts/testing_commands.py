import discord
from discord import app_commands
from discord.ext import commands
import json
import os
import asyncio

from .atomic_saver import AtomicSaver

class TestingCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.lock = asyncio.Lock()
        
    def reformat_player_name(self, name, clan_tag):
        # 1. Strip BEFORE checking to catch names like " [UN] balls"
        clean_name = name.strip()
        prefix = f"[{clan_tag.upper()}]"
        
        if clean_name.upper().startswith(prefix):
            return clean_name[len(prefix):].strip()
        return clean_name

    @app_commands.command(name="reformat_clan_data", description="Reformats the clan data for all clans.")
    async def reformat_clan_data(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        root_dir = "./bot_data/clan_data"
        bg_loop = self.bot.get_cog("BackgroundLoop")
        
        if getattr(self.bot, 'is_swarm_active', False) or (bg_loop and not bg_loop.live_queue.empty()):
            await interaction.followup.send("Waiting for active background loads and live queues to finish before reformatting clan data...")
            while getattr(self.bot, 'is_swarm_active', False) or (bg_loop and not bg_loop.live_queue.empty()):
                await asyncio.sleep(1)

        for e in os.scandir(root_dir):
            if e.is_dir() and os.path.exists(os.path.join(e.path, "matches.json")) and os.path.exists(os.path.join(e.path, "player_stats.json")):
                clan_tag = e.name

                # --- 1. REFORMAT MATCHES ---
                path = os.path.join(e.path, "matches.json")
                with open(path, "r") as f:
                    matches = json.load(f)
                
                for match in matches:
                    old_clan_p = match.get("clanPlayers", [])
                    
                    if isinstance(old_clan_p, list):
                        new_clan_p = []
                        for p_name in old_clan_p:
                            new_name = self.reformat_player_name(p_name, clan_tag)
                            new_clan_p.append(new_name)
                        match["clanPlayers"] = new_clan_p
                        
                    elif isinstance(old_clan_p, dict):
                        new_clan_p = {}
                        for p_name, p_data in old_clan_p.items():
                            new_name = self.reformat_player_name(p_name, clan_tag)
                            new_clan_p[new_name] = p_data
                        match["clanPlayers"] = new_clan_p

                await AtomicSaver.save_json_async(path, matches, lock=self.lock)
                
                # --- 2. REFORMAT PLAYER STATS ---
                path = os.path.join(e.path, "player_stats.json")
                with open(path, "r") as f:
                    player_stats = json.load(f)
                
                old_players = player_stats.get("players", {})
                new_players = {}

                for p_name, p_data in old_players.items():
                    new_name = self.reformat_player_name(p_name, clan_tag)

                    if new_name not in new_players:
                        new_players[new_name] = {
                            "games_played": 0, 
                            "wins": 0, 
                            "winstreak": 0, 
                            "highest_winstreak": 0
                        }

                    new_players[new_name]["games_played"] += p_data.get("games_played", 0)
                    new_players[new_name]["wins"] += p_data.get("wins", 0)
                    new_players[new_name]["highest_winstreak"] = max(new_players[new_name]["highest_winstreak"], p_data.get("highest_winstreak", 0))
                    
                    games = new_players[new_name]["games_played"]
                    if games > 0:
                        new_players[new_name]["winrate"] = round((new_players[new_name]["wins"] / games) * 100, 2)

                player_stats["players"] = new_players

                await AtomicSaver.save_json_async(path, player_stats, lock=self.lock)
                
                # --- 3. CLEAR FROM LIVE MEMORY ---
                # This forces the bot to reload the cleanly formatted files from disk
                # the next time this clan is accessed, preventing the bot from overwriting 
                # our hard work with its old cached memory.
                tag_upper = clan_tag.upper()
                if tag_upper in self.bot.clan_manager.clans:
                    del self.bot.clan_manager.clans[tag_upper]

        await interaction.followup.send("Clan data reformatted and cache cleared successfully.")

    @app_commands.command(name="sync_processed_games", description="Fixes mismatches between saved matches and processed IDs.")
    async def sync_processed(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        root_dir = "./bot_data/clan_data"
        
        for e in os.scandir(root_dir):
            if e.is_dir():
                matches_path = os.path.join(e.path, "matches.json")
                processed_path = os.path.join(e.path, "processed_games.json")
                
                if os.path.exists(matches_path) and os.path.exists(processed_path):
                    with open(matches_path, "r") as f:
                        matches = json.load(f)
                    
                    # 1. Rebuild the set of processed IDs strictly from what is saved
                    true_processed_ids = [m.get("gameId") for m in matches if m.get("gameId")]
                    
                    # 2. Overwrite the processed_games file with the corrected list
                    await AtomicSaver.save_json_async(processed_path, true_processed_ids, lock=self.lock)
                    
                    # 3. Clear the cache so the bot reloads the synchronized data
                    tag_upper = e.name.upper()
                    if tag_upper in self.bot.clan_manager.clans:
                        del self.bot.clan_manager.clans[tag_upper]

        await interaction.followup.send("Successfully synced `processed_games.json` with actual match data for all clans.")

async def setup(bot):
    await bot.add_cog(TestingCommands(bot))