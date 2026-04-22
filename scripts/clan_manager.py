from datetime import time
import os
import json
import asyncio

class ClanDataManager:
    def __init__(self, base_dir):
        self.base_dir = os.path.join(base_dir, "bot_data", "clan_data")
        os.makedirs(self.base_dir, exist_ok=True)
        self.clans = {}
        self.lock = asyncio.Lock()

    def _get_paths(self, clan_tag):
        tag = clan_tag.upper()
        clan_dir = os.path.join(self.base_dir, tag)
        os.makedirs(clan_dir, exist_ok=True)
        return {
            "stats": os.path.join(clan_dir, "player_stats.json"),
            "processed": os.path.join(clan_dir, "processed_games.json"),
            "matches": os.path.join(clan_dir, "matches.json")
        }

    async def load_clan(self, clan_tag):
        tag = clan_tag.upper()
        if tag in self.clans:
            return
            
        paths = self._get_paths(tag)
        clan_data = {
            "stats": {
                "total_games": 0, 
                "wins": 0, 
                "winstreak": 0, 
                "highest_winstreak": 0, 
                "load_time_seconds": 0,
                "latest_cursor": None,
                "players": {}
            },
            "processed": [],
            "matches": []
        }
        
        for key, path in paths.items():
            if os.path.exists(path):
                try:
                    with open(path, "r") as f:
                        loaded = json.load(f)
                        if key == "stats":
                            clan_data[key].update(loaded)
                        elif key == "processed":
                            clan_data[key] = set(loaded)
                        else:
                            clan_data[key] = loaded
                except Exception as e:
                    print(f"Error loading {path}: {e}")
        
        if isinstance(clan_data["processed"], list):
            clan_data["processed"] = set(clan_data["processed"])
                    
        self.clans[tag] = clan_data

    async def save_clan(self, clan_tag):
        """Atomic save: Writes to .tmp first to prevent corruption, then swaps.
           Includes a retry loop to bypass Windows file locking."""
        tag = clan_tag.upper()
        if tag not in self.clans:
            return
            
        paths = self._get_paths(tag)

        def _write_files():
            for key, path in paths.items():
                temp_path = f"{path}.tmp"
                with open(temp_path, "w") as f:
                    to_save = list(self.clans[tag][key]) if key == "processed" else self.clans[tag][key]
                    json.dump(to_save, f, indent=4)
                
                # FIX: Bulletproof Windows retry mechanism
                retries = 10
                for i in range(retries):
                    try:
                        os.replace(temp_path, path)
                        break # Success! Break out of the retry loop
                    except (PermissionError, OSError) as e:
                        if i == retries - 1:
                            raise e # If it failed 10 times, throw the error
                        time.sleep(0.5) # Wait half a second and try again

        async with self.lock:
            await asyncio.to_thread(_write_files)

    async def is_processed(self, clan_tag, game_id):
        await self.load_clan(clan_tag)
        return game_id in self.clans[clan_tag.upper()]["processed"]

    async def get_processed_count(self, clan_tag):
        await self.load_clan(clan_tag)
        return len(self.clans[clan_tag.upper()]["processed"])

    async def get_clan_stats(self, clan_tag):
        await self.load_clan(clan_tag)
        return self.clans[clan_tag.upper()]["stats"]

    async def reset_clan_stats(self, clan_tag):
        tag = clan_tag.upper()
        await self.load_clan(tag)
        async with self.lock:
            self.clans[tag]["stats"] = {
                "total_games": 0, "wins": 0, "winstreak": 0, "highest_winstreak": 0, 
                "players": {}, "initial_scan_time": self.clans[tag]["stats"].get("initial_scan_time", 0), 
                "load_time_seconds": self.clans[tag]["stats"].get("load_time_seconds", 0)
            }
            self.clans[tag]["processed"] = []
            self.clans[tag]["matches"] = []
        await self.save_clan(tag)

    async def process_game(self, clan_tag, session_data, info_data, mode="live"):
        tag = clan_tag.upper()
        await self.load_clan(tag)
        
        game_id = session_data.get("gameId")
        if not game_id:
            return False

        if mode in ["live", "backfill"] and game_id in self.clans[tag]["processed"]:
            return False

        is_win = session_data.get("hasWon", False)
        score = session_data.get("score", 0)
        num_teams = session_data.get("numTeams", "?")
        
        config = info_data.get("config", {})
        max_players = config.get("maxPlayers", 0)
        player_teams = config.get("playerTeams", 0)
        
        gamemode_raw = session_data.get("playerTeams", "Unknown")
        if str(gamemode_raw).lower() in ["trios", "quads", "duos"]:
            gamemode = f"{gamemode_raw} ({num_teams} Teams)"
        else:
            gamemode = f"{num_teams} teams of {max_players // player_teams}" if max_players and player_teams else "Unknown Mode"

        all_players = info_data.get("players", [])
        clan_players = [p.get("username", "Unknown") for p in all_players if p.get("clanTag", "").upper() == tag]

        async with self.lock:
            match_record = {
                "gameId": game_id, 
                "start": info_data.get("start"), 
                "end": info_data.get("end"),
                "hasWon": is_win, "score": score, "gamemode": gamemode,
                "totalPlayersInMatch": len(all_players), "clanPlayers": clan_players
            }
            self.clans[tag]["matches"].append(match_record)
            self.clans[tag]["processed"].add(game_id)

            stats = self.clans[tag]["stats"]
            stats["total_games"] = stats.get("total_games", 0) + 1
            if is_win:
                stats["wins"] = stats.get("wins", 0) + 1
                stats["winstreak"] = stats.get("winstreak", 0) + 1
                if stats["winstreak"] > stats.get("highest_winstreak", 0):
                    stats["highest_winstreak"] = stats["winstreak"]
            else:
                stats["winstreak"] = 0

            counted = set()
            for p_name in clan_players:
                if p_name in counted: continue
                counted.add(p_name)
                
                if p_name not in stats["players"]:
                    stats["players"][p_name] = {"games_played": 0, "wins": 0, "winstreak": 0, "highest_winstreak": 0}
                    
                p_stats = stats["players"][p_name]
                p_stats["games_played"] += 1
                if is_win:
                    p_stats["wins"] += 1
                    p_stats["winstreak"] += 1
                    if p_stats["winstreak"] > p_stats["highest_winstreak"]:
                        p_stats["highest_winstreak"] = p_stats["winstreak"]
                else:
                    p_stats["winstreak"] = 0
                    
                p_stats["winrate"] = round((p_stats["wins"] / p_stats["games_played"]) * 100, 2)
                
        if mode == "live":
            await self.save_clan(tag)
        return True
    
    async def finalize_batch_update(self, clan_tag):
        tag = clan_tag.upper()
        await self.load_clan(tag)
        
        async with self.lock:
            # 1. Sort matches chronologically by start time
            self.clans[tag]["matches"].sort(key=lambda x: x.get("start", 0))

            old_stats = self.clans[tag]["stats"]
            
            # 2. Reset stats to recalculate from the sorted match history
            stats = {
                "total_games": 0, "wins": 0, "winstreak": 0, "highest_winstreak": 0, 
                "players": {}, 
                "load_time_seconds": old_stats.get("load_time_seconds", 0),
                "initial_scan_time": old_stats.get("initial_scan_time", 0),
                "latest_cursor": old_stats.get("latest_cursor")
            }
            
            # 3. Re-process every match in the now-sorted list
            for match in self.clans[tag]["matches"]:
                is_win = match.get("hasWon", False)
                stats["total_games"] += 1
                
                if is_win:
                    stats["wins"] += 1
                    stats["winstreak"] += 1
                    if stats["winstreak"] > stats["highest_winstreak"]:
                        stats["highest_winstreak"] = stats["winstreak"]
                else:
                    stats["winstreak"] = 0
                    
                for p_name in match.get("clanPlayers", []):
                    if p_name not in stats["players"]:
                        stats["players"][p_name] = {"games_played": 0, "wins": 0, "winstreak": 0, "highest_winstreak": 0}
                    
                    p_stats = stats["players"][p_name]
                    p_stats["games_played"] += 1
                    if is_win:
                        p_stats["wins"] += 1
                        p_stats["winstreak"] += 1
                        if p_stats["winstreak"] > p_stats["highest_winstreak"]:
                            p_stats["highest_winstreak"] = p_stats["winstreak"]
                    else:
                        p_stats["winstreak"] = 0
                    
                    p_stats["winrate"] = round((p_stats["wins"] / p_stats["games_played"]) * 100, 2)

            self.clans[tag]["stats"] = stats
            
        # 4. Save the sorted matches and updated stats
        await self.save_clan(tag)