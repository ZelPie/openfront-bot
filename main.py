import discord
from discord.ext import commands
import json
import os
from dotenv import load_dotenv

load_dotenv()
intents = discord.Intents.default()
bot = commands.Bot(command_prefix='!', intents=intents)

token = os.getenv('BOT_TOKEN', '')

DATA_FILE = os.path.join(os.path.dirname(__file__), "bot_data", "tracking_data.json")
PLAYER_DATA_FILE = os.path.join(os.path.dirname(__file__), "bot_data", "player_data.json")
PROCESSED_GAMES_DIR = os.path.join(os.path.dirname(__file__), "bot_data", "processed_games")
LEGACY_PROCESSED_FILE = os.path.join(os.path.dirname(__file__), "bot_data", "processed_games.json")

os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
os.makedirs(PROCESSED_GAMES_DIR, exist_ok=True)

# Centralized data dictionaries
bot.server_data = {}
bot.player_data = {}
bot.processed_games = {} 

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            raw_data = json.load(f)
            for guild_id_str, data in raw_data.items():
                guild_id = int(guild_id_str)
                if isinstance(data, dict) and "trackers" not in data:
                    bot.server_data[guild_id] = {"server_name": "Unknown Server", "trackers": [data]}
                elif isinstance(data, list):
                    bot.server_data[guild_id] = {"server_name": "Unknown Server", "trackers": data}
                else:
                    bot.server_data[guild_id] = data
            print(f"Loaded tracking data for {len(bot.server_data)} servers.")
    
    if os.path.exists(PLAYER_DATA_FILE):
        with open(PLAYER_DATA_FILE, "r") as f:
            bot.player_data = json.load(f)
            print(f"Loaded player statistics for {len(bot.player_data)} clans.")

    if os.path.exists(LEGACY_PROCESSED_FILE):
        print("Migrating legacy processed_games.json to separate clan files...")
        with open(LEGACY_PROCESSED_FILE, "r") as f:
            bot.processed_games = json.load(f)
            
        for clan_tag, games in bot.processed_games.items():
            filepath = os.path.join(PROCESSED_GAMES_DIR, f"{clan_tag}.json")
            with open(filepath, "w") as out_f:
                json.dump(games, out_f)
                
        os.rename(LEGACY_PROCESSED_FILE, LEGACY_PROCESSED_FILE + ".bak")
        print("Migration complete! Old file renamed to .bak")
    else:
        for filename in os.listdir(PROCESSED_GAMES_DIR):
            if filename.endswith(".json"):
                clan_tag = filename[:-5] 
                filepath = os.path.join(PROCESSED_GAMES_DIR, filename)
                try:
                    with open(filepath, "r") as f:
                        bot.processed_games[clan_tag] = json.load(f)
                except Exception as e:
                    print(f"Error loading {filename}: {e}")
                    
    total_games = sum(len(games) for games in bot.processed_games.values())
    print(f"Loaded {total_games} processed games across {len(bot.processed_games)} clans.")

def save_data():
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(bot.server_data, f, indent=4) 
    with open(PLAYER_DATA_FILE, "w") as f:
        json.dump(bot.player_data, f, indent=4) 
        
    os.makedirs(PROCESSED_GAMES_DIR, exist_ok=True)
    for clan_tag, games in bot.processed_games.items():
        filepath = os.path.join(PROCESSED_GAMES_DIR, f"{clan_tag}.json")
        with open(filepath, "w") as f:
            json.dump(games, f)

bot.save_data = save_data
load_data()

@bot.event
async def setup_hook():
    await bot.load_extension("scripts.tracking_cmds")
    await bot.load_extension("scripts.stats_cmds")
    await bot.load_extension("scripts.main_loop")
    await bot.load_extension("scripts.load_players")
    
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash command(s).")
    except Exception as e:
        print(f"Failed to sync commands: {e}")

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name}')
    
    names_updated = False
    for guild in bot.guilds:
        if guild.id in bot.server_data:
            if bot.server_data[guild.id].get("server_name") != guild.name:
                bot.server_data[guild.id]["server_name"] = guild.name
                names_updated = True

    if names_updated:
        bot.save_data()
        print("Updated server names in tracking data.")

@bot.command()
@commands.is_owner() 
async def sync(ctx):
    try:
        synced = await bot.tree.sync()
        await ctx.send(f"Forced Sync: {len(synced)} commands registered!")
    except Exception as e:
        await ctx.send(f"Sync failed: {e}")

bot.run(token)