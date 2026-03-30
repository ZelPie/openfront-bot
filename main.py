import discord
from discord.ext import commands
import json
import os
from dotenv import load_dotenv

load_dotenv()
intents = discord.Intents.default()
bot = commands.Bot(command_prefix='!', intents=intents)

token = os.getenv('BOT_TOKEN')

DATA_FILE = os.path.join(os.path.dirname(__file__), "bot_data", "tracking_data.json")
PLAYER_DATA_FILE = os.path.join(os.path.dirname(__file__), "bot_data", "player_data.json")
LOADED_PLAYER_DATA = os.path.join(os.path.dirname(__file__), "bot_data", "loaded_player_data.json")
PROCESSED_GAMES_FILE = os.path.join(os.path.dirname(__file__), "bot_data", "processed_games.json")

if not os.path.exists(os.path.dirname(DATA_FILE)):
    os.makedirs(os.path.dirname(DATA_FILE))

# Attach data directly to the bot so it can be accessed from any Cog
bot.server_data = {}
bot.player_data = {}
bot.loaded_player_data = {}
bot.processed_games = {} # Global tracking list for game IDs

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            raw_data = json.load(f)
            for guild_id_str, data in raw_data.items():
                guild_id = int(guild_id_str)
                # Auto-migration
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
    
    if os.path.exists(LOADED_PLAYER_DATA):
        with open(LOADED_PLAYER_DATA, "r") as f:
            bot.loaded_player_data = json.load(f)
            print(f"Loaded cached player data for {len(bot.loaded_player_data)} clans.")
            
    if os.path.exists(PROCESSED_GAMES_FILE):
        with open(PROCESSED_GAMES_FILE, "r") as f:
            bot.processed_games = json.load(f)
            # Count the total games across all clan lists
            total_games = sum(len(games) for games in bot.processed_games.values())
            print(f"Loaded {total_games} processed games across {len(bot.processed_games)} clans.")
    


# Attach the save function to the bot as well
def save_data():
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(bot.server_data, f, indent=4) 
    with open(PLAYER_DATA_FILE, "w") as f:
        json.dump(bot.player_data, f, indent=4) 
    with open(LOADED_PLAYER_DATA, "w") as f:
        json.dump(bot.loaded_player_data, f, indent=4)
    with open(PROCESSED_GAMES_FILE, "w") as f:
        # Keep only the last 5000 games so the file doesn't grow infinitely large
        save_processed = {
            clan: games[-5000:] for clan, games in bot.processed_games.items()
        }
        json.dump(save_processed, f, indent=4)

bot.save_data = save_data
load_data()

# --- COG LOADING & EVENTS ---
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