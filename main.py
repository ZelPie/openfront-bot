import discord
from discord.ext import commands
import ujson as json
import os
from dotenv import load_dotenv

from scripts.clan_manager import ClanDataManager
from scripts.atomic_saver import AtomicSaver

load_dotenv()
intents = discord.Intents.default()
bot = commands.Bot(command_prefix='!', intents=intents)

token = os.getenv('BOT_TOKEN', '')

DATA_FILE = os.path.join(os.path.dirname(__file__), "bot_data", "tracking_data.json")

bot.server_data = {}
bot.clan_manager = ClanDataManager(os.path.dirname(__file__))

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

def save_data():
    AtomicSaver.save_json(DATA_FILE, bot.server_data)

bot.save_data = save_data
load_data()

@bot.event
async def setup_hook():
    await bot.load_extension("scripts.tracking_cmds")
    await bot.load_extension("scripts.stats_cmds")
    await bot.load_extension("scripts.main_loop")
    await bot.load_extension("scripts.load_players")
    await bot.load_extension("scripts.tests")
    await bot.load_extension("scripts.recheck_cmds")
    await bot.load_extension("scripts.testing_commands")
    
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