# import discord
# from discord.ext import tasks, commands
# from discord import app_commands
# import aiohttp
# import asyncio
# from datetime import datetime, timedelta, timezone

# class FFALoop(commands.Cog):
#     def __init__(self, bot):
#         self.bot = bot
#         if not hasattr(bot, 'ffa_loop_task'):
#             bot.ffa_loop_task = self.ffa_loop.start()
        
#         self.live_queue = asyncio.Queue()
#         self.queued_games = set()  # To track which games are currently queued
#         self.match_details_cache = {}  # Cache for match details to avoid redundant API calls

#         self.worker_task = self.bot.loop.create_task(self.live_worker())

#         def script_unload():
#             self.ffa_loop.cancel()
#             if hasattr(self.bot, 'ffa_loop_task'):
#                 self.bot.ffa_loop_task.cancel()
        
#         async def create
        