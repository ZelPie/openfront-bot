import discord
from discord import app_commands
from discord.ext import commands
import aiohttp
import io
import urllib.parse
import re

class MapUpload(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="upload-map", description="Physically downloads and uploads a map image directly to chat.")
    @app_commands.describe(map_name="The name of the map (e.g., Between Two Seas)")
    async def upload_map(self, interaction: discord.Interaction, map_name: str):
        await interaction.response.defer()

        # 1. Format the URL
        normalized_map = map_name.lower().strip()
        normalized_map = re.sub(r'\s+', '_', normalized_map)
        normalized_map = re.sub(r'[.()]+', '', normalized_map)
        
        if not normalized_map:
            await interaction.followup.send("Please provide a valid map name.")
            return

        encoded_map = urllib.parse.quote(normalized_map)
        map_url = f"https://openfront.io/maps/{encoded_map}/thumbnail.webp"

        # --- THE FIX: Fake a real web browser ---
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "image/webp,image/apng,image/*,*/*;q=0.8"
        }

        # 2. Download the image using the fake headers
        try:
            # Pass the headers into the session here!
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(map_url, timeout=5) as resp:
                    if resp.status == 200:
                        image_bytes = await resp.read()
                        map_file = discord.File(io.BytesIO(image_bytes), filename=f"{normalized_map}.webp")
                        await interaction.followup.send(content=f"🗺️ **{map_name.title()}**", file=map_file)
                        
                    elif resp.status == 404:
                        await interaction.followup.send(f"Could not find the map '{map_name}' on the server (404 Not Found).")
                    elif resp.status == 403:
                        await interaction.followup.send("403 Forbidden: The server's bot-protection is still blocking the download.")
                    else:
                        await interaction.followup.send(f"Failed to download image. Status code: {resp.status}")
                        
        except Exception as e:
            await interaction.followup.send(f"A network error occurred: {e}")

async def setup(bot):
    await bot.add_cog(MapUpload(bot))