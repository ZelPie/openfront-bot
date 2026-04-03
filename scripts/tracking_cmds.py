import discord
from discord.ext import commands
from discord import app_commands
import re

class TrackingCmds(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    # Command allows the user to start tracking a clan's new matches in a specific channel, with an option to track losses as well
    @app_commands.command(name="track", description="Set up advanced match tracking for a clan.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., CAF)", channel="The channel to post updates in", track_losses="Post match losses? (Default: False)")
    async def track_clan(self, interaction: discord.Interaction, clan_tag: str, channel: discord.TextChannel, track_losses: bool = False):
        guild_id = interaction.guild_id
        tag_upper = clan_tag.upper()
        server_name = interaction.guild.name

        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message("You don't have permission to manage channels, which is required to set up tracking.", ephemeral=True)
            return

        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)  # Sanitize input to prevent issues

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return
        
        if guild_id not in self.bot.server_data:
            self.bot.server_data[guild_id] = {"server_name": server_name, "trackers": []}
        else:
            self.bot.server_data[guild_id]["server_name"] = server_name
            
        for tracker in self.bot.server_data[guild_id]["trackers"]:
            if tracker["clan_tag"] == tag_upper and tracker["channel_id"] == channel.id:
                tracker["track_losses"] = track_losses
                self.bot.save_data()
                await interaction.response.send_message(f"Updated tracker for **[{tag_upper}]** in {channel.mention}. Tracking losses: `{track_losses}`.")
                return

        new_tracker = {
            "channel_id": channel.id,
            "clan_tag": tag_upper,
            "api_url": f"https://api.openfront.io/public/clan/{tag_upper.lower()}/sessions", 
            "last_session_id": None,
            "track_losses": track_losses
        }
        
        self.bot.server_data[guild_id]["trackers"].append(new_tracker)
        self.bot.save_data() 
        
        loss_text = "and losses" if track_losses else "(wins only)"
        await interaction.response.send_message(f"Now tracking matches {loss_text} for **[{tag_upper}]** in {channel.mention}!")

    # Command allows users to stop the tracking of a clan in a specific channel
    @app_commands.command(name="untrack", description="Stop tracking a clan in a specific channel.")
    @app_commands.describe(clan_tag="The clan's tag (e.g., CAF)", channel="The channel to stop posting updates in")
    async def untrack_clan(self, interaction: discord.Interaction, clan_tag: str, channel: discord.TextChannel):
        if not interaction.user.guild_permissions.manage_channels:
            await interaction.response.send_message("You don't have permission to manage channels, which is required to remove tracking.", ephemeral=True)
            return

        guild_id = interaction.guild_id
        tag_upper = clan_tag.upper()

        tag_upper = re.sub(r'[^A-Za-z0-9]', '', tag_upper)  # Sanitize input to prevent issues

        if len(tag_upper) == 0 or len(tag_upper) > 5:
            await interaction.response.send_message("Please provide a valid clan tag (1-5 alphanumeric characters).", ephemeral=True)
            return

        if guild_id not in self.bot.server_data or not self.bot.server_data[guild_id].get("trackers"):
            await interaction.response.send_message(f"This server isn't tracking any clans yet!", ephemeral=True)
            return

        trackers = self.bot.server_data[guild_id]["trackers"]
        original_length = len(trackers)

        self.bot.server_data[guild_id]["trackers"] = [
            t for t in trackers 
            if not (t["clan_tag"] == tag_upper and t["channel_id"] == channel.id)
        ]

        if len(self.bot.server_data[guild_id]["trackers"]) < original_length:
            self.bot.save_data()
            await interaction.response.send_message(f"Successfully stopped tracking **[{tag_upper}]** in {channel.mention}.")
        else:
            await interaction.response.send_message(f"Could not find an active tracker for **[{tag_upper}]** in {channel.mention}.", ephemeral=True)

async def setup(bot):
    await bot.add_cog(TrackingCmds(bot))