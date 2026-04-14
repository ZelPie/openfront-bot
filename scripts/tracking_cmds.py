import discord
from discord.ext import commands
from discord import app_commands
import re
from datetime import datetime, timezone
from .pages import LbDisplay

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
        channel_name = str(interaction.channel)

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
            "track_losses": track_losses,
            "channel_name": channel_name,
            "initial_scan_time": int(datetime.now(timezone.utc).timestamp() * 1000)
        }
        
        self.bot.server_data[guild_id]["trackers"].append(new_tracker)
        self.bot.save_data() 
        
        loss_text = "and losses" if track_losses else "(wins only)"
        await interaction.response.send_message(f"Now tracking matches {loss_text} for **[{tag_upper}]** in {channel.mention}!")

        all_clans = set()

        for guild in self.bot.server_data:
            for tracker in self.bot.server_data[guild]["trackers"]:
                all_clans.add(tracker["clan_tag"])

        print(f"New tracker for [{tag_upper}] in server {server_name} (ID: {guild_id}). Server tracking {len(self.bot.server_data[guild_id]['trackers'])} clans.\nTotal clans being tracked: {len(all_clans)} | Total servers: {len(self.bot.server_data)}")

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


    @app_commands.command(name="list_trackers", description="List all tracked clans and their specific channel settings.")
    async def list_trackers(self, interaction: discord.Interaction):
        guild_id = interaction.guild_id

        # Safely get the guild data
        guild_info = self.bot.server_data.get(guild_id, {})
        trackers = guild_info.get("trackers", [])
        
        if not trackers:
            await interaction.response.send_message("No active trackers in this server.", ephemeral=True)
            return

        # Grouping logic: { 'UN': [{'name': '#general', 'losses': True}, ...] }
        grouped_trackers = {}
        for t in trackers:
            tag = t.get("clan_tag", "UNKNOWN").upper()
            
            # Determine the channel name
            channel_id = t.get("channel_id")
            channel = self.bot.get_channel(channel_id)
            if channel:
                name = f"#{channel.name}"
            else:
                name = t.get("channel_name", f"Unknown ({channel_id})")

            # Get tracking preference
            track_losses = t.get("track_losses", False)
                    
            if tag not in grouped_trackers:
                grouped_trackers[tag] = []
            
            # Check if this specific channel/setting combo is already added
            exists = any(item['name'] == name and item['losses'] == track_losses for item in grouped_trackers[tag])
            if not exists:
                grouped_trackers[tag].append({'name': name, 'losses': track_losses})

        # Sort alphabetically by tag
        sorted_items = sorted(grouped_trackers.items())

        def format_tracker(rank, item):
            tag, channel_list = item
            
            channel_lines = []
            for c in channel_list:
                # Create a label for the tracking mode
                mode = "Wins + Losses" if c['losses'] else "Wins Only"
                channel_lines.append(f"`{c['name']}` ({mode})")

            channels_str = "\n> " + "\n> ".join(channel_lines)
            return f"\n\n**{rank}. [{tag}]**{channels_str}"
        
        # Use LbDisplay for pagination
        view = LbDisplay(
            data=sorted_items, 
            formatter_func=format_tracker, 
            title=f"Active Trackers in {interaction.guild.name}", 
            items_per_page=5
        )
        
        await interaction.response.send_message(embed=view.format_page(), view=view)

async def setup(bot):
    await bot.add_cog(TrackingCmds(bot))