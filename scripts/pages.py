import discord

class LbDisplay(discord.ui.View):
    def __init__(self, data, formatter_func, title="Leaderboard", items_per_page=5, rank_offset=1, extra_footer=""):
        super().__init__(timeout=180) 
        self.data = data
        self.formatter_func = formatter_func
        self.title = title
        self.items_per_page = items_per_page
        self.rank_offset = rank_offset
        self.extra_footer = extra_footer
        
        self.current_page = 0
        self.max_pages = max(1, (len(data) + self.items_per_page - 1) // self.items_per_page)
        self.update_buttons()

    def update_buttons(self):
        self.children[0].disabled = self.current_page == 0
        self.children[1].disabled = self.current_page == self.max_pages - 1

    def format_page(self):
        embed = discord.Embed(
            title=f"{self.title} (Page {self.current_page + 1}/{self.max_pages})", 
            color=discord.Color.green()
        )
        
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        page_data = self.data[start_idx:end_idx]

        description = ""
        # The start= parameter ensures the rank numbers stay accurate across pages
        for i, item in enumerate(page_data, start=start_idx + self.rank_offset):
            description += self.formatter_func(i, item)

        embed.description = description or "No data found."
        
        if self.extra_footer:
            embed.set_footer(text=self.extra_footer)
            
        return embed

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.blurple)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.blurple)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.format_page(), view=self)