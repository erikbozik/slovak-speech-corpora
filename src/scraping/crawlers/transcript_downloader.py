import asyncio
import random

from aiohttp import ClientSession

from .parent import Scraper


class TranscriptDownloader(Scraper):
    def __init__(self, url: str):
        self.url = url

    async def scrape(self, client: ClientSession):
        async with client.get(self.url) as response:
            await asyncio.sleep(random.uniform(1, 5))
            yield await response.read()
