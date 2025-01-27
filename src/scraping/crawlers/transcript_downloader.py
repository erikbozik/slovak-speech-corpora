import asyncio
import random
import re
import unicodedata

from aiohttp import ClientSession

from ..link_queue import FileRecord
from ..link_queue.schemas import MetaData, URLRecord
from .parent import Scraper


class TranscriptDownloader(Scraper):
    url: str
    metadata: MetaData

    def __init__(self, data: URLRecord):
        self.url = str(data.url)
        self.metadata = data.metadata

    async def scrape(self, client: ClientSession):
        async with client.get(self.url) as response:
            await asyncio.sleep(random.uniform(1, 3))
            content = await response.read()
            content_type = response.headers.get("Content-Type", "")
            extension = self.get_extension_from_content_type(content_type)
            yield FileRecord(name=self.create_filename(extension), content=content)

    def create_filename(self, extension: str) -> str:
        name = self.metadata.name
        category = self.metadata.category if self.metadata.category else ""
        date = (
            self.metadata.snapshot.strftime("%Y-%m-%d")
            if self.metadata.snapshot
            else ""
        )
        final = f"{name}_{category}_{date}"
        final = unicodedata.normalize("NFKD", final)
        final = re.sub(r"\s+", "", final).lower()
        final = final.encode("ascii", "ignore").decode("ascii")
        return f"{final}.{extension}"

    @staticmethod
    def get_extension_from_content_type(content_type: str) -> str:
        if (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            in content_type
        ):
            return "docx"
        elif "application/msword" in content_type:
            return "doc"
        elif "text/html" in content_type:
            return "html"
        else:
            return "bin"
