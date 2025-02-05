import asyncio
import random
import re
from typing import AsyncGenerator

import structlog
from aiohttp import ClientSession
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import NRSRTranscript

from ..link_queue.schemas import MetaData, URLRecord
from .parent import Scraper

logger = structlog.get_logger()


class TranscriptDownloader(Scraper):
    url: str
    metadata: MetaData

    def __init__(self, data: URLRecord):
        self.url = str(data.url)
        self.metadata = data.metadata

    async def scrape(
        self, client: ClientSession
    ) -> AsyncGenerator[NRSRTranscript, None]:
        async with client.get(self.url) as response:
            await asyncio.sleep(random.uniform(1, 3))
            content = await response.read()
            content_type = response.headers.get("Content-Type", "")
            extension = self.get_extension_from_content_type(content_type)
            yield NRSRTranscript(
                meeting_name=self.metadata.name,
                meeting_num=self.get_meeting_num(),
                snapshot=self.metadata.snapshot if self.metadata.snapshot else None,
                scraped_file=content,
                scraped_file_type=extension,
            )

    async def save(self, item: NRSRTranscript, session: AsyncSession, **kwargs):
        async with session.begin():
            session.add(item)
        await logger.ainfo(f"{item.meeting_name} added to database")

    def get_meeting_num(self) -> int | None:
        number_match = re.findall(
            r"(\d+)\.\s*schôdza",
            str(self.metadata.category),
        )
        if number_match:
            return int(number_match[0])
        return None

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
