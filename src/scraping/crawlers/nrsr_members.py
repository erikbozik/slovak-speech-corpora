from typing import AsyncGenerator

import structlog
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import Members
from src.scraping.link_queue.schemas import MetaData, URLRecord

from .parent import Scraper

logger = structlog.get_logger()


class NRSRMembers(Scraper):
    url: str
    metadata: MetaData

    def __init__(self, data: URLRecord):
        self.url = str(data.url)
        self.metadata = data.metadata

    async def scrape(
        self, client: ClientSession
    ) -> AsyncGenerator[Members | None, None]:
        async with client.get(self.url) as response:
            content = await response.text()

        async for item in self.parse(content):
            yield item

    async def save(self, item: Members, session: AsyncSession, redis: Redis):
        async with session.begin():
            session.add(item)
            await redis.sadd("members", str(item.name), str(item.surname))  # type: ignore
        await session.commit()

    async def parse(self, content: str) -> AsyncGenerator[Members | None, None]:
        soup = BeautifulSoup(content, "html.parser")

        for a in soup.select("div.mps_list_block ul li a"):
            full_name = a.get_text(strip=True)
            if "," in full_name:
                surname, first_name = [part.strip() for part in full_name.split(",", 1)]
            else:
                surname, first_name = "", full_name
            member = Members(
                name=first_name, surname=surname, term=int(self.metadata.name)
            )
            await logger.ainfo(f"{member.name}, {member.surname} added to database")
            yield member
