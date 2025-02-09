import asyncio
import random
import re
from typing import AsyncGenerator
from urllib.parse import urljoin

import structlog
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from pydantic import HttpUrl

from ..link_queue import LinkQueue
from ..link_queue.schemas import MetaData, URLRecord
from .parent import Scraper

logger = structlog.get_logger()


class TermsRecording(Scraper):
    url: str
    metadata: MetaData

    def __init__(self, data: URLRecord):
        self.url = str(data.url)
        self.metadata = data.metadata

    async def scrape(self, client: ClientSession) -> AsyncGenerator[URLRecord, None]:
        content = await self.browse(self.url, client)
        while content:
            async for meeting in self.parse_meetings(content):
                yield meeting

            next_url = self.get_next_page(content)
            await asyncio.sleep(random.uniform(1, 3))
            if next_url:
                content = await self.browse(next_url, client)
            else:
                content = None

    async def save(self, item: URLRecord, target_queue: LinkQueue):
        await target_queue.add(item)
        await logger.ainfo(f"{item} added")

    async def parse_meetings(self, html: str) -> AsyncGenerator[URLRecord, None]:
        def normalize_filename(text: str) -> str:
            text = text.lower()
            text = re.sub(r"\s+", "_", text)
            text = re.sub(r"[^a-z0-9_\-\.]", "", text)
            return text

        soup = BeautifulSoup(html, "html.parser")
        meeting_divs = soup.find_all("div", class_="item row")

        for div in meeting_divs:
            h2 = div.find("h2", class_="no-tt")
            if h2 is None:
                await logger.awarning("Element <h1> with class 'no-tt' not found.")
                continue

            a_tag = h2.find("a")
            if a_tag is None:
                await logger.awarning("Element <a> withing <h2> not found.")
                continue

            link_href = a_tag.get("href")
            name = a_tag.get_text(strip=True)

            span_tag = h2.find("span")
            if span_tag is None:
                await logger.awarning("Element <span> containing date not found.")
                continue

            date_text = span_tag.get_text(strip=True)

            raw_filename = f"{name}_{date_text}"
            normalized_filename = normalize_filename(raw_filename)

            yield URLRecord(
                url=HttpUrl(urljoin(self.url, link_href)),
                metadata=MetaData(name=normalized_filename),
            )

    def get_next_page(self, html: str) -> str | None:
        soup = BeautifulSoup(html, "html.parser")
        next_url = soup.find(class_="next")

        if not next_url:
            return None

        next_url = next_url.find("a")

        if not next_url:
            return None

        return urljoin(self.url, next_url.get("href"))  # type: ignore

    async def browse(self, url: str, client: ClientSession) -> str:
        async with client.get(url) as response:
            content = await response.text()

        return content
