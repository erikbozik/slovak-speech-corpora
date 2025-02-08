import re
from datetime import datetime
from typing import AsyncGenerator
from urllib.parse import urlencode

import structlog
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from pydantic import HttpUrl

from src.scraping.link_queue import LinkQueue
from src.scraping.link_queue.schemas import MetaData, URLRecord

from .parent import Scraper

logger = structlog.get_logger()


class RecordingPages(Scraper):
    url: str
    metadata: MetaData

    def __init__(self, data: URLRecord):
        self.url = str(data.url)
        self.metadata = data.metadata

    async def scrape(
        self, client: ClientSession
    ) -> AsyncGenerator[URLRecord | None, None]:
        content = await self.browse(self.url, client)

        async for item in self.parse(content):
            yield item

    async def save(self, item: URLRecord, target_queue: LinkQueue):
        await target_queue.add(item)
        await logger.ainfo(f"{item} added")

    async def browse(self, url: str, client: ClientSession) -> str:
        async with client.get(url) as response:
            content = await response.text()

        return content

    def get_params(self, meeting_date: str):
        params = {"MeetingDate": meeting_date, "DisplayChairman": "true"}
        return params

    async def parse(self, html: str) -> AsyncGenerator[URLRecord | None, None]:
        soup = BeautifulSoup(html, "html.parser")

        h1_element = soup.find("h1")

        if h1_element:
            h1_text = h1_element.get_text(strip=True)
            meeting_num = re.findall(
                r"(\d+)\.\s*schôdza",
                str(h1_text),
            )
            if meeting_num:
                meeting_num = int(meeting_num[0])
            else:
                await logger.aerror(f"Could not parse meeting num from {h1_text}")
                yield None
        else:
            await logger.aerror(
                f"No h1 element found in video recording page. {self.url}"
            )
            yield None

        select_element = soup.find("select", id="SelectedDate")

        if not select_element:
            await logger.aerror(
                f"No select element found in video recording page. {self.url}"
            )
            yield None
        else:
            options = select_element.find_all("option")  # type: ignore
            for option in options:
                value = option.get("value")

                try:
                    date_obj = datetime.strptime(value, "%d%m%Y")
                except ValueError as e:
                    await logger.aerror(f"Date could not be parsed from: {value}")
                    raise e

                url = f"{self.url}?{urlencode(self.get_params(value))}"
                yield URLRecord(
                    url=HttpUrl(url), metadata=MetaData(name=h1_text, snapshot=date_obj)
                )
