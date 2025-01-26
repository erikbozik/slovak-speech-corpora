import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import structlog
from playwright.async_api import async_playwright
from pydantic import HttpUrl

from ..link_queue.schemas import MetaData, URLRecord
from .parent import Scraper

logger = structlog.get_logger()


class DLTranscript(Scraper):
    url: str

    def __init__(self, url: str):
        self.url = url

    async def scrape(self):
        async with async_playwright() as p:
            async for item in self._crawl(p):
                yield item

    async def _crawl(self, p):
        browser = await p.chromium.launch(headless=False)
        try:
            async for item in self._browse(browser):
                yield item
        finally:
            await browser.close()

    async def _browse(self, browser):
        page = await browser.new_page()
        await page.goto(self.url)
        await page.wait_for_selector("//a[text()='Prepis zo schôdze']")
        await page.click("//a[text()='Prepis zo schôdze']")
        current_page = 1
        while True:
            await logger.ainfo(f"Scraping page {current_page}...", url=self.url)
            await page.wait_for_selector("td.doc a")

            elements = await page.query_selector_all("td.doc a")
            for el in elements:
                link = await el.get_attribute("href")

                full_text = (await el.text_content()).strip()
                metadata = await self.get_metadata(full_text)
                yield URLRecord(
                    url=HttpUrl(urljoin(self.url, link)),
                    metadata=metadata,
                )

            current_page += 1
            try:
                await page.click(
                    f"//div[@class='pager']//span[text()='{current_page}']",
                    timeout=3000,
                )
                await asyncio.sleep(2)
            except Exception:
                await logger.ainfo("No more pages.", url=self.url)
                break

    async def get_metadata(self, full_text: str) -> MetaData:
        non_valid_strings = ["Stenografická správa", "Stenozáznam"]
        if any(s in full_text for s in non_valid_strings):
            await logger.awarning(f"{full_text} could not be validly parsed.")
            return MetaData(name=full_text)

        splitted = full_text.split(",")

        if len(splitted) != 3:
            await logger.awarning(f"{full_text} could not be properly splitted.")
            raise ValueError

        name = splitted[0]

        date_match = re.search(
            r"\b(0?[1-9]|[12][0-9]|3[01])\s?\.\s?(0?[1-9]|1[0-2])\s?\.\s?(\d{4})\b",
            full_text,
        )

        category = splitted[1]

        if not date_match:
            await logger.awarning(f"{full_text} does not have snapshot match")
            raise ValueError
        else:
            day, month, year = map(int, date_match.groups())
            snapshot = datetime(year, month, day)

        return MetaData(name=name, category=category, snapshot=snapshot)
