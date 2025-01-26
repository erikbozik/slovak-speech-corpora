import asyncio
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
                yield URLRecord(
                    url=HttpUrl(urljoin(self.url, link)),
                    metadata=MetaData(name="dummy"),
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
