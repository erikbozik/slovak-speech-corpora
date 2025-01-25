import asyncio
from urllib.parse import urljoin

import structlog
from playwright.async_api import async_playwright

logger = structlog.get_logger()


class DLTranscript:
    url: str

    def __init__(self, url: str):
        self.url = url

    async def scrape(self):
        async with async_playwright() as p:
            return await self._crawl(p)

    async def _crawl(self, p):
        browser = await p.chromium.launch(headless=False)
        try:
            return await self._browse(browser)
        finally:
            await browser.close()

    async def _browse(self, browser):
        page = await browser.new_page()
        await page.goto(self.url)
        await page.wait_for_selector("//a[text()='Prepis zo schôdze']")
        await page.click("//a[text()='Prepis zo schôdze']")
        all_links = []
        current_page = 1
        while True:
            await logger.ainfo(f"Scraping page {current_page}...", url=self.url)
            await page.wait_for_selector("td.doc a")

            elements = await page.query_selector_all("td.doc a")
            for el in elements:
                link = await el.get_attribute("href")
                all_links.append(link)

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

        await logger.ainfo(f"Total links: {len(all_links)}", url=self.url)

        return [urljoin(self.url, link) for link in all_links]
