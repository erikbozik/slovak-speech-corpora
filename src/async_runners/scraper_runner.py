import asyncio
import random
from typing import Any, Type

import structlog
from aiohttp import ClientSession
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.scraping.crawlers import (
    DLTranscript,
    RecordingPages,
    Scraper,
    TermsRecording,
    TranscriptDownloader,
    VideoDownloader,
)
from src.scraping.link_queue import LinkQueue, URLRecord

logger = structlog.get_logger()


class ScraperRunner:
    session_maker: async_sessionmaker[AsyncSession]

    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.session_maker = session_maker

    async def run_tasks(self, tasks: list):
        async with asyncio.TaskGroup() as group:
            for task in tasks:
                group.create_task(task)

    async def scrape(
        self,
        queue: LinkQueue,
        scraper: Type[Scraper],
        scraping_kwargs: dict[str, Any] = {},
        saving_kwargs: dict[str, Any] = {},
    ):
        try:
            item_to_scrape: URLRecord = await queue.pop()
            while item_to_scrape:
                crawler = scraper(item_to_scrape)
                async for item in crawler.scrape(**scraping_kwargs):
                    if item:
                        await crawler.save(item, **saving_kwargs)
                    else:
                        await logger.awarning(f"{item} parsed None")

                    await asyncio.sleep(random.uniform(1, 3))

                item_to_scrape: URLRecord = await queue.pop()
        except (asyncio.exceptions.CancelledError, Exception) as e:
            await queue.rollback(item_to_scrape)
            raise e

    async def transcript_task(
        self, source_queue: LinkQueue, http_client: ClientSession
    ):
        async with self.session_maker() as session:
            await self.scrape(
                source_queue,
                TranscriptDownloader,
                scraping_kwargs={"client": http_client},
                saving_kwargs={"session": session},
            )

    async def terms_task(self, source_queue: LinkQueue, target_queue: LinkQueue):
        await self.scrape(
            source_queue,
            DLTranscript,
            saving_kwargs={"target_queue": target_queue},
        )

    async def list_recordings_task(
        self,
        source_queue: LinkQueue,
        target_queue: LinkQueue,
        http_client: ClientSession,
    ):
        await self.scrape(
            source_queue,
            TermsRecording,
            scraping_kwargs={"client": http_client},
            saving_kwargs={"target_queue": target_queue},
        )

    async def list_video_recordings_task(
        self,
        source_queue: LinkQueue,
        target_queue: LinkQueue,
        http_client: ClientSession,
    ):
        await self.scrape(
            source_queue,
            RecordingPages,
            scraping_kwargs={"client": http_client},
            saving_kwargs={"target_queue": target_queue},
        )

    async def download_video_recordings(
        self, source_queue: LinkQueue, http_client: ClientSession
    ):
        async with self.session_maker() as session:
            await self.scrape(
                source_queue,
                VideoDownloader,
                scraping_kwargs={"client": http_client},
                saving_kwargs={"session": session},
            )
