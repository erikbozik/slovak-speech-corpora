import asyncio
import os
from pathlib import Path
from typing import Any, AsyncGenerator, Type

import aiofiles
import aiohttp
import structlog

from metadata.scraping_metadata import links
from src.redis_client import redis_client
from src.scraping.crawlers import DLTranscript, Scraper, TranscriptDownloader
from src.scraping.link_queue import LinkQueue, URLRecord

logger = structlog.get_logger()
LOCK = asyncio.Lock()
ACTIVE = {}
TRANSCRIPT_DIR = Path("data") / "nrsr" / "transcripts"
os.makedirs(TRANSCRIPT_DIR, exist_ok=True)


async def scrape(
    queue: LinkQueue, scraper: Type[Scraper], task_id: int, **kwargs
) -> AsyncGenerator[Any, None]:
    length = await queue.length()
    while length > 0:
        item_to_scrape: URLRecord = await queue.pop()

        if not item_to_scrape:
            break

        async with LOCK:
            ACTIVE[task_id] = item_to_scrape

        crawler = scraper(item_to_scrape)
        async for item in crawler.scrape(**kwargs):
            yield item


async def push_result_to_queue(
    source_queue: LinkQueue,
    target_queue: LinkQueue,
    scraper: Type[Scraper],
    task_id: int,
):
    async for item in scrape(source_queue, scraper, task_id):
        await target_queue.add(item)
        await logger.ainfo(f"{item} added")


async def save_result(
    source_queue: LinkQueue, scraper: Type[Scraper], task_id: int, **kwargs
):
    async for item in scrape(source_queue, scraper, task_id=task_id, **kwargs):
        filename = Path(TRANSCRIPT_DIR) / item.name
        async with aiofiles.open(filename, mode="wb") as file:
            await file.write(item.content)
            await logger.ainfo(f"{filename} saved")


async def start_terms(meetings_queue: LinkQueue, transcripts_queue: LinkQueue):
    async with asyncio.TaskGroup() as group:
        for i in range(3):
            group.create_task(
                push_result_to_queue(
                    meetings_queue, transcripts_queue, DLTranscript, task_id=i
                )
            )


async def start_transcripts(transcripts_queue: LinkQueue):
    async with aiohttp.ClientSession() as client:
        async with asyncio.TaskGroup() as group:
            for i in range(3):
                group.create_task(
                    save_result(
                        transcripts_queue,
                        TranscriptDownloader,
                        task_id=i,
                        client=client,
                    )
                )


async def main():
    meetings_queue = LinkQueue("terms", redis_client)
    transcripts_queue = LinkQueue("transcripts", redis_client)

    await meetings_queue.add(links)
    try:
        await start_terms(meetings_queue, transcripts_queue)
    except Exception as e:
        for item in ACTIVE.values():
            await meetings_queue.rollback(item)
        raise e
    finally:
        ACTIVE.clear()

    try:
        await start_transcripts(transcripts_queue)
    except Exception as e:
        for item in ACTIVE.values():
            await transcripts_queue.rollback(item)
        raise e
    finally:
        ACTIVE.clear()


asyncio.run(main())
