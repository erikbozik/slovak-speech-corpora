import asyncio
from typing import Any, AsyncGenerator, Type

import structlog

from metadata.scraping_metadata import links
from src.redis_client import redis_client
from src.scraping.crawlers import DLTranscript, Scraper
from src.scraping.link_queue import LinkQueue, URLRecord

logger = structlog.get_logger()


async def scrape(queue: LinkQueue, scraper: Type[Scraper]) -> AsyncGenerator[Any, None]:
    length = await queue.length()
    while length > 0:
        item_to_scrape: URLRecord = await queue.pop()
        try:
            crawler = scraper(str(item_to_scrape.url))
            async for item in crawler.scrape():
                yield item
        except Exception as e:
            await logger.awarning(f"Rolling back {item_to_scrape}")
            await queue.rollback(item_to_scrape)
            await logger.aerror(f"{e}. Error occured, exiting.")
            raise e


async def push_result_to_queue(
    source_queue: LinkQueue, target_queue: LinkQueue, scraper: Type[Scraper]
):
    async for item in scrape(source_queue, scraper):
        await target_queue.add(item)
        await logger.ainfo(f"{item} added")


async def main():
    meetings_queue = LinkQueue("meetings", redis_client)
    transcripts_queue = LinkQueue("transcripts", redis_client)

    await meetings_queue.add(links)

    async with asyncio.TaskGroup() as group:
        for _ in range(3):
            group.create_task(
                push_result_to_queue(meetings_queue, transcripts_queue, DLTranscript)
            )


asyncio.run(main())
