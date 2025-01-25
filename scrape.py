import asyncio

import structlog

from metadata.scraping_metadata import links
from src.redis_client import redis_client
from src.scraping.crawlers import DLTranscript
from src.scraping.link_queue import LinkQueue

logger = structlog.get_logger()


async def scrape_meetings(queue: LinkQueue):
    length = await queue.length()
    while length > 0:
        url = await queue.pop()
        try:
            crawler = DLTranscript(str(url.url))
            await crawler.scrape()
        except Exception as e:
            await queue.rollback(url)
            await logger.aerror(f"{e}. Error occured, exiting.")
            raise e
        length = await queue.length()


async def main():
    meetings_queue = LinkQueue("transcripts", redis_client)
    await meetings_queue.add(links)

    async with asyncio.TaskGroup() as group:
        for _ in range(3):
            group.create_task(scrape_meetings(meetings_queue))


asyncio.run(main())
