import asyncio

from metadata.scraping_metadata import links
from src.redis_client import redis_client
from src.scraping.crawlers import DLTranscript
from src.scraping.link_queue import LinkQueue


async def scrape_meetings(queue: LinkQueue):
    length = await queue.length()
    while length > 0:
        url = await queue.pop()
        try:
            crawler = DLTranscript(str(url.url))
            await crawler.scrape()
            length = await queue.length()
        except Exception:
            await queue.rollback(url)


async def main():
    meetings_queue = LinkQueue("transcripts", redis_client)
    await meetings_queue.add(links)
    await asyncio.gather(*[scrape_meetings(meetings_queue) for _ in range(3)])


asyncio.run(main())
