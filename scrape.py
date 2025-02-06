import asyncio

import structlog
from aiohttp import ClientSession
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from metadata.scraping_metadata import dl_links
from src.async_runners import ScraperRunner
from src.database import Base, async_engine
from src.redis_client import redis_client
from src.scraping.link_queue import LinkQueue

logger = structlog.get_logger()


async def main():
    session_maker = async_sessionmaker(
        async_engine, class_=AsyncSession, expire_on_commit=False
    )

    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    meetings_queue = LinkQueue("terms", redis_client)
    transcripts_queue = LinkQueue("transcripts", redis_client)
    await meetings_queue.add(dl_links)

    runner = ScraperRunner(session_maker)

    meetings_tasks = [
        runner.terms_task(source_queue=meetings_queue, target_queue=transcripts_queue)
        for _ in range(9)
    ]

    await runner.run_tasks(meetings_tasks)

    async with ClientSession() as client:
        transcript_tasks = [
            runner.transcript_task(source_queue=transcripts_queue, http_client=client)
            for _ in range(10)
        ]

        await runner.run_tasks(transcript_tasks)


asyncio.run(main())
