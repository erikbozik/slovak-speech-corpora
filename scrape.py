import asyncio
import logging
from functools import wraps

import structlog
from aiohttp import ClientSession

from metadata.scraping_metadata import dl_links, members_links, recording_links
from src.database import Base, async_engine
from src.redis_client import async_redis_client
from src.runners import ScraperRunner, init_db
from src.scraping.link_queue import LinkQueue

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)
logger = structlog.get_logger()


def with_client_session(func):
    """Decorator to provide an async ClientSession to the wrapped function."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with ClientSession() as client:
            return await func(client, *args, **kwargs)

    return wrapper


@with_client_session
async def main(client: ClientSession | None = None):
    if not client:
        raise ValueError(f"Client is not assigned. Client value: {client}")

    session_maker = await init_db(engine=async_engine, Base=Base)

    meetings_queue = LinkQueue("terms", async_redis_client)
    transcripts_queue = LinkQueue("transcripts", async_redis_client)

    recording_list = LinkQueue("recordings_list", async_redis_client)
    recording_pages = LinkQueue("recording_pages", async_redis_client)
    video_recordings = LinkQueue("video_recordings", async_redis_client)
    nrsr_members_queue = LinkQueue("nrsr_members", async_redis_client)

    await meetings_queue.add(dl_links)
    await recording_list.add(recording_links)
    await nrsr_members_queue.add(members_links)

    runner = ScraperRunner(session_maker)

    meetings_tasks = [
        runner.terms_task(source_queue=meetings_queue, target_queue=transcripts_queue)
        for _ in range(5)
    ]

    await runner.run_tasks(meetings_tasks)

    transcript_tasks = [
        runner.transcript_task(source_queue=transcripts_queue, http_client=client)
        for _ in range(5)
    ]

    await runner.run_tasks(transcript_tasks)
    recording_list_tasks = [
        runner.list_recordings_task(recording_list, recording_pages, http_client=client)
        for _ in range(5)
    ]

    await runner.run_tasks(recording_list_tasks)
    video_recordings_tasks = [
        runner.list_video_recordings_task(
            recording_pages, video_recordings, http_client=client
        )
        for _ in range(5)
    ]
    await runner.run_tasks(video_recordings_tasks)

    video_downloading_tasks = [
        runner.download_video_recordings(video_recordings, client) for _ in range(5)
    ]

    await runner.run_tasks(video_downloading_tasks)

    member_scraping_tasks = [
        runner.get_nrsr_members(nrsr_members_queue, client, redis=async_redis_client)
        for _ in range(5)
    ]

    await runner.run_tasks(member_scraping_tasks)


asyncio.run(main())
