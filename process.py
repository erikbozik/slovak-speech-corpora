import os
import sys
from functools import wraps

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "whisperX")))
import logging

import structlog
from aiohttp import ClientSession
from sqlalchemy.orm import sessionmaker

from src.database import Base, async_engine, engine
from src.runners import ParserRunner, TikaRunner, VadRunner, init_db

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)


def with_client_session(func):
    """Decorator to provide an async ClientSession to the wrapped function."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        async with ClientSession() as client:
            return await func(client, *args, **kwargs)

    return wrapper


@with_client_session
async def tika(client: ClientSession | None = None):
    if not client:
        raise Exception("Client is None")
    session_maker = await init_db(engine=async_engine, Base=Base)

    async with session_maker() as session:
        runner = TikaRunner("http://localhost:9998/tika", session, client)
        await runner.run_tika(parallel=20, total=2111)


def parse_to_json():
    Base.metadata.create_all(bind=engine)
    s_maker = sessionmaker(bind=engine)

    with s_maker() as session:
        runner = ParserRunner(session)
        runner.run(10)


def apply_vad():
    Base.metadata.create_all(bind=engine)
    s_maker = sessionmaker(bind=engine)
    with s_maker() as session:
        runner = VadRunner(session)

    runner.run()


apply_vad()
# parse_to_json()

# asyncio.run(tika())
