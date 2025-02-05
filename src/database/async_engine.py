from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from ..schemas import DatabaseSettings

async_engine = create_async_engine(
    URL.create(drivername="postgresql+asyncpg", **DatabaseSettings().model_dump())  # type: ignore
)
