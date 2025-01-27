from typing import Any

import structlog
from redis.asyncio import Redis

from .schemas import URLRecord

logger = structlog.get_logger(level="INFO")


class LinkQueue:
    id: str
    client: Redis

    def __init__(self, id: str, redis_client: Redis) -> None:
        self.id = id
        self.registry_key = f"{id}_registry"
        self.client = redis_client

    async def add(self, records: URLRecord | list[URLRecord]):
        if not isinstance(records, list):
            records = [records]

        async with self.client.pipeline() as pipe:
            for record in records:
                pipe.sismember(self.registry_key, str(record.url))
            result = await pipe.execute()

        async with self.client.pipeline() as pipe:
            for record, already_in in zip(records, result):
                if already_in:
                    logger.warning(f"{record} is already in. Skipping")
                else:
                    pipe.sadd(self.registry_key, str(record.url))
                    pipe.lpush(self.id, record.model_dump_json())
            await pipe.execute()

    async def check_registry(self, url: str) -> bool:
        return bool(await self.client.sismember(self.registry_key, url))  # type: ignore

    async def rollback(self, record: URLRecord):
        logger.warning(f"{record} is being rolled back")
        await self.client.rpush(self.id, record.model_dump_json())  # type: ignore

    async def pop(self) -> Any:
        record = await self.client.brpop(self.id, timeout=1)  # type: ignore
        if not record:
            return None
        return URLRecord.model_validate_json(record[1])

    async def length(self):
        return await self.client.llen(self.id)  # type: ignore
