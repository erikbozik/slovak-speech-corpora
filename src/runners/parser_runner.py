from datetime import date
from typing import Generator

from redis.asyncio import Redis
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from src.database import NRSRTranscript
from src.redis_client import redis_client
from ..processors import TranscriptParser


class ParserRunner:
    session: Session

    def __init__(self, session: Session):
        self.session = session

    def fetch_db(self, n: int) -> Generator[NRSRTranscript, None, None]:
        result = self.session.execute(
            select(NRSRTranscript)
            .where(
                and_(
                    NRSRTranscript.json_parsed == None,  # noqa
                    NRSRTranscript.scraped_file_type == "docx",
                    NRSRTranscript.snapshot > date(2010, 1, 1),
                )
            )
            .limit(n)
        )

        for i in result.scalars():
            yield i

    async def run(self, n: int, redis: Redis):
        records = [i for i in self.fetch_db(n)]

        while records:
            await self.transform_records(records, redis)
            # self.session.commit()
            records = [i for i in self.fetch_db(n)]

    async def transform_records(self, records: list[NRSRTranscript], redis: Redis):
        parsers = [TranscriptParser(str(i.xhtml_parsed), redis_client) for i in records]

        for i in parsers:
            await i.parse()
        # with ProcessPoolExecutor() as executor:
        #     tasks = [executor.submit(parser.parse) for parser in parsers]

        # for future, record in tqdm(zip(tasks, records)):
        #     record.json_parsed = future.result()  # type: ignore
