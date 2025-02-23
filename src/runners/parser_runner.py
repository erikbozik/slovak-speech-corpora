from concurrent.futures import ProcessPoolExecutor
from datetime import date
from typing import Generator

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from src.database import NRSRTranscript

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

    def run(self, n: int):
        records = [i for i in self.fetch_db(n)]

        while records:
            self.transform_records(records)

            self.session.commit()
            records = [i for i in self.fetch_db(n)]

    def transform_records(self, records: list[NRSRTranscript]):
        parsers = [TranscriptParser(str(i.xhtml_parsed)) for i in records]

        with ProcessPoolExecutor() as pool:
            futures = [pool.submit(i.parse) for i in parsers]

        for transcript_future, record in zip(futures, records):
            record.json_parsed = transcript_future.result()  # type: ignore
