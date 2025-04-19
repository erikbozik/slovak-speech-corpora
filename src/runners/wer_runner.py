from typing import Generator

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.database import NRSRTranscript

from ..processors import WerProcessor


class WerRunner:
    def __init__(self, session: Session) -> None:
        self.session = session

    def fetch_db(self) -> Generator[NRSRTranscript, None, None]:
        result = self.session.execute(
            select(NRSRTranscript).where(
                NRSRTranscript.whisper_transcript.is_not(None),
            )
        )

        for i in result.scalars():
            yield i

    def run(self) -> None:
        processor = WerProcessor()
        for i in self.fetch_db():
            wer = processor.process(
                i.whisper_transcript,
                i.json_parsed,
            )
            i.wer = wer

            self.session.commit()
