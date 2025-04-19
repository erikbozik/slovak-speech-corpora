from typing import Generator

import structlog
from sqlalchemy import String, cast, func, literal, select
from sqlalchemy.orm import Session, aliased

from src.database import NRSRRecording, NRSRTranscript

from ..processors import ForceAligner
from ..schemas import RecordingToProcess

logger = structlog.get_logger()


class AlignerRunner:
    aligner: ForceAligner

    def __init__(self, session: Session) -> None:
        self.session = session
        self.aligner = ForceAligner()

    def fetch_db(self) -> Generator[RecordingToProcess, None, None]:
        nr = aliased(NRSRRecording)
        tran = aliased(NRSRTranscript)

        result = self.session.execute(
            select(
                nr.id,
                (
                    cast(nr.meeting_num, String)
                    + literal("_")
                    + func.to_char(nr.snapshot, "DD-MM-YYYY")
                    + literal(".mp3")
                ).label("filename"),
                tran.whisper_transcript,
            )
            .select_from(nr)
            .join(
                tran,
                (tran.meeting_num == nr.meeting_num) & (tran.snapshot == nr.snapshot),
                isouter=True,
            )
            .where(tran.json_parsed.isnot(None) & tran.whisper_transcript.isnot(None))
            .order_by(func.random())
        )

        for i in result:
            yield RecordingToProcess(
                id=i.id, filename=i.filename, transcript=i.whisper_transcript
            )

    def fetch_transcript(self, recording_id: int) -> NRSRTranscript:
        return self.session.get(NRSRTranscript, recording_id)

    def run_align_whisper(self):
        for i in self.fetch_db():
            audio = self.aligner.load_audio(file_path=i.file_path)
            logger.debug("Aligning", file_path=i.file_path)
            aligned = self.aligner.transform_record(
                audio,
                i.transcript["segments"],  # type: ignore
            )
            logger.debug("Aligned", file_path=i.file_path)
            transcript = self.fetch_transcript(i.id)
            transcript.word_timestamps_whisper = aligned  # type: ignore
            self.session.commit()
