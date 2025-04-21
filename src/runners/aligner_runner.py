from typing import Any, Generator

import structlog
from sqlalchemy import String, cast, desc, func, literal, select
from sqlalchemy.orm import Session, aliased

from src.database import NRSRRecording, NRSRTranscript

from ..processors import ForceAligner

logger = structlog.get_logger()

FILENAME = "/mnt/bigben/nrsr_recordings"


class AlignerRunner:
    aligner: ForceAligner

    def __init__(self, session: Session) -> None:
        self.session = session
        self.aligner = ForceAligner()

    def fetch_db(self) -> Generator[Any, None, None]:
        nr = aliased(NRSRRecording)
        tran = aliased(NRSRTranscript)

        filename = (
            cast(tran.meeting_num, String)
            + literal("_")
            + func.to_char(
                tran.snapshot, "DD-MM-YYYY"
            )  # ← single quotes for the format mask
            + literal(".mp3")
        ).label("filename")

        result = self.session.execute(
            select(tran, filename, nr.vad_segments)  # full transcript + filename
            .join(  # INNER JOIN ⇒ intersection
                nr,
                (tran.meeting_num == nr.meeting_num) & (tran.snapshot == nr.snapshot),
            )
            # .where(tran.id == 1033)
            # .where(tran.json_parsed.isnot(None))
            .where(tran.json_parsed.isnot(None))
            # .where(tran.json_parsed.isnot(None) & tran.whisper_transcript.isnot(None))
            .order_by(desc(tran.aligned_segments), tran.whisper_transcript)
        )

        for i in result:
            yield i

    def fetch_transcript(self, transcript_id: int) -> NRSRTranscript:
        return self.session.get(NRSRTranscript, transcript_id)

    @staticmethod
    def plus_n(iterable: list, index: int):
        return range(
            min(len(iterable), index + 1),
            min(len(iterable), index + 4),
        )

    @staticmethod
    def minus_n(index: int):
        return range(max(0, index - 3), index)

    def run(self):
        for i, filename, vad_segments in self.fetch_db():
            file_path = f"{FILENAME}/{filename}"
            audio = None
            # audio = self.aligner.load_audio(file_path=file_path)

            # result = self.aligner.force_align_entire(
            #     audio=audio, segments=i.json_parsed, vad=vad_segments
            # )
            # i.aligned_segments = result
            # self.session.commit()

            if not i.whisper_transcript:
                audio = self.aligner.load_audio(file_path=file_path)
                trans = self.aligner.transcribe(audio)
                transcript = self.fetch_transcript(i.id)
                transcript.whisper_transcript = trans  # type: ignore
                self.session.commit()

            if not i.word_timestamps_whisper:
                if audio is None:
                    audio = self.aligner.load_audio(file_path=file_path)
                logger.debug("Aligning", file_path=file_path)
                aligned = self.aligner.force_align(
                    audio,
                    i.whisper_transcript["segments"],  # type: ignore
                )
                logger.debug("Aligned", file_path=file_path)
                transcript = self.fetch_transcript(i.id)
                transcript.word_timestamps_whisper = aligned  # type: ignore
                self.session.commit()
            if not i.aligned_segments:
                logger.info(f"{i.id}, {i.meeting_num}, {i.snapshot}")
                gt = i.json_parsed
                wt = i.word_timestamps_whisper["segments"]
                aligned = self.aligner.align(
                    gt,
                    wt,
                )
                segments = self.aligner.segment(
                    aligned=aligned,
                    gt_words=[
                        token for entry in gt for token in entry["transcript"].split()
                    ],
                )
                transcript = self.fetch_transcript(i.id)
                transcript.aligned_segments = segments  # type: ignore
                self.session.commit()

            # if not i.word_timestamps:
            #     if audio is None:
            #         audio = self.aligner.load_audio(file_path=file_path)
            #     logger.debug("Aligning", file_path=file_path)
            #     final_aligned = self.aligner.force_align(
            #         audio,
            #         i.aligned_segments,  # type: ignore
            #     )
            #     logger.debug("Aligned", file_path=file_path)
            #     transcript = self.fetch_transcript(i.id)
            #     transcript.word_timestamps = final_aligned  # type: ignore
            #     self.session.commit()
            # logger.debug("Aligned segments", file_path=file_path)
