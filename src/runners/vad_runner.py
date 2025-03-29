import concurrent.futures
import os
import queue
import random
import threading
import time
from typing import Generator, List

import structlog
from pydantic import BaseModel, computed_field
from sqlalchemy import String, cast, func, literal, select
from sqlalchemy.orm import Session, aliased

from src.database import NRSRRecording, NRSRTranscript

from ..processors import VadProcessor

FILENAME = "/mnt/bigben/nrsr_recordings"

logger = structlog.get_logger()


class RecordingToProcess(BaseModel):
    id: int
    filename: str

    @computed_field
    @property
    def file_path(self) -> str:
        return f"{FILENAME}/{self.filename}"


class VadRunner:
    session: Session
    processor: VadProcessor
    q: queue.Queue

    def __init__(self, session: Session) -> None:
        self.session = session
        self.processor = VadProcessor()
        self.q = queue.Queue(maxsize=3)

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
            )
            .select_from(nr)
            .join(
                tran,
                (tran.meeting_num == nr.meeting_num) & (tran.snapshot == nr.snapshot),
                isouter=True,
            )
            .where(tran.json_parsed.isnot(None))
            .order_by(func.random())
        )

        for i in result:
            yield RecordingToProcess(id=i.id, filename=i.filename)

    def fetch_recording(self, recording_id: int) -> NRSRRecording:
        return self.session.get(NRSRRecording, recording_id)

    def load_audio(self, file_path: str):
        full = self.q.full()
        if full:
            logger.info("Queue is full, waiting to load audio")
        while full:
            time.sleep(random.uniform(0.1, 0.5))
            full = self.q.full()
        return self.processor.load_audio(file_path)

    def load_worker(
        self, items: List[RecordingToProcess], max_workers: int = 3
    ) -> None:
        """
        Loads audio files concurrently using multiple threads and places (item, audio) pairs onto the queue.
        If any exception occurs during loading, the entire process is terminated.
        """
        try:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_to_item = {
                    executor.submit(self.load_audio, file_path=item.file_path): item
                    for item in items
                }
                for future in concurrent.futures.as_completed(
                    list(future_to_item.keys())
                ):
                    item = future_to_item.pop(future, None)
                    if item is not None:
                        try:
                            audio = future.result()
                            self.q.put((item, audio))
                            logger.info(f"Queue size: {self.q.qsize()}")
                        except Exception as exc:
                            print(f"Error loading audio for {item.file_path}: {exc}")
                            os._exit(1)
            self.q.put(None)
        except Exception as e:
            print(f"Loader encountered an error: {e}")
            os._exit(1)

    def vad_worker(self) -> None:
        """
        Consumes (item, audio) pairs from the queue, processes the audio with VAD,
        and writes the corresponding record to db. If any exception occurs, the entire process is terminated.
        """
        try:
            while True:
                data = self.q.get()
                if data is None:
                    break
                item, audio = data
                transformed = self.processor.transform_record(
                    file_path=item.file_path, audio=audio
                )
                recording = self.fetch_recording(item.id)
                recording.vad_segments = [  # type: ignore
                    segment.model_dump() for segment in transformed.vad_segments
                ]  # type: ignore
                recording.vad_duration_s = transformed.vad_duration_s  # type: ignore
                self.session.commit()
                logger.info("Recording processed", id=item.id)
        except Exception as e:
            print(f"VAD worker encountered an error: {e}")
            os._exit(1)

    def run(self):
        """
        Fetches records from the DB, loads audio files concurrently, and processes them one at a time with VAD.
        If an exception occurs in any thread, the entire program terminates.
        """
        items = list(self.fetch_db())

        loader_thread = threading.Thread(target=self.load_worker, args=(items,))
        loader_thread.start()

        vad_thread = threading.Thread(target=self.vad_worker)
        vad_thread.start()

        loader_thread.join()
        vad_thread.join()
