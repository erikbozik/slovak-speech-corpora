import warnings

import numpy as np
import structlog
from pydantic import BaseModel, Field
from typing import Any


logger = structlog.get_logger()
warnings.filterwarnings(
    "ignore", message="TensorFloat-32 (TF32) has been disabled", category=UserWarning
)


class VadSegment(BaseModel):
    start: float
    end: float
    segments: list[tuple[float, float]] = Field(exclude=True)


class VadResponse(BaseModel):
    vad_segments: list[VadSegment]
    vad_duration_s: float


class VadProcessor:
    SAMPLE_RATE: int = 16000
    device: str = "cuda"
    chunk_size: int = 30
    vad_model: Any
    default_vad_options = {
        "chunk_size": 30,
        "vad_onset": 0.500,
        "vad_offset": 0.363,
    }

    def __init__(self) -> None:
        import torch
        from whisperX.whisperx.vads import Pyannote

        self.vad_model = Pyannote(
            torch.device(self.device), use_auth_token=None, **self.default_vad_options
        )

    def load_audio(self, file_path: str):
        from whisperX.whisperx import load_audio

        logger.debug("Loading audio", file_path=file_path)
        audio = load_audio(file_path)
        logger.debug("Audio loaded", file_path=file_path)
        return audio

    def transform_record(self, file_path: str, audio: np.ndarray) -> VadResponse:
        logger.debug("Preprocessing audio", file_path=file_path)
        waveform = self.vad_model.preprocess_audio(audio)
        logger.debug("Audio preprocessed", file_path=file_path)
        logger.debug("Running VAD", file_path=file_path)
        vad_segments = self.vad_model(
            {"waveform": waveform, "sample_rate": self.SAMPLE_RATE}
        )
        logger.debug("VAD completed", file_path=file_path)
        logger.debug("Merging VAD segments", file_path=file_path)
        vad_segments = self.vad_model.merge_chunks(
            vad_segments,
            self.chunk_size,
            onset=self.default_vad_options["vad_onset"],
            offset=self.default_vad_options["vad_offset"],
        )
        logger.debug("VAD segments merged", file_path=file_path)
        vad_segments = [VadSegment(**i) for i in vad_segments]

        return VadResponse(
            vad_segments=vad_segments,
            vad_duration_s=sum([i.end - i.start for i in vad_segments]),
        )
