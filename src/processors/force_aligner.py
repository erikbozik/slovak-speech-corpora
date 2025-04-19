from typing import Any, Literal

import numpy as np
import structlog
import torch
import whisperx
from whisperx import load_audio

logger = structlog.get_logger()


class ForceAligner:
    device: Any
    model: Any
    metadata: Any

    def __init__(self, device: Literal["cuda", "cpu"] = "cuda") -> None:
        self.device = torch.device(device)
        self.model, self.metadata = whisperx.load_align_model(
            language_code="sk", device=self.device
        )

    def load_audio(self, file_path: str):
        logger.debug("Loading audio", file_path=file_path)
        audio = load_audio(file_path)
        logger.debug("Audio loaded", file_path=file_path)
        return audio

    def transform_record(self, audio: np.ndarray, segments: dict):
        result = whisperx.align(
            segments,
            self.model,
            self.metadata,
            audio,
            self.device,
            return_char_alignments=False,
        )
        return result
