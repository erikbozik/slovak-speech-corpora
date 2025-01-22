from io import BytesIO

from pydantic import BaseModel
from soundfile import SoundFile


class AnalyzeAudio(BaseModel):
    duration: float
    sampling_rate: int


class AudioAnalyzer:
    file: bytes

    def __init__(self, file: bytes) -> None:
        self.file = file

    def analyze(self) -> AnalyzeAudio:
        with SoundFile(BytesIO(self.file)) as audio_file:
            analyzed = AnalyzeAudio(
                duration=(len(audio_file) / audio_file.samplerate) * 1000,
                sampling_rate=audio_file.samplerate,
            )
        return analyzed
