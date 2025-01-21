from functools import wraps
from io import BufferedReader

from pydantic import BaseModel
from soundfile import SoundFile


def reset_file_position(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        original_position = self.file.tell()
        try:
            return method(self, *args, **kwargs)
        finally:
            self.file.seek(original_position)

    return wrapper


class AnalyzeAudio(BaseModel):
    duration: float
    samplerate: int


class AudioAnalyzer:
    file: BufferedReader

    def __init__(self, file: BufferedReader) -> None:
        self.file = file

    @reset_file_position
    def analyze(self) -> AnalyzeAudio:
        with SoundFile(self.file) as audio_file:
            analyzed = AnalyzeAudio(
                duration=(len(audio_file) / audio_file.samplerate) * 1000,
                samplerate=audio_file.samplerate,
            )
        return analyzed
