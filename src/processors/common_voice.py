import os
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from src.database import Recording
from src.processors import AudioAnalyzer

from ..schemas import DataMetaData
from .parent import Extractor


class CommonVoice(Extractor):
    data: DataFrame
    source_part: str
    audio_dir_path: Path
    source: str

    def __init__(
        self, data: DataMetaData, source: str = "common_voice", *args, **kwargs
    ) -> None:
        super().__init__(data, source, *args, **kwargs)
        self.data = pd.read_csv(self.data_path, delimiter="\t")

    def construct_recording(self, data: Series) -> Recording:
        data = data.replace(np.nan, None)

        filename = data["path"]
        _path = self.get_path_to_audio(filename)
        transcript = data["sentence"]
        audio = self.convert_mp3_to_wav(open(_path, "rb").read())
        audio_size = os.path.getsize(_path) / 1024**2
        speaker_id = data["client_id"]
        gender = data["gender"]

        _analyzed = AudioAnalyzer(audio).analyze()
        duration_ms = _analyzed.duration
        sampling_rate = _analyzed.sampling_rate

        _other_cols = [
            col
            for col in self.data.columns
            if col not in {"path", "sentence", "client_id", "gender"}
        ]

        other_data = {col_name: data[col_name] for col_name in _other_cols}

        return Recording(
            filename=filename,
            transcript=transcript,
            audio=audio,
            source=self.source,
            source_part=self.source_part,
            duration_ms=duration_ms,
            audio_size=audio_size,
            speaker_id=speaker_id,
            speaker_gender=gender,
            sampling_rate=sampling_rate,
            other_data=other_data,
        )

    @staticmethod
    def convert_mp3_to_wav(mp3_data: bytes) -> bytes:
        process = subprocess.run(
            ["ffmpeg", "-i", "pipe:0", "-f", "wav", "pipe:1"],
            input=mp3_data,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        return process.stdout
