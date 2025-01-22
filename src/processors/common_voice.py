import json
import os
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from pydub import AudioSegment
from tqdm import tqdm

from src.database import Recording
from src.processors import AudioAnalyzer

from ..schemas import DataMetaData


class CommonVoice:
    data: DataFrame
    source_part: str
    audio_dir_path: Path
    source: str

    def __init__(self, data: DataMetaData, source: str = "common_voice") -> None:
        self.source = source
        self.source_part = data.source_part
        self.audio_dir_path = Path(data.audio_dir_path)
        self.data = pd.read_csv(data.tsv_path, delimiter="\t")

    def extract(self):
        for _, data in tqdm(self.data.iterrows(), total=len(self.data)):
            yield self._construct_recording(data=data)

    def _construct_recording(self, data: Series) -> Recording:
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

        other_data = json.dumps(
            {col_name: data[col_name] for col_name in _other_cols},
            indent=4,
            ensure_ascii=False,
        )

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

    def get_path_to_audio(self, filename: str) -> Path:
        path = self.audio_dir_path / filename

        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"{path} does not exist.")
        return path

    @staticmethod
    def convert_mp3_to_wav(mp3_data: bytes) -> bytes:
        audio = AudioSegment.from_file(BytesIO(mp3_data), format="mp3")

        wav_buffer = BytesIO()

        audio.export(wav_buffer, format="wav")

        return wav_buffer.getvalue()
