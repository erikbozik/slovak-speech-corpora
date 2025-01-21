import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame, Series
from tqdm import tqdm

from src.database import Recording
from src.processors import AudioAnalyzer

from ..schemas import DataMetaData


class VoxPopuli:
    data: DataFrame
    source_part: str
    audio_dir_path: Path
    source: str

    def __init__(self, data: DataMetaData, source: str = "voxpopuli") -> None:
        self.source = source
        self.source_part = data.source_part
        self.audio_dir_path = Path(data.audio_dir_path)
        self.data = pd.read_csv(data.tsv_path, delimiter="\t")

    def extract(self):
        for _, data in tqdm(self.data.iterrows(), total=len(self.data)):
            yield self._construct_recording(data=data)

    def _construct_recording(self, data: Series) -> Recording:
        data = data.replace(np.nan, None)
        filename = data["id"]
        _path = self.get_path_to_audio(filename=filename)
        transcript = data["raw_text"]
        audio = open(_path, "rb")
        audio_size = os.path.getsize(_path) / 1024**2
        speaker_id = int(data["speaker_id"]) if data["speaker_id"] else None
        gender = data["gender"]

        _analyzed = AudioAnalyzer(audio).analyze()
        duration_ms = _analyzed.duration
        sampling_rate = _analyzed.sampling_rate

        _other_cols = [
            col
            for col in self.data.columns
            if col not in {"id", "raw_text", "speaker_id", "gender"}
        ]

        other_data = json.dumps(
            {col_name: data[col_name] for col_name in _other_cols},
            indent=4,
            ensure_ascii=False,
        )

        return Recording(
            filename=filename,
            transcript=transcript,
            audio=audio.read(),
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
        if ".wav" not in filename:
            filename += ".wav"

        path = self.audio_dir_path / filename

        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"{path} does not exist.")
        return path
