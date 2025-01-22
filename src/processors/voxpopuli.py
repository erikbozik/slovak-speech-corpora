import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from src.database import Recording
from src.processors import AudioAnalyzer

from ..schemas import DataMetaData
from .parent import Extractor


class VoxPopuli(Extractor):
    data: DataFrame
    source_part: str
    audio_dir_path: Path
    source: str

    def __init__(
        self, data: DataMetaData, source: str = "voxpopuli", *args, **kwargs
    ) -> None:
        super().__init__(data, source, *args, **kwargs)
        self.data = pd.read_csv(data.data_path, delimiter="\t")

    def construct_recording(self, data: Series) -> Recording:
        data = data.replace(np.nan, None)
        filename = data["id"]
        _path = self.get_path_to_audio(filename=filename)
        transcript = data["raw_text"]
        audio = open(_path, "rb").read()
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
        if ".wav" not in filename:
            filename += ".wav"

        return super().get_path_to_audio(filename)
