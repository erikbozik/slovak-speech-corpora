import os

import numpy as np
import pandas as pd
from pandas import Series

from src.database import Recording
from src.extractors.utils import AudioAnalyzer

from ..schemas import DataMetaData
from .parent import Extractor


class Fleurs(Extractor):
    def __init__(
        self, data: DataMetaData, source: str = "fleurs", *args, **kwargs
    ) -> None:
        super().__init__(data, source, *args, **kwargs)
        self.data = pd.read_csv(self.data_path, delimiter="\t", header=None)

    def construct_recording(self, data: Series) -> Recording:
        data = data.replace(np.nan, None)

        filename = data[1]
        _path = self.get_path_to_audio(filename)
        transcript = data[2]
        audio = open(_path, "rb").read()
        audio_size = os.path.getsize(_path) / 1024**2
        speaker_id = None
        gender = data[6]

        _analyzed = AudioAnalyzer(audio).analyze()
        duration_ms = _analyzed.duration
        sampling_rate = _analyzed.sampling_rate

        _other_cols = [col for col in range(7) if col not in {1, 2, 6}]

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
