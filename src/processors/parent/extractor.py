from pathlib import Path

from pandas import DataFrame, Series
from tqdm import tqdm

from src.database import Recording
from src.schemas import DataMetaData


class Extractor:
    data_path: str
    data: DataFrame
    source_part: str
    audio_dir_path: Path
    source: str

    def __init__(self, data: DataMetaData, source: str):
        self.data_path = data.data_path
        self.audio_dir_path = Path(data.audio_dir_path)
        self.source = source
        self.source_part = data.source_part

    def extract(self):
        for _, data in tqdm(self.data.iterrows(), total=len(self.data)):
            yield self.construct_recording(data=data)

    def construct_recording(self, data: Series) -> Recording:
        return Recording()

    def get_path_to_audio(self, filename: str) -> Path:
        path = self.audio_dir_path / filename

        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"{path} does not exist.")
        return path
