import pandas as pd

from ..schemas import DataMetaData
from .parent import Extractor


class Fleurs(Extractor):
    def __init__(
        self, data: DataMetaData, source: str = "fleurs", *args, **kwargs
    ) -> None:
        super().__init__(data, source, *args, **kwargs)
        self.data = pd.read_csv(self.data_path, delimiter="\t")
