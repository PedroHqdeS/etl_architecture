from abc import ABC, abstractmethod
import polars as pl

from utils.logging_utils import get_logger


class FileFormatConnector(ABC):
    def __init__(self):
        self._logger = get_logger(name=__name__)
        self._file_format = None

    @abstractmethod
    def extract_data(self, path: str) -> pl.DataFrame:
        pass

    @abstractmethod
    def load_data(self, dataframe: pl.DataFrame, path: str) -> None:
        pass
