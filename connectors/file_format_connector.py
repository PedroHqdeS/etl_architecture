from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from utils.logging_utils import get_logger

class FileFormatConnector(ABC):
    def __init__(self):
        self._logger = get_logger(name=__name__)
        self._file_format = None

    @abstractmethod
    def extract_data(self, path: str) -> DataFrame:
        pass

    @abstractmethod
    def load_data(self, dataframe: DataFrame, path: str) -> None:
        pass
