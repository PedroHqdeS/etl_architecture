import logging
from abc import ABC, abstractmethod
from utils.spark_utils import _start_spark_session

from pyspark.sql import DataFrame

class FileFormatConnector(ABC):
    def __init__(self, app_name: str):
        self._spark_session = _start_spark_session(app_name=app_name)
        self._logger = logging.getLogger(__name__)
        self._file_format = None

    @abstractmethod
    def extract_data(self) -> DataFrame:
        pass

    @abstractmethod
    def load_data(self) -> None:
        pass
