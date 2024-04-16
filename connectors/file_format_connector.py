import logging
from abc import ABC, abstractmethod
from utils.spark_utils import _start_spark_session

from pyspark.sql import DataFrame

class FileFormatConnector(ABC):
    def __init__(self, app_name: str):
        self._spark_session = _start_spark_session(app_name=app_name)
        self._logger = logging.getLogger(__name__)
        self._file_format = None

    def _extract_data(self, path: str, **kwargs) -> DataFrame:
        df = self._spark_session.read.format(self._file_format).options(**kwargs).load(path)
        return df

    @abstractmethod
    def _load_data(self):
        pass
