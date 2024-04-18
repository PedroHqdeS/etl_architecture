import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

class ExternalSource(ABC):
    def __init__(self, credentials: dict):
        self._credentials = credentials
        self._logger = logging.getLogger(__name__)

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def extract_data(self) -> DataFrame:
        pass