from abc import ABC, abstractmethod
import polars as pl

from utils.logging_utils import get_logger


class ExternalSource(ABC):
    def __init__(self, credentials: dict):
        self._credentials = credentials
        self._logger = get_logger(name=__name__)

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def extract_data(self) -> pl.DataFrame:
        pass