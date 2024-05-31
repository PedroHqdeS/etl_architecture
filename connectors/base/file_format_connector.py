import polars as pl
from abc import ABC, abstractmethod
from utils.logging_utils import get_logger


class FileFormatConnector(ABC):
    """
    Base code defining the responsibilities of a component
    to extract and write data from any Data Lake's layer in
    any needed format.
    """
    def __init__(self):
        self._logger = get_logger(name=__name__)
        self._file_format = None

    @abstractmethod
    def extract_data(self, path: str) -> pl.DataFrame:
        """
        This method must be implemented to
        extract data from the Data Lake's layer
        according the format of the data stored.

        Parameters
        ----------
        path: str
            Data Lake's path from where data will be extracted.

        Returns
        -------
        DataFrame
        """
        pass

    @abstractmethod
    def load_data(self, dataframe: pl.DataFrame, path: str) -> None:
        """
        This method must be implemented to
        write data in the Data Lake's layer
        according the desired format to store.

        Parameters
        ----------
        dataframe: DataFrame
            Contains the data will be stored.
        path: str
            Data Lake's path where data will be stored.

        Returns
        -------
        None
        """
        pass
