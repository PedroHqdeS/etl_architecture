import polars as pl
from abc import ABC, abstractmethod
from utils.logging_utils import get_logger


class ExternalSource(ABC):
    """
    Base code defining the responsibilities of a component
    to connect and extract data from any external source.

    Parameters
    ----------
    credentials: dict
        Contains the connection parameters for the source.
    """
    def __init__(self, credentials: dict):
        self._credentials = credentials
        self._logger = get_logger(name=__name__)

    @abstractmethod
    def connect(self) -> None:
        """
        This method must be implemented to
        create the connection to the external
        source before its data is consumed.

        Returns
        -------
        None
        """
        pass

    @abstractmethod
    def extract_data(self) -> pl.DataFrame:
        """
        This method must be implemented to
        extract data from the external source
        using the connection created in above
        function.

        Returns
        -------
        DataFrame
        """
        pass
