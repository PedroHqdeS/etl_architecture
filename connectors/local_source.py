import polars as pl
from connectors.base.external_source import ExternalSource
from connectors.csv_connector import CsvConnector


class LocalSource(ExternalSource):
    """
    Component to connect and extract data from a local source.
    Data must be in CSV format.

    Parameters
    ----------
    url: str
        Contains the path for the data source
    processing_date: dict
        Contains the columns (key) and values by which data will
        be filtered. This filter must be Datetime format.
    """
    def __init__(self, url: str, processing_date: dict):
        super().__init__(credentials={})
        self._url = url
        self._processing_date = processing_date

    def connect(self) -> str:
        pass

    def extract_data(self) -> pl.DataFrame:
        """
        Extracts CSV data from the local source.
        Afterward, it filters data according the
        date(s) passed to filter.

        Returns
        -------
        DataFrame
        """
        csv = CsvConnector()
        dataframe = csv.extract_data(path=self._url)
        for column, value in self._processing_date.items():
            dataframe = dataframe.filter(
                pl.col(column)
                    .str.strptime(pl.Datetime)
                    .dt.date().cast(str) == value)
        return dataframe
