import polars as pl
from connectors.base.external_source import ExternalSource
from connectors.csv_connector import CsvConnector


class LocalSource(ExternalSource):
    def __init__(self, processing_date: dict):
        super().__init__(credentials={})
        self._processing_date = processing_date

    def connect(self) -> str:
        url = "./datasets"
        return url

    def extract_data(self) -> pl.DataFrame:
        path = self.connect()
        csv = CsvConnector()
        dataframe = csv.extract_data(path=path)
        for column, value in self._processing_date.items():
            dataframe = dataframe.filter(
                pl.col(column)
                    .str.strptime(pl.Datetime)
                    .dt.date().cast(str) == value)
        return dataframe
