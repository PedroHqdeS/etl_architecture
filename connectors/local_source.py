import polars as pl
from connectors.base.external_source import ExternalSource
from connectors.csv_connector import CsvConnector


class LocalSource(ExternalSource):
    def __init__(self):
        super().__init__(credentials={})

    def connect(self) -> str:
        url = "./datasets"
        return url

    def extract_data(self) -> pl.DataFrame:
        path = self.connect()
        csv = CsvConnector()
        dataframe = csv.extract_data(path=path)
        return dataframe
