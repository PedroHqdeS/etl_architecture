from connectors.external_source import ExternalSource
from connectors.csv_connector import CsvConnector

from pyspark.sql import DataFrame

class LocalSource(ExternalSource):
    def __init__(self):
        super().__init__(credentials={})

    def connect(self) -> str:
        url = "./datasets/"
        return url

    def extract_data(self) -> DataFrame:
        path = self.connect() + "base_csv.csv"
        csv = CsvConnector()
        dataframe = csv.extract_data(path=path)
        return dataframe
