import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

class DataPipeline(ABC):
    def __init__(self, **kwargs):
        self._logger = logging.getLogger(__name__)
        self._source_connector = kwargs["source_connector"]
        self._source_layer = kwargs["source_layer"]
        self._target_connector = kwargs["target_connector"]
        self._target_layer = kwargs["target_layer"]

    def extract(self) -> DataFrame:
        dataframe = self._source_connector.extract_data(
            path=self._source_layer.get_file_path()
        )
        return dataframe

    def transform(self, dataframe: DataFrame) -> DataFrame:
        return dataframe

    def load(self, dataframe: DataFrame) -> None:
        self._target_connector.load_data(
            dataframe=dataframe,
            path=self._target_layer.get_file_path()
        )

    def start_ingestion(self) -> None:
        dataframe = self.extract()
        if not dataframe.isEmpty():
            dataframe = self.transform(dataframe=dataframe)
            self.load(dataframe=dataframe)
        else:
            print("DATA FRAME VAZIOOOOOOOO")
