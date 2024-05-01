import polars as pl

from utils.logging_utils import get_logger


class DataPipeline:
    def __init__(self, **kwargs):
        self._logger = get_logger(name=__name__)
        self._source_connector = kwargs["source_connector"]
        self._source_layer = kwargs["source_layer"]
        self._target_connector = kwargs["target_connector"]
        self._target_layer = kwargs["target_layer"]

    def extract(self) -> pl.DataFrame:
        dataframe = self._source_connector.extract_data(
            path=self._source_layer.get_file_path()
        )
        return dataframe

    def transform(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        return dataframe

    def load(self, dataframe: pl.DataFrame) -> None:
        self._target_connector.load_data(
            dataframe=dataframe,
            path=self._target_layer.get_file_path()
        )

    def start_ingestion(self) -> None:
        dataframe = self.extract()
        dataframe = self.transform(dataframe=dataframe)
        self.load(dataframe=dataframe)
