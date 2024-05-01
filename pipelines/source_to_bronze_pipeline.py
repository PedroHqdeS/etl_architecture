import polars as pl

from pipelines.data_pipeline import DataPipeline
from connectors.base.external_source import ExternalSource
from connectors.base.file_format_connector import FileFormatConnector
from layers.bronze_layer_path import BronzeLayerPath


class SourceToBronzePipeline(DataPipeline):
    def __init__(self,
                 source_connector: ExternalSource,
                 bronze_path: BronzeLayerPath,
                 target_connector: FileFormatConnector):
        super().__init__(
            source_connector=source_connector,
            source_layer=None,
            target_connector=target_connector,
            target_layer=bronze_path
        )

    def extract(self) -> pl.DataFrame:
        return self._source_connector.extract_data()
