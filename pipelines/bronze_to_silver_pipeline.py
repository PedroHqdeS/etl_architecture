from datetime import datetime
from uuid import uuid4
import polars as pl

from pipelines.data_pipeline import DataPipeline
from connectors.base.file_format_connector import FileFormatConnector
from connectors.delta_connector import DeltaConnector
from layers.bronze_layer_path import BronzeLayerPath
from layers.silver_layer_path import SilverLayerPath


class BronzeToSilverPipeline(DataPipeline):
    def __init__(self,
                 bronze_path: BronzeLayerPath,
                 source_connector: FileFormatConnector,
                 silver_path: SilverLayerPath,
                 target_connector: DeltaConnector):
        super().__init__(
            source_connector=source_connector,
            source_layer=bronze_path,
            target_connector=target_connector,
            target_layer=silver_path
        )

    def transform(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        if not dataframe.is_empty():
            execution_time = datetime.now()
            surrogate_keys = [str(uuid4()) for _ in range(len(dataframe))]
            dataframe = dataframe.with_columns([
                pl.Series(name="surrogate_key", values=surrogate_keys),
                pl.lit(execution_time).alias("data_lake_load_at"),
                pl.lit(execution_time.strftime(format="%Y")).alias("year"),
                pl.lit(execution_time.strftime(format="%m")).alias("month"),
                pl.lit(execution_time.strftime(format="%d")).alias("day")
            ])
            return dataframe
