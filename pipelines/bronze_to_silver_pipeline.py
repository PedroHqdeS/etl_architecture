from datetime import datetime
from uuid import uuid4
import polars as pl
from connectors.base.file_format_connector import FileFormatConnector
from connectors.delta_connector import DeltaConnector
from layers.bronze_layer_path import BronzeLayerPath
from layers.silver_layer_path import SilverLayerPath
from pipelines.base.data_pipeline import DataPipeline


class BronzeToSilverPipeline(DataPipeline):
    def __init__(self,
                 bronze_path: BronzeLayerPath,
                 source_connector: FileFormatConnector,
                 silver_path: SilverLayerPath,
                 target_connector: DeltaConnector,
                 cast_map: dict=None):
        super().__init__(
            source_connector=source_connector,
            source_layer=bronze_path,
            target_connector=target_connector,
            target_layer=silver_path
        )
        self._cast_map = cast_map

    def _cast_columns(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        for column, dtype in self._cast_map.items():
            if dtype == pl.Datetime:
                dataframe = dataframe.with_columns([
                    pl.col(column).str.to_datetime()])
            else:
                dataframe = dataframe.with_columns(
                    pl.col(column).cast(dtype))
        return dataframe

    def transform(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        if not dataframe.is_empty():
            dataframe = self._cast_columns(dataframe=dataframe)
            execution_time = datetime.now()
            surrogate_keys = [str(uuid4()) for _ in range(len(dataframe))]
            dataframe = dataframe.with_columns([
                pl.Series(name="surrogate_key", values=surrogate_keys),
                pl.lit(execution_time).alias("data_lake_load_at"),
                pl.col("updated_at").dt.strftime("%Y").alias("year"),
                pl.col("updated_at").dt.strftime("%m").alias("month"),
                pl.col("updated_at").dt.strftime("%d").alias("day")
            ])
        return dataframe
