import polars as pl
from connectors.delta_connector import DeltaConnector
from layers.silver_layer_path import SilverLayerPath
from layers.gold_layer_path import GoldLayerPath
from pipelines.base.data_pipeline import DataPipeline


class SilverToGoldPipeline(DataPipeline):
    def __init__(self,
                 silver_path: SilverLayerPath,
                 source_connector: DeltaConnector,
                 gold_path: GoldLayerPath,
                 target_connector: DeltaConnector):
        super().__init__(
            source_connector=source_connector,
            source_layer=silver_path,
            target_connector=target_connector,
            target_layer=gold_path
        )

    def transform(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        if not dataframe.is_empty():
            dataframe = dataframe.with_columns(
                pl.col("updated_at").rank(descending=True)
                    .over("order_description").alias("rank"))
            dataframe = (
                dataframe.filter(pl.col("rank") == 1).drop("rank"))
        return dataframe
