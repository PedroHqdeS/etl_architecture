import polars as pl
from connectors.delta_connector import DeltaConnector
from layers.silver_layer_path import SilverLayerPath
from layers.gold_layer_path import GoldLayerPath
from pipelines.base.data_pipeline import DataPipeline


class SilverToGoldPipeline(DataPipeline):
    """
    Component to standardize the data processing
    from Data Lake's Silver layer to Gold Layer.

    Parameters
    ----------
    source_connector: DeltaConnector
        Component that defines the behavior in how extracting
        data in Delta format from Data Lake's Silver layer.
    silver_path: SilverLayerPath
        Component that define and standardize the path patterns
        for Silver Layer in Data Lake.
    target_connector: DeltaConnector
        Component that defines the behavior in how writing
        data as Delta format in Data Lake's Gold layer.
    gold_path: GoldLayerPath
        Component that define and standardize the path patterns
        for Gold Layer in Data Lake.
    """
    def __init__(self,
                 source_connector: DeltaConnector,
                 silver_path: SilverLayerPath,
                 target_connector: DeltaConnector,
                 gold_path: GoldLayerPath):
        super().__init__(
            source_connector=source_connector,
            source_layer=silver_path,
            target_connector=target_connector,
            target_layer=gold_path
        )

    def transform(self, dataframe: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the standard transformations according the rule
        defined for data stored in Gold Layer. The rule is the
        records in Gold Layer must be in its latest version
        (Snapshot vision).

        Parameters
        ----------
        dataframe: DataFrame
            Contains the data being processed.

        Returns
        -------
        DataFrame
        """
        if not dataframe.is_empty():
            dataframe = dataframe.with_columns(
                pl.col("updated_at").rank(descending=True)
                    .over("order_description").alias("rank"))
            dataframe = (
                dataframe.filter(pl.col("rank") == 1).drop("rank"))
        return dataframe
