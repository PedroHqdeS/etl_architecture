from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, lit

from pipelines.data_pipeline import DataPipeline
from connectors.file_format_connector import FileFormatConnector
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

    def transform(self, dataframe: DataFrame) -> DataFrame:
        execution_time = datetime.now()
        dataframe = (
            dataframe
                .withColumn(
                    "surrogate_key",
                    expr("uuid()"))
                .withColumn(
                    "data_lake_load_at",
                    lit(execution_time))
                .withColumn(
                    "year",
                    lit(execution_time.strftime(format="%Y")))
                .withColumn(
                    "month",
                    lit(execution_time.strftime(format="%m")))
                .withColumn(
                    "day",
                    lit(execution_time.strftime(format="%d"))))
        dataframe.show()
        return dataframe
