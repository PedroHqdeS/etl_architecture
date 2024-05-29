from datetime import datetime
import polars as pl
from utils.logging_utils import get_logger
from connectors.csv_connector import CsvConnector
from connectors.delta_connector import DeltaConnector
from connectors.local_source import LocalSource
from layers.bronze_layer_path import BronzeLayerPath
from layers.silver_layer_path import SilverLayerPath
from layers.gold_layer_path import GoldLayerPath
from pipelines.bronze_to_silver_pipeline import BronzeToSilverPipeline
from pipelines.source_to_bronze_pipeline import SourceToBronzePipeline
from pipelines.silver_to_gold_pipeline import SilverToGoldPipeline


def init():
    processing_date = (
        datetime.strptime("2024-02-02", "%Y-%m-%d"))
    logger = get_logger(name=__name__)

    # Initializing Bronze Layer
    bronze_params = {
        "entity": "entity",
        "processing_date": processing_date
    }
    bronze_layer = BronzeLayerPath(parameters=bronze_params)
    # Instantiation of connectors according to
    # the file that will be worked on for Bronze
    local_source = LocalSource(
        url="./datasets",
        processing_date={
            "updated_at": processing_date.strftime("%Y-%m-%d")
        }
    )
    csv = CsvConnector()
    logger.info(msg="Starting ingestion to Bronze ...")
    # Starting ingestion to Bronze
    source_to_bronze = SourceToBronzePipeline(
        source_connector=local_source,
        bronze_path=bronze_layer,
        target_connector=csv
    )
    source_to_bronze.start_ingestion()
    logger.info(msg="Success!")
    ##############################################
    # Initializing Silver Layer
    silver_params = {
        "entity": "entity"
    }
    silver_layer = SilverLayerPath(
        parameters=silver_params
    )
    # Instantiation of connectors according to
    # the file that will be worked on for Silver
    silver_delta = DeltaConnector(
        partition_dict={
            "year": "*",
            "month": "*",
            "day": "*"
        }
    )
    cast_map = {
        "quantity": pl.UInt8,
        "created_at": pl.Datetime,
        "updated_at": pl.Datetime
    }
    logger.info(msg="Starting ingestion to Silver ...")
    bronze_to_silver = BronzeToSilverPipeline(
        source_connector=csv,
        bronze_path=bronze_layer,
        target_connector=silver_delta,
        silver_path=silver_layer,
        cast_map=cast_map
    )
    bronze_to_silver.start_ingestion()
    logger.info(msg="Success!")
    ##############################################
    # Initializing Gold Layer
    gold_params = {
        "entity": "entity"
    }
    gold_layer = GoldLayerPath(
        parameters=gold_params
    )
    # Instantiation of connectors according to
    # the file that will be worked on for Gold
    gold_delta = DeltaConnector(partition_dict={})
    logger.info(msg="Starting ingestion to Gold ...")
    silver_to_gold = SilverToGoldPipeline(
        source_connector=silver_delta,
        silver_path=silver_layer,
        target_connector=gold_delta,
        gold_path=gold_layer
    )
    silver_to_gold.start_ingestion()
    logger.info(msg="Success!")


if __name__ == "__main__":
    init()
