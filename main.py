from datetime import datetime

from pipelines.bronze_to_silver_pipeline import BronzeToSilverPipeline
# from pipelines.source_to_bronze_pipeline import SourceToBronzePipeline

from layers.bronze_layer_path import BronzeLayerPath
from layers.silver_layer_path import SilverLayerPath

from connectors.csv_connector import CsvConnector
from connectors.delta_connector import DeltaConnector
# from connectors.local_source import LocalSource


# local_source = LocalSource()
csv = CsvConnector()
bronze_params = {
    "entity": "entity",
    "execution_time": datetime.now()
}
bronze_layer = BronzeLayerPath(parameters=bronze_params)
# #
# source_to_bronze = SourceToBronzePipeline(
#     source_connector=local_source,
#     bronze_path=bronze_layer,
#     target_connector=csv
# )
#
# source_to_bronze.start_ingestion()

##############################################
delta = DeltaConnector(
    partition_dict={
        "ano": "*",
        "mes": "*",
        "dia": "2"
    }
)

silver_params = {
    "entity": "entity"
}
silver_layer = SilverLayerPath(
    parameters=silver_params
)
# bronze_to_silver = BronzeToSilverPipeline(
#     source_connector=csv,
#     bronze_path=bronze_layer,
#     target_connector=delta,
#     silver_path=silver_layer
# )
#
# bronze_to_silver.start_ingestion()

print(delta.extract_data(path=silver_layer.get_file_path()).drop("year", "month", "day").sort("id"))

