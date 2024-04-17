from datetime import datetime
import pandas as pd
from connectors.csv_connector import CsvConnector
from pipelines.bronze_layer_path import BronzeLayerPath

connector = CsvConnector(app_name="Teste")

_options = {
    "header": "true",
    "delimiter": ";"
}
input_path = "datasets/csv_test.csv"
output_path = "data_lake/file.csv"

df = connector._extract_data(path=input_path, **_options)

df.show()

connector._load_data(dataframe=df, path=output_path)

params = {
    "main_entity": "orders",
    "entity": "order-items",
    "execution_time": datetime.now()
}

bronze = BronzeLayerPath(parameters=params)
print(bronze.get_file_path())
