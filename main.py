from datetime import datetime
from layers.bronze_layer_path import BronzeLayerPath
from layers.silver_layer_path import SilverLayerPath
from layers.gold_layer_path import GoldLayerPath

# connector = CsvConnector(app_name="Teste")
#
# _options = {
#     "header": "true",
#     "delimiter": ";"
# }
# input_path = "datasets/csv_test.csv"
# output_path = "data_lake/file.csv"
#
# df = connector._extract_data(path=input_path, **_options)
#
# df.show()
#
# connector._load_data(dataframe=df, path=output_path)

params = {
    "entity": "orders",
    "execution_time": datetime.now()
}

bronze = BronzeLayerPath(parameters=params)
print(bronze.get_file_path())

silver = SilverLayerPath(parameters=params)
print(silver.get_file_path())

gold = GoldLayerPath(parameters=params)
print(gold.get_file_path())
