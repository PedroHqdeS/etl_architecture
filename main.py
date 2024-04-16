import pandas as pd
from connectors.csv_connector import CsvConnector

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