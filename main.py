import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Teste").getOrCreate()

_options = {
    "header": "true",
    "delimiter": ";"
}
df = spark.read.format("csv").options(**_options).load("./csv_test.csv")

df.show()

spark.stop()