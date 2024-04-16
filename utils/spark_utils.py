from pyspark.sql import SparkSession

def _start_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark