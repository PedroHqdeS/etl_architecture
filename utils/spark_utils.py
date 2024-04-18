from pyspark.sql import SparkSession, DataFrame

def start_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def read_spark_dataframe(path: str,
                          spark_session: SparkSession,
                          file_format: str,
                          **kwargs) -> DataFrame:
    df = (spark_session
            .read
            .format(file_format)
            .options(**kwargs)
            .load(path))
    return df