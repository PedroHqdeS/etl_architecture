from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
import logging

def start_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def read_spark_dataframe(path: str,
                          spark_session: SparkSession,
                          file_format: str,
                          **kwargs) -> DataFrame:
    logger = logging.getLogger(__name__)
    try:
        dataframe = (spark_session
                        .read
                        .format(file_format)
                        .options(**kwargs)
                        .load(path))
        logger.info(msg="Data extracted with success.")
        return dataframe
    except Exception as e:
        if "Path does not exist" in e.__str__():
            logger.warning(
                msg=f"'{path}' does not exist."
            )
            dataframe = spark_session.createDataFrame(
                data=[],
                schema=StructType([])
            )
            return dataframe
        else:
            logger.error(msg=e)
            raise
