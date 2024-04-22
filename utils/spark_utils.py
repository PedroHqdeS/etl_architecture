from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

from utils.logging_utils import get_logger

def start_spark_session(app_name: str) -> SparkSession:
    spark = (SparkSession
                .builder
                .appName(app_name)
                .master("local")
                .getOrCreate())
    return spark

def read_spark_dataframe(path: str,
                          spark_session: SparkSession,
                          file_format: str,
                          **kwargs) -> DataFrame:
    logger = get_logger(name=__name__)
    try:
        logger.info(msg=f"Extractiong data from '{path}'")
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
