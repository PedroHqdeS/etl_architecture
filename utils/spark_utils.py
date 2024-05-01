from typing import List

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
        logger.info(msg=f"Extracting data from '{path}'")
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


def write_spark_dataframe(dataframe: DataFrame,
                          path: str,
                          spark_session: SparkSession,
                          file_format: str,
                          write_mode: str="overwrite",
                          partitions: List[str]=None,
                          **kwargs):
    logger = get_logger(name=__name__)
    if not dataframe.isEmpty():
        logger.info(msg=f"Writing data in '{path}' ...")
        dataframe_writer = (
            dataframe
                .write
                .format(source=file_format)
                .mode(saveMode=write_mode)
                .options(**kwargs))
        if partitions is not None:
            dataframe_writer = dataframe_writer.partitionBy(partitions)
        dataframe_writer.save(path=path)
        logger.info(msg="Data written with success.")
    else:
        logger.warn(msg="Empty dataframe. There is nothing to write.")
