import os
from utils.logging_utils import get_logger


def get_url() -> str:
    return "./data_lake/"


def verify_if_directory_exists(dir: str):
    logger = get_logger(name=__name__)
    if not os.path.exists(dir):
        logger.info(
            msg=f"Directory does not exist. Creating directory '{dir}' ..."
        )
        os.makedirs(dir)
    else:
        logger.info(msg=f"Directory '{dir}' already exists.")
