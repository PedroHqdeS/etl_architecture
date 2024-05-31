import os
from utils.logging_utils import get_logger


def get_url() -> str:
    """
    Returns the url to Data Lake.

    Returns
    -------
    str
    """
    return "./data_lake/"


def verify_if_directory_exists(dir: str) -> None:
    """
    Verify if the path passed already exists in Data Lake.
    If it does not exist yet, it is created.

    Parameters
    ----------
    dir: str
        The path to be verified.

    Returns
    -------
    None
    """
    logger = get_logger(name=__name__)
    if not os.path.exists(dir):
        logger.info(
            msg=f"Directory does not exist. Creating directory '{dir}' ..."
        )
        os.makedirs(dir)
    else:
        logger.info(msg=f"Directory '{dir}' already exists.")
