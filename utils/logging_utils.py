import logging


def get_logger(name: str):
    """
    Configure the object to print logs while code
    is running.

    Parameters
    ----------
    name: str
        Logger name.

    Returns
    -------
    logging
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name=name)
    return logger
