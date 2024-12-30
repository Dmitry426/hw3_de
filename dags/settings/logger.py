import logging


def get_airflow_logger(name: str):
    """
    Returns a custom logger for Airflow.

    :param name: Name of the logger.
    :return: Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)  # Set desired logging level
    handler = logging.StreamHandler()  # Log to the console
    formatter = logging.Formatter(
        "[%(asctime)s] {%(name)s:%(lineno)d} %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

