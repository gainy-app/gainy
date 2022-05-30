import logging
import os

ENV = os.getenv('ENV')
DEFAULT_LOGGING_LEVEL = logging.INFO if ENV == 'production' else logging.DEBUG


def get_logger(name, level=DEFAULT_LOGGING_LEVEL):
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
