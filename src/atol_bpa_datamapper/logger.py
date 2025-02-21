import logging
import sys


def setup_logger(log_level="INFO"):
    logger = logging.getLogger("atol_bpa_datamapper")
    logger.setLevel(log_level)
    if not logger.hasHandlers():
        handler = logging.StreamHandler(sys.stderr)
        if log_level:
            handler.setLevel(log_level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        for handler in logger.handlers:
            handler.setLevel(log_level)

    return logger


logger = setup_logger()
