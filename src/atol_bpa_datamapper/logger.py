from .arg_parser import shared_args
import logging
import sys


def setup_logger(log_level="INFO"):
    logger = logging.getLogger("atol_bpa_datamapper")
    handler = logging.StreamHandler(sys.stderr)
    if log_level:
        logger.setLevel(log_level)
        handler.setLevel(log_level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


parser, input_group, output_group, options_group = shared_args()
args = parser.parse_args()
logger = setup_logger(args.log_level)
