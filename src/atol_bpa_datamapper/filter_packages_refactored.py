"""
Refactored filter_packages.py using the processing framework.
"""

from .arg_parser import parse_args_for_filtering
from .logger import setup_logger
from .processing_framework import FilterProcessor


def main():
    args = parse_args_for_filtering()
    setup_logger(args.log_level)
    
    processor = FilterProcessor(args)
    processor.run()


if __name__ == "__main__":
    main()
