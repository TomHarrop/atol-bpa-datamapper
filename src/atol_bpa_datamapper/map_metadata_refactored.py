"""
Refactored map_metadata.py using the processing framework.
"""

from .arg_parser import parse_args_for_mapping
from .logger import setup_logger
from .processing_framework import MappingProcessor


def main():
    args = parse_args_for_mapping()
    setup_logger(args.log_level)
    
    processor = MappingProcessor(args)
    processor.run()


if __name__ == "__main__":
    main()
