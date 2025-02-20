from atol_bpa_datamapper import config
import argparse
import gzip
import importlib.resources as pkg_resources
import jsonlines
import logging
import sys


class OutputWriter:
    def __init__(self, output_dest, dry_run=False):
        self.output_dest = output_dest
        self.dry_run = dry_run
        self.file_object = None
        self.writer = None

    def __enter__(self):
        self._open_file()
        return self

    def _open_file(self):
        if self.dry_run:
            self.file_object = (
                self.output_dest
                if self.output_dest is sys.stdout.buffer
                else open(self.output_dest.name, "w")
            )
        else:
            self.file_object = (
                gzip.open(self.output_dest, "wt")
                if self.output_dest is not sys.stdout.buffer
                else gzip.GzipFile(fileobj=self.output_dest, mode="w")
            )
        self.writer = jsonlines.Writer(self.file_object)
        return self

    def write_data(self, data):
        try:
            self.writer.write(data)
        except (AttributeError, RuntimeError) as e:
            self._open_file()
            self.writer.write(data)
            self._close_file()

    def _close_file(self):
        if self.writer:
            self.writer.close()
        if self.file_object and self.file_object is not self.output_dest:
            self.file_object.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_file()


def get_config_filepath(filename):
    return pkg_resources.files(config).joinpath(filename)


def parse_args_for_filtering():
    parser, input_group, output_group, options_group = shared_args()
    parser.description = "Filter packages from jsonlines.gz"

    return parser.parse_args()


def parse_args_for_mapping():
    parser, input_group, output_group, options_group = shared_args()
    parser.description = "Map metadata in filtered jsonlines.gz"

    return parser.parse_args()


def read_input(input_source):
    with gzip.open(input_source, "rt") as f:
        reader = jsonlines.Reader(f)
        for obj in reader:
            yield obj


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


def shared_args():
    parser = argparse.ArgumentParser()

    input_group = parser.add_argument_group("Input")

    input_group.add_argument(
        "-i",
        "--input",
        type=argparse.FileType("rb"),
        default=sys.stdin.buffer,
        help="Input file (default: stdin)",
    )

    output_group = parser.add_argument_group("Output")
    output_group.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("wb"),
        default=sys.stdout.buffer,
        help="Output file (default: stdout)",
    )

    options_group = parser.add_argument_group("Options")

    options_group.add_argument(
        "-f",
        "--field_mapping_file",
        type=argparse.FileType("r"),
        help="Field mapping file in json.",
        default=get_config_filepath("field_mapping_bpa_to_atol.json"),
    )

    options_group.add_argument(
        "-v",
        "--value_mapping_file",
        type=argparse.FileType("r"),
        help="Value mapping file in json.",
        default=get_config_filepath("value_mapping_bpa_to_atol.json"),
    )

    options_group.add_argument(
        "-l",
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO)",
    )

    options_group.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Test mode. Output will be uncompressed jsonlines.",
    )

    return parser, input_group, output_group, options_group
