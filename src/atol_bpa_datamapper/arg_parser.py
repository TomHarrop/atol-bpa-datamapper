from atol_bpa_datamapper import config
import argparse
import importlib.resources as pkg_resources
import sys


def get_config_filepath(filename):
    return pkg_resources.files(config).joinpath(filename)


def parse_args_for_filtering():
    parser, counter_group = shared_args()
    parser.description = "Filter packages from jsonlines.gz"

    counter_group.add_argument(
        "--bpa_field_usage",
        help="File for BPA field usage counts",
    )

    counter_group.add_argument(
        "--bpa_value_usage",
        help="File for BPA value usage counts",
    )

    filter_group = parser.add_argument_group("Filtering options")

    filter_group.add_argument(
        "--decision_log",
        help="Compressed CSV file to record the filtering decisions for each package",
    )

    parser.add_argument(
        "--sanitization_config",
        help="Path to sanitization config file",
        type=str,
        required=False
    )

    parser.add_argument(
        "--sanitization-change-file",
        help="Path to write sanitization changes",
        type=str,
        default="sanitization_changes.json.gz"
    )

    return parser.parse_args()


def parse_args_for_mapping():
    parser, counter_group = shared_args()
    parser.description = "Map metadata in filtered jsonlines.gz"

    counter_group.add_argument(
        "--raw_value_usage",
        help="File for value usage counts in the raw data",
    )

    counter_group.add_argument(
        "--mapped_field_usage",
        help="File for counts of how many times each BPA field was mapped to an AToL field",
    )

    counter_group.add_argument(
        "--mapped_value_usage",
        help="File for counts of the values mapped from BPA fields to AToL fields",
    )

    counter_group.add_argument(
        "--unused_field_counts",
        help="File for counts of fields in the BPA data that weren't used",
    )

    mapping_group = parser.add_argument_group("Mapping options")

    mapping_group.add_argument(
        "--mapping_log",
        help="Compressed CSV file to record the mapping used for each package",
    )

    parser.add_argument(
        "--sanitization_config",
        help="Path to sanitization config file",
        type=str,
        required=False
    )

    parser.add_argument(
        "--sanitization-change-file",
        help="Path to write sanitization changes",
        type=str,
        default="sanitization_changes.json.gz"
    )

    return parser.parse_args()


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

    options_group = parser.add_argument_group("General options")

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

    counter_group = parser.add_argument_group("Counters")
    counter_group.add_argument(
        "--raw_field_usage",
        help="File for field usage counts in the raw data",
    )

    return parser, counter_group
