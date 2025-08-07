from atol_bpa_datamapper import config
import argparse
import importlib.resources as pkg_resources
import sys
import os
from pathlib import Path


def get_config_filepath(filename):
    return pkg_resources.files(config).joinpath(filename)


def parse_args_for_transform():
    parser, input_group, output_group, options_group = shared_args()
    parser.description = "Transform mapped metadata to extract unique samples"
    
    # Remove mapping file arguments that aren't needed for transform
    for action in list(options_group._group_actions):
        if action.dest in ["package_field_mapping_file", "resource_field_mapping_file", "value_mapping_file"]:
            options_group._group_actions.remove(action)
    
    transform_group = parser.add_argument_group("Transform options")
    
    transform_group.add_argument(
        "--sample-conflicts",
        help="File to record conflicts between samples with the same bpa_sample_id",
    )
    
    transform_group.add_argument(
        "--sample-package-map",
        help="File to record which packages relate to each unique sample",
    )
    
    transform_group.add_argument(
        "--transformation-changes",
        help="File to record the transformation changes made during sample merging",
    )
    
    
    transform_group.add_argument(
        "--unique-organisms",
        help="File to record unique organisms extracted from the data",
    )
    
    transform_group.add_argument(
        "--organism-conflicts",
        help="File to record conflicts between organisms with the same organism_grouping_key",
    )
    
    transform_group.add_argument(
        "--organism-package-map",
        help="File to record which packages relate to each unique organism",
    )
    
    transform_group.add_argument(
        "--sample-ignored-fields",
        help="Comma-separated list of sample fields to ignore when determining uniqueness. Conflicts in these fields will still be reported but won't prevent inclusion in the unique samples list.",
    )
    
    transform_group.add_argument(
        "--organism-ignored-fields",
        help="Comma-separated list of organism fields to ignore when determining uniqueness. Conflicts in these fields will still be reported but won't prevent inclusion in the unique organisms list.",
    )
    
    transform_group.add_argument(
        "--experiments-output",
        help="File to record extracted experiments data",
    )
    
    return parser.parse_args()


def parse_args_for_filtering():
    parser, input_group, output_group, options_group, counter_group = field_value_args()
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

    return parser.parse_args()


def parse_args_for_mapping():
    parser, input_group, output_group, options_group, counter_group = field_value_args()
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

    # Add an option to specify where to save sanitization changes
    mapping_group.add_argument(
        "--sanitization_changes",
        help="File to record the sanitization changes made during mapping",
    )

    # organism mapping
    input_group.add_argument(
        "--nodes",
        required=True,
        help="NCBI nodes.dmp file from taxdump",
    )

    input_group.add_argument(
        "--names",
        required=True,
        help="NCBI names.dmp file from taxdump",
    )

    input_group.add_argument(
        "--taxids_to_busco_dataset_mapping",
        required=True,
        help="BUSCO placement file from https://busco-data.ezlab.org/v5/data/placement_files/",
    )

    mapping_group.add_argument(
        "--grouping_log",
        help="Compressed CSV file to record derived organism info for each package",
    )

    mapping_group.add_argument(
        "--grouped_packages",
        help="JSON file of Package IDs grouped by organism grouping_key",
    )

    options_group.add_argument(
        "--cache_dir",
        help="Directory to cache the NCBI taxonomy after processing",
        default=Path(
            os.getenv("XDG_CACHE_HOME", os.path.expanduser("~/.cache")),
            "atol_bpa_datamapper",
        ),
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
        "--package_field_mapping_file",
        type=argparse.FileType("r"),
        help="Package-level field mapping file in json.",
        default=get_config_filepath("field_mapping_bpa_to_atol_packages.json"),
    )

    options_group.add_argument(
        "-r",
        "--resource_field_mapping_file",
        type=argparse.FileType("r"),
        help="Resource-level field mapping file in json.",
        default=get_config_filepath("field_mapping_bpa_to_atol_resources.json"),
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


def field_value_args():

    parser, input_group, output_group, options_group = shared_args()

    counter_group = parser.add_argument_group("Counters")
    counter_group.add_argument(
        "--raw_field_usage",
        help="File for field usage counts in the raw data",
    )

    return parser, input_group, output_group, options_group, counter_group
