#!/usr/bin/env python3

import argparse
import csv
import datetime
import json
import logging
from operator import itemgetter
from pathlib import Path

# configure logger
logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler("data-finder.log", encoding="utf-8", mode="w")
logger.addHandler(console_handler)
logger.addHandler(file_handler)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.setLevel("DEBUG")
console_handler.setLevel("INFO")

# add arguments
argument_parser = argparse.ArgumentParser(
    description="This script iterates over the jsondump of raw CKAN data packages on the data portal to find packages uploaded after a supplied date. You can optionally specify any initiatives you wish to target.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
initiative_group = argument_parser.add_argument_group("Target Initiatives")
argument_parser.add_argument(
    "--metadata_path",
    type=Path,
    help="the file path containing the raw ckan packages to be examined. Format should be jsonl"
)
argument_parser.add_argument(
    "--date_last_checked",
    type=datetime.date.fromisoformat,
    help="the date the mapper was last run (or the date after which you would like to check for relevant packages). Output is inclusive of packages transferred on the supplied date."
)
initiative_group.add_argument(
    "--initiatives_for_AToL",
    action="store_true",
    help="selecting this option will return new packages found across initiatives in which AToL are included in the data agreement (Australian Grasslands Initiative, Australian Venom Innovation and Discovery Initiative, Threatened Species Initiative, Australian Avian Genomics, Genomics for Forest Resilience, Integrated Pest Management 'Omics, Australian Fish Genomics, Fungi Functional 'Omics)"
)
initiative_group.add_argument(
    "--TSI",
    action="store_true",
    help="selecting this option will return new packages found for the Threatened Species Initiative"
)
initiative_group.add_argument(
    "--AVID",
    action="store_true",
    help="selecting this option will return new packages found for the Australian Venom Innovation and Discovery Initiative"
)
initiative_group.add_argument(
    "--Avian",
    action="store_true",
    help="selecting this option will return new packages found for the Australian Avian Genomics Initiative"
)
initiative_group.add_argument(
    "--Grasslands",
    action="store_true",
    help="selecting this option will return new packages found for the Australian Grasslands Initiative"
)
initiative_group.add_argument(
    "--Forest",
    action="store_true",
    help="selecting this option will return new packages found for the Genomics for Forest Resilience Initiative"
)
initiative_group.add_argument(
    "--IPM",
    action="store_true",
    help="selecting this option will return new packages found for the Integrated Pest Management 'Omics Initiative"
)
initiative_group.add_argument(
    "--Fungi",
    action="store_true",
    help="selecting this option will return new packages found for the Fungi Functional 'Omics Initiative"
)
initiative_group.add_argument(
    "--Fish",
    action="store_true",
    help="selecting this option will return new packages found for the Australian Fish Genomics Initiative"
)
args = argument_parser.parse_args()

# functions
def check_initiative():
    target_initiatives = []
    if args.initiatives_for_AToL:
        target_initiatives = ["Australian Grasslands Initiative", "Australian Venom Innovation and Discovery Initiative", "Threatened Species Initiative", "Australian Avian Genomics", "Genomics for Forest Resilience", "Integrated Pest Management 'Omics", "Australian Fish Genomics", "Fungi Functional 'Omics"]
        logger.info(f"Target initiative/s: {target_initiatives}")
        return target_initiatives
    if args.TSI:
        target_initiatives.append("Threatened Species Initiative")
    if args.AVID:
        target_initiatives.append("Australian Venom Innovation and Discovery Initiative")
    if args.Avian:
        target_initiatives.append("Australian Avian Genomics")
    if args.Grasslands:
        target_initiatives.append("Australian Grasslands Initiative")
    if args.Forest:
        target_initiatives.append("Genomics for Forest Resilience")
    if args.IPM:
        target_initiatives.append("Integrated Pest Management 'Omics")
    if args.Fungi:
        target_initiatives.append("Fungi Functional 'Omics")
    if args.Fish:
        target_initiatives.append("Australian Fish Genomics")
    if target_initiatives:
        logger.info(f"Target initiative/s: {target_initiatives}")
    return target_initiatives

def get_date(raw_date, date_format):
    transfer_datetime = datetime.datetime.strptime(raw_date, date_format)
    transfer_date = transfer_datetime.date()
    return transfer_date

def add_new_package(package, date):
    new_package_summary = {}
    logger.debug(f"the transfer_date being checked is {transfer_date}")
    new_package_summary["initiative"] = package.get("organization", {}).get("title")
    new_package_summary["package_id"] = package.get("id")
    new_package_summary["data_context"] = package.get("data_context")
    new_package_summary["verbatim_data_type"] = package.get("resources", [{}])[0].get("resource_type")
    new_package_summary["taxon_id"] = package.get("taxon_id")
    new_package_summary["species"] = package.get("scientific_name")
    new_package_summary["platform"] = package.get("sequencing_platform")
    new_package_summary["library_source"] = package.get("library_source")
    new_package_summary["library_strategy"] = package.get("library_strategy")
    new_package_summary["metadata_created"] = package.get("metadata_created")
    new_package_summary["date_of_transfer_to_archive"] = package.get("date_of_transfer_to_archive")
    new_package_summary["date_of_transfer"] = package.get("date_of_transfer")
    new_packages_summary.append(new_package_summary)
    new_packages_in_full.append(package)
    filter_by_data_type(new_package_summary)

def filter_by_data_type(package_summary):
    data_type = package_summary.get("verbatim_data_type")
    library_strategy = package_summary.get("library_strategy")
    if "pacbio-hifi" in data_type:
        package_summary["inferred_data_type"] = "pacbio-hifi"
        new_assembly_packages.append(package_summary)
    elif "-ont" in data_type:
        package_summary["inferred_data_type"] = "ONT"
        new_assembly_packages.append(package_summary)
    elif "hi-c" in data_type:
        package_summary["inferred_data_type"] = "hi-c"
        new_assembly_packages.append(package_summary)
    elif library_strategy and "Hi-C" in library_strategy:
        package_summary["inferred_data_type"] = "hi-c"
        new_assembly_packages.append(package_summary)
    else:
        package_summary["inferred_data_type"] = None
        logger.debug(f"not a relevant data type: {data_type}")

def sort_output(output):
    sorted_output = sorted(output, key=itemgetter('metadata_created'), reverse=True)
    return sorted_output

def write_to_json(sorted_packages, output_path):
    try:
        with open(output_path, "wt") as f:
            json.dump(sorted_packages, f)
            logger.info(f"Writing output to: {output_path}")
    except Exception as e:
        logger.error(f"Unable to write output: {e}")

def write_to_csv(sorted_list, output_path):
    try:
        with open(output_path, "wt") as f:
            # Write the header
            first_package_summary = sorted_list[0]
            header = list(first_package_summary.keys())
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            # Write the rows
            writer.writerows(sorted_list)
            logger.info(f"Writing output to: {output_path}")
    except Exception as e:
        logger.error(f"Unable to write output: {e}")

# variables
new_packages_summary = []
new_packages_in_full = []
new_assembly_packages = []
undated_package_counter = 0

summary_path = "results/new_package_summary.json"
full_packages = "results/new_package_details.json"
table_path = "results/new_package_table.csv"
relevant_table_path = "results/filtered_package_table.csv"

# starting script
logger.info(f"Starting script")

target_initiatives = check_initiative()

with open(args.metadata_path, "rt") as f:
    for line in f:
        package = json.loads(line)
        if package.get("metadata_created"): # using metadata_created as best estimate of when data were actually published on the DP (so far all packages have had this field)
            transfer_date = get_date(raw_date=package.get("metadata_created"), date_format='%Y-%m-%dT%H:%M:%S.%f')
            logger.debug("using 'metadata_created' date")
        elif package.get("date_of_transfer_to_archive"):
            transfer_date = get_date(raw_date=package.get("date_of_transfer_to_archive"), date_format='%Y-%m-%d')
            logger.debug("using 'date_of_transfer_to_archive' date")
        elif package.get("date_of_transfer"):
            transfer_date = get_date(raw_date=package.get("date_of_transfer"), date_format='%Y-%m-%d')
            logger.debug("using 'date_of_transfer' date")
        else:
            undated_package_counter =+ 1
            logger.debug("found a package without 'metadata_created', 'date_of_transfer' or 'date_of_transfer_to_archive' field")
            continue
        if args.date_last_checked <= transfer_date:
            logger.debug("found a new package!")
            if not target_initiatives or package.get("organization", {}).get("title", None) in target_initiatives:
                logger.debug("it's a target initiative!")
                add_new_package(package, transfer_date)
        else:
            logger.debug(f"an older package - dated {transfer_date}")
    logger.info(f"Found {undated_package_counter} packages missing 'metadata_created', 'date_of_transfer' or 'date_of_transfer_to_archive' fields in their metadata.")

# sorting and writing output
if len(new_packages_summary) > 0:
    logger.info(f"Found {len(new_packages_summary)} new packages.")
    sorted_summary = sort_output(new_packages_summary)
    sorted_details = sort_output(new_packages_in_full)
    write_to_json(sorted_summary, summary_path)
    write_to_json(sorted_details, full_packages)
    write_to_csv(sorted_summary, table_path)
else:
    logger.info("No new packages found for target initiative/s.")

if len(new_assembly_packages) > 0:
    logger.info(f"Found {len(new_assembly_packages)} new packages with relevant data types.")
    sorted_filtered_summary = sort_output(new_assembly_packages)
    write_to_csv(sorted_filtered_summary, relevant_table_path)
else:
    logger.info("No new packages with relevant data types found for target initiative/s.")
