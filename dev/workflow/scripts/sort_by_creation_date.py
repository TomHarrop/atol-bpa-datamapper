#!/usr/bin/env python3

import argparse
import datetime
from logging import FileHandler
from pathlib import Path
import re

from atol_bpa_datamapper.io import read_jsonl_file
from atol_bpa_datamapper.logger import logger
import pandas as pd


def check_data_type(package_summary: dict[str, str]) -> tuple[bool, str]:
    try:
        verbatim_data_type = package_summary.get("verbatim_data_type", "").lower()
    except AttributeError:
        return (False, "")

    if "pacbio-hifi" in verbatim_data_type:
        return (True, "pacbio-hifi")

    if "-ont" in verbatim_data_type:
        return (True, "ONT")

    if "hi-c" in verbatim_data_type:
        return (True, "hi-c")

    library_strategy = package_summary.get("library_strategy", "").lower()

    if library_strategy and "hi-c" in library_strategy:
        return (True, "hi-c")

    return (False, "")


def get_date(package: dict[str, str]) -> datetime.date | None:
    # using metadata_created as best estimate of when data were actually
    # published on the DP (so far all packages have had this field)
    if package.get("metadata_created", None) is not None:
        return parse_date(
            raw_date=package.get("metadata_created", ""),
            date_format="%Y-%m-%dT%H:%M:%S.%f",
        )

    if package.get("date_of_transfer_to_archive", None) is not None:
        return parse_date(
            raw_date=package.get("date_of_transfer_to_archive", ""),
            date_format="%Y-%m-%d",
        )

    if package.get("date_of_transfer", None) is not None:
        return parse_date(
            raw_date=package.get("date_of_transfer", ""), date_format="%Y-%m-%d"
        )

    return None


def get_package_data(package: dict[str, str]) -> dict[str, str]:

    verbatim_keys = [
        "data_context",
        "date_of_transfer_to_archive",
        "date_of_transfer",
        "id",
        "library_source",
        "library_strategy",
        "metadata_created",
        "scientific_name",
        "sequencing_platform",
        "taxon_id",
    ]

    package_data = {}

    for key in verbatim_keys:
        package_data[key] = package.get(key, "")

    package_data["verbatim_data_type"] = package.get("resources", [{}])[0].get(
        "resource_type", ""
    )

    package_data["package_date"] = get_date(package)

    return package_data


def parse_args() -> argparse.Namespace:

    # add arguments
    argument_parser = argparse.ArgumentParser(
        description=(
            "This script iterates over the jsonl.gz dump of raw CKAN packages "
            "on the data portal to sort by initiative and date."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    _ = argument_parser.add_argument(
        "metadata_path",
        type=Path,
        help=(
            "The file path containing the raw CKAN "
            "packages to be examined. Format should be jsonl.gz"
        ),
    )

    _ = argument_parser.add_argument(
        "outdir",
        type=Path,
        help=("Directory to write the sorted initiative files."),
    )

    return argument_parser.parse_args()


def parse_date(raw_date: str, date_format: str) -> datetime.date:
    transfer_datetime = datetime.datetime.strptime(raw_date, date_format)
    transfer_date = transfer_datetime.date()
    return transfer_date


package_by_initiative = {"REJECTED": []}


def main():

    if snakemake:
        metadata_path = Path(snakemake.input["bpa_data"])
        outdir = Path(snakemake.output["outdir"])
        fh = FileHandler(snakemake.log[0])
        fh.setLevel(logger.level)
        logger.handlers.clear()
        logger.addHandler(fh)
    else:
        args = parse_args()
        metadata_path = args.metadata_path
        outdir = args.outdir

    # starting script
    logger.info(f"Starting script")
    i, j = (0, 0)

    for package in read_jsonl_file(metadata_path):
        # don't look at Packages with no data
        if len(package.get("resources", [])) > 0:

            i += 1

            if i % 1e4 == 0:
                logger.info(f"Checked {i} packages")

            initiative = package.get("organization", {}).get("title", "")

            package_data = get_package_data(package)
            keep, data_type = check_data_type(package_data)

            if keep:
                j += 1
                if j % 1e3 == 0:
                    logger.debug(f"Kept {j} packages")

                package_data["inferred_data_type"] = data_type
                if initiative in package_by_initiative:
                    package_by_initiative[initiative].append(package_data)
                else:
                    package_by_initiative[initiative] = [package_data]
            else:
                package_by_initiative["REJECTED"].append(package_data)

    logger.info(f"Kept {j} packages.")
    logger.info(f"Rejected {len(package_by_initiative["REJECTED"])} packages.")

    # check if the output directory exists
    _ = outdir.mkdir(parents=True, exist_ok=True)

    # write the output
    for k, v in package_by_initiative.items():
        filename = Path(outdir, f"{re.sub(r"[\W_]+", "", k)}.csv.gz")
        df = pd.DataFrame(v).sort_values(by="package_date", ascending=False)
        logger.info(f"Writing {k} output to {filename}.")
        df.to_csv(filename, index=False)


if __name__ == "__main__":
    main()
