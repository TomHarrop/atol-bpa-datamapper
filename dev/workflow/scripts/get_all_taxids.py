#!/usr/bin/env python3

from atol_bpa_datamapper.arg_parser import get_config_filepath
from atol_bpa_datamapper.logger import logger
from atol_bpa_datamapper.utils.common import parse_taxon_id
from pathlib import Path
import argparse
import gzip
import json
import jsonlines
import sys


def read_jsonl_file(input_source):
    """
    Read generic jsonl.gz objects.
    """
    with gzip.open(input_source, "rt") as f:
        reader = jsonlines.Reader(f)
        for obj in reader:
            if isinstance(obj, dict):
                yield obj
            else:
                logger.warning(f"Skipping non-dictionary object: {obj}")
                continue


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("datasets")
    parser.add_argument("output")

    return parser.parse_args()


def main():

    args = parse_arguments()
    all_datasets = Path(args.datasets)
    output = Path(args.output)

    # The field_mapping file defines which BPA fields contain TaxIds
    field_mapping_bpa_to_atol_packages = get_config_filepath(
        "field_mapping_bpa_to_atol_packages.json"
    )

    with open(field_mapping_bpa_to_atol_packages, "rt") as f:
        field_mapping = json.load(f)
        taxon_id_fields = field_mapping.get("organism").get("taxon_id")

    # Read the datasets
    taxon_id_list = list()
    packages = read_jsonl_file(all_datasets)
    for package in packages:
        candidate_taxon_ids = [package.get(x, None) for x in taxon_id_fields]
        for candidate_taxon_id in candidate_taxon_ids:
            try:
                parsed_taxon_id = parse_taxon_id(candidate_taxon_id)
                if (parsed_taxon_id is not None) and (not parsed_taxon_id == 0):
                    taxon_id_list.append(parsed_taxon_id)
            except ValueError as e:
                logger.debug(f"Dropping taxon_id {candidate_taxon_id}")

    unique_taxon_ids = sorted(set(taxon_id_list))
    logger.info(
        f"Writing {len(unique_taxon_ids)} unique_taxon_ids to {output.as_posix()}"
    )

    with open(output, "wt") as f:
        f.writelines(str(i) + "\n" for i in unique_taxon_ids)


if __name__ == "__main__":
    main()
