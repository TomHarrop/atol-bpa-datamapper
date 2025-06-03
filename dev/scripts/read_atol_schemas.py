#!/usr/bin/env python3

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from atol_bpa_datamapper.logger import logger
import json
import pandas as pd
import re

outdir = f"{datetime.today().strftime("%Y-%m-%d")}_{uuid4()}"

sample_schema = (
    "https://docs.google.com/spreadsheets/d/"
    "1ml5hASZ-qlAuuTrwHeGzNVqqe1mXsmmoDTekd6d9pto"
    "/export?gid=2142397762&format=tsv"
)

experiment_schema = (
    "https://docs.google.com/spreadsheets/d/"
    "1ml5hASZ-qlAuuTrwHeGzNVqqe1mXsmmoDTekd6d9pto"
    "/export?gid=1743767073&format=tsv"
)

reads_schema = (
    "https://docs.google.com/spreadsheets/d/"
    "1ml5hASZ-qlAuuTrwHeGzNVqqe1mXsmmoDTekd6d9pto"
    "/export?gid=1596363671&format=tsv"
)

vocabulary_file = (
    "https://docs.google.com/spreadsheets/d/"
    "1ml5hASZ-qlAuuTrwHeGzNVqqe1mXsmmoDTekd6d9pto"
    "/export?gid=1596130605&format=tsv"
)


def read_schema(schema_file):
    df = pd.read_csv(
        schema_file,
        delimiter="\t",
        header=3,
        usecols=[0, 1, 2],
        names=["category", "atol_field", "bpa_field"],
        dtype={
            "category": pd.StringDtype,
            "atol_field": pd.StringDtype,
            "bpa_field": pd.StringDtype,
        },
        na_values=["", "[unmapped]"],
        true_values=[
            "mandatory"
        ],  # this is for the mandatory column (not implemented yet)
        false_values=["not mandatory"],
    )
    return df


def read_vocabulary(vocabulary_file):
    df = pd.read_csv(
        vocabulary_file,
        delimiter="\t",
        header=0,
        usecols=[0, 1, 2, 3],
        names=["category", "atol_field", "atol_value", "allowed_value"],
        dtype={
            "category": pd.StringDtype,
            "atol_field": pd.StringDtype,
            "atol_value": pd.StringDtype,
            "allowed_value": pd.StringDtype,
        },
        na_values=["", "[unmapped]"],
    )
    return df


def sanitise_field_name(field_string):
    allowed_chars = re.compile("[a-zA-Z0-9 _]+")
    match = allowed_chars.match(field_string)
    if match:
        field_name = match.group(0).strip()
        sanitised_field_name = re.sub(r"\s+", "_", field_name)
        return sanitised_field_name

    return field_string


def df_to_dict(df):
    # dict of dicts, values of inner dict are lists
    output_data = defaultdict(lambda: defaultdict(list))

    # Iterate through the DataFrame rows
    for _, row in df.iterrows():
        atol_field = sanitise_field_name(row["atol_field"].strip())
        bpa_field = row["bpa_field"]
        category = row["category"].strip()

        if pd.notna(bpa_field):
            output_data[category][atol_field].extend(bpa_field.split(", "))
        else:
            print(f"Empty mapping for {atol_field}")
            output_data[category][atol_field] = []

    output_data = {k: dict(v) for k, v in output_data.items()}

    return output_data


def write_output(output_data, json_output_file):
    Path(json_output_file.parent).mkdir(parents=True, exist_ok=True)
    with open(json_output_file, mode="w", encoding="utf-8") as json_file:
        json.dump(output_data, json_file, indent=4)


def main():

    # the field mappings
    logger.info(f"read_schema {sample_schema}")
    sample_data = read_schema(sample_schema)
    logger.info(f"read_schema {experiment_schema}")
    experiment_data = read_schema(experiment_schema)

    package_level_data = pd.concat([sample_data, experiment_data])
    logger.info(f"read_schema {reads_schema}")
    resource_level_data = read_schema(reads_schema)

    logger.info(f"df_to_dict package_level_data")
    package_level_dict = df_to_dict(package_level_data)
    logger.info(f"df_to_dict resource_level_data")
    resource_level_dict = df_to_dict(resource_level_data)

    package_mapping_file = Path(
        "results", outdir, "field_mapping_bpa_to_atol_packages.json"
    )
    logger.info(f"write_output to {package_mapping_file}")
    write_output(package_level_dict, package_mapping_file)

    resource_mapping_file = Path(
        "results", outdir, "field_mapping_bpa_to_atol_resources.json"
    )
    write_output(resource_level_dict, resource_mapping_file)

    # the controlled vocabs
    logger.info(f"read_vocabulary {vocabulary_file}")
    vocabulary = read_vocabulary(vocabulary_file)
    # dict of dict of dicts, values of innermost dict are vocab sets
    vocab_data = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    for _, row in vocabulary.iterrows():
        atol_field = row["atol_field"].strip()
        category = row["category"].strip()
        atol_value = row["atol_value"].strip()
        allowed_value = row["allowed_value"].strip()
        vocab_data[category][atol_field][atol_value].update([allowed_value])

    # coerce vocab sets to list during conversion
    vocab_dict = {
        category: {
            atol_field: {
                atol_value: list(sorted(allowed_values))
                for atol_value, allowed_values in atol_values.items()
            }
            for atol_field, atol_values in fields.items()
        }
        for category, fields in vocab_data.items()
    }

    vocab_output_file = Path("results", outdir, "value_mapping_bpa_to_atol.json")
    write_output(vocab_dict, vocab_output_file)


if __name__ == "__main__":
    main()
