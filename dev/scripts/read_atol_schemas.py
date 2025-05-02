#!/usr/bin/env python3

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from uuid import uuid4
import json
import pandas as pd
import re

outdir = f"{datetime.today().strftime("%Y-%m-%d")}_{uuid4()}"

sample_schema = (
    "https://docs.google.com/spreadsheets/d/"
    "1ml5hASZ-qlAuuTrwHeGzNVqqe1mXsmmoDTekd6d9pto"
    "/export?gid=2142397762&format=tsv"
)

reads_scema = (
    "https://docs.google.com/spreadsheets/d/"
    "1ml5hASZ-qlAuuTrwHeGzNVqqe1mXsmmoDTekd6d9pto"
    "/export?gid=1743767073&format=tsv"
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


def main():

    # the field mappings
    sample_data = read_schema(sample_schema)
    reads_data = read_schema(reads_scema)

    df = pd.concat([sample_data, reads_data])

    # Initialize the structure for the JSON output
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

    # Convert defaultdict to a regular dict for JSON serialization
    output_data = {k: dict(v) for k, v in output_data.items()}

    # Write the JSON output
    json_output_file = Path("results", outdir, "field_mapping_bpa_to_atol.json")
    Path(json_output_file.parent).mkdir(parents=True, exist_ok=True)
    with open(json_output_file, mode="w", encoding="utf-8") as json_file:
        json.dump(output_data, json_file, indent=4)

    # the controlled vocabs
    vocabulary = read_vocabulary(vocabulary_file)
    vocab_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    for _, row in vocabulary.iterrows():
        atol_field = row["atol_field"].strip()
        category = row["category"].strip()
        atol_value = row["atol_value"].strip()
        allowed_value = row["allowed_value"].strip()
        # print(category, atol_field, atol_value, allowed_value)
        vocab_dict[category][atol_field][atol_value].update([allowed_value])

    vocab_output = {
        category: {
            atol_field: {
                atol_value: list(sorted(allowed_values))
                for atol_value, allowed_values in atol_values.items()
            }
            for atol_field, atol_values in fields.items()
        }
        for category, fields in vocab_dict.items()
    }

    # Write the JSON output
    vocab_output_file = Path("results", outdir, "value_mapping_bpa_to_atol.json")
    Path(vocab_output_file.parent).mkdir(parents=True, exist_ok=True)
    with open(vocab_output_file, mode="w", encoding="utf-8") as json_file:
        json.dump(vocab_output, json_file, indent=4)


if __name__ == "__main__":
    main()
