#!/usr/bin/env python3

from collections import defaultdict
import json
import pandas as pd
import re

sample_schema = "dev/sample_schema_2025-04-16.tsv"
reads_scema = "dev/reads_schema_2025-04-16.tsv"
json_output_file = "results/2025-04-17/field_mapping_bpa_to_atol.json"


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


def sanitise_field_name(field_string):
    allowed_chars = re.compile("[a-zA-Z0-9 _]+")
    match = allowed_chars.match(field_string)
    if match:
        field_name = match.group(0).strip()
        sanitised_field_name = re.sub(r"\s+", "_", field_name)
        return sanitised_field_name

    return field_string


def main():

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
    with open(json_output_file, mode="w", encoding="utf-8") as json_file:
        json.dump(output_data, json_file, indent=4)


if __name__ == "__main__":
    main()
