#!/usr/bin/env python

from atol_bpa_datamapper.logger import logger
import gzip
import jsonlines
import pandas as pd


class MetadataObject(dict):
    def __init__(self, data):
        super().__init__()
        self.update(data)


def read_input(input_source):
    logger.info(f"Reading input from {input_source}")
    with gzip.open(input_source, "rt") as f:
        reader = jsonlines.Reader(f)
        for obj in reader:
            yield MetadataObject(obj)


mapped_metadata = read_input("results/2025-06-12_9371/m.jsonl.gz")

flat_dicts = []
for metadata_dict in mapped_metadata:
    flat_dict = {}
    for db_component, mapping_dict in metadata_dict.items():
        for field, value in mapping_dict.items():
            key = f"{db_component}.{field}"
            flat_dict.update({key: value})
    flat_dicts.append(flat_dict)

metadata_df = pd.DataFrame(flat_dicts)

metadata_df.loc[:, "organism.organism_grouping_key"]

metadata_df.loc[
    metadata_df["organism.organism_grouping_key"] == "Haloragodendron_lucasii_434543", :
]
