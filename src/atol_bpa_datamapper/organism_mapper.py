#!/usr/bin/env python

from .logger import logger
from pathlib import Path
import pandas as pd
import shelve
import skbio.io

# This may get integrated into the package handler


CACHE_DIR = Path("dev/taxdump_cache")


def read_taxdump_file(file_path, scheme):
    cache_file = Path(CACHE_DIR, f"{Path(file_path).stem}_{scheme}.db")
    Path.mkdir(cache_file.parent, exist_ok=True, parents=True)
    with shelve.open(cache_file) as cache:
        if "data" in cache:
            logger.info(f"Reading {scheme} from cache {cache_file}")
            return cache["data"]
        else:
            data = skbio.io.read(file_path, "taxdump", into=pd.DataFrame, scheme=scheme)
            logger.info(f"Writing {scheme} to cache {cache_file}")
            cache["data"] = data
            return data


class NcbiTaxdump:
    def __init__(self, nodes_file, names_file):
        logger.info(f"Reading NCBI taxonomy from {nodes_file}")
        self.nodes = self.nodes = read_taxdump_file(nodes_file, "nodes")

        logger.info(f"Reading NCBI taxon names from {names_file}")
        self.names = read_taxdump_file(names_file, "names")

    def get_rank(self, taxid):
        return self.nodes.at[taxid, "rank"]

    def get_scientific_name_txt(self, taxid):
        return self.names.loc[
            (self.names.index == taxid)
            & (self.names["name_class"] == "scientific name"),
            "name_txt",
        ].iat[0]


class OrganismSection(dict):

    def __init__(self, package_data, ncbi_taxdump):
        super().__init__()
        self.update(package_data)
        print(self)
        self.has_taxid = self.get("taxon_id") not in [None, ""]
        if self.has_taxid:
            self.raw_taxid = self.get("taxon_id")
        else:
            self.raw_taxid = None
            self.taxid = None

        if self.raw_taxid != None:
            self.format_taxid()

    def format_taxid(self):
        # Check if the taxid is an Int
        try:
            self.taxid = int(self.raw_taxid)
            self.raw_taxid_is_int = True
        except (ValueError, TypeError):
            self.raw_taxid_is_int = False

        # Check if we can coerce taxid to Int
        try:
            self.taxid = int(float(self.raw_taxid))
            self.raw_taxid_coerced_to_int = True
        except TypeError:
            self.raw_taxid_coerced_to_int = False
            self.taxid = None
        except ValueError:
            print(self.raw_taxid)
            quit(1)
