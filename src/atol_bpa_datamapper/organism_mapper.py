#!/usr/bin/env python

from .logger import logger
from pathlib import Path
import hashlib
import pandas as pd
import shelve
import skbio.io
from skbio.tree import TreeNode
import re

# This may get integrated into the package handler


CACHE_DIR = Path("dev/taxdump_cache")
NULL_VALUES = [None, "NONE", "NULL", "NAN", "NA", "N/A", ""]


def compute_sha256(file_path):
    logger.debug(f"Computing sha256 checksum for {file_path}.")
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            sha256.update(block)
    hex_digest = sha256.hexdigest()
    logger.debug(f"Checksum: {hex_digest}")
    return hex_digest


def read_taxdump_file(file_path, scheme):
    """
    Reads the taxdump file and caches it in a shelve file.

    Return a tuple of the data and a boolean indicating whether the cache was
    updated.
    """
    cache_file = Path(CACHE_DIR, f"{Path(file_path).stem}_{scheme}.db")
    Path.mkdir(cache_file.parent, exist_ok=True, parents=True)
    current_checksum = compute_sha256(file_path)

    with shelve.open(cache_file) as cache:
        if (
            "data" in cache
            and "checksum" in cache
            and cache["checksum"] == current_checksum
        ):
            logger.info(f"Reading {scheme} from cache {cache_file}")
            return (cache["data"], False)
        else:
            data = skbio.io.read(file_path, "taxdump", into=pd.DataFrame, scheme=scheme)
            logger.info(f"Writing {scheme} to cache {cache_file}")
            cache["data"] = data
            cache["checksum"] = current_checksum
            return (data, True)


def generate_taxonomy_tree(names, nodes, update_tree=False):
    cache_file = Path(CACHE_DIR, "taxonomy_tree.db")
    with shelve.open(cache_file) as cache:
        if "tree" in cache and not update_tree:
            logger.info(f"Reading taxonomy tree from {cache_file}")
            return cache["tree"]
        else:
            logger.info("Generating taxonomy tree")
            tree = TreeNode.from_taxdump(nodes, names)
            cache["tree"] = tree
            return tree


def sanitise_string(string):
    allowed_chars = re.compile("[a-zA-Z0-9 ]")
    return "".join(allowed_chars.findall(re.sub(r"\s+", " ", string))).strip()


class NcbiTaxdump:
    def __init__(self, nodes_file, names_file):
        logger.info(f"Reading NCBI taxonomy from {nodes_file}")
        self.nodes, nodes_changed = read_taxdump_file(nodes_file, "nodes")

        logger.info(f"Reading NCBI taxon names from {names_file}")
        self.names, names_changed = read_taxdump_file(names_file, "names")

        update_tree = any([nodes_changed, names_changed])

        # TODO: find out which ranks are lower than species
        self.tree = generate_taxonomy_tree(
            self.names, self.nodes, update_tree=update_tree
        )

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
        self.has_taxid = self.get("taxon_id") not in NULL_VALUES + ["0", "0.0"]

        # get the taxid
        self.raw_taxid = self.get("taxon_id") if self.has_taxid else None
        self.taxid = None

        if self.raw_taxid is not None:
            self.format_taxid()

        # look up taxid in NCBI taxonomy
        self.check_ncbi_taxonomy_for_taxid(ncbi_taxdump)

        # check for species information in the raw metadata
        if not all([self.scientific_name, self.scientific_name_source == "ncbi"]):
            if str(self.scientific_name).upper() not in NULL_VALUES:
                # TODO: look this up in the taxonomy
                print(self)
                print(self.__dict__)
                quit(1)

        # check for subspecies information
        if str(self.get("infraspecific_epithet")).upper() not in NULL_VALUES:
            self.has_subspecies_information = True
            self.subspecies = self.get("infraspecific_epithet")
            self.subspecies_sanitised = sanitise_string(self.subspecies)
        else:
            self.has_subspecies_information = False
            self.subspecies = None
            self.subspecies_sanitised = None

    def check_ncbi_taxonomy_for_taxid(self, ncbi_taxdump):
        # Check if it's an NCBI taxid
        self.taxid_is_ncbi_node = (
            self.has_taxid and self.taxid in ncbi_taxdump.nodes.index
        )

        if self.taxid_is_ncbi_node:
            self.rank = ncbi_taxdump.get_rank(self.taxid)
            self.scientific_name = ncbi_taxdump.get_scientific_name_txt(self.taxid)
            self.scientific_name_source = "ncbi"
        else:
            self.rank = None
            self.scientific_name = None
            self.scientific_name_source = None

        # TODO: there are ranks lower than species
        self.has_species_level_taxid = self.rank == "species"

    def format_taxid(self):
        # check if the raw_taxid is an int
        try:
            self.taxid = int(self.raw_taxid)
            self.raw_taxid_is_int = True
            self.raw_taxid_coerced_to_int = False
        except (ValueError, TypeError):
            self.raw_taxid_is_int = False
            # check if we can coerce taxid to int
            try:
                self.taxid = int(float(self.raw_taxid))
                self.raw_taxid_coerced_to_int = True
            except (ValueError, TypeError):
                self.raw_taxid_coerced_to_int = False
                self.taxid = None
