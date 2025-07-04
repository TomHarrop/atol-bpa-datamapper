#!/usr/bin/env python

from .logger import logger
from pathlib import Path
import hashlib
import pandas as pd
import shelve
import skbio.io
from skbio.tree import TreeNode
import re


def compute_sha256(file_path):
    logger.debug(f"Computing sha256 checksum for {file_path}.")
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            sha256.update(block)
    hex_digest = sha256.hexdigest()
    logger.debug(f"Checksum: {hex_digest}")
    return hex_digest


def read_taxdump_file(file_path, cache_dir, scheme):
    """
    Reads the taxdump file and caches it in a shelve file.

    Return a tuple of the data and a boolean indicating whether the cache was
    updated.
    """
    cache_file = Path(cache_dir, f"{Path(file_path).stem}_{scheme}.db")
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


def generate_taxonomy_tree(names, nodes, cache_dir, update_tree=False):
    cache_file = Path(cache_dir, "taxonomy_tree.db")
    with shelve.open(cache_file) as cache:
        if "tree" in cache and not update_tree:
            logger.info(f"Reading taxonomy tree from {cache_file}")
            return cache["tree"]
        else:
            logger.info("Generating taxonomy tree")
            tree = TreeNode.from_taxdump(nodes, names)
            cache["tree"] = tree
            return tree


def find_lower_ranks(tree, top_rank="species", excluded_ranks=["no rank"]):
    rank_list = recursive_find_lower_ranks(tree, top_rank)
    return [rank for rank in sorted(set(rank_list)) if rank not in excluded_ranks]


def recursive_find_lower_ranks(
    tree, top_rank="species", rank_list=[], top_rank_or_lower=False
):
    if tree.rank == top_rank:
        top_rank_or_lower = True
    if top_rank_or_lower:
        for node in tree.traverse():
            rank_list.append(node.rank)
    else:
        for node in tree.children:
            recursive_find_lower_ranks(node, top_rank, rank_list, top_rank_or_lower)
    return rank_list


def sanitise_string(string):
    allowed_chars = re.compile("[a-zA-Z0-9 ]")
    return "".join(allowed_chars.findall(re.sub(r"\s+", " ", string))).strip()


def split_scientific_name(scientific_name, null_values):
    my_scientific_name = sanitise_string(scientific_name)
    if my_scientific_name.upper() in null_values:
        logger.debug(f"{my_scientific_name} matched null_values")
        return None

    name_parts = [sanitise_string(x) for x in my_scientific_name.split(" ")]

    if not len(name_parts) == 2:
        logger.debug(f"Length of {name_parts} is not 2")
        return None

    for part in name_parts:
        if part.upper() in null_values:
            logger.debug(f"Name part {part} matched null_values")
            return None

    logger.debug(f"Parsed {name_parts} from {scientific_name}")
    return name_parts


def remove_whitespace(string):
    allowed_chars = re.compile("[a-zA-Z0-9]")
    return re.sub(r"[^a-zA-Z0-9]+", "_", string)


class NcbiTaxdump:

    def __init__(self, nodes_file, names_file, cache_dir, resolve_to_rank="species"):
        logger.info(f"Reading NCBI taxonomy from {nodes_file}")
        self.nodes, nodes_changed = read_taxdump_file(
            nodes_file, cache_dir, "nodes_slim"
        )

        logger.info(f"Reading NCBI taxon names from {names_file}")
        names, names_changed = read_taxdump_file(names_file, cache_dir, "names")

        # Create a dictionary for faster lookups
        scientific_names = names[names["name_class"] == "scientific name"]
        self.scientific_name_dict = scientific_names["name_txt"].to_dict()
        self.name_to_taxids = {}
        for taxid, name in self.scientific_name_dict.items():
            key = name.lower()
            self.name_to_taxids.setdefault(key, []).append(taxid)

        update_tree = any([nodes_changed, names_changed])

        self.tree = generate_taxonomy_tree(
            names, self.nodes, cache_dir, update_tree=update_tree
        )

        logger.info(f"Traversing the tree for rank information")
        self.resolve_to_rank = resolve_to_rank
        self.accepted_ranks = find_lower_ranks(self.tree, self.resolve_to_rank)
        logger.debug(
            f"Accepted ranks including and below {self.resolve_to_rank}:\n{self.accepted_ranks}"
        )

    def get_rank(self, taxid):
        return self.nodes.at[taxid, "rank"]

    def get_scientific_name_txt(self, taxid):
        return self.scientific_name_dict.get(taxid, None)

    def search_by_binomial_name(self, genus, species, package_id):
        search_string = f"{genus} {species}"
        logger.debug(f"Searching for {search_string}")

        candidate_taxids = self.name_to_taxids.get(search_string.lower(), [])
        if len(candidate_taxids) == 0:
            logger.debug(f"No results found for {search_string}")
            return None
        accepted_level_taxids = [
            taxid
            for taxid in candidate_taxids
            if self.get_rank(taxid) in self.accepted_ranks
        ]

        if len(accepted_level_taxids) == 1:
            return accepted_level_taxids[0]
        else:
            logger.debug(f"Didn't find a single taxid for {search_string}")
            logger.debug(accepted_level_taxids)

        return None


class OrganismSection(dict):

    def __init__(self, package_id, package_data, ncbi_taxdump, null_values=None):

        super().__init__()
        self.update(package_data)
        self.has_taxid = self.get("taxon_id") not in null_values + ["0", "0.0"]

        # get the taxon_id
        self.raw_taxon_id = self.get("taxon_id") if self.has_taxid else None
        self.taxon_id = None

        if self.raw_taxon_id is not None:
            self.format_taxon_id()

        # look up taxon_id in NCBI taxonomy
        self.check_ncbi_taxonomy_for_taxon_id(ncbi_taxdump)

        # Check for species information in the raw metadata. Realistically, we
        # can only do this if we can parse the scientific name into a Genus and
        # Species, or get that information from the Genus and Species fields,
        # and the names table has a single exact match at the species level.
        # Otherwise, too risky?
        self.taxid_retrieved_from_metadata = False
        if not self.scientific_name:
            self.check_bpa_metadata_for_species_information(
                ncbi_taxdump, package_id, null_values
            )

        self.check_for_subspecies_information(ncbi_taxdump, package_id, null_values)

        # generate a key for grouping the organisms
        # TODO: this should be some sort of UUID
        if self.has_taxid_at_accepted_level:
            self.organism_grouping_key = "_".join(
                [remove_whitespace(self.atol_scientific_name), str(self.taxon_id)]
            )
        else:
            self.organism_grouping_key = None

        logger.debug(f"OrganismSection\nProperties: {self}\ndict: {self.__dict__}")
        self.mapped_metadata = self.__dict__

    def check_bpa_metadata_for_species_information(
        self, ncbi_taxdump, package_id, null_values
    ):
        bpa_scientific_name = sanitise_string(str(self.get("scientific_name")))
        retrieved_taxid = None

        # check whatever's in the scientific name field
        logger.debug(f"Attempting to parse scientific name {bpa_scientific_name}")
        name_parts = split_scientific_name(bpa_scientific_name, null_values)

        if name_parts:
            retrieved_taxid = ncbi_taxdump.search_by_binomial_name(
                name_parts[0],
                name_parts[1],
                package_id,
            )
        else:
            logger.debug(f"Gave up on scientific name {bpa_scientific_name}")

        if not retrieved_taxid:
            # check if we have genus and species fields
            genus = sanitise_string(str(self.get("genus")))
            species = sanitise_string(str(self.get("species")))

            if genus.upper() not in null_values and species.upper() not in null_values:
                logger.debug(
                    f"Attempting to parse separate genus {genus} and species {species}"
                )
                retrieved_taxid = ncbi_taxdump.search_by_binomial_name(
                    genus,
                    species,
                    package_id,
                )

        if not retrieved_taxid:
            logger.debug(
                f"Could not match metadata to taxid at accepted level for package {package_id}"
            )

        # process the results
        if retrieved_taxid:
            logger.debug(f"Found single taxid at accepted level {retrieved_taxid}")
            self.taxon_id = retrieved_taxid
            self.has_taxid = True
            self.taxid_retrieved_from_metadata = True

            self.check_ncbi_taxonomy_for_taxon_id(ncbi_taxdump)
            logger.debug(
                f"Assigning scientific name {self.scientific_name} to package {package_id}"
            )

    def check_for_subspecies_information(self, ncbi_taxdump, package_id, null_values):
        # some taxids resolve lower than species, use these first
        if (
            self.has_taxid_at_accepted_level
            and self.rank != ncbi_taxdump.resolve_to_rank
        ):
            self.has_subspecies_information = True
            self.atol_scientific_name = self.scientific_name
            self.subspecies_source = "ncbi"
            return

        # try to resolve the subspecies information manually
        if (
            self.scientific_name
            and str(self.get("infraspecific_epithet")).upper() not in null_values
        ):
            logger.debug(
                f'{package_id} has subspecies information but taxon_id {self.taxon_id} rank "{self.rank}" is not lower than "{ncbi_taxdump.resolve_to_rank}"'
            )
            logger.debug("Accepted ranks: {ncbi_taxdump.accepted_ranks}")
            self.has_subspecies_information = True
            subspecies_sanitised = sanitise_string(self.get("infraspecific_epithet"))
            self.atol_scientific_name = " ".join(
                [self.scientific_name, subspecies_sanitised]
            )
            self.subspecies_source = "parsed"
            logger.debug("Assigning {self.atol_scientific_name}")
            return

        self.atol_scientific_name = self.scientific_name
        self.has_subspecies_information = False
        self.subspecies_source = None

    def check_ncbi_taxonomy_for_taxon_id(self, ncbi_taxdump):
        # Check if it's an NCBI taxid
        self.taxid_is_ncbi_node = (
            self.has_taxid and self.taxon_id in ncbi_taxdump.nodes.index
        )

        if self.taxid_is_ncbi_node:
            self.rank = ncbi_taxdump.get_rank(self.taxon_id)
            self.scientific_name = ncbi_taxdump.get_scientific_name_txt(self.taxon_id)
            self.scientific_name_source = "ncbi"
        else:
            self.rank = None
            self.scientific_name = None
            self.scientific_name_source = None

        self.has_taxid_at_accepted_level = self.rank in ncbi_taxdump.accepted_ranks

    def format_taxon_id(self):
        # check if the raw_taxon_id is an int
        if self.raw_taxon_id is None:
            return
            
        try:
            self.taxon_id = int(self.raw_taxon_id)
            self.raw_taxon_id_is_int = True
            self.raw_taxon_id_coerced_to_int = False
        except (ValueError, TypeError):
            self.raw_taxon_id_is_int = False
            # check if we can coerce taxon_id to int
            try:
                self.taxon_id = int(float(self.raw_taxon_id))
                self.raw_taxon_id_coerced_to_int = True
            except (ValueError, TypeError):
                self.raw_taxon_id_coerced_to_int = False
                self.taxon_id = None
