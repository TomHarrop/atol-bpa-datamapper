#!/usr/bin/env python

from .io import read_gzip_textfile
from .logger import logger
from pathlib import Path
from skbio.tree import TreeNode
import hashlib
import pandas as pd
import shelve
import skbio.io

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
            # I think omitting the names means we get taxids as names (better
            # for searching). To include names, use
            # `TreeNode.from_taxdump(nodes, names)`.
            tree = TreeNode.from_taxdump(nodes)
            logger.info("Indexing tree")
            tree.index_tree()
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


def read_augustus_mapping(taxids_to_augustus_dataset_mapping):
    taxid_to_dataset = {}
    with open(taxids_to_augustus_dataset_mapping, "rt") as f:
        for i, line in enumerate(f, 1):
            splits = line.strip().split(maxsplit=1)
            taxid_to_dataset.update({int(splits[0]): str(splits[1])})

    logger.debug(taxid_to_dataset)
    return taxid_to_dataset


def read_busco_mapping(taxids_to_busco_dataset_mapping):
    dataset_mapping = read_gzip_textfile(taxids_to_busco_dataset_mapping)
    next(dataset_mapping)  # skip the header
    taxid_to_dataset = {}
    for mapping in dataset_mapping:
        splits = mapping.strip().split(maxsplit=1)
        taxid_to_dataset.update({int(splits[0]): str(splits[1])})
    logger.debug(taxid_to_dataset)
    return taxid_to_dataset


def remove_whitespace(string):
    allowed_chars = re.compile("[a-zA-Z0-9]")
    return re.sub(r"[^a-zA-Z0-9]+", "_", string)


def get_node(tree, taxid):
    try:
        node = tree.find(int(taxid))
    except skbio.tree._exception.MissingNodeError as e:
        logger.debug(f"Node {taxid} not found, trying a string search")
        node = tree.find(str(taxid))
        logger.debug(f"    ... found {node}")

    if isinstance(node, skbio.tree._tree.TreeNode):
        return node
    else:
        logger.warning(f"Node for taxid {taxid} not found in tree.")


class NcbiTaxdump:

    def __init__(
        self,
        nodes_file,
        names_file,
        taxids_to_busco_dataset_mapping,
        taxids_to_augustus_dataset_mapping,
        cache_dir,
        resolve_to_rank="species",
    ):

        logger.info(f"Reading NCBI taxonomy from {nodes_file}")
        self.nodes, nodes_changed = read_taxdump_file(
            nodes_file, cache_dir, "nodes_slim"
        )

        logger.info(f"Reading NCBI taxon names from {names_file}")
        names, names_changed = read_taxdump_file(names_file, cache_dir, "names")

        # Generate taxonomoic info dictionaries for faster lookups
        scientific_names = names[names["name_class"] == "scientific name"]
        self.scientific_name_dict = scientific_names["name_txt"].to_dict()

        # prefer genbank common name, fallback to common name
        genbank_common_names = names[names["name_class"] == "genbank common name"]
        genbank_common_name_dict = genbank_common_names["name_txt"].to_dict()
        common_names = names[names["name_class"] == "common name"]
        common_name_dict = common_names["name_txt"].to_dict()
        self.common_name_dict = {**common_name_dict, **genbank_common_name_dict}

        self.authority_dict = names[names["name_class"] == "authority"][
            "name_txt"
        ].to_dict()

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

        logger.info(
            f"Reading BUSCO dataset mapping from {taxids_to_busco_dataset_mapping}"
        )
        self.busco_mapping = read_busco_mapping(taxids_to_busco_dataset_mapping)
        logger.info(
            f"    ... found {len(self.busco_mapping.keys())} datasets in BUSCO mapping file"
        )

        logger.info(
            f"Reading Augustus dataset mapping from {taxids_to_augustus_dataset_mapping}"
        )
        self.augustus_mapping = read_augustus_mapping(
            taxids_to_augustus_dataset_mapping
        )
        logger.info(
            f"    ... found {len(self.augustus_mapping.keys())} datasets in Augustus mapping file"
        )

        logger.info(f"Generating tree for Augustus datasets")
        augustus_tree = self.tree.copy(deep=True)
        initial_node_count = int(augustus_tree.count())

        # Shear works on tips but not all Augustus nodes are tips. We
        # need to remove all descendants of the Augustus nodes first.
        augustus_nodes = [
            get_node(augustus_tree, x) for x in self.augustus_mapping.keys()
        ]
        augustus_node_names = [x.name for x in augustus_nodes]
        logger.debug(
            f"Found {len(augustus_node_names)} Augustus taxids in tree:\n{augustus_node_names}"
        )

        for node in augustus_nodes:
            if node.has_children():
                node_children = node.children
                logger.debug(
                    f"Node {node.name} has children {[x.name for x in node_children]}"
                )
                logger.debug(f"\n{node.ascii_art()}")
                # I don't know why, but this sometimes leaves one child.
                for child in node_children:
                    logger.debug(f"Removing node {child.name}")
                    removed = node.remove(child)
                    logger.debug(f"Removed? {removed}")

                # Running pop removes the remaining child.
                if not node.is_tip():
                    node.pop()

            logger.debug(f"Is node {node.name} a tip? {node.is_tip()}")
            logger.debug(f"\n{node.ascii_art()}")

        # Now we can shear the tree
        self.augustus_tree = augustus_tree.shear(
            names=augustus_node_names,
            prune=False,
            inplace=False,
            strict=False,
        )

        final_node_count = int(self.augustus_tree.count())
        nodes_removed = initial_node_count - final_node_count

        logger.debug(f"    ... NCBI tree had {initial_node_count} nodes.")
        logger.debug(f"    ... Removed {nodes_removed} nodes.")
        logger.debug(f"    ... Augustus tree has {final_node_count} nodes.")

        logger.debug("Getting tip names of the Augustus tree")
        self.augustus_tip_names = [x.name for x in self.augustus_tree.tips()]
        logger.debug(f"    ... {self.augustus_tip_names}")

    def get_authority_txt(self, taxid):
        return self.authority_dict.get(taxid, None)

    def get_common_name_txt(self, taxid):
        return self.common_name_dict.get(taxid, None)

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

    def get_ancestor_taxids(self, taxid):
        logger.debug(f"Looking up ancestors for taxid {taxid}")

        node = get_node(self.tree, taxid)

        ancestor_taxids = [x.name for x in node.ancestors()]
        logger.debug(f"ancestor_taxids: {ancestor_taxids}")
        return ancestor_taxids

    def get_busco_lineage(self, taxid, ancestor_taxids):
        """
        Find the closest ancestor that is in the BUSCO taxid map and return the
        lineage name.
        """

        logger.debug(f"Looking up BUSCO dataset name for taxid {taxid}")
        logger.debug(f"Checking ancestor_taxids {ancestor_taxids}")

        for taxid in ancestor_taxids:
            if int(taxid) in self.busco_mapping.keys():
                return self.busco_mapping[int(taxid)]
            if taxid in self.busco_mapping.keys():
                return self.busco_mapping[taxid]

    def get_augustus_lineage(self, taxid, ancestor_taxids):

        logger.debug(f"Looking up Augustus dataset name for taxid {taxid}")

        node = get_node(self.tree, taxid)

        # Include taxid in the search, in case it has been trained
        ancestor_taxids.insert(0, taxid)

        # Find the first ancestor that is present in the Augustus sub-tree
        closest_taxid_in_augustus_tree = None
        while not closest_taxid_in_augustus_tree and len(ancestor_taxids) > 0:
            taxid = ancestor_taxids.pop(0)
            logger.debug(f"Checking Augustus tree for {taxid}")
            try:
                closest_taxid_in_augustus_tree = get_node(self.augustus_tree, taxid)
            except skbio.tree._exception.MissingNodeError:
                logger.debug(f"Node {taxid} not in Augustus tree")

        logger.debug(
            f"Found closest_taxid_in_augustus_tree {closest_taxid_in_augustus_tree.name}"
        )

        if closest_taxid_in_augustus_tree:
            dist_to_dataset = {}
            for taxid in self.augustus_mapping.keys():
                logger.debug(f"Calculating distance to {taxid}")
                try:
                    dest_node = get_node(self.augustus_tree, taxid)
                    dist_to_dataset[taxid] = closest_taxid_in_augustus_tree.distance(
                        dest_node, use_length=False
                    )
                    logger.debug(f"    ...{dist_to_dataset[taxid]}")
                except skbio.tree._exception.MissingNodeError:
                    logger.debug(f"{taxid} not in Augustus tree")

            logger.debug(f"Distances: {dist_to_dataset}")
            closest_dataset = min(dist_to_dataset, key=dist_to_dataset.get)

            logger.debug(
                f"Closest dataset is {closest_dataset} with dist {dist_to_dataset[closest_dataset]}"
            )

            return self.augustus_mapping[closest_dataset]


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

        # generate a key for grouping the organisms and lookup lineage information
        if self.has_taxid_at_accepted_level and self.scientific_name_source == "ncbi":
            self.organism_grouping_key = f"taxid{self.taxon_id}"
            ancestor_taxids = ncbi_taxdump.get_ancestor_taxids(self.taxon_id)
            ancestor_names_all = [
                ncbi_taxdump.get_scientific_name_txt(int(x)) for x in ancestor_taxids
            ]
            ancestor_names = [x for x in ancestor_names_all if x not in [None, "root"]]

            if len(ancestor_names) > 0:
                name_list = ancestor_names[::-1]
                name_list.append(self.scientific_name)
                self.tax_string = "; ".join(name_list)
            else:
                self.tax_string = None

            self.busco_dataset_name = ncbi_taxdump.get_busco_lineage(
                self.taxon_id, ancestor_taxids
            )
            logger.debug(f"Found BUSCO dataset {self.busco_dataset_name}")
            self.augustus_dataset_name = ncbi_taxdump.get_augustus_lineage(
                self.taxon_id, ancestor_taxids
            )
            logger.debug(f"Found Augustus dataset {self.augustus_dataset_name}")
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
            self.subspecies_source = "parsed"

            # Using the BPA subspecies info is disabled, see
            # https://github.com/TomHarrop/atol-bpa-datamapper/issues/26
            # subspecies_sanitised = sanitise_string(self.get("infraspecific_epithet"))
            # self.atol_scientific_name = " ".join(
            #     [self.scientific_name, subspecies_sanitised]
            # )
            # logger.debug("Assigning {self.atol_scientific_name}")
            self.atol_scientific_name = self.scientific_name
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

            self.ncbi_common_name = ncbi_taxdump.get_common_name_txt(self.taxon_id)
            self.authority = ncbi_taxdump.get_authority_txt(self.taxon_id)

        else:
            self.rank = None
            self.scientific_name = None
            self.scientific_name_source = None

            self.ncbi_common_name = None
            self.authority = None

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
