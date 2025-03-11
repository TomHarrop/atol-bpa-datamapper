from .arg_parser import parse_args_for_mapping
from .io import read_input, OutputWriter, write_mapping_log_to_csv
from .logger import logger, setup_logger
from .organism_mapper import OrganismSection, NcbiTaxdump
from .config_parser import MetadataMap


nodes_file = "dev/nodes.dmp"
names_file = "dev/names.dmp"
mapping_log_file = "test/organism_mapping_log.csv.gz"


def main():

    # debugging options
    max_iterations = 10
    manual_record = None

    args = parse_args_for_mapping()
    setup_logger(args.log_level)

    ncbi_taxdump = NcbiTaxdump(nodes_file, names_file, resolve_to_rank="species")

    mapping_log = {}
    n_packages = 0

    bpa_to_atol_map = MetadataMap(args.field_mapping_file, args.value_mapping_file)
    input_data = read_input(args.input)
    for package in input_data:

        # debugging
        if manual_record and package.id != manual_record:
            continue

        if max_iterations and n_packages > max_iterations:
            break

        n_packages += 1

        package.map_metadata(bpa_to_atol_map)
        # TODO: parallelise, this is currently very slow.
        organism_section = OrganismSection(
            package.id, package.mapped_metadata["organism"], ncbi_taxdump
        )

        # just for now, do this properly later
        mapping_log[package.id] = [organism_section.__dict__]

        if n_packages % 10 == 0:
            logger.info(f"Processed {n_packages} packages")

        # if (
        #     organism_section.taxid_retrieved_from_metadata
        #     # and organism_section.has_species_level_taxid
        # ):
        #     print(package.id)
        #     print(organism_section)
        #     print(organism_section.__dict__)
        #     quit(1)

    write_mapping_log_to_csv(mapping_log, mapping_log_file)
