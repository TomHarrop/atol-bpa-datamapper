from .arg_parser import parse_args_for_mapping
from .io import read_input, OutputWriter
from .logger import logger, setup_logger
from .organism_mapper import OrganismSection, NcbiTaxdump
from .config_parser import MetadataMap


nodes_file = "dev/nodes.dmp"
names_file = "dev/names.dmp"


def main():

    args = parse_args_for_mapping()
    setup_logger(args.log_level)

    ncbi_taxdump = NcbiTaxdump(nodes_file, names_file)
    print(ncbi_taxdump.get_rank(3075944))
    print(ncbi_taxdump.get_scientific_name_txt(3075944))
    quit(1)

    bpa_to_atol_map = MetadataMap(args.field_mapping_file, args.value_mapping_file)
    input_data = read_input(args.input)
    for package in input_data:
        package.map_metadata(bpa_to_atol_map)
        organism_section = OrganismSection(
            package.mapped_metadata["organism"], ncbi_taxdump
        )
        print(organism_section.__dict__)
