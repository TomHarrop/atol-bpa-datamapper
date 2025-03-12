from .arg_parser import parse_args_for_mapping
from .config_parser import MetadataMap
from .io import read_input, OutputWriter, write_mapping_log_to_csv
from .logger import logger, setup_logger
from .organism_mapper import OrganismSection, NcbiTaxdump

# profiling
import cProfile
import pstats
from io import StringIO

nodes_file = "dev/nodes.dmp"
names_file = "dev/names.dmp"
mapping_log_file = "test/organism_mapping_log.csv.gz"


def main():

    # debugging options
    max_iterations = 10
    manual_record = None

    args = parse_args_for_mapping()
    setup_logger(args.log_level)

    # shared objects
    ncbi_taxdump = NcbiTaxdump(nodes_file, names_file, resolve_to_rank="species")
    bpa_to_atol_map = MetadataMap(args.field_mapping_file, args.value_mapping_file)
    input_data = read_input(args.input)

    pr = cProfile.Profile()
    pr.enable()

    n_packages = 0
    mapping_log = {}

    with OutputWriter(args.output, args.dry_run) as writer:
        for package in input_data:

            n_packages += 1

            # debugging
            if manual_record and package.id != manual_record:
                continue

            if max_iterations and n_packages > max_iterations:
                break

            package.map_metadata(bpa_to_atol_map)
            organism_section = OrganismSection(
                package.id, package.mapped_metadata["organism"], ncbi_taxdump
            )

            writer.write_data(organism_section.__dict__)
            mapping_log[package.id] = [organism_section.__dict__]

            if n_packages % 10 == 0:
                logger.info(f"Processed {n_packages} packages")

    pr.disable()
    s = StringIO()
    sortby = "cumulative"
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    logger.warning(s.getvalue())

    write_mapping_log_to_csv(mapping_log, mapping_log_file)
