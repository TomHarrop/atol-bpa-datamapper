from .arg_parser import parse_args_for_mapping
from .io import read_input, OutputWriter
from .config_parser import MetadataMap
from .logger import logger, setup_logger


def main():

    max_iterations = None

    args = parse_args_for_mapping()
    setup_logger(args.log_level)

    mapping_config = MetadataMap(args.field_mapping_file, args.value_mapping_file)

    input_data = read_input(args.input)

    n_packages = 0

    with OutputWriter(args.output, args.dry_run) as output_writer:
        for package in input_data:
            n_packages += 1

            package.map_metadata(mapping_config)

            output_writer.write_data(package.mapped_metadata)

            if max_iterations and n_packages >= max_iterations:
                break

    logger.info(f"Processed {n_packages} packages")


if __name__ == "__main__":
    main()
