from .common import parse_args_for_filtering, read_input, OutputWriter, setup_logger
from .config_parser import MetadataMap
import json


def main():

    max_iterations = 10

    args = parse_args_for_filtering()

    logger = setup_logger(args.log_level)

    logger.debug(f"Reading field mapping from {args.field_mapping_file}")
    logger.debug(f"Reading value mapping from {args.value_mapping_file}")
    bpa_to_atol_map = MetadataMap(args.field_mapping_file, args.value_mapping_file)
    logger.debug(f"Defined fields:\n{bpa_to_atol_map.expected_fields}")
    logger.debug(f"Controlled vocabularies:\n{bpa_to_atol_map.controlled_vocabularies}")

    logger.debug(f"Reading input from {args.input.name}")
    input_data = read_input(args.input)

    quit(1)

    i = 0

    with OutputWriter(args.output, args.dry_run) as output_writer:
        for item in input_data:
            i += 1
            # Process each item
            output_writer.write_data(item)

            if i >= max_iterations:
                break


if __name__ == "__main__":
    main()
