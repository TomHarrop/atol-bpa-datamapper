from .io import parse_args_for_mapping, read_input, OutputWriter
from .config_parser import MetadataMap


def main():

    max_iterations = 10

    args = parse_args_for_mapping()

    mapping_config = MetadataMap(args.field_mapping_file, args.value_mapping_file)
    print(mapping_config)
    quit(1)

    input_data = read_input(args.input)

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
