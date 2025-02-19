from .utils import parse_args_for_filtering, read_input, OutputWriter


def main():

    max_iterations = 10

    args = parse_args_for_filtering()
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
