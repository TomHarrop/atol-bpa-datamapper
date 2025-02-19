import argparse
from .utils import parse_args, read_input, OutputWriter


def main():
    args = parse_args()

    input_data = read_input(args.input)
    with OutputWriter(args.output, args.dry_run) as writer:
        for item in input_data:
            # Process each item
            writer.write_data(item)
            quit(1)


if __name__ == "__main__":
    main()
