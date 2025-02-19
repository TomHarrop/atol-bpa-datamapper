import sys
import gzip
import jsonlines
import argparse


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read jsonlines.gz from stdin and write jsonlines.gz to stdout."
    )
    input_group = parser.add_argument_group("Input")

    input_group.add_argument(
        "-i",
        "--input",
        type=argparse.FileType("rb"),
        default=sys.stdin.buffer,
        help="Input file (default: stdin)",
    )

    output_group = parser.add_argument_group("Output")
    output_group.add_argument(
        "-o",
        "--output",
        type=argparse.FileType("wb"),
        default=sys.stdout.buffer,
        help="Output file (default: stdout)",
    )

    options_group = parser.add_argument_group("Options")
    options_group.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Test mode. Output will be uncompressed jsonlines.",
    )

    return parser.parse_args()


def read_input(input_source):
    with gzip.open(input_source, "rt") as f:
        reader = jsonlines.Reader(f)
        for obj in reader:
            yield obj


class OutputWriter:
    def __init__(self, output_dest, dry_run=False):
        self.output_dest = output_dest
        self.dry_run = dry_run
        self.file_object = None

    def __enter__(self):
        if self.dry_run:
            self.file_object = (
                self.output_dest
                if self.output_dest is sys.stdout.buffer
                else open(self.output_dest.name, "w")
            )
        else:
            self.file_object = (
                gzip.open(self.output_dest, "wt")
                if self.output_dest is not sys.stdout.buffer
                else gzip.GzipFile(fileobj=self.output_dest, mode="w")
            )
        self.writer = jsonlines.Writer(self.file_object)
        return self

    def write_data(self, data):
        self.writer.write(data)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.writer.close()
        if self.file_object is not self.output_dest:
            self.file_object.close()
