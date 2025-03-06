from .logger import logger
from .package_handler import BpaPackage
import csv
import gzip
import json
import jsonlines
import sys
from typing import Union, TextIO, BinaryIO, Any


class OutputWriter:
    """Write output data to a file or stdout."""

    def __init__(self, output_dest: Union[str, TextIO, BinaryIO], dry_run=False):
        """Initialize writer with output destination."""
        self.output_dest = output_dest
        self.dry_run = dry_run
        if isinstance(output_dest, str):
            logger.info(f"Writing output to {self.output_dest}")
        else:
            logger.info(f"Writing output to {self.output_dest.name}")

    def __enter__(self):
        return self

    def write_data(self, data: Any):
        """Write data to output destination."""
        if isinstance(self.output_dest, str):
            # File output
            if self.output_dest.endswith('.gz'):
                with gzip.open(self.output_dest, 'wt', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
            else:
                with open(self.output_dest, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
        else:
            # Stdout or other file-like object
            if hasattr(self.output_dest, 'mode') and 'b' in self.output_dest.mode:
                # Binary mode - wrap in GzipFile for stdout
                with gzip.GzipFile(fileobj=self.output_dest, mode='wb') as gz:
                    writer = jsonlines.Writer(gz)
                    writer.write(data)
            else:
                # Text mode
                json.dump(data, self.output_dest, indent=2)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def read_input(input_source):
    logger.info(f"Reading input from {input_source.name}")
    with gzip.open(input_source, "rt") as f:
        reader = jsonlines.Reader(f)
        for obj in reader:
            yield BpaPackage(obj)


def write_decision_log_to_csv(decision_log, file_path):
    """
    Write the decision log to a CSV file.
    """
    with gzip.open(file_path, "wt") as file:
        writer = csv.writer(file)
        # Write the header
        header = ["id"] + list(next(iter(decision_log.values())).keys())
        writer.writerow(header)
        # Write the rows
        for id, decisions in decision_log.items():
            row = [id] + list(decisions.values())
            writer.writerow(row)


def write_mapping_log_to_csv(mapping_log, file_path):
    with gzip.open(file_path, "wt") as file:
        writer = csv.writer(file)
        # Write the header
        first_package = next(iter(mapping_log.values()))
        first_entry = first_package[0]
        header = ["id"] + list(first_entry.keys())
        writer.writerow(header)
        # Write the rows
        for package_id, fields in mapping_log.items():
            for field_data in fields:
                row = [package_id] + [field_data.get(col) for col in first_entry.keys()]
                writer.writerow(row)


def write_json(data, file):
    with gzip.open(file, "wb") as f:
        jsonlines.Writer(f).write(data)
