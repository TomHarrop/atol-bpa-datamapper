from .logger import logger
from .package_handler import BpaPackage
import csv
import gzip
import jsonlines
import sys

class OutputWriter:
    def __init__(self, output_dest, dry_run=False):
        self.output_dest = output_dest
        self.dry_run = dry_run
        self.file_object = None
        self.writer = None
        logger.info(f"Writing output to {self.output_dest.name}")

    def __enter__(self):
        self._open_file()
        return self

    def _open_file(self):
        logger.debug(f"Opening {self.output_dest.name} for writing")
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
        try:
            self.writer.write(data)
        except (AttributeError, RuntimeError) as e:
            self._open_file()
            self.writer.write(data)
            self._close_file()

    def _close_file(self):
        logger.debug(f"Closing {self.output_dest.name}")
        if self.writer:
            self.writer.close()
        if self.file_object and self.file_object is not self.output_dest:
            self.file_object.close()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_file()


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
    writer = csv.writer(file_path)
    # Write the header
    header = ["id"] + list(next(iter(decision_log.values())).keys())
    writer.writerow(header)
    # Write the rows
    for id, decisions in decision_log.items():
        row = [id] + list(decisions.values())
        writer.writerow(row)


def write_json(data, file):
    with gzip.open(file, "wb") as f:
        jsonlines.Writer(f).write(data)
