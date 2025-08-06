from .logger import logger
from .package_handler import BpaPackage
import csv
import gzip
import jsonlines
import sys
import tarfile


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


def _extract_tarfile(file_path):
    with tarfile.open(file_path, "r:gz") as tar:
        for member in tar.getmembers():
            if member.isfile() and not member.name.startswith("."):
                for line in tar.extractfile(member).read().decode().splitlines():
                    yield (line)


def read_gzip_textfile(file_path):
    if file_path.endswith(".tar.gz") or file_path.endswith(".tgz"):
        f = _extract_tarfile(file_path)
    else:
        f = gzip.open(file_path, "rt")

    for i, line in enumerate(f, 1):
        if "\x00" in line:
            raise ValueError(f"Null bytes at line {i} of {file_path}")
        yield line


def read_input(input_source):
    """
    Construct BpaPackage objects from BPA metadata .jsonl.gz files.
    """
    for obj in read_jsonl_file(input_source):
        yield BpaPackage(obj)


def read_jsonl_file(input_source):
    """
    Read generic jsonl.gz objects.
    """
    logger.info(f"Reading input from {input_source.name}")
    with gzip.open(input_source, "rt") as f:
        reader = jsonlines.Reader(f)
        for obj in reader:
            if isinstance(obj, dict):
                yield obj
            else:
                logger.warning(f"Skipping non-dictionary object: {obj}")
                continue


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
