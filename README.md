# atol-bpa-datamapper

Map data from the BPA data portal for AToL's Genome Engine.

## Installation

### Recommended: Use the [BioContainer](https://quay.io/repository/biocontainers/atol-bpa-datamapper?tab=tags)

*e.g.* with Apptainer/Singularity:

```bash
apptainer exec \
  docker://quay.io/biocontainers/atol-bpa-datamapper:0.1.2--pyhdfd78af_0 \
  filter-packages
```

### Via `pip` or `conda`

Local installation isn't supported, but can be done with `pip`
from this repo, or from 
[bioconda](https://anaconda.org/bioconda/atol-bpa-datamapper).

## Usage

The input is compressed jsonlines data output from the `ckanapi search datasets` command.

Output is compressed jsonlines data.

See [`dev/scripts/test_commands.sh`](dev/scripts/test_commands.sh) for an example.

### filter-packages

```
usage: filter-packages [-h] [-i INPUT] [-o OUTPUT] [-f PACKAGE_FIELD_MAPPING_FILE] [-r RESOURCE_FIELD_MAPPING_FILE] [-v VALUE_MAPPING_FILE] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [-n] [--raw_field_usage RAW_FIELD_USAGE] [--bpa_field_usage BPA_FIELD_USAGE] [--bpa_value_usage BPA_VALUE_USAGE] [--decision_log DECISION_LOG]

Filter packages from jsonlines.gz

options:
  -h, --help            show this help message and exit

Input:
  -i INPUT, --input INPUT
                        Input file (default: stdin)

Output:
  -o OUTPUT, --output OUTPUT
                        Output file (default: stdout)

General options:
  -f PACKAGE_FIELD_MAPPING_FILE, --package_field_mapping_file PACKAGE_FIELD_MAPPING_FILE
                        Package-level field mapping file in json.
  -r RESOURCE_FIELD_MAPPING_FILE, --resource_field_mapping_file RESOURCE_FIELD_MAPPING_FILE
                        Resource-level field mapping file in json.
  -v VALUE_MAPPING_FILE, --value_mapping_file VALUE_MAPPING_FILE
                        Value mapping file in json.
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level (default: INFO)
  -n, --dry-run         Test mode. Output will be uncompressed jsonlines.

Counters:
  --raw_field_usage RAW_FIELD_USAGE
                        File for field usage counts in the raw data
  --bpa_field_usage BPA_FIELD_USAGE
                        File for BPA field usage counts
  --bpa_value_usage BPA_VALUE_USAGE
                        File for BPA value usage counts

Filtering options:
  --decision_log DECISION_LOG
                        Compressed CSV file to record the filtering decisions for each package
```

### map-metadata

```
usage: map-metadata [-h] [-i INPUT] [-o OUTPUT] [-f PACKAGE_FIELD_MAPPING_FILE] [-r RESOURCE_FIELD_MAPPING_FILE] [-v VALUE_MAPPING_FILE] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [-n] [--raw_field_usage RAW_FIELD_USAGE] [--raw_value_usage RAW_VALUE_USAGE] [--mapped_field_usage MAPPED_FIELD_USAGE] [--mapped_value_usage MAPPED_VALUE_USAGE]
                    [--unused_field_counts UNUSED_FIELD_COUNTS] [--mapping_log MAPPING_LOG] [--sanitization_changes SANITIZATION_CHANGES] --nodes NODES --names NAMES [--grouping_log GROUPING_LOG] [--grouped_packages GROUPED_PACKAGES] [--cache_dir CACHE_DIR]

Map metadata in filtered jsonlines.gz

options:
  -h, --help            show this help message and exit

Input:
  -i INPUT, --input INPUT
                        Input file (default: stdin)
  --nodes NODES         NCBI nodes.dmp file from taxdump
  --names NAMES         NCBI names.dmp file from taxdump

Output:
  -o OUTPUT, --output OUTPUT
                        Output file (default: stdout)

General options:
  -f PACKAGE_FIELD_MAPPING_FILE, --package_field_mapping_file PACKAGE_FIELD_MAPPING_FILE
                        Package-level field mapping file in json.
  -r RESOURCE_FIELD_MAPPING_FILE, --resource_field_mapping_file RESOURCE_FIELD_MAPPING_FILE
                        Resource-level field mapping file in json.
  -v VALUE_MAPPING_FILE, --value_mapping_file VALUE_MAPPING_FILE
                        Value mapping file in json.
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level (default: INFO)
  -n, --dry-run         Test mode. Output will be uncompressed jsonlines.
  --cache_dir CACHE_DIR
                        Directory to cache the NCBI taxonomy after processing

Counters:
  --raw_field_usage RAW_FIELD_USAGE
                        File for field usage counts in the raw data
  --raw_value_usage RAW_VALUE_USAGE
                        File for value usage counts in the raw data
  --mapped_field_usage MAPPED_FIELD_USAGE
                        File for counts of how many times each BPA field was mapped to an AToL field
  --mapped_value_usage MAPPED_VALUE_USAGE
                        File for counts of the values mapped from BPA fields to AToL fields
  --unused_field_counts UNUSED_FIELD_COUNTS
                        File for counts of fields in the BPA data that weren't used

Mapping options:
  --mapping_log MAPPING_LOG
                        Compressed CSV file to record the mapping used for each package
  --sanitization_changes SANITIZATION_CHANGES
                        File to record the sanitization changes made during mapping
  --grouping_log GROUPING_LOG
                        Compressed CSV file to record derived organism info for each package
  --grouped_packages GROUPED_PACKAGES
                        JSON file of Package IDs grouped by organism grouping_key
```

### Deployment

The package comes with metadata mapping specifications in
[`src/atol_bpa_datamapper/config`](src/atol_bpa_datamapper/config). The field
mapping spec can be generated from [AToL's metadata
schema](https://docs.google.com/spreadsheets/d/1ml5hASZ-qlAuuTrwHeGzNVqqe1mXsmmoDTekd6d9pto)
using the script at
[`dev/scripts/read_atol_schemas.py`](dev/scripts/read_atol_schemas.py).

