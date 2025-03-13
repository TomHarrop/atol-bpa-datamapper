# atol-bpa-datamapper

Map data from the BPA data portal for AToL's Genome Engine.

## Installation

### Recommended

- TODO: Use the Docker container from BioContainers

## Usage

The input is compressed jsonlines data output from the `ckanapi search datasets` command.

Output is compressed jsonlines data.

### filter-packages

```
usage: filter-packages [-h] [-i INPUT] [-o OUTPUT] [-f FIELD_MAPPING_FILE] [-v VALUE_MAPPING_FILE]
                       [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [-n] [--raw_field_usage RAW_FIELD_USAGE]
                       [--bpa_field_usage BPA_FIELD_USAGE] [--bpa_value_usage BPA_VALUE_USAGE]
                       [--decision_log DECISION_LOG]

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
  -f FIELD_MAPPING_FILE, --field_mapping_file FIELD_MAPPING_FILE
                        Field mapping file in json.
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
usage: map-metadata [-h] [-i INPUT] [-o OUTPUT] [-f FIELD_MAPPING_FILE] [-v VALUE_MAPPING_FILE]
                    [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [-n] [--raw_field_usage RAW_FIELD_USAGE]
                    [--raw_value_usage RAW_VALUE_USAGE] [--mapped_field_usage MAPPED_FIELD_USAGE]
                    [--mapped_value_usage MAPPED_VALUE_USAGE] [--unused_field_counts UNUSED_FIELD_COUNTS]
                    [--mapping_log MAPPING_LOG]

Map metadata in filtered jsonlines.gz

options:
  -h, --help            show this help message and exit

Input:
  -i INPUT, --input INPUT
                        Input file (default: stdin)

Output:
  -o OUTPUT, --output OUTPUT
                        Output file (default: stdout)

General options:
  -f FIELD_MAPPING_FILE, --field_mapping_file FIELD_MAPPING_FILE
                        Field mapping file in json.
  -v VALUE_MAPPING_FILE, --value_mapping_file VALUE_MAPPING_FILE
                        Value mapping file in json.
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level (default: INFO)
  -n, --dry-run         Test mode. Output will be uncompressed jsonlines.

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
```
### resolve-organism

```
usage: resolve-organism [-h] [-i INPUT] [-o OUTPUT] [-f FIELD_MAPPING_FILE] [-v VALUE_MAPPING_FILE] [-l {DEBUG,INFO,WARNING,ERROR,CRITICAL}] [-n] --nodes NODES --names NAMES [--rejected_packages REJECTED_PACKAGES]
                        [--mapping_log MAPPING_LOG] [--cache_dir CACHE_DIR]

Group packages in *filtered* metadata, according to derived species information

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
  --rejected_packages REJECTED_PACKAGES
                        Text list of packages that had insufficient organism information
  --mapping_log MAPPING_LOG
                        Compressed CSV file to record derived organism info for each package

General options:
  -f FIELD_MAPPING_FILE, --field_mapping_file FIELD_MAPPING_FILE
                        Field mapping file in json.
  -v VALUE_MAPPING_FILE, --value_mapping_file VALUE_MAPPING_FILE
                        Value mapping file in json.
  -l {DEBUG,INFO,WARNING,ERROR,CRITICAL}, --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set the logging level (default: INFO)
  -n, --dry-run         Test mode. Output will be uncompressed jsonlines.
  --cache_dir CACHE_DIR
                        Directory to cache the NCBI taxonomy after processing

```