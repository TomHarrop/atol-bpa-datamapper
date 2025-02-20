from .arg_parser import parse_args_for_filtering
from .config_parser import MetadataMap
from .io import read_input, OutputWriter
from .logger import logger
from collections import Counter


def main():

    max_iterations = 100

    args = parse_args_for_filtering()
    print(args.field_usage)
    quit(1)

    bpa_to_atol_map = MetadataMap(args.field_mapping_file, args.value_mapping_file)
    input_data = read_input(args.input)

    # set up counters
    counters = {
        "field_usage": Counter(),
        "bpa_key_usage": {
            atol_field: Counter()
            for atol_field in bpa_to_atol_map.controlled_vocabularies
        },
        "bpa_value_usage": {
            atol_field: Counter()
            for atol_field in bpa_to_atol_map.controlled_vocabularies
        },
    }

    print(counters)

    n_packages = 0
    n_kept = 0

    with OutputWriter(args.output, args.dry_run) as output_writer:
        for package in input_data:
            n_packages += 1
            counters["field_usage"].update(package.fields)

            package.filter(bpa_to_atol_map)
            for atol_field, bpa_field in package.bpa_fields.items():
                counters["bpa_key_usage"][atol_field].update([bpa_field])
            for atol_field, bpa_value in package.bpa_values.items():
                counters["bpa_value_usage"][atol_field].update([bpa_value])

            # Process each item
            if package.keep:
                n_kept += 1
                output_writer.write_data(package)

            if n_packages >= max_iterations:
                break

    logger.info(f"Processed {n_packages} packages")
    logger.info(f"Kept {n_kept} packages")

if __name__ == "__main__":
    main()
