from .arg_parser import parse_args_for_filtering
from .config_parser import MetadataMap
from .io import read_input, OutputWriter, write_json, write_decision_log_to_csv
from .logger import logger
from collections import Counter


def main():

    args = parse_args_for_filtering()

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

    # set up decision log
    decision_log = {}

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

            decision_log[package.id] = package.decisions

            # Process each item
            if package.keep:
                n_kept += 1
                output_writer.write_data(package)

    logger.info(f"Processed {n_packages} packages")
    logger.info(f"Kept {n_kept} packages")

    if args.decision_log and not args.dry_run:
        logger.info("Writing decision log")
        write_decision_log_to_csv(decision_log, args.decision_log)

    if not args.dry_run:
        logger.info("Writing usage counts")
        if args.field_usage:
            write_json(args.field_usage, counters["field_usage"])
        if args.bpa_key_usage:
            write_json(args.bpa_key_usage, counters["bpa_key_usage"])
        if args.bpa_value_usage:
            write_json(args.bpa_value_usage, counters["bpa_value_usage"])


if __name__ == "__main__":
    main()
