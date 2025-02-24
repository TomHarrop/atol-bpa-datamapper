from .arg_parser import parse_args_for_mapping
from .config_parser import MetadataMap
from .io import read_input, OutputWriter, write_mapping_log_to_csv, write_json
from .logger import logger, setup_logger
from collections import Counter


def main():

    max_iterations = None

    args = parse_args_for_mapping()
    setup_logger(args.log_level)

    bpa_to_atol_map = MetadataMap(args.field_mapping_file, args.value_mapping_file)
    input_data = read_input(args.input)

    # set up counters
    counters = {
        "raw_field_usage": Counter(),
        "raw_value_usage": {},
        "mapped_field_usage": {
            atol_field: Counter() for atol_field in bpa_to_atol_map.expected_fields
        },
        "mapped_value_usage": {
            atol_field: Counter() for atol_field in bpa_to_atol_map.expected_fields
        },
        "unused_field_counts": Counter(),
    }

    # set up mapping log
    mapping_log = {}

    n_packages = 0

    with OutputWriter(args.output, args.dry_run) as output_writer:
        for package in input_data:
            n_packages += 1
            logger.debug(f"Processing package {package.id}")
            counters["raw_field_usage"].update(package.fields)
            for bpa_field in package.fields:
                if bpa_field not in counters["raw_value_usage"]:
                    counters["raw_value_usage"][bpa_field] = Counter()
                try:
                    counters["raw_value_usage"][bpa_field].update([package[bpa_field]])
                except TypeError:
                    pass

            package.map_metadata(bpa_to_atol_map)
            output_writer.write_data(package.mapped_metadata)
            mapping_log[package.id] = package.mapping_log

            # update counts
            counters["unused_field_counts"].update(package.unused_fields)

            for section in package.mapped_metadata.values():
                for atol_field, mapped_value in section.items():
                    bpa_field = package.field_mapping[atol_field]
                    counters["mapped_field_usage"][atol_field].update([bpa_field])
                    counters["mapped_value_usage"][atol_field].update([mapped_value])

            if max_iterations and n_packages >= max_iterations:
                break
    logger.info(f"Processed {n_packages} packages")

    # write optional output
    if not args.dry_run:
        if args.mapping_log:
            logger.info(f"Writing mapping log to {args.mapping_log}")
            write_mapping_log_to_csv(mapping_log, args.mapping_log)
        if args.raw_field_usage:
            logger.info(f"Writing field usage counts to {args.raw_field_usage}")
            write_json(counters["raw_field_usage"], args.raw_field_usage)
        if args.raw_value_usage:
            logger.info(f"Writing field usage counts to {args.raw_value_usage}")
            write_json(counters["raw_value_usage"], args.raw_value_usage)
        if args.mapped_field_usage:
            logger.info(f"Writing BPA key usage counts to {args.mapped_field_usage}")
            write_json(counters["mapped_field_usage"], args.mapped_field_usage)
        if args.mapped_value_usage:
            logger.info(f"Writing BPA value usage counts to {args.mapped_value_usage}")
            write_json(counters["mapped_value_usage"], args.mapped_value_usage)
        if args.mapped_value_usage:
            logger.info(f"Writing unused field counts to {args.unused_field_counts}")
            write_json(counters["unused_field_counts"], args.unused_field_counts)


if __name__ == "__main__":
    main()
