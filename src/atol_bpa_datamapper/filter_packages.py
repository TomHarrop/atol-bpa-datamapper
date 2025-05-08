from .arg_parser import parse_args_for_filtering
from .config_parser import MetadataMap
from .io import read_input, OutputWriter, write_json, write_decision_log_to_csv
from .logger import logger, setup_logger
from collections import Counter


def main():

    max_iterations = None

    args = parse_args_for_filtering()
    setup_logger(args.log_level)

    package_level_map = MetadataMap(
        args.package_field_mapping_file, args.value_mapping_file
    )

    resource_level_map = MetadataMap(
        args.resource_field_mapping_file, args.value_mapping_file
    )

    input_data = read_input(args.input)

    # set up counters
    all_controlled_vocabularies = sorted(
        set(
            package_level_map.controlled_vocabularies
            + resource_level_map.controlled_vocabularies
        )
    )
    counters = {
        "raw_field_usage": Counter(),
        "bpa_field_usage": {
            atol_field: Counter() for atol_field in all_controlled_vocabularies
        },
        "bpa_value_usage": {
            atol_field: Counter() for atol_field in all_controlled_vocabularies
        },
    }

    # set up decision log
    decision_log = {}

    n_packages = 0
    n_kept = 0

    with OutputWriter(args.output, args.dry_run) as output_writer:
        for package in input_data:
            n_packages += 1
            logger.debug(f"Processing package {package.id}")
            counters["raw_field_usage"].update(package.fields)

            package.filter(package_level_map)
            for atol_field, bpa_field in package.bpa_fields.items():
                counters["bpa_field_usage"][atol_field].update([bpa_field])
            for atol_field, bpa_value in package.bpa_values.items():
                counters["bpa_value_usage"][atol_field].update([bpa_value])

            decision_log[package.id] = package.decisions

            if package.keep:
                n_kept += 1
                output_writer.write_data(package)

            for k, v in package.resources.items():
                v.filter(resource_level_map)
                if v.keep is True:
                    logger.error(decision_log)
                    logger.error(v.decisions)
                    quit(1)

            if max_iterations and n_packages >= max_iterations:
                break

    logger.info(f"Processed {n_packages} packages")
    logger.info(f"Kept {n_kept} packages")

    # write optional output
    if not args.dry_run:
        if args.decision_log:
            logger.info(f"Writing decision log to {args.decision_log}")
            write_decision_log_to_csv(decision_log, args.decision_log)
        if args.raw_field_usage:
            logger.info(f"Writing field usage counts to {args.raw_field_usage}")
            write_json(counters["raw_field_usage"], args.raw_field_usage)
        if args.bpa_field_usage:
            logger.info(f"Writing BPA key usage counts to {args.bpa_field_usage}")
            write_json(counters["bpa_field_usage"], args.bpa_field_usage)
        if args.bpa_value_usage:
            logger.info(f"Writing BPA value usage counts to {args.bpa_value_usage}")
            write_json(counters["bpa_value_usage"], args.bpa_value_usage)


if __name__ == "__main__":
    main()
