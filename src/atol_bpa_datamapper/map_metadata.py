from .arg_parser import parse_args_for_mapping
from .config_parser import MetadataMap
from .io import read_input, OutputWriter, write_mapping_log_to_csv, write_json
from .logger import logger, setup_logger
from collections import Counter


def main():

    max_iterations = None

    args = parse_args_for_mapping()
    setup_logger(args.log_level)

    package_level_map = MetadataMap(
        args.package_field_mapping_file, args.value_mapping_file
    )
    resource_level_map = MetadataMap(
        args.resource_field_mapping_file, args.value_mapping_file
    )

    input_data = read_input(args.input)

    # set up counters
    all_fields = sorted(
        set(package_level_map.expected_fields + resource_level_map.expected_fields)
    )

    counters = {
        "raw_field_usage": Counter(),
        "raw_value_usage": {},
        "mapped_field_usage": {atol_field: Counter() for atol_field in all_fields},
        "mapped_value_usage": {atol_field: Counter() for atol_field in all_fields},
        "unused_field_counts": Counter(),
    }

    # set up mapping log
    mapping_log = {}

    # set up sanitization changes log
    sanitization_changes = {}

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

            package.map_metadata(package_level_map)

            # map the resource-level metadata
            resource_mapped_metadata = {
                section: [] for section in resource_level_map.metadata_sections
            }
            for resource_id, resource in package.resources.items():
                resource.map_metadata(resource_level_map, package)
                for section in resource_mapped_metadata:
                    resource_mapped_metadata[section].append(
                        resource.mapped_metadata[section]
                    )

            for section, resource_metadata in resource_mapped_metadata.items():
                package.mapped_metadata[section] = resource_metadata


            output_writer.write_data(package.mapped_metadata)
            mapping_log[package.id] = package.mapping_log

            # Store sanitization changes if any were made
            if (
                hasattr(package, "sanitization_changes")
                and package.sanitization_changes
            ):
                sanitization_changes[package.id] = package.sanitization_changes

            # update counts
            counters["unused_field_counts"].update(package.unused_fields)

            logger.debug(package.mapped_metadata)

            for section_name, section in package.mapped_metadata.items():
                if isinstance(section, list):
                    section = section[0]

                logger.debug(f"{section_name}\n{section}")

                for atol_field, mapped_value in section.items():
                    try:
                        bpa_field = package.field_mapping[atol_field]
                    except KeyError:
                        # for Resources, we have to look up the key in the
                        # Resource section.
                        bpa_field = sorted(
                            set(
                                x.field_mapping[atol_field]
                                for x in list(package.resources.values())
                            )
                        )[0]
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
        if args.unused_field_counts:
            logger.info(f"Writing unused field counts to {args.unused_field_counts}")
            write_json(counters["unused_field_counts"], args.unused_field_counts)
        if args.sanitization_changes and sanitization_changes:
            logger.info(f"Writing sanitization changes to {args.sanitization_changes}")
            write_json(sanitization_changes, args.sanitization_changes)


if __name__ == "__main__":
    main()
