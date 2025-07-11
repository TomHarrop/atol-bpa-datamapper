from .arg_parser import parse_args_for_mapping
from .config_parser import MetadataMap
from .io import read_input, OutputWriter, write_mapping_log_to_csv, write_json
from .logger import logger, setup_logger
from .organism_mapper import OrganismSection, NcbiTaxdump
from collections import Counter


def main():

    # debugging options
    max_iterations = None
    manual_record = None

    args = parse_args_for_mapping()
    setup_logger(args.log_level)

    # read the schemas
    package_level_map = MetadataMap(
        args.package_field_mapping_file, args.value_mapping_file
    )
    resource_level_map = MetadataMap(
        args.resource_field_mapping_file, args.value_mapping_file
    )

    null_values = package_level_map.sanitization_config.get("null_values")

    # set up taxonomy data
    ncbi_taxdump = NcbiTaxdump(
        args.nodes,
        args.names,
        args.cache_dir,
        resolve_to_rank="species",
    )

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

    # set up logs
    mapping_log = {}
    grouping_log = {}
    grouped_packages = {}
    sanitization_changes = {}

    input_data = read_input(args.input)
    n_packages = 0

    with OutputWriter(args.output, args.dry_run) as output_writer:
        for package in input_data:
            logger.debug(f"Processing package {package.id}")

            # debugging
            if manual_record and package.id != manual_record:
                continue
            if max_iterations and n_packages >= max_iterations:
                break

            n_packages += 1

            counters["raw_field_usage"].update(package.fields)
            for bpa_field in package.fields:
                if bpa_field not in counters["raw_value_usage"]:
                    counters["raw_value_usage"][bpa_field] = Counter()
                try:
                    counters["raw_value_usage"][bpa_field].update([package[bpa_field]])
                except TypeError:
                    pass

            package.map_metadata(package_level_map)
            mapping_log[package.id] = package.mapping_log

            # map the organism
            organism_section = OrganismSection(
                package.id,
                package.mapped_metadata["organism"],
                ncbi_taxdump,
                null_values,
            )
            grouping_log[package.id] = [organism_section.mapped_metadata]
            grouping_key = organism_section.organism_grouping_key
            if grouping_key is not None:
                grouped_packages.setdefault(grouping_key, []).append(package.id)

            logger.debug(f"Mapped organism info: {organism_section.mapped_metadata}")

            # overwrite values in the organism section
            for key, value in organism_section.mapped_metadata.items():
                if key in package_level_map.expected_fields:
                    logger.debug(
                        f"organism_section mapped_metadata has key {key} with value {value}"
                    )

                    try:
                        current_value = package.mapped_metadata["organism"][key]
                    except KeyError:
                        current_value = None

                    if not value == current_value:
                        logger.debug(
                            f"Updating organism key {key} from {current_value} to {value}"
                        )
                        package.mapped_metadata["organism"][key] = value

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

            # write the output
            output_writer.write_data(package.mapped_metadata)

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
                    counters["mapped_value_usage"][atol_field].update([mapped_value])

                    bpa_field = None
                    if atol_field in package.field_mapping:
                        bpa_field = package.field_mapping[atol_field]
                    else:
                        bpa_fields = {
                            x.field_mapping[atol_field]
                            for x in package.resources.values()
                            if atol_field in x.field_mapping
                        }
                        if bpa_fields:
                            bpa_field = sorted(bpa_fields)[0]

                    if bpa_field is not None:
                        counters["mapped_field_usage"][atol_field].update([bpa_field])

    logger.info(f"Processed {n_packages} packages")

    # write optional output
    if not args.dry_run:
        if args.mapping_log:
            logger.info(f"Writing mapping log to {args.mapping_log}")
            write_mapping_log_to_csv(mapping_log, args.mapping_log)
        if args.grouping_log:
            logger.info(f"Writing grouping log to {args.grouping_log}")
            write_mapping_log_to_csv(grouping_log, args.grouping_log)
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
        if args.grouped_packages:
            logger.info(f"Writing grouped_packages to {args.grouped_packages}")
            write_json(grouped_packages, args.grouped_packages)
        if args.unused_field_counts:
            logger.info(f"Writing unused field counts to {args.unused_field_counts}")
            write_json(counters["unused_field_counts"], args.unused_field_counts)
        if args.sanitization_changes and sanitization_changes:
            logger.info(f"Writing sanitization changes to {args.sanitization_changes}")
            write_json(sanitization_changes, args.sanitization_changes)


if __name__ == "__main__":
    main()
