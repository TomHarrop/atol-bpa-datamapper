#!/usr/bin/env python3

import os


def format_call(step_name, use_container):

    if use_container is False:

        call = (
            "python3 -m cProfile -o "
            f"{result_path}/{step_name}/cProfile.stats "
            f"-m atol_bpa_datamapper.{step_name}"
        )
    else:
        call = step_name.replace("_", "-")

    return call


def get_datamapper_version():
    dm_container = os.getenv("DM_CONTAINER")
    if dm_container is None:
        no_container()

    # run locally if the container isn't set
    if dm_container == "0":
        import importlib.metadata

        return importlib.metadata.version("atol_bpa_datamapper"), False, dm_container

    try:
        if int(dm_container) != 0:
            no_container()
    except ValueError:
        return Path(dm_container).name, True, dm_container

    no_container()


def no_container():
    raise KeyError("Set DM_CONTAINER to the container URL, or 0 to run locally")


datamapper_version, use_container, container_uri = get_datamapper_version()


rule mapper_version:
    input:
        filtered=Path(result_path, "filtered.jsonl.gz"),
        mapped=Path(result_path, "mapped.jsonl.gz"),
        transformed=Path(result_path, "transformed.jsonl.gz"),
        datasets_timestamp=ancient("resources/datasets.jsonl.gz.TIMESTAMP"),
        busco_timestamp=ancient("resources/mapping_taxids-busco_dataset_name.TIMESTAMP"),
        taxdump_timestamp=ancient("resources/new_taxdump/TIMESTAMP"),
    output:
        version=Path(result_path, "mapper_version.txt"),
        datasets_timestamp=Path(result_path, "datasets.jsonl.gz.TIMESTAMP"),
        busco_timestamp=Path(result_path, "mapping_taxids-busco_dataset_name.TIMESTAMP"),
        taxdump_timestamp=Path(result_path, "new_taxdump.TIMESTAMP"),
    params:
        version=datamapper_version,
    shell:
        'echo "{params.version}" > {output.version} && '
        "cp {input.datasets_timestamp} {output.datasets_timestamp} && "
        "cp {input.busco_timestamp} {output.busco_timestamp} && "
        "cp {input.taxdump_timestamp} {output.taxdump_timestamp}"


rule transform_data:
    input:
        mapped=Path(result_path, "mapped.jsonl.gz"),
    output:
        transformed=Path(result_path, "transformed.jsonl.gz"),
        sample_conflicts=Path(
            result_path, "transform_data", "sample_conflicts.jsonl.gz"
        ),
        sample_package_map=Path(
            result_path, "transform_data", "sample_package_map.jsonl.gz"
        ),
        transformation_changes=Path(
            result_path, "transform_data", "transformation_changes.jsonl.gz"
        ),
        unique_organisms=Path(
            result_path, "transform_data", "unique_organisms.jsonl.gz"
        ),
        organism_conflicts=Path(
            result_path, "transform_data", "organism_conflicts.jsonl.gz"
        ),
        organism_package_map=Path(
            result_path, "transform_data", "organism_package_map.jsonl.gz"
        ),
    params:
        call=format_call("transform_data", use_container),
    log:
        log=Path(result_path, "logs", "transform_data.log"),
    container:
        container_uri if use_container else None
    shell:
        "{params.call} "
        "--sample_conflicts {output.sample_conflicts} "
        "--sample_package_map {output.sample_package_map} "
        "--transformation_changes {output.transformation_changes} "
        "--unique_organisms {output.unique_organisms} "
        "--organism_conflicts {output.organism_conflicts} "
        "--organism_package_map {output.organism_package_map} "
        "<{input.mapped} "
        ">{output.transformed} "
        "2> {log.log}"


rule map_metadata:
    input:
        filtered=Path(result_path, "filtered.jsonl.gz"),
        nodes=ancient("resources/new_taxdump/nodes.dmp"),
        names=ancient("resources/new_taxdump/names.dmp"),
        taxids_to_busco_dataset_mapping=ancient(
            "resources/"
            "mapping_taxids-busco_dataset_name.eukaryota_odb10.2019-12-16.txt.tar.gz"
        ),
    output:
        mapped=Path(result_path, "mapped.jsonl.gz"),
        raw_field_usage=Path(result_path, "map_metadata", "raw_field_usage.jsonl.gz"),
        raw_value_usage=Path(result_path, "map_metadata", "raw_value_usage.jsonl.gz"),
        mapped_field_usage=Path(
            result_path, "map_metadata", "mapped_field_usage.jsonl.gz"
        ),
        mapped_value_usage=Path(
            result_path, "map_metadata", "mapped_value_usage.jsonl.gz"
        ),
        unused_field_counts=Path(
            result_path, "map_metadata", "unused_field_counts.jsonl.gz"
        ),
        grouped_packages=Path(result_path, "map_metadata", "grouped_packages.jsonl.gz"),
        sanitization_changes=Path(
            result_path, "map_metadata", "sanitization_changes.jsonl.gz"
        ),
    params:
        call=format_call("map_metadata", use_container),
    log:
        log=Path(result_path, "logs", "map_metadata.log"),
        mapping_log=Path(result_path, "map_metadata", "mapping_log.csv.gz"),
        grouping_log=Path(result_path, "map_metadata", "grouping_log.csv.gz"),
    container:
        container_uri if use_container else None
    shell:
        "{params.call} "
        "--grouped_packages {output.grouped_packages} "
        "--grouping_log {log.grouping_log} "
        "--mapped_field_usage {output.mapped_field_usage} "
        "--mapped_value_usage {output.mapped_value_usage} "
        "--mapping_log {log.mapping_log} "
        "--names {input.names} "
        "--nodes {input.nodes} "
        "--raw_field_usage {output.raw_field_usage} "
        "--raw_value_usage {output.raw_value_usage} "
        "--sanitization_changes {output.sanitization_changes} "
        "--taxids_to_busco_dataset_mapping {input.taxids_to_busco_dataset_mapping} "
        "--unused_field_counts {output.unused_field_counts} "
        "<{input.filtered} "
        ">{output.mapped} "
        "2> {log.log}"


rule filter_packages:
    input:
        bpa_data=ancient("resources/datasets.jsonl.gz"),
    output:
        filtered=Path(result_path, "filtered.jsonl.gz"),
        raw_field_usage=Path(result_path, "filter_packages", "raw_field_usage.jsonl.gz"),
        bpa_field_usage=Path(result_path, "filter_packages", "bpa_field_usage.jsonl.gz"),
        bpa_value_usage=Path(result_path, "filter_packages", "bpa_value_usage.jsonl.gz"),
    params:
        call=format_call("filter_packages", use_container),
    log:
        log=Path(result_path, "logs", "filter_packages.log"),
        decision_log=Path(result_path, "filter_packages", "decision_log.jsonl.gz"),
    container:
        container_uri if use_container else None
    shell:
        "{params.call} "
        "--bpa_field_usage {output.bpa_field_usage} "
        "--bpa_value_usage {output.bpa_value_usage} "
        "--decision_log {log.decision_log} "
        "--raw_field_usage {output.raw_field_usage} "
        "< {input.bpa_data} "
        "> {output.filtered} "
        "2> {log.log}"
