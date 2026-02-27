
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


rule map_metadata:
    input:
        nodes=ancient("resources/new_taxdump/nodes.dmp"),
        names=ancient("resources/new_taxdump/names.dmp"),
        taxids_to_busco_dataset_mapping=ancient(
            "resources/"
            "mapping_taxids-busco_dataset_name.eukaryota_odb10.2019-12-16.txt.tar.gz"
        ),
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
