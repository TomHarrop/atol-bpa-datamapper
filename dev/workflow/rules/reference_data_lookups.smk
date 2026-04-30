

rule taxonomy_version:
    input:
        reference_data=Path(
            result_path, "organism_info", "organism_reference_data.json"
        ),
        timestamp=Path("resources", "new_taxdump", "TIMESTAMP"),
    output:
        timestamp=Path(result_path, "organism_info", "ncbi_taxdump_timestamp.txt"),
    shell:
        "cp {input.timestamp} {output.timestamp} "


rule reference_data_lookups:
    input:
        taxid_list=Path(result_path, "organism_info", "all_taxon_ids.txt"),
        busco_odb12_dataset_mapping=Path(
            "resources",
            "mapping_taxids-busco_dataset_name.eukaryota_odb12.2025-01-15.txt.tar.gz",
        ),
        busco_odb10_dataset_mapping=Path(
            "resources",
            "mapping_taxids-busco_dataset_name.eukaryota_odb10.2019-12-16.txt.tar.gz",
        ),
        nodes=Path("resources", "new_taxdump", "nodes.dmp"),
        names=Path("resources", "new_taxdump", "names.dmp"),
        oatk_taxid_file=Path("resources", "oatk.TAXID.tsv"),
    output:
        reference_data=Path(
            result_path, "organism_info", "organism_reference_data.json"
        ),
    log:
        Path(result_path, "logs", "reference_data_lookups.log"),
    container:
        "docker://quay.io/biocontainers/atol-reference-data-lookups:0.4.0--pyhdfd78af_0"
    params:
        cache_dir=Path("resources", "cache"),
    shell:
        "atol-reference-data-lookups "
        "--taxid-list {input.taxid_list} "
        "--nodes {input.nodes} "
        "--names {input.names} "
        "--taxids_to_busco_odb12_dataset_mapping {input.busco_odb12_dataset_mapping} "
        "--taxids_to_busco_odb10_dataset_mapping {input.busco_odb10_dataset_mapping} "
        "--oatk_taxid_file {input.oatk_taxid_file} "
        "--cache_dir {params.cache_dir} "
        "> {output.reference_data} "
        "2> {log}"


rule get_all_taxids:
    input:
        bpa_data=ancient("resources/datasets.jsonl.gz"),
    output:
        taxid_list=Path(result_path, "organism_info", "all_taxon_ids.txt"),
    log:
        Path(result_path, "logs", "get_all_taxids.log"),
    container:
        container_uri if use_container else None
    shell:
        "python3 dev/workflow/scripts/get_all_taxids.py "
        "{input.bpa_data} "
        "{output.taxid_list} "
        "&> {log}"


rule get_reference_data:
    output:
        nodes=Path("resources", "new_taxdump", "nodes.dmp"),
        names=Path("resources", "new_taxdump", "names.dmp"),
        busco_odb12_dataset_mapping=Path(
            "resources",
            "mapping_taxids-busco_dataset_name.eukaryota_odb12.2025-01-15.txt.tar.gz",
        ),
        busco_odb10_dataset_mapping=Path(
            "resources",
            "mapping_taxids-busco_dataset_name.eukaryota_odb10.2019-12-16.txt.tar.gz",
        ),
        oatk_taxid_file=Path("resources", "oatk.TAXID.tsv"),
        timestamp=Path("resources", "new_taxdump", "TIMESTAMP"),
    log:
        Path(result_path, "logs", "get_reference_data.log"),
    retries: 2
    container:
        "docker://quay.io/biocontainers/atol-reference-data-lookups:0.4.0--pyhdfd78af_0"
    shell:
        "get-remote-files &> {log}"
