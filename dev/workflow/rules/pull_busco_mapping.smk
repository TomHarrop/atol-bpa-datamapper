#!/usr/bin/env python3

placement_file_url = (
    "https://busco-data.ezlab.org/v5/data/placement_files/"
    "mapping_taxids-busco_dataset_name.eukaryota_odb10.2019-12-16.txt.tar.gz"
)


rule download_busco_placement_file:
    output:
        placement_file=(
            "resources/"
            "mapping_taxids-busco_dataset_name.eukaryota_odb10.2019-12-16.txt.tar.gz"
        ),
        timestamp="resources/mapping_taxids-busco_dataset_name.TIMESTAMP",
    params:
        url=placement_file_url,
    log:
        "resources/download_busco_placement_file.log",
    shadow:
        "minimal"
    container:
        "docker://quay.io/biocontainers/gnu-wget:1.18--hb829ee6_10"
    shell:
        "wget {params.url} -O {output.placement_file} &> {log}"
        "&& "
        "printf $(date -Iseconds) > {output.timestamp}"
