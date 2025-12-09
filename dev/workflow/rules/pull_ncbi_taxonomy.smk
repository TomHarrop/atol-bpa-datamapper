#!/usr/bin/env python3

taxdump_url = "ftp://ftp.ncbi.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz"

taxdump_files = [
    "citations.dmp",
    "delnodes.dmp",
    "division.dmp",
    "excludedfromtype.dmp",
    "fullnamelineage.dmp",
    "gencode.dmp",
    "host.dmp",
    "images.dmp",
    "merged.dmp",
    "names.dmp",
    "nodes.dmp",
    "rankedlineage.dmp",
    "taxidlineage.dmp",
    "typematerial.dmp",
    "typeoftype.dmp",
]


rule expand_taxdump:
    input:
        taxdump="resources/new_taxdump.tar.gz",
    output:
        [Path("resources/new_taxdump", x).as_posix() for x in taxdump_files],
        timestamp="resources/new_taxdump/TIMESTAMP",
    params:
        outdir=subpath(output[0], parent=True),
    container:
        "docker://debian:stable-20250113"
    shell:
        "mkdir -p {params.outdir} && "
        "tar -xzf {input.taxdump} -C {params.outdir} && "
        "printf $(date -Iseconds) > {output.timestamp}"


rule download_taxdump_file:
    output:
        taxdump=temp("resources/new_taxdump.tar.gz"),
    params:
        url=taxdump_url,
        name=lambda wildcards: Path(taxdump_url).name,
    resources:
        runtime=60,
    log:
        "resources/download_taxdump_file.log",
    shadow:
        "minimal"
    container:
        "docker://quay.io/biocontainers/gnu-wget:1.18--hb829ee6_10"
    shell:
        "wget {params.url} -O {params.name} &> {log} && "
        "wget {params.url}.md5 -O {params.name}.md5 &>> {log} && "
        "md5sum -c {params.name}.md5 &>> {log} && "
        "mv {params.name} {output.taxdump}"
