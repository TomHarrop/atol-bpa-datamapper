#!/usr/bin/env python3


rule flatten_mapping_output:
    input:
        mapped=Path(result_path, "mapped.jsonl.gz"),
    output:
        mapped_packages=Path(result_path, "map_metadata", "mapped_packages.csv.gz"),
        mapped_resources=Path(result_path, "map_metadata", "mapped_resources.csv.gz"),
    log:
        Path(result_path, "logs", "flatten_mapping_output.log"),
    container:
        "docker://ghcr.io/tomharrop/r-containers:r2u_24.04_cv1"
    script:
        "../scripts/flatten_mapping_output.R"


rule analyse_decision_log:
    input:
        decision_log=Path(result_path, "filter_packages", "decision_log.csv.gz"),
    output:
        failed_counts=Path(result_path, "filter_packages", "failed_counts.csv"),
        single_fails=Path(result_path, "filter_packages", "single_fail_values.csv"),
    log:
        Path(result_path, "logs", "analyse_decision_log.log"),
    container:
        "docker://ghcr.io/tomharrop/r-containers:r2u_24.04_cv1"
    script:
        "../scripts/analyse_decision_log.R"


rule analysis_target:
    input:
        rules.flatten_mapping_output.output,
        rules.analyse_decision_log.output,
