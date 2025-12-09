import os


envvars:
    "BPA_APIKEY",


rule ckanapi_search_datasets:
    output:
        bpa_data="resources/datasets.jsonl.gz",
        timestamp="resources/datasets.jsonl.gz.TIMESTAMP",
    params:
        bpa_apikey=os.environ["BPA_APIKEY"],
    shell:
        "ckanapi search datasets "
        "-a {params.bpa_apikey} "
        "include_private=true "
        "-O {output.bpa_data} "
        "-z -r https://data.bioplatforms.com "
        "&& "
        "printf $(date -Iseconds) > {output.timestamp}"
