#!/usr/bin/env python3

import os

# This script will generate a result dir if RESULT_DIRNAME is not set
try:
    result_base = os.environ["RESULT_DIRNAME"]
except KeyError:
    from datetime import date
    from random import randint

    today = date.today().isoformat()
    rand = randint(1, int(1e7))
    result_base = f"{str(today)}_{str(rand)}"

result_path = Path("results", result_base)


include: "rules/analysis.smk"
include: "rules/datamapper.smk"
include: "rules/pull_bpa_db.smk"
include: "rules/pull_busco_mapping.smk"
include: "rules/pull_ncbi_taxonomy.smk"


rule target:
    default_target: True
    input:
        rules.mapper_version.output,
        rules.analysis_target.input,
