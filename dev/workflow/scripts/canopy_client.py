#!/usr/bin/env python3

from collections.abc import Generator
import gzip
import json
from time import sleep
import urllib

import requests
from requests.adapters import HTTPAdapter, Retry
from snakemake.logging import logger


def get_request_body(json_file: str) -> Generator[dict[str, str]]:
    if json_file.endswith(".jsonl.gz"):
        raise NotImplementedError(f"TODO: read {json_file} in get_request_body")
    elif json_file.endswith(".gz"):
        f = gzip.open(json_file, "rt")
    elif json_file.endswith(".json"):
        f = open(json_file, "rt")
    else:
        raise NotImplementedError(f"TODO: read {json_file} in get_request_body")

    return json.loads(f.read())


# FIXME. Hard coded defaults for now.
_api_url = "https://api.atol.test.biocommons.org.au/api/v1/"

_endpoints = {
    "experiments_bulk_import": ("/api/v1/experiments/bulk-import", "POST"),
    "organisms_bulk_import": ("/api/v1/organisms/bulk-import", "POST"),
    "samples_bulk_import_derived": ("/api/v1/samples/bulk-import-derived", "POST"),
    "samples_bulk_import_specimens": ("/api/v1/samples/bulk-import-specimens", "POST"),
    "taxonomy_info_bulk_upsert": ("/api/v1/taxonomy-info/bulk-upsert", "POST"),
}


def main():

    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=30,
        status_forcelist=[504],
        allowed_methods=["POST", "GET"],
        raise_on_redirect=False,
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))

    request_header = snakemake.params["auth_header"]

    request_body = None
    if snakemake.input["request_body"]:
        request_body = get_request_body(snakemake.input["request_body"])

    endpoint_url, request_type = _endpoints.get(
        snakemake.params["endpoint"], (None, None)
    )

    request_url = urllib.parse.urljoin(_api_url, endpoint_url)

    if request_type == "GET":
        raise NotImplementedError("TODO: implement GET")
    if request_type == "POST":
        response = s.post(
            request_url, headers=request_header, data=json.dumps(request_body)
        )

    if response.status_code not in [200, 504]:
        response.raise_for_status()

    logger.warning(f"Status {response.status_code}")

    with open(snakemake.output["response"], "wb") as f:
        f.write(response.content)


if __name__ == "__main__":
    main()
