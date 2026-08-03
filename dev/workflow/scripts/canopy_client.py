#!/usr/bin/env python3

from collections.abc import Generator
import gzip
import json
from pathlib import Path
import urllib

from atol_bpa_datamapper.io import read_gzip_textfile
import requests


def get_request_body(json_file: str) -> Generator[dict[str, str]]:
    if json_file.endswith(".gz"):
        f = gzip.open(json_file, "rt")
    else:
        raise NotImplementedError(json_file)

    return json.loads(f.read())


# FIXME. Hard coded defaults for now.
_api_url = "https://api.atol.test.biocommons.org.au/api/v1/"

_endpoints = {"organisms_bulk_import": ("/api/v1/organisms/bulk-import", "POST")}


def main():

    request_header = snakemake.params["auth_header"]

    request_body = None
    if snakemake.input["request_body"]:
        request_body = get_request_body(snakemake.input["request_body"])

    endpoint_url, request_type = _endpoints.get(
        snakemake.params["endpoint"], (None, None)
    )

    request_url = urllib.parse.urljoin(_api_url, endpoint_url)

    if request_type == "GET":
        raise NotImplementedError("TODO implement GET")
    if request_type == "POST":
        response = requests.post(
            request_url, headers=request_header, data=json.dumps(request_body)
        )

    if snakemake.output["response"]:
        with open(snakemake.output["response"], "wt") as f:
            f.write(json.dumps(response.json()))


if __name__ == "__main__":
    main()
