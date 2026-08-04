#!/usr/bin/env python3


import requests
import urllib

from os import getenv


def canopy_login(canopy_username: str, canopy_password: str) -> str:
    # log in to API
    login = requests.post(
        urllib.parse.urljoin(_api_url, "auth/login"),
        data={"username": canopy_username, "password": canopy_password},
    )

    # Stop if login failed
    login.raise_for_status()

    canopy_token = login.json().get("access_token")

    auth_header = {"Authorization": f"Bearer {canopy_token}"}

    return auth_header


def check_env_var(env_var_name: str) -> str:
    env_var_value = getenv(env_var_name)
    if env_var_value is None:
        raise EnvironmentError(f"Set the {env_var_name} environment variable")
    return env_var_value


# FIXME. Hard coded defaults for now.
_api_url = "https://api.atol.test.biocommons.org.au/api/v1/"

# login once per run, dodgy?
_auth_header = canopy_login(
    check_env_var("CANOPY_USERNAME"), check_env_var("CANOPY_PASSWORD")
)


envvars:
    "CANOPY_USERNAME",
    "CANOPY_PASSWORD",


rule experiments_bulk_import:
    input:
        request_body=Path(result_path, "transform_data", "experiments_output.json.gz"),
        samples_bulk_import_derived=Path(
            result_path, "canopy_import", "samples_bulk_import_derived.response"
        ),
    output:
        response=Path(result_path, "canopy_import", "experiments_bulk_import.response"),
    log:
        Path(result_path, "logs", "experiments_bulk_import.log"),
    container:
        container_uri if use_container else None
    params:
        endpoint="experiments_bulk_import",
        auth_header=_auth_header,
    script:
        "../scripts/canopy_client.py"


rule samples_bulk_import_derived:
    input:
        request_body=Path(result_path, "transformed.json.gz"),
        samples_bulk_import_specimens=Path(
            result_path, "canopy_import", "samples_bulk_import_specimens.response"
        ),
    output:
        response=Path(
            result_path, "canopy_import", "samples_bulk_import_derived.response"
        ),
    log:
        Path(result_path, "logs", "samples_bulk_import_derived.log"),
    container:
        container_uri if use_container else None
    params:
        endpoint="samples_bulk_import_derived",
        auth_header=_auth_header,
    script:
        "../scripts/canopy_client.py"


rule samples_bulk_import_specimens:
    input:
        request_body=Path(result_path, "transform_data", "specimens_output.json.gz"),
        taxonomy_info_bulk_upsert=Path(
            result_path, "canopy_import", "taxonomy_info_bulk_upsert.response"
        ),
    output:
        response=Path(
            result_path, "canopy_import", "samples_bulk_import_specimens.response"
        ),
    log:
        Path(result_path, "logs", "samples_bulk_import_specimens.log"),
    container:
        container_uri if use_container else None
    params:
        endpoint="samples_bulk_import_specimens",
        auth_header=_auth_header,
    script:
        "../scripts/canopy_client.py"


# This takes up to 10 minues, see
# https://github.com/AustralianBioCommons/atol-canopy/issues/42
rule taxonomy_info_bulk_upsert:
    input:
        request_body=Path(result_path, "organism_info", "organism_reference_data.json"),
        organisms_bulk_import=Path(
            result_path, "canopy_import", "organisms_bulk_import.response"
        ),
    output:
        response=Path(
            result_path, "canopy_import", "taxonomy_info_bulk_upsert.response"
        ),
    log:
        Path(result_path, "logs", "taxonomy_info_bulk_upsert.log"),
    container:
        container_uri if use_container else None
    params:
        endpoint="taxonomy_info_bulk_upsert",
        auth_header=_auth_header,
    script:
        "../scripts/canopy_client.py"


rule organisms_bulk_import:
    input:
        request_body=Path(result_path, "transform_data", "unique_organisms.json.gz"),
    output:
        response=Path(result_path, "canopy_import", "organisms_bulk_import.response"),
    log:
        Path(result_path, "logs", "organisms_bulk_import.log"),
    container:
        container_uri if use_container else None
    params:
        endpoint="organisms_bulk_import",
        auth_header=_auth_header,
    script:
        "../scripts/canopy_client.py"
