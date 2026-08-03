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


envvars:
    "CANOPY_USERNAME",
    "CANOPY_PASSWORD",


rule organisms_bulk_import:
    input:
        request_body=Path(result_path, "transform_data", "unique_organisms.json.gz"),
    output:
        response=Path(result_path, "canopy_import", "organisms_bulk_import.json"),
    container:
        container_uri if use_container else None
    params:
        endpoint="organisms_bulk_import",
        auth_header=canopy_login(
            check_env_var("CANOPY_USERNAME"), check_env_var("CANOPY_PASSWORD")
        ),
    script:
        "../scripts/canopy_client.py"
