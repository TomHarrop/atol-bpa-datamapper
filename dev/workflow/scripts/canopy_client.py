#!/usr/bin/env python3


import requests
import urllib


def canopy_login(canopy_username: str, canopy_password: str) -> str:
    # log in to API
    login = requests.post(
        urllib.parse.urljoin(_api_url, _endpoints.get("auth_login")),
        data={"username": canopy_username, "password": canopy_password},
    )

    # Stop if login failed
    login.raise_for_status()

    canopy_token = login.json().get("access_token")

    auth_header = {"Authorization": f"Bearer {canopy_token}"}

    return auth_header


# FIXME. Hard coded defaults for now.
_api_url = "https://api.atol.test.biocommons.org.au/api/v1/"

_endpoints = {
    "auth_login": "auth/login",
}


def main():

    deets = canopy_login(
        snakemake.params["canopy_username"], snakemake.params["canopy_password"]
    )
    raise ValueError(deets)


if __name__ == "__main__":
    main()
