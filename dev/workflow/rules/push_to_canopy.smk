#!/usr/bin/env python3

from os import getenv


def check_env_var(env_var_name: str) -> str:
    env_var_value = getenv(env_var_name)
    if env_var_value is None:
        raise EnvironmentError(f"Set the {env_var_name} environment variable")
    return env_var_value


envvars:
    "CANOPY_USERNAME",
    "CANOPY_PASSWORD",


rule organisms_bulk_import:
    input:
        unique_organisms=Path(
            result_path, "transform_data", "unique_organisms.jsonl.gz"
        ),
    container:
        container_uri if use_container else None
    params:
        request_type="POST",
        endpoint="/api/v1/organisms/bulk-import",
        body="",
        canopy_username=check_env_var("CANOPY_USERNAME"),
        canopy_password=check_env_var("CANOPY_PASSWORD"),
    script:
        "../scripts/canopy_client.py"
