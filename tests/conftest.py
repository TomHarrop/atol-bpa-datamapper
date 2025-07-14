"""Test fixtures for atol-bpa-datamapper."""

import json
import os
import pytest
import tempfile
from pathlib import Path


@pytest.fixture
def sample_bpa_package():
    """Sample BPA package data with multiple resources."""
    return {
        "id": "test_package",
        "scientific_name": "Undetermined species",
        "project_aim": "genome_assembly",
        "resources": [
            {
                "id": "resource1",
                "type": "illumina",
                "library_name": "lib_001",
                "platform": "illumina"
            },
            {
                "id": "resource2",
                "type": "pacbio",
                "library_name": "lib_002",
                "platform": "pacbio"
            }
        ]
    }


@pytest.fixture
def field_mapping_data():
    """Sample field mapping data."""
    return {
        "organism": {
            "scientific_name": ["scientific_name"]
        },
        "sample": {
            "project_aim": ["project_aim"]
        },
        "runs": {
            "platform": ["resources.platform", "platform_type"],
            "library_name": ["resources.library_name"],
            "type": ["resources.type"]
        }
    }


@pytest.fixture
def value_mapping_data():
    """Sample value mapping data."""
    return {
        "organism": {
            "scientific_name": {
                "Undetermined sp": ["Undetermined species"]
            }
        },
        "sample": {
            "project_aim": {
                "genome_assembly": ["genome_assembly"]
            }
        },
        "runs": {
            "platform": {
                "illumina_genomic": ["illumina"],
                "pacbio_hifi": ["pacbio"]
            }
        }
    }


@pytest.fixture
def test_fixtures_dir():
    """Return the path to the test fixtures directory."""
    return os.path.join(os.path.dirname(__file__), "fixtures")


@pytest.fixture
def field_mapping_file(test_fixtures_dir):
    """Return the path to the test field mapping file."""
    return os.path.join(test_fixtures_dir, "test_field_mapping_packages.json")

@pytest.fixture
def field_mapping_file_resources(test_fixtures_dir):
    """Return the path to the test field mapping file."""
    return os.path.join(test_fixtures_dir, "test_field_mapping_resources.json")


@pytest.fixture
def value_mapping_file(test_fixtures_dir):
    """Return the path to the test value mapping file."""
    return os.path.join(test_fixtures_dir, "test_value_mapping.json")


@pytest.fixture
def sanitization_config_file(test_fixtures_dir):
    """Return the path to the test sanitization config file."""
    return os.path.join(test_fixtures_dir, "test_sanitization_config.json")


@pytest.fixture
def invalid_json_file(test_fixtures_dir):
    """Return the path to an invalid JSON file for testing error handling."""
    return os.path.join(test_fixtures_dir, "invalid_json.json")


@pytest.fixture
def invalid_structure_file(test_fixtures_dir):
    """Return the path to a file with invalid structure for testing validation."""
    return os.path.join(test_fixtures_dir, "invalid_structure.json")


@pytest.fixture
def package_metadata_map(field_mapping_file, value_mapping_file):
    """Create a package-level MetadataMap instance for testing."""
    from atol_bpa_datamapper.config_parser import MetadataMap
    return MetadataMap(field_mapping_file, value_mapping_file)

@pytest.fixture
def resource_metadata_map(field_mapping_file_resources, value_mapping_file):
    """Create a resource-level MetadataMap instance for testing."""
    from atol_bpa_datamapper.config_parser import MetadataMap
    return MetadataMap(field_mapping_file_resources, value_mapping_file)
