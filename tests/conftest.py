"""Test configuration and fixtures for the atol-bpa-datamapper package."""

import json
from pathlib import Path
import pytest

# Get the path to the test fixtures directory
FIXTURES_DIR = Path(__file__).parent / "fixtures"

@pytest.fixture
def field_mapping_file():
    """Path to the test field mapping file."""
    return str(FIXTURES_DIR / "test_field_mapping.json")

@pytest.fixture
def value_mapping_file():
    """Path to the test value mapping file."""
    return str(FIXTURES_DIR / "test_value_mapping.json")

@pytest.fixture
def empty_mapping_file():
    """Path to an empty mapping file."""
    return str(FIXTURES_DIR / "empty_mapping.json")

@pytest.fixture
def sample_bpa_package():
    """Sample BPA package data."""
    return {
        "id": "test_package_001",
        "resources": [
            {
                "id": "resource_001",
                "name": "test_file_1.fastq",
                "type": "illumina",
                "library_name": "lib_001",
                "library_source": "genomic DNA",
                "insert_size": "500"
            },
            {
                "id": "resource_002",
                "name": "test_file_2.fastq",
                "type": "illumina genomic",
                "library_name": "lib_001",
                "library_source": "genomic dna",
                "insert_size": "500"
            }
        ],
        "scientific_name": "Undetermined sp.",
        "species": "sp",
        "genus": "Undetermined",
        "family": "Unknown",
        "order": "Unknown",
        "project_aim": "genome_assembly",
        "voucher_id": "V12345"
    }

@pytest.fixture
def expected_mapped_metadata():
    """Expected output after mapping the sample BPA package."""
    return {
        "organism": {
            "scientific_name": "Undetermined sp",
            "species": "sp",
            "genus": "Undetermined",
            "family": "Unknown",
            "order_or_group": "Unknown"
        },
        "reads": [
            {
                "platform": "illumina_genomic",
                "library_name": "lib_001",
                "library_source": "DNA",
                "insert_size": "500"
            },
            {
                "platform": "illumina_genomic",
                "library_name": "lib_001",
                "library_source": "DNA",
                "insert_size": "500"
            }
        ],
        "sample": {
            "data_context": "genome_assembly",
            "voucher_id": "V12345"
        }
    }
