"""Test fixtures for atol-bpa-datamapper."""

import json
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
        "reads": {
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
        "reads": {
            "platform": {
                "illumina_genomic": ["illumina"],
                "pacbio_hifi": ["pacbio"]
            }
        }
    }


@pytest.fixture
def field_mapping_file(field_mapping_data):
    """Create a temporary field mapping file."""
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
        json.dump(field_mapping_data, f)
        temp_file = f.name
    
    yield temp_file
    
    # Clean up
    Path(temp_file).unlink()


@pytest.fixture
def value_mapping_file(value_mapping_data):
    """Create a temporary value mapping file."""
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
        json.dump(value_mapping_data, f)
        temp_file = f.name
    
    yield temp_file
    
    # Clean up
    Path(temp_file).unlink()
