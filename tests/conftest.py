"""Test fixtures for atol-bpa-datamapper."""

import json
import os
import pytest
import os
import json
from pathlib import Path
from unittest.mock import MagicMock, patch
from atol_bpa_datamapper.config_parser import MetadataMap
from unittest.mock import MagicMock, patch


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
                "bpa_library_id": "lib_001",
                "platform": "illumina"
            },
            {
                "id": "resource2",
                "type": "pacbio",
                "bpa_library_id": "lib_002",
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
            "bpa_library_id": ["resources.bpa_library_id"],
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
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "tests", "fixtures")


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


@pytest.fixture
def metadata_map_factory():
    """Factory fixture that creates a mock metadata map with configurable behavior."""
    def _factory(
        metadata_sections=None,
        expected_fields=None,
        get_atol_section_func=None,
        get_bpa_fields_func=None,
        get_allowed_values_func=None,
        map_value_func=None,
        sanitize_value_func=None,
    ):
        metadata_map = MagicMock()
        
        # Set default values
        metadata_map.metadata_sections = metadata_sections or ["dataset"]
        metadata_map.expected_fields = expected_fields or []
        metadata_map.sanitization_config = {"null_values": [""]}
        
        # Define default functions
        def default_get_atol_section(field):
            return "dataset"
        
        def default_get_bpa_fields(section):
            return ["field1", "field2"]
        
        def default_get_allowed_values(field):
            return ["value1", "value2"]
        
        def default_map_value(field, value):
            return value
        
        def default_sanitize_value(section, field, value):
            return (value, [])
        
        def default_check_default_value(field):
            return (False, None)
        
        # Set mock methods
        metadata_map.get_atol_section = get_atol_section_func or default_get_atol_section
        metadata_map.get_bpa_fields = get_bpa_fields_func or default_get_bpa_fields
        metadata_map.get_allowed_values = get_allowed_values_func or default_get_allowed_values
        metadata_map.map_value = map_value_func or default_map_value
        metadata_map._sanitize_value = sanitize_value_func or default_sanitize_value
        metadata_map.check_default_value = default_check_default_value
        
        # Set up __getitem__
        def getitem(self, key):
            return {"bpa_fields": metadata_map.get_bpa_fields(key)}
        
        metadata_map.__getitem__ = getitem
        
        return metadata_map
    
    return _factory


@pytest.fixture
def ncbi_taxdump_factory():
    """Factory fixture that creates a mock NcbiTaxdump with configurable behavior."""
    def _factory(nodes_df=None, names_df=None):
        mock_taxdump = MagicMock()
        
        # Set default values if not provided
        if nodes_df is None:
            import pandas as pd
            nodes_df = pd.DataFrame({
                'tax_id': [1, 2, 3],
                'parent_tax_id': [0, 1, 1],
                'rank': ['no rank', 'superkingdom', 'superkingdom']
            })
        
        if names_df is None:
            import pandas as pd
            names_df = pd.DataFrame({
                'tax_id': [1, 2, 3],
                'name_txt': ['root', 'Bacteria', 'Archaea'],
                'name_class': ['scientific name', 'scientific name', 'scientific name']
            })
        
        # Set properties
        mock_taxdump.nodes_df = nodes_df
        mock_taxdump.names_df = names_df
        
        # Mock methods
        mock_taxdump.get_lineage.return_value = ["root", "Bacteria", "Species"]
        mock_taxdump.get_rank.return_value = "species"
        
        return mock_taxdump
    
    return _factory


@pytest.fixture
def mock_args_factory():
    """Factory fixture for creating mock command line arguments."""
    def _factory(
        input_file="test_input.json",
        output_file=None,
        field_mapping=None,
        value_mapping=None,
        log_level="INFO",
        dry_run=False,
        stats_file=None,
        mapping_log=None,
        sanitization_changes=None,
        **kwargs
    ):
        args = MagicMock()
        args.input_file = input_file
        args.output_file = output_file
        args.field_mapping = field_mapping
        args.value_mapping = value_mapping
        args.log_level = log_level
        args.dry_run = dry_run
        args.stats_file = stats_file
        args.mapping_log = mapping_log
        args.sanitization_changes = sanitization_changes
        
        # Add any additional kwargs as attributes
        for key, value in kwargs.items():
            setattr(args, key, value)
            
        return args
    
    return _factory
