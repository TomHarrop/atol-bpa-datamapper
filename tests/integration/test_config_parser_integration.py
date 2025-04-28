"""Integration tests for config_parser.py."""

import os
import json
import pytest
from atol_bpa_datamapper.config_parser import MetadataMap


@pytest.fixture
def test_fixtures_dir():
    """Return the path to the test fixtures directory."""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures")


@pytest.fixture
def field_mapping_file(test_fixtures_dir):
    """Return the path to the test field mapping file."""
    return os.path.join(test_fixtures_dir, "test_field_mapping.json")


@pytest.fixture
def value_mapping_file(test_fixtures_dir):
    """Return the path to the test value mapping file."""
    return os.path.join(test_fixtures_dir, "test_value_mapping.json")


@pytest.fixture
def sanitization_config_file(test_fixtures_dir):
    """Return the path to the test sanitization config file."""
    return os.path.join(test_fixtures_dir, "test_sanitization_config.json")


@pytest.fixture
def metadata_map(field_mapping_file, value_mapping_file):
    """Create a MetadataMap instance for testing."""
    return MetadataMap(field_mapping_file, value_mapping_file)


def test_metadata_map_initialization(metadata_map, field_mapping_file, value_mapping_file):
    """Test that the MetadataMap is initialized correctly."""
    # Check that the expected fields are loaded
    assert "scientific_name" in metadata_map.expected_fields
    assert "data_context" in metadata_map.expected_fields
    assert "platform" in metadata_map.expected_fields
    
    # Check that the metadata sections are correct
    assert "organism" in metadata_map.metadata_sections
    assert "sample" in metadata_map.metadata_sections
    assert "runs" in metadata_map.metadata_sections
    assert "dataset" in metadata_map.metadata_sections
    
    # Check that the controlled vocabularies are loaded
    assert "scientific_name" in metadata_map.controlled_vocabularies
    assert "data_context" in metadata_map.controlled_vocabularies
    assert "platform" in metadata_map.controlled_vocabularies


def test_get_allowed_values(metadata_map):
    """Test the get_allowed_values method."""
    # Test a field with a controlled vocabulary
    allowed_values = metadata_map.get_allowed_values("scientific_name")
    assert allowed_values is not None
    assert "Undetermined sp." in allowed_values
    assert "Undetermined species" in allowed_values
    
    # Test a field without a controlled vocabulary
    allowed_values = metadata_map.get_allowed_values("bpa_id")
    assert allowed_values is None


def test_get_bpa_fields(metadata_map):
    """Test the get_bpa_fields method."""
    # Test fields from different sections
    assert metadata_map.get_bpa_fields("scientific_name") == ["scientific_name", "species_name", "taxon_or_organism"]
    assert metadata_map.get_bpa_fields("data_context") == ["project_aim", "data_context"]
    assert metadata_map.get_bpa_fields("platform") == ["resources.type", "sequence_data_type", "data_type", "type", "platform"]


def test_get_atol_section(metadata_map):
    """Test the get_atol_section method."""
    assert metadata_map.get_atol_section("scientific_name") == "organism"
    assert metadata_map.get_atol_section("data_context") == "sample"
    assert metadata_map.get_atol_section("platform") == "runs"
    assert metadata_map.get_atol_section("bpa_id") == "dataset"


def test_keep_value(metadata_map):
    """Test the keep_value method."""
    # Test values that should be kept
    assert metadata_map.keep_value("scientific_name", "Undetermined sp.") is True
    assert metadata_map.keep_value("data_context", "genome_assembly") is True
    
    # Test values that should not be kept
    assert metadata_map.keep_value("scientific_name", "Unknown Species") is False
    assert metadata_map.keep_value("data_context", "transcriptome") is False
    
    # Test fields without a controlled vocabulary
    assert metadata_map.keep_value("bpa_id", "any-value") is True


def test_map_value(metadata_map):
    """Test the map_value method."""
    # Test mapping values with controlled vocabulary
    assert metadata_map.map_value("scientific_name", "Undetermined sp.") == "Undetermined sp"
    assert metadata_map.map_value("data_context", "genome_assembly") == "genome_assembly"
    assert metadata_map.map_value("platform", "illumina") == "illumina_genomic"
    
    # Test mapping values without controlled vocabulary
    with pytest.raises(KeyError):
        metadata_map.map_value("scientific_name", "Unknown Species")


def test_sanitize_value(metadata_map, test_fixtures_dir, monkeypatch):
    """Test the _sanitize_value method."""
    # Mock the sanitization config
    sanitization_config = {
        "organism": {
            "scientific_name": ["text_sanitization", "empty_string_sanitization"]
        },
        "runs": {
            "platform": ["text_sanitization"],
            "file_format": ["text_sanitization"]
        },
        "sanitization_rules": {
            "text_sanitization": {
                "description": "Strip double whitespace, unicode whitespace characters"
            },
            "empty_string_sanitization": {
                "description": "Convert empty strings to null"
            },
            "integer_sanitization": {
                "description": "Ensure integer values, remove decimals"
            }
        }
    }
    
    # Patch the sanitization config
    monkeypatch.setattr(metadata_map, "sanitization_config", sanitization_config)
    
    # Test sanitization with text_sanitization rule
    value_to_sanitize = "  Homo   sapiens  "
    sanitized_value, applied_rules = metadata_map._sanitize_value("organism", "scientific_name", value_to_sanitize)
    assert sanitized_value == "Homo sapiens"
    assert "text_sanitization" in applied_rules
    
    # Test sanitization with empty_string_sanitization rule
    sanitized_value, applied_rules = metadata_map._sanitize_value("organism", "scientific_name", "")
    assert sanitized_value is None
    assert "empty_string_sanitization" in applied_rules
    
    # Test sanitization with file format
    sanitized_value, applied_rules = metadata_map._sanitize_value("runs", "file_format", "  FASTQ  ")
    assert sanitized_value == "FASTQ"
    assert "text_sanitization" in applied_rules
    
    # Test sanitization without rules
    sanitized_value, applied_rules = metadata_map._sanitize_value("sample", "data_context", "genome assembly")
    assert sanitized_value == "genome assembly"
    assert not applied_rules  # No rules applied
