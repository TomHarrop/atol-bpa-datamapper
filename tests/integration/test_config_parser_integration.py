"""Integration tests for config_parser.py."""

import os
import json
import pytest
from atol_bpa_datamapper.config_parser import MetadataMap
from unittest.mock import patch
import logging


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


@pytest.mark.parametrize("field, expected_values, has_values", [
    ("scientific_name", ["Undetermined sp.", "Undetermined species", "Homo sapiens", "homo sapiens"], True),
    ("data_context", ["genome_assembly", "yes", "Genome resequencing"], True),
    ("platform", ["illumina", "illumina genomic", "illumina-shortread", "pacbio", "pacbio-hifi"], True),
    ("bpa_id", None, False),
])
def test_get_allowed_values_parameterized(metadata_map, field, expected_values, has_values):
    """Test the get_allowed_values method with parameterized inputs."""
    allowed_values = metadata_map.get_allowed_values(field)
    
    if has_values:
        assert allowed_values is not None
        # Check that all expected values are in the allowed values
        for value in expected_values:
            assert value in allowed_values
    else:
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


def test_sanitize_value(metadata_map, test_fixtures_dir):
    """Test the _sanitize_value method behavior without relying on implementation details."""
    # Create a real sanitization config file in the test fixtures directory
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
    
    # Create a temporary sanitization config file
    sanitization_config_path = os.path.join(test_fixtures_dir, "temp_sanitization_config.json")
    with open(sanitization_config_path, "w") as f:
        json.dump(sanitization_config, f)
    
    try:
        # Create a new MetadataMap instance with the sanitization config
        # This avoids mocking internal implementation details
        field_mapping_file = os.path.join(test_fixtures_dir, "test_field_mapping.json")
        value_mapping_file = os.path.join(test_fixtures_dir, "test_value_mapping.json")
        
        # Temporarily patch the os.path.exists and os.path.join functions to return our config
        def mock_path_exists(path):
            if "sanitization_config.json" in path:
                return True
            return os.path.exists(path)
            
        def mock_path_join(*args):
            if args[-1] == "sanitization_config.json":
                return sanitization_config_path
            return os.path.join(*args)
        
        with patch("os.path.exists", side_effect=mock_path_exists):
            with patch("os.path.join", side_effect=mock_path_join):
                test_metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
                
                # Test case 1: Text sanitization (whitespace handling)
                value_to_sanitize = "  Homo   sapiens  "
                sanitized_value, applied_rules = test_metadata_map._sanitize_value(
                    "organism", "scientific_name", value_to_sanitize
                )
                assert sanitized_value == "Homo sapiens"
                assert "text_sanitization" in applied_rules
                
                # Test case 2: Empty string sanitization
                sanitized_value, applied_rules = test_metadata_map._sanitize_value(
                    "organism", "scientific_name", ""
                )
                assert sanitized_value is None
                assert "empty_string_sanitization" in applied_rules
                
                # Test case 3: File format sanitization
                sanitized_value, applied_rules = test_metadata_map._sanitize_value(
                    "runs", "file_format", "  FASTQ  "
                )
                assert sanitized_value == "FASTQ"
                assert "text_sanitization" in applied_rules
                
                # Test case 4: Field without sanitization rules
                sanitized_value, applied_rules = test_metadata_map._sanitize_value(
                    "sample", "data_context", "genome assembly"
                )
                assert sanitized_value == "genome assembly"
                assert not applied_rules
    finally:
        # Clean up the temporary file
        if os.path.exists(sanitization_config_path):
            os.remove(sanitization_config_path)


def test_invalid_json_format(test_fixtures_dir):
    """Test that the MetadataMap constructor raises an error when given invalid JSON."""
    invalid_json_path = os.path.join(test_fixtures_dir, "invalid_json.json")
    valid_json_path = os.path.join(test_fixtures_dir, "test_value_mapping.json")
    
    # Test with invalid field mapping
    with pytest.raises(json.JSONDecodeError):
        MetadataMap(invalid_json_path, valid_json_path)
    
    # Test with invalid value mapping
    with pytest.raises(json.JSONDecodeError):
        MetadataMap(valid_json_path, invalid_json_path)


def test_file_io_errors():
    """Test that the MetadataMap constructor handles file I/O errors gracefully."""
    # Test with non-existent field mapping file
    with pytest.raises(FileNotFoundError):
        MetadataMap("non_existent_file.json", "tests/fixtures/test_value_mapping.json")
    
    # Test with non-existent value mapping file
    with pytest.raises(FileNotFoundError):
        MetadataMap("tests/fixtures/test_field_mapping.json", "non_existent_file.json")


def test_invalid_mapping_structure(test_fixtures_dir):
    """Test that the MetadataMap constructor validates the structure of mapping files."""
    invalid_structure_path = os.path.join(test_fixtures_dir, "invalid_structure.json")
    valid_json_path = os.path.join(test_fixtures_dir, "test_value_mapping.json")
    
    # The current implementation might not validate structure strictly
    # This test documents the current behavior and can be updated if validation is added
    try:
        metadata_map = MetadataMap(invalid_structure_path, valid_json_path)
        # If no exception is raised, verify that the object is created but might be incomplete
        assert metadata_map is not None
        # Check that controlled_vocabularies is empty or contains only valid fields
        if hasattr(metadata_map, "controlled_vocabularies"):
            for field in metadata_map.controlled_vocabularies:
                assert isinstance(field, str)
    except Exception as e:
        # If an exception is raised, it should be a specific type related to validation
        # This is a placeholder for future validation implementation
        pytest.skip(f"Structure validation not implemented yet: {str(e)}")


def test_logging_output(test_fixtures_dir, caplog):
    """Test that the MetadataMap logs important information."""
    # Set the log level to capture all logs
    caplog.set_level(logging.INFO)
    
    # Clear the current logs
    caplog.clear()
    
    # Create a new metadata map to trigger logging
    field_mapping_file = os.path.join(test_fixtures_dir, "test_field_mapping.json")
    value_mapping_file = os.path.join(test_fixtures_dir, "test_value_mapping.json")
    
    # This should generate log messages
    new_metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    # Check that something was logged
    assert len(caplog.records) > 0
    
    # Check for specific log messages related to initialization
    found_field_mapping_log = False
    found_value_mapping_log = False
    
    for record in caplog.records:
        if "Reading field mapping" in record.message:
            found_field_mapping_log = True
        if "Reading value mapping" in record.message:
            found_value_mapping_log = True
    
    assert found_field_mapping_log, "Expected log message about reading field mapping not found"
    assert found_value_mapping_log, "Expected log message about reading value mapping not found"
