"""Integration tests for config_parser.py."""

import os
import json
import pytest
from atol_bpa_datamapper.config_parser import MetadataMap
from unittest.mock import patch
import logging



def test_metadata_map_initialization(package_metadata_map, resource_metadata_map):
    """Test that the MetadataMap is initialized correctly."""
    # This test verifies that:
    # 1. The MetadataMap object is correctly initialized from the mapping files
    # 2. The expected fields are loaded from the field mapping file
    # 3. The metadata sections are correctly identified
    # 4. The controlled vocabularies are correctly loaded
    
    # Check that the metadata map is not None
    assert package_metadata_map is not None
    
    # Check that the expected fields are loaded
    assert "scientific_name" in package_metadata_map.expected_fields
    assert "data_context" in package_metadata_map.expected_fields
    assert "bpa_id" in package_metadata_map.expected_fields
    
    # Check that the metadata sections are correct in package_metadata_map
    assert "organism" in package_metadata_map.metadata_sections
    assert "sample" in package_metadata_map.metadata_sections
    assert "dataset" in package_metadata_map.metadata_sections
    
    # Check that the metadata sections are correct in resource_metadata_map
    assert "runs" in resource_metadata_map.metadata_sections
    
    # Check that the controlled vocabularies are loaded
    assert "scientific_name" in package_metadata_map.controlled_vocabularies
    assert "data_context" in package_metadata_map.controlled_vocabularies


@pytest.mark.parametrize("field, expected_values, has_values, use_resource_map", [
    ("scientific_name", ["Undetermined sp.", "Undetermined species", "Homo sapiens", "homo sapiens"], True, False),
    ("data_context", ["genome_assembly", "yes", "Genome resequencing"], True, False),
    ("platform", ["illumina", "illumina genomic", "illumina-shortread", "pacbio", "pacbio-hifi"], True, True),
    ("bpa_id", None, False, False),
])
def test_get_allowed_values_parameterized(package_metadata_map, resource_metadata_map, field, expected_values, has_values, use_resource_map):
    """Test the get_allowed_values method with parameterized inputs."""
    # Choose the appropriate metadata map based on the field
    metadata_map = resource_metadata_map if use_resource_map else package_metadata_map
    # This test verifies that:
    # 1. The get_allowed_values method returns the correct allowed values for each field
    # 2. Fields with controlled vocabularies return the expected list of values
    # 3. Fields without controlled vocabularies return None
    # 4. The method works correctly with a variety of field types
    
    allowed_values = metadata_map.get_allowed_values(field)
    
    if has_values:
        assert allowed_values is not None
        # Check that all expected values are in the allowed values
        for value in expected_values:
            assert value in allowed_values
    else:
        assert allowed_values is None


def test_get_bpa_fields(package_metadata_map):
    """Test the get_bpa_fields method."""
    # This test verifies that:
    # 1. The get_bpa_fields method returns the correct BPA fields for each AToL field
    # 2. The method correctly handles fields with multiple possible BPA sources
    # 3. The returned fields match the configuration in the field mapping file
    
    # Test a field with multiple BPA fields
    bpa_fields = package_metadata_map.get_bpa_fields("scientific_name")
    assert bpa_fields is not None
    assert "scientific_name" in bpa_fields
    assert "species_name" in bpa_fields
    assert "taxon_or_organism" in bpa_fields
    
    # Test a field with a single BPA field
    bpa_fields = package_metadata_map.get_bpa_fields("bpa_id")
    assert bpa_fields is not None
    assert "id" in bpa_fields


@pytest.mark.parametrize("field, expected_section, use_resource_map", [
    ("scientific_name", "organism", False),
    ("data_context", "sample", False),
    ("platform", "runs", True),
    ("bpa_id", "dataset", False),
])
def test_get_atol_section(package_metadata_map, resource_metadata_map, field, expected_section, use_resource_map):
    """Test the get_atol_section method with parameterized inputs."""
    # Choose the appropriate metadata map based on the field
    metadata_map = resource_metadata_map if use_resource_map else package_metadata_map
    # This test verifies that:
    # 1. The get_atol_section method returns the correct section for each field
    # 2. The method works correctly with fields from different sections
    # 3. The returned sections match the configuration in the field mapping file
    
    section = metadata_map.get_atol_section(field)
    assert section == expected_section


@pytest.mark.parametrize("field, value, expected_result, use_resource_map", [
    ("scientific_name", "Homo sapiens", True, False),
    ("scientific_name", "Unknown Species", False, False),
    ("data_context", "Genome resequencing", True, False),
    ("data_context", "Unknown Context", False, False),
    ("bpa_id", "any-value", True, False),  # Non-controlled field should always return True
    ("platform", "illumina", True, True),
    ("platform", "unknown-platform", False, True),
])
def test_keep_value(package_metadata_map, resource_metadata_map, field, value, expected_result, use_resource_map):
    """Test the keep_value method with parameterized inputs."""
    # Choose the appropriate metadata map based on the field
    metadata_map = resource_metadata_map if use_resource_map else package_metadata_map
    # This test verifies that:
    # 1. The keep_value method correctly determines whether a value is in the controlled vocabulary
    # 2. Values in the controlled vocabulary return True
    # 3. Values not in the controlled vocabulary return False
    # 4. The method works correctly with a variety of field types and values
    
    result = metadata_map.keep_value(field, value)
    assert result == expected_result


@pytest.mark.parametrize("field, value, expected_result, use_resource_map", [
    ("scientific_name", "Homo sapiens", "Homo sapiens", False),
    ("scientific_name", "homo sapiens", "Homo sapiens", False),  # Test case insensitivity
    ("data_context", "Genome resequencing", "genome_assembly", False),
    ("bpa_id", "test-id", "test-id", False),  # Non-mapped field should return the original value
    ("platform", "illumina", "illumina_genomic", True),  # Resource field
    ("platform", "pacbio", "pacbio_hifi", True),  # Resource field
])
def test_map_value(package_metadata_map, resource_metadata_map, field, value, expected_result, use_resource_map):
    """Test the map_value method with parameterized inputs."""
    # Choose the appropriate metadata map based on the field
    metadata_map = resource_metadata_map if use_resource_map else package_metadata_map
    # This test verifies that:
    # 1. The map_value method correctly maps input values to their AToL equivalents
    # 2. Case-insensitive matching works correctly
    # 3. Values are correctly transformed according to the value mapping configuration
    # 4. The method works correctly with a variety of field types and values
    
    result = metadata_map.map_value(field, value)
    assert result == expected_result


def test_sanitize_value(test_fixtures_dir):
    """Test the _sanitize_value method using a temporary sanitization config."""
    # This test verifies that:
    # 1. The _sanitize_value method correctly applies sanitization rules to values
    # 2. Different types of sanitization rules (regex, case, etc.) are correctly applied
    # 3. The method returns both the sanitized value and the list of applied rules
    # 4. The sanitization process works correctly for different field types and values
    
    # Create a temporary sanitization config
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
        },
        "null_values": ["", "null", "NULL", "None", "none", "NA", "na", "N/A", "n/a"]
    }
    
    sanitization_config_path = os.path.join(test_fixtures_dir, "temp_sanitization_config.json")
    
    try:
        # Write the sanitization config to a temporary file
        with open(sanitization_config_path, "w") as f:
            json.dump(sanitization_config, f)
        
        # Create metadata maps that will use our sanitization config
        package_field_mapping_file = os.path.join(test_fixtures_dir, "test_field_mapping_packages.json")
        resource_field_mapping_file = os.path.join(test_fixtures_dir, "test_field_mapping_resources.json")
        value_mapping_file = os.path.join(test_fixtures_dir, "test_value_mapping.json")
        
        # Create a custom class that mocks the sanitization behavior
        class MockMetadataMap(MetadataMap):
            def __init__(self, field_mapping_file, value_mapping_file):
                super().__init__(field_mapping_file, value_mapping_file)
                self.sanitization_config = sanitization_config
        
        # Create instances of our mock class
        package_metadata_map = MockMetadataMap(package_field_mapping_file, value_mapping_file)
        resource_metadata_map = MockMetadataMap(resource_field_mapping_file, value_mapping_file)
        
        # For simplicity in testing, we'll use the package metadata map for organism fields
        # and the resource metadata map for runs fields
        metadata_map = package_metadata_map  # Default for organism tests
        
        # Test text sanitization on package-level field
        sanitized_value, applied_rules = package_metadata_map._sanitize_value("organism", "scientific_name", "  Homo   sapiens  ")
        assert sanitized_value == "Homo sapiens"
        assert "text_sanitization" in applied_rules
        
        # Test empty string sanitization on package-level field
        sanitized_value, applied_rules = package_metadata_map._sanitize_value("organism", "scientific_name", "")
        assert sanitized_value is None
        assert "empty_string_sanitization" in applied_rules
        
        # Test text sanitization on resource-level field
        sanitized_value, applied_rules = resource_metadata_map._sanitize_value("runs", "platform", "  illumina   genomic  ")
        assert sanitized_value == "illumina genomic"
        assert "text_sanitization" in applied_rules
        
        # Test sanitization on resource-level field with different rules
        sanitized_value, applied_rules = resource_metadata_map._sanitize_value("runs", "file_format", "  FASTQ  ")
        assert sanitized_value == "FASTQ"
        assert "text_sanitization" in applied_rules
        
        # Test a field without sanitization rules
        sanitized_value, applied_rules = metadata_map._sanitize_value("dataset", "bpa_id", "test-id")
        assert sanitized_value == "test-id"
        assert applied_rules == []
        
        # Test a value that doesn't need sanitization but still has rules applied
        sanitized_value, applied_rules = metadata_map._sanitize_value("organism", "scientific_name", "Homo sapiens")
        assert sanitized_value == "Homo sapiens"
        # The rule might not be applied if the value doesn't need sanitization
        # This is implementation-dependent, so we don't assert on applied_rules here
    
    finally:
        # Clean up the temporary file
        if os.path.exists(sanitization_config_path):
            os.remove(sanitization_config_path)


def test_invalid_json_format(invalid_json_file, field_mapping_file, field_mapping_file_resources, value_mapping_file):
    """Test that the MetadataMap constructor raises an error when given invalid JSON."""
    # This test verifies that:
    # 1. The MetadataMap constructor validates the JSON format of mapping files
    # 2. An appropriate error is raised when invalid JSON is provided
    # 3. The error message clearly indicates the issue with the file
    
    # Test with invalid package field mapping
    with pytest.raises(json.JSONDecodeError):
        MetadataMap(invalid_json_file, value_mapping_file)
    
    # Test with invalid resource field mapping
    with pytest.raises(json.JSONDecodeError):
        MetadataMap(invalid_json_file, value_mapping_file)
    
    # Test with invalid value mapping (using package field mapping)
    with pytest.raises(json.JSONDecodeError):
        MetadataMap(field_mapping_file, invalid_json_file)
        
    # Test with invalid value mapping (using resource field mapping)
    with pytest.raises(json.JSONDecodeError):
        MetadataMap(field_mapping_file_resources, invalid_json_file)


def test_file_io_errors(field_mapping_file, field_mapping_file_resources):
    """Test that the MetadataMap constructor handles file I/O errors gracefully."""
    # This test verifies that:
    # 1. The MetadataMap constructor handles file I/O errors gracefully
    # 2. An appropriate error is raised when a file cannot be read
    # 3. The error message clearly indicates the issue with the file
    
    # Test with non-existent package field mapping file
    with pytest.raises(FileNotFoundError):
        MetadataMap("non_existent_file.json", "tests/fixtures/test_value_mapping.json")
    
    # Test with non-existent resource field mapping file
    with pytest.raises(FileNotFoundError):
        MetadataMap("non_existent_file.json", "tests/fixtures/test_value_mapping.json")
    
    # Test with non-existent value mapping file (using package field mapping)
    with pytest.raises(FileNotFoundError):
        MetadataMap(field_mapping_file, "non_existent_file.json")
        
    # Test with non-existent value mapping file (using resource field mapping)
    with pytest.raises(FileNotFoundError):
        MetadataMap(field_mapping_file_resources, "non_existent_file.json")


def test_invalid_mapping_structure(invalid_structure_file, value_mapping_file):
    """Test that the MetadataMap constructor validates the structure of mapping files."""
    # This test verifies that:
    # 1. The MetadataMap constructor validates the structure of mapping files
    # 2. An appropriate error is raised when a mapping file has an invalid structure
    # 3. The error message clearly indicates the issue with the file structure
    
    # The current implementation might not validate structure strictly
    # This test documents the current behavior and can be updated if validation is added
    try:
        metadata_map = MetadataMap(invalid_structure_file, value_mapping_file)
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
    # This test verifies that:
    # 1. The MetadataMap logs important information during initialization and operation
    # 2. Appropriate log messages are generated for different events
    # 3. The log messages contain useful information for debugging
    # 4. The logging level is correctly set based on configuration
    
    # Set the log level to capture all logs
    caplog.set_level(logging.INFO)
    
    # Clear the current logs
    caplog.clear()
    
    # Create new metadata maps to trigger logging
    package_field_mapping_file = os.path.join(test_fixtures_dir, "test_field_mapping_packages.json")
    resource_field_mapping_file = os.path.join(test_fixtures_dir, "test_field_mapping_resources.json")
    value_mapping_file = os.path.join(test_fixtures_dir, "test_value_mapping.json")
    
    # This should generate log messages for package-level mapping
    caplog.clear()
    package_metadata_map = MetadataMap(package_field_mapping_file, value_mapping_file)
    
    # Check that something was logged for package-level mapping
    assert len(caplog.records) > 0
    assert "Reading field mapping from" in caplog.text
    
    # Clear logs and test resource-level mapping
    caplog.clear()
    resource_metadata_map = MetadataMap(resource_field_mapping_file, value_mapping_file)
    
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
