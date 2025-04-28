"""Unit tests for package_handler.py."""

import pytest
from unittest.mock import MagicMock
from atol_bpa_datamapper.package_handler import BpaPackage, get_nested_value


def test_bpa_package_initialization():
    """Test BpaPackage initialization."""
    # Create a simple package
    package_data = {
        "id": "test-package-123",
        "field1": "value1",
        "field2": "value2",
        "resources": [
            {"id": "resource1", "type": "fastq"},
            {"id": "resource2", "type": "bam"}
        ]
    }
    package = BpaPackage(package_data)
    
    # Check that the package was initialized correctly
    assert package.id == "test-package-123"
    assert package["field1"] == "value1"
    assert package["field2"] == "value2"
    assert package.fields == ["bpa_id", "field1", "field2", "id", "resources"]
    assert package.resource_ids == ["resource1", "resource2"]


def test_choose_value_with_no_fields():
    """Test choose_value with no fields to check."""
    package = BpaPackage({"id": "test-package-123"})
    value, bpa_field, keep = package.choose_value([], None)
    assert value is None
    assert bpa_field is None
    assert keep is False


def test_choose_value_with_missing_fields():
    """Test choose_value with fields that don't exist in the package."""
    package = BpaPackage({"id": "test-package-123"})
    value, bpa_field, keep = package.choose_value(["field1", "field2"], None)
    assert value is None
    assert bpa_field is None
    assert keep is False


def test_choose_value_with_no_controlled_vocabulary():
    """Test choose_value with no controlled vocabulary."""
    package = BpaPackage({"id": "test-package-123", "field1": "value1"})
    value, bpa_field, keep = package.choose_value(["field1"], None)
    assert value == "value1"
    assert bpa_field == "field1"
    assert keep is True


def test_choose_value_with_controlled_vocabulary_match():
    """Test choose_value with a controlled vocabulary that matches."""
    package = BpaPackage({"id": "test-package-123", "field1": "value1"})
    value, bpa_field, keep = package.choose_value(["field1"], ["value1", "value2"])
    assert value == "value1"
    assert bpa_field == "field1"
    assert keep is True


def test_choose_value_with_controlled_vocabulary_no_match():
    """Test choose_value with a controlled vocabulary that doesn't match."""
    package = BpaPackage({"id": "test-package-123", "field1": "value1"})
    value, bpa_field, keep = package.choose_value(["field1"], ["value2", "value3"])
    assert value == "value1"
    assert bpa_field == "field1"
    assert keep is False


def test_choose_value_with_multiple_fields():
    """Test choose_value with multiple fields to check."""
    package = BpaPackage({
        "id": "test-package-123", 
        "field1": "value1",
        "field2": "value2"
    })
    value, bpa_field, keep = package.choose_value(["field1", "field2"], None)
    assert value == "value1"
    assert bpa_field == "field1"
    assert keep is True


def test_map_metadata_multiple_resources():
    """Test mapping metadata with multiple resources."""
    # Create a package with multiple resources
    package_data = {
        "id": "test-package-123",
        "field1": "value1",
        "field2": "value2",
        "resources": [
            {"id": "resource1", "type": "fastq", "resource_field": "resource1_value"},
            {"id": "resource2", "type": "fastq", "resource_field": "resource2_value"}
        ]
    }
    package = BpaPackage(package_data)
    
    # Create a simple metadata map
    metadata_map = MagicMock()
    metadata_map.metadata_sections = ["dataset", "organism", "reads"]
    metadata_map.expected_fields = ["field_a", "field_b", "field_c"]
    
    def mock_get_atol_section(field):
        if field == "field_c":
            return "reads"
        return "dataset"
    
    def mock_get_bpa_fields(field):
        if field == "field_a":
            return ["field1"]
        elif field == "field_b":
            return ["field2"]
        elif field == "field_c":
            return ["resources.resource_field"]
        return []
    
    def mock_get_allowed_values(field):
        return None
    
    def mock_map_value(field, value):
        return value
        
    def mock_sanitize_value(section, field, value):
        return (value, [])  # Return tuple of (sanitized_value, applied_rules)
    
    metadata_map.get_atol_section = mock_get_atol_section
    metadata_map.get_bpa_fields = mock_get_bpa_fields
    metadata_map.get_allowed_values = mock_get_allowed_values
    metadata_map.map_value = mock_map_value
    metadata_map._sanitize_value = mock_sanitize_value
    
    # Map the metadata
    result = package.map_metadata(metadata_map)
    
    # Check that the result has the expected structure
    assert "dataset" in result
    assert "reads" in result
    assert len(result["reads"]) == 2
    
    # Check that the dataset fields were mapped correctly
    assert result["dataset"]["field_a"] == "value1"
    assert result["dataset"]["field_b"] == "value2"
    
    # Check that the reads fields were mapped correctly
    assert result["reads"][0]["field_c"] == "resource1_value"
    assert result["reads"][0]["resource_id"] == "resource1"
    assert result["reads"][1]["field_c"] == "resource2_value"
    assert result["reads"][1]["resource_id"] == "resource2"


def test_map_metadata_with_controlled_vocabulary():
    """Test mapping metadata with a controlled vocabulary."""
    # Create a package
    package_data = {
        "id": "test-package-123",
        "field1": "old_value1",
        "field2": "old_value2"
    }
    package = BpaPackage(package_data)
    
    # Create a metadata map with controlled vocabulary
    metadata_map = MagicMock()
    metadata_map.metadata_sections = ["dataset"]
    metadata_map.expected_fields = ["field_a", "field_b"]
    metadata_map.controlled_vocabularies = ["field_a"]
    
    def mock_get_atol_section(field):
        return "dataset"
    
    def mock_get_bpa_fields(field):
        if field == "field_a":
            return ["field1"]
        elif field == "field_b":
            return ["field2"]
        return []
    
    def mock_get_allowed_values(field):
        if field == "field_a":
            return ["old_value1"]
        return None
    
    def mock_map_value(field, value):
        if field == "field_a" and value == "old_value1":
            return "new_value1"
        return value
        
    def mock_sanitize_value(section, field, value):
        return (value, [])  # Return tuple of (sanitized_value, applied_rules)
    
    metadata_map.get_atol_section = mock_get_atol_section
    metadata_map.get_bpa_fields = mock_get_bpa_fields
    metadata_map.get_allowed_values = mock_get_allowed_values
    metadata_map.map_value = mock_map_value
    metadata_map._sanitize_value = mock_sanitize_value
    
    # Map the metadata
    result = package.map_metadata(metadata_map)
    
    # Check that the result has the expected structure
    assert "dataset" in result
    
    # Check that the fields were mapped correctly
    assert result["dataset"]["field_a"] == "new_value1"
    assert result["dataset"]["field_b"] == "old_value2"


def test_map_metadata_with_empty_resources():
    """Test mapping metadata with no resources."""
    # Create a package with no resources
    package_data = {
        "id": "test-package-123",
        "field1": "value1",
        "resources": []
    }
    package = BpaPackage(package_data)
    
    # Create a simple metadata map
    metadata_map = MagicMock()
    metadata_map.metadata_sections = ["dataset", "reads"]
    metadata_map.expected_fields = ["field_a", "field_c"]
    
    def mock_get_atol_section(field):
        if field == "field_c":
            return "reads"
        return "dataset"
    
    def mock_get_bpa_fields(field):
        if field == "field_a":
            return ["field1"]
        elif field == "field_c":
            return ["resources.resource_field"]
        return []
    
    def mock_get_allowed_values(field):
        return None
    
    def mock_map_value(field, value):
        return value
        
    def mock_sanitize_value(section, field, value):
        return (value, [])  # Return tuple of (sanitized_value, applied_rules)
    
    metadata_map.get_atol_section = mock_get_atol_section
    metadata_map.get_bpa_fields = mock_get_bpa_fields
    metadata_map.get_allowed_values = mock_get_allowed_values
    metadata_map.map_value = mock_map_value
    metadata_map._sanitize_value = mock_sanitize_value
    
    # Map the metadata
    result = package.map_metadata(metadata_map)
    
    # Check that the result has the expected structure
    assert "dataset" in result
    assert "reads" in result
    
    # Check that the dataset fields were mapped correctly
    assert result["dataset"]["field_a"] == "value1"
    
    # Check that the reads section is empty
    assert result["reads"] == []


def test_map_metadata_with_nested_fields():
    """Test mapping metadata with nested fields."""
    # Create a package with nested fields
    package_data = {
        "id": "test-package-123",
        "nested": {
            "field": "nested_value"
        }
    }
    package = BpaPackage(package_data)
    
    # Create a simple metadata map
    metadata_map = MagicMock()
    metadata_map.metadata_sections = ["dataset"]
    metadata_map.expected_fields = ["nested_field"]
    
    def mock_get_atol_section(field):
        return "dataset"
    
    def mock_get_bpa_fields(field):
        if field == "nested_field":
            return ["nested.field"]
        return []
    
    def mock_get_allowed_values(field):
        return None
    
    def mock_map_value(field, value):
        return value
        
    def mock_sanitize_value(section, field, value):
        return (value, [])  # Return tuple of (sanitized_value, applied_rules)
    
    metadata_map.get_atol_section = mock_get_atol_section
    metadata_map.get_bpa_fields = mock_get_bpa_fields
    metadata_map.get_allowed_values = mock_get_allowed_values
    metadata_map.map_value = mock_map_value
    metadata_map._sanitize_value = mock_sanitize_value
    
    # Map the metadata
    result = package.map_metadata(metadata_map)
    
    # Check that the result has the expected structure
    assert "dataset" in result
    
    # Check that the nested field was mapped correctly
    assert result["dataset"]["nested_field"] == "nested_value"


def test_map_metadata_with_null_values():
    """Test mapping metadata with null values."""
    # Create a package with null values
    package_data = {
        "id": "test-package-123",
        "field1": None
    }
    package = BpaPackage(package_data)
    
    # Create a simple metadata map
    metadata_map = MagicMock()
    metadata_map.metadata_sections = ["dataset"]
    metadata_map.expected_fields = ["field_a"]
    
    def mock_get_atol_section(field):
        return "dataset"
    
    def mock_get_bpa_fields(field):
        if field == "field_a":
            return ["field1"]
        return []
    
    def mock_get_allowed_values(field):
        return None
    
    def mock_map_value(field, value):
        return value
        
    def mock_sanitize_value(section, field, value):
        return (value, [])  # Return tuple of (sanitized_value, applied_rules)
    
    metadata_map.get_atol_section = mock_get_atol_section
    metadata_map.get_bpa_fields = mock_get_bpa_fields
    metadata_map.get_allowed_values = mock_get_allowed_values
    metadata_map.map_value = mock_map_value
    metadata_map._sanitize_value = mock_sanitize_value
    
    # Map the metadata
    result = package.map_metadata(metadata_map)
    
    # Check that the result has the expected structure
    assert "dataset" in result
    
    # Check that the null value was mapped correctly
    assert result["dataset"]["field_a"] is None


def test_map_metadata_with_fallback_fields():
    """Test mapping metadata with fallback fields."""
    # Create a package with multiple possible fields
    package_data = {
        "id": "test-package-123",
        "name": "Test Package",
        "title": "This is a test package",
        "resources": [
            {"id": "resource1", "format": "FASTQ", "file_type": None}
        ]
    }
    package = BpaPackage(package_data)
    
    # Create a metadata map with fallback fields
    metadata_map = MagicMock()
    metadata_map.metadata_sections = ["dataset", "reads"]
    metadata_map.expected_fields = ["package_name", "file_format"]
    
    def mock_get_atol_section(field):
        if field == "file_format":
            return "reads"
        return "dataset"
    
    def mock_get_bpa_fields(field):
        if field == "package_name":
            return ["title", "name", "id"]  # Try title first, then name, then id
        elif field == "file_format":
            return ["resources.file_type", "resources.format"]  # Try file_type first, then format
        return []
    
    def mock_get_allowed_values(field):
        return None
    
    def mock_map_value(field, value):
        return value
        
    def mock_sanitize_value(section, field, value):
        return (value, [])  # Return tuple of (sanitized_value, applied_rules)
    
    metadata_map.get_atol_section = mock_get_atol_section
    metadata_map.get_bpa_fields = mock_get_bpa_fields
    metadata_map.get_allowed_values = mock_get_allowed_values
    metadata_map.map_value = mock_map_value
    metadata_map._sanitize_value = mock_sanitize_value
    
    # Map the metadata
    result = package.map_metadata(metadata_map)
    
    # Check that the result has the expected structure
    assert "dataset" in result
    assert "reads" in result
    
    # Check that the first available field was used
    assert result["dataset"]["package_name"] == "This is a test package"
    
    # Check that the fallback field was used when the first choice was None
    assert result["reads"][0]["file_format"] == "FASTQ"


def test_get_nested_value():
    """Test get_nested_value function."""
    # Create a dictionary with nested values
    data = {
        "field1": "value1",
        "nested": {
            "field2": "value2",
            "deeply": {
                "field3": "value3"
            }
        },
        "list": [
            {"id": "item1", "value": "value4"},
            {"id": "item2", "value": "value5"}
        ]
    }
    
    # Test getting simple values
    assert get_nested_value(data, "field1") == "value1"
    
    # Test getting nested values
    assert get_nested_value(data, "nested.field2") == "value2"
    assert get_nested_value(data, "nested.deeply.field3") == "value3"
    
    # Test getting values from non-existent paths
    assert get_nested_value(data, "field2") is None
    assert get_nested_value(data, "nested.field3") is None
    assert get_nested_value(data, "nested.deeply.field4") is None
    
    # Test with None input
    assert get_nested_value(None, "field1") is None
    assert get_nested_value(data, None) is None
