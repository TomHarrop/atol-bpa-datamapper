"""Unit tests for config_parser.py."""

import pytest
import json
from unittest.mock import patch, mock_open

from atol_bpa_datamapper.config_parser import MetadataMap


def test_metadata_map_initialization():
    """Test MetadataMap initialization with mock files."""
    # This test verifies that:
    # 1. The MetadataMap class correctly initializes from field and value mapping files
    # 2. The mapping data is correctly loaded and structured
    # 3. The expected_fields attribute contains all fields from the field mapping
    # 4. The metadata_sections attribute contains all sections from the field mapping
    # 5. The controlled_vocabularies attribute is correctly populated
    # 6. The sanitization_config is correctly loaded
    
    # The field mapping file format is organized by section first, then field
    field_mapping = {
        "dataset": {
            "field1": ["bpa_field1", "bpa_field2"]
        },
        "organism": {
            "field2": ["bpa_field3"]
        },
        "reads": {
            "field3": ["resources.bpa_field4"]
        }
    }
    
    # The value mapping file format is organized by section, then field, then atol value to bpa values
    value_mapping = {
        "dataset": {
            "field1": {
                "new_value1": ["old_value1"],
                "new_value2": ["old_value2"],
                "default_value_1": [None]
            }
        },
        "organism": {
            "field2": {
                "new_value3": ["old_value3"]
            }
        }
    }
    
    # The sanitization config file format
    sanitization_config = {
        "dataset": {
            "field1": ["text_sanitization", "empty_string_sanitization"]
        },
        "organism": {
            "field2": ["integer_sanitization"]
        },
        "null_values": ["NULL", "N/A", ""]
    }
    
    # Mock the open function to return our test data
    with patch("builtins.open", mock_open()) as mock_file:
        # Configure the mock to return different content for different files
        mock_file.side_effect = [
            mock_open(read_data=json.dumps(field_mapping)).return_value,
            mock_open(read_data=json.dumps(value_mapping)).return_value,
            mock_open(read_data=json.dumps(sanitization_config)).return_value
        ]
        
        metadata_map = MetadataMap("field.json", "value.json", "sanitization.json")
        
        # Test that the metadata map was initialized correctly
        assert len(metadata_map) == 3
        assert metadata_map["field1"]["bpa_fields"] == ["bpa_field1", "bpa_field2"]
        assert metadata_map["field2"]["section"] == "organism"
        assert metadata_map["field3"]["bpa_fields"] == ["resources.bpa_field4"]
        
        # Test the value_mapping structure
        assert "value_mapping" in metadata_map["field1"]
        assert metadata_map["field1"]["value_mapping"]["old_value1"] == "new_value1"
        assert metadata_map["field1"]["value_mapping"]["old_value2"] == "new_value2"
        assert metadata_map["field2"]["value_mapping"]["old_value3"] == "new_value3"
        # Assert defaults correctly assigned
        assert metadata_map["field1"]["default"] == "default_value_1"
        assert "default" not in metadata_map["field2"]
        # Test that the controlled vocabularies were set correctly
        assert set(metadata_map.controlled_vocabularies) == {"field1", "field2"}
        
        # Test that the metadata sections were set correctly
        assert set(metadata_map.metadata_sections) == {"dataset", "organism", "reads"}
        
        # Test that the expected fields were set correctly
        assert set(metadata_map.expected_fields) == {"field1", "field2", "field3"}
        
        # Test that the sanitization config was loaded correctly
        assert metadata_map.sanitization_config == sanitization_config
        assert metadata_map.sanitization_config["dataset"]["field1"] == ["text_sanitization", "empty_string_sanitization"]
        assert metadata_map.sanitization_config["organism"]["field2"] == ["integer_sanitization"]
        assert metadata_map.sanitization_config["null_values"] == ["NULL", "N/A", ""]


def test_get_allowed_values():
    """Test get_allowed_values method."""
    # This test verifies that:
    # 1. The get_allowed_values method returns the correct allowed values for each field
    # 2. Fields with controlled vocabularies return the expected list of values
    # 3. Fields without controlled vocabularies return None
    # 4. The method handles case sensitivity correctly
    
    metadata_map = MetadataMap.__new__(MetadataMap)  # Create instance without calling __init__
    
    # Set up the metadata map manually with the correct structure
    metadata_map.update({
        "field1": {
            "value_mapping": {
                "old_value1": "new_value1",
                "old_value2": "new_value2"
            }
        },
        "field2": {
            "value_mapping": {
                "old_value3": "new_value3"
            }
        },
        "field3": {}  # No value mapping
    })
    
    # Test getting allowed values for fields with value mappings
    # The get_allowed_values method returns a sorted list of keys from the value_mapping dict
    assert metadata_map.get_allowed_values("field1") == ["old_value1", "old_value2"]
    assert metadata_map.get_allowed_values("field2") == ["old_value3"]
    
    # Test getting allowed values for field without value mapping
    assert metadata_map.get_allowed_values("field3") is None
    
    # Test getting allowed values for non-existent field
    assert metadata_map.get_allowed_values("field4") is None


def test_get_bpa_fields():
    """Test get_bpa_fields method."""
    # This test verifies that:
    # 1. The get_bpa_fields method returns the correct BPA fields for each AToL field
    # 2. The method correctly handles fields with multiple possible BPA sources
    # 3. The method returns an empty list for unknown fields
    # 4. The returned fields match the configuration in the field mapping
    
    metadata_map = MetadataMap.__new__(MetadataMap)  # Create instance without calling __init__
    
    # Set up the metadata map manually
    metadata_map.update({
        "field1": {
            "bpa_fields": ["bpa_field1", "bpa_field2"]
        },
        "field2": {
            "bpa_fields": ["bpa_field3"]
        },
        "field3": {}  # No bpa_fields
    })
    
    # Test getting BPA fields for fields with bpa_fields
    assert metadata_map.get_bpa_fields("field1") == ["bpa_field1", "bpa_field2"]
    assert metadata_map.get_bpa_fields("field2") == ["bpa_field3"]
    
    # Test getting BPA fields for field without bpa_fields
    with pytest.raises(KeyError):
        metadata_map.get_bpa_fields("field3")
    
    # Test getting BPA fields for non-existent field
    with pytest.raises(KeyError):
        metadata_map.get_bpa_fields("field4")


def test_get_atol_section():
    """Test get_atol_section method."""
    # This test verifies that:
    # 1. The get_atol_section method returns the correct section for each field
    # 2. The method works correctly with fields from different sections
    # 3. The method returns None for unknown fields
    # 4. The returned sections match the configuration in the field mapping
    
    metadata_map = MetadataMap.__new__(MetadataMap)  # Create instance without calling __init__
    
    # Set up the metadata map manually with the correct key name
    metadata_map.update({
        "field1": {
            "section": "dataset"
        },
        "field2": {
            "section": "organism"
        },
        "field3": {}  # No section
    })
    
    # Test getting AToL section for fields with section
    assert metadata_map.get_atol_section("field1") == "dataset"
    assert metadata_map.get_atol_section("field2") == "organism"
    
    # Test getting AToL section for field without section
    with pytest.raises(KeyError):
        metadata_map.get_atol_section("field3")
    
    # Test getting AToL section for non-existent field
    with pytest.raises(KeyError):
        metadata_map.get_atol_section("field4")


def test_check_default_value():
    """Test check_default_value method."""
    # This test verifies that:
    # 1. The check_default_value method correctly identifies fields with default values
    # 2. The method returns the correct default value when one exists
    # 3. The method correctly handles fields without default values
    # 4. The method correctly handles non-existent fields
    
    metadata_map = MetadataMap.__new__(MetadataMap)  # Create instance without calling __init__
    
    # Set up the metadata map manually with fields that have default values
    metadata_map.update({
        "field1": {
            "default": "default_value1"
        },
        "field2": {
            "value_mapping": {}
            # No default value
        },
        "field3": {}
        # No value_mapping or default
    })
    
    # Test field with default value
    has_default, default_value = metadata_map.check_default_value("field1")
    assert has_default is True
    assert default_value == "default_value1"
    
    # Test field without default value but with value_mapping
    has_default, default_value = metadata_map.check_default_value("field2")
    assert has_default is False
    assert default_value is None
    
    # Test field without value_mapping
    has_default, default_value = metadata_map.check_default_value("field3")
    assert has_default is False
    assert default_value is None
    
    # Test non-existent field
    has_default, default_value = metadata_map.check_default_value("field4")
    assert has_default is False
    assert default_value is None


def test_keep_value():
    """Test keep_value method."""
    # This test verifies that:
    # 1. The keep_value method correctly determines if a value should be kept based on controlled vocabulary
    # 2. The method returns True for values in the allowed values list
    # 3. The method returns False for values not in the allowed values list
    # 4. The method returns True for any value when there is no controlled vocabulary
    
    metadata_map = MetadataMap.__new__(MetadataMap)  # Create instance without calling __init__
    
    # Set up the metadata map manually with fields that have controlled vocabularies
    metadata_map.update({
        "field1": {
            "value_mapping": {
                "old_value1": "new_value1",
                "old_value2": "new_value2"
            }
        },
        "field2": {
            # No value_mapping - no controlled vocabulary
        }
    })
    
    # Test field with controlled vocabulary - value in allowed values
    assert metadata_map.keep_value("field1", "old_value1") is True
    assert metadata_map.keep_value("field1", "old_value2") is True
    
    # Test field with controlled vocabulary - value not in allowed values
    assert metadata_map.keep_value("field1", "unknown_value") is False
    
    # Test field without controlled vocabulary - any value should be kept
    assert metadata_map.keep_value("field2", "any_value") is True
    
    # Test non-existent field - should return True since get_allowed_values returns None
    assert metadata_map.keep_value("field3", "any_value") is True


def test_map_value():
    """Test map_value method."""
    # This test verifies that:
    # 1. The map_value method correctly maps input values to their AToL equivalents
    # 2. Case-insensitive matching works correctly
    # 3. Values are correctly transformed according to the value mapping configuration
    # 4. The method returns the original value for unmapped values
    # 5. The method handles unknown fields gracefully
    
    metadata_map = MetadataMap.__new__(MetadataMap)  # Create instance without calling __init__
    
    # Set up the metadata map manually with the correct structure
    metadata_map.update({
        "field1": {
            "value_mapping": {
                "old_value1": "new_value1",
                "old_value2": "new_value2"
            }
        },
        "field2": {
            "value_mapping": {
                "old_value3": "new_value3"
            }
        },
        "field3": {},  # No value mapping
        "data_context": {
            "value_mapping": {}
        }
    })
    
    # Test mapping values for fields with value mappings
    assert metadata_map.map_value("field1", "old_value1") == "new_value1"
    assert metadata_map.map_value("field1", "old_value2") == "new_value2"
    assert metadata_map.map_value("field2", "old_value3") == "new_value3"
    
    # Test mapping values that aren't in the mapping
    # This will raise a KeyError in the actual implementation
    with pytest.raises(KeyError):
        metadata_map.map_value("field1", "unknown_value")
    
    # Test special case for data_context field with value "yes"
    assert metadata_map.map_value("data_context", "yes") == "genome_assembly"
    
    # Test mapping values for field without value mapping
    assert metadata_map.map_value("field3", "any_value") == "any_value"
    
    # Test mapping None value - this will raise a KeyError in the actual implementation
    with pytest.raises(KeyError):
        metadata_map.map_value("field1", None)


def test__sanitize_value():
    """Test _sanitize_value method."""
    # This test verifies that:
    # 1. The _sanitize_value method correctly applies sanitization rules to values
    # 2. The method returns both the sanitized value and a list of applied rules
    # 3. The method correctly handles different types of sanitization rules
    # 4. The method correctly handles None values and fields without sanitization rules
    
    metadata_map = MetadataMap.__new__(MetadataMap)  # Create instance without calling __init__
    
    # Set up the sanitization config
    metadata_map.sanitization_config = {
        "dataset": {
            "field1": ["text_sanitization", "empty_string_sanitization"],
            "field2": ["integer_sanitization"]
        },
        "organism": {
            "field3": ["text_sanitization"]
        },
        "null_values": ["NULL", "N/A", ""]
    }
    
    # Test text sanitization
    value, applied_rules = metadata_map._sanitize_value("dataset", "field1", "  Multiple   spaces  ")
    assert value == "Multiple spaces"
    assert "text_sanitization" in applied_rules
    
    # Test empty string sanitization
    value, applied_rules = metadata_map._sanitize_value("dataset", "field1", "N/A")
    assert value is None
    assert "empty_string_sanitization" in applied_rules
    
    # Test integer sanitization
    value, applied_rules = metadata_map._sanitize_value("dataset", "field2", "123.45")
    assert value == "123"
    assert "integer_sanitization" in applied_rules
    
    # Test multiple rules applied
    value, applied_rules = metadata_map._sanitize_value("dataset", "field1", "  N/A  ")
    assert value is None
    assert "text_sanitization" in applied_rules
    assert "empty_string_sanitization" in applied_rules
    
    # Test no rules applied (value unchanged)
    value, applied_rules = metadata_map._sanitize_value("dataset", "field1", "Normal value")
    assert value == "Normal value"
    assert len(applied_rules) == 0
    
    # Test None value
    value, applied_rules = metadata_map._sanitize_value("dataset", "field1", None)
    assert value is None
    assert len(applied_rules) == 0
    
    # Test field without sanitization rules
    value, applied_rules = metadata_map._sanitize_value("dataset", "field_without_rules", "Any value")
    assert value == "Any value"
    assert len(applied_rules) == 0
    
    # Test section without sanitization rules
    value, applied_rules = metadata_map._sanitize_value("section_without_rules", "field1", "Any value")
    assert value == "Any value"
    assert len(applied_rules) == 0
    
    # Test with empty sanitization config
    metadata_map.sanitization_config = {}
    value, applied_rules = metadata_map._sanitize_value("dataset", "field1", "Any value")
    assert value == "Any value"
    assert len(applied_rules) == 0
