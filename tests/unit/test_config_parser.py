"""Unit tests for config_parser.py."""

import pytest
import json
from unittest.mock import patch, mock_open

from atol_bpa_datamapper.config_parser import MetadataMap


def test_metadata_map_initialization():
    """Test MetadataMap initialization with mock files."""
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
                "new_value2": ["old_value2"]
            }
        },
        "organism": {
            "field2": {
                "new_value3": ["old_value3"]
            }
        }
    }
    
    # Mock the open function to return our test data
    with patch("builtins.open", mock_open()) as mock_file:
        # Configure the mock to return different content for different files
        mock_file.side_effect = [
            mock_open(read_data=json.dumps(field_mapping)).return_value,
            mock_open(read_data=json.dumps(value_mapping)).return_value
        ]
        
        metadata_map = MetadataMap("field.json", "value.json")
        
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
        
        # Test that the controlled vocabularies were set correctly
        assert set(metadata_map.controlled_vocabularies) == {"field1", "field2"}
        
        # Test that the metadata sections were set correctly
        assert set(metadata_map.metadata_sections) == {"dataset", "organism", "reads"}
        
        # Test that the expected fields were set correctly
        assert set(metadata_map.expected_fields) == {"field1", "field2", "field3"}


def test_get_allowed_values():
    """Test get_allowed_values method."""
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


def test_map_value():
    """Test map_value method."""
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
