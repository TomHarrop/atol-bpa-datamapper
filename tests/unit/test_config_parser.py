"""Unit tests for config_parser.py."""

import pytest
from atol_bpa_datamapper.config_parser import MetadataMap


def test_metadata_map_initialization_unit(mocker):
    """Unit test for MetadataMap initialization."""
    # Mock file operations with correct data format
    field_mapping = '{"section": {"field": ["value"]}}'
    value_mapping = '{"section": {"field": {"mapped": ["original"]}}}'
    mock_open = mocker.mock_open()
    mock_open.side_effect = [
        mocker.mock_open(read_data=field_mapping).return_value,
        mocker.mock_open(read_data=value_mapping).return_value
    ]
    mocker.patch('builtins.open', mock_open)
    
    metadata_map = MetadataMap("field.json", "value.json")
    
    assert metadata_map.expected_fields == ["field"]
    assert metadata_map.metadata_sections == ["section"]
    assert metadata_map["field"]["section"] == "section"
    assert metadata_map["field"]["bpa_fields"] == ["value"]


def test_get_atol_section_unit(mocker):
    """Unit test for get_atol_section method."""
    # Mock file operations with correct data format
    field_mapping = '{"section": {"field": ["value"]}}'
    value_mapping = '{"section": {"field": {"mapped": ["original"]}}}'
    mock_open = mocker.mock_open()
    mock_open.side_effect = [
        mocker.mock_open(read_data=field_mapping).return_value,
        mocker.mock_open(read_data=value_mapping).return_value
    ]
    mocker.patch('builtins.open', mock_open)
    
    metadata_map = MetadataMap("field.json", "value.json")
    
    assert metadata_map.get_atol_section("field") == "section"
    with pytest.raises(KeyError):
        metadata_map.get_atol_section("nonexistent")


def test_get_bpa_fields_unit(mocker):
    """Unit test for get_bpa_fields method."""
    # Mock file operations with correct data format
    field_mapping = '{"section": {"field": ["value1", "value2"]}}'
    value_mapping = '{"section": {"field": {"mapped": ["original"]}}}'
    mock_open = mocker.mock_open()
    mock_open.side_effect = [
        mocker.mock_open(read_data=field_mapping).return_value,
        mocker.mock_open(read_data=value_mapping).return_value
    ]
    mocker.patch('builtins.open', mock_open)
    
    metadata_map = MetadataMap("field.json", "value.json")
    
    assert metadata_map.get_bpa_fields("field") == ["value1", "value2"]
    with pytest.raises(KeyError):
        metadata_map.get_bpa_fields("nonexistent")


def test_map_value_unit(mocker):
    """Unit test for map_value method."""
    # Mock file operations with correct data format
    field_mapping = '{"section": {"field": ["value"]}}'
    value_mapping = '{"section": {"field": {"mapped": ["original"]}}}'
    mock_open = mocker.mock_open()
    mock_open.side_effect = [
        mocker.mock_open(read_data=field_mapping).return_value,
        mocker.mock_open(read_data=value_mapping).return_value
    ]
    mocker.patch('builtins.open', mock_open)
    
    metadata_map = MetadataMap("field.json", "value.json")
    
    # Test mapping with controlled vocabulary
    assert metadata_map.map_value("field", "original") == "mapped"
    
    # Test mapping with unknown value
    with pytest.raises(KeyError):
        metadata_map.map_value("field", "unknown")
    
    # Test mapping without controlled vocabulary
    assert metadata_map.map_value("other", "value") == "value"

