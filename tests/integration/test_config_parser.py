"""Integration tests for config_parser.py."""

import pytest
from atol_bpa_datamapper.config_parser import MetadataMap


def test_metadata_map_initialization(field_mapping_file, value_mapping_file):
    """Test MetadataMap initialization with mapping files."""
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    # Check that fields were loaded correctly
    assert isinstance(metadata_map.expected_fields, list)
    assert isinstance(metadata_map.metadata_sections, list)
    assert isinstance(metadata_map.controlled_vocabularies, list)


def test_get_atol_section(field_mapping_file, value_mapping_file):
    """Test getting AToL section for a field."""
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    # Test getting section for existing field
    assert metadata_map.get_atol_section("scientific_name") == "organism"
    
    # Test getting section for non-existent field
    with pytest.raises(KeyError):
        metadata_map.get_atol_section("nonexistent")


def test_get_bpa_fields(field_mapping_file, value_mapping_file):
    """Test getting BPA fields for an AToL field."""
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    # Test getting fields for existing field
    bpa_fields = metadata_map.get_bpa_fields("scientific_name")
    assert isinstance(bpa_fields, list)
    assert "scientific_name" in bpa_fields
    
    # Test getting fields for non-existent field
    with pytest.raises(KeyError):
        metadata_map.get_bpa_fields("nonexistent")


def test_get_allowed_values(field_mapping_file, value_mapping_file):
    """Test getting allowed values for a field."""
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    # Test field with controlled vocabulary
    allowed_values = metadata_map.get_allowed_values("scientific_name")
    assert isinstance(allowed_values, list)
    assert "Undetermined species" in allowed_values
    
    # Test field without controlled vocabulary
    assert metadata_map.get_allowed_values("nonexistent") is None


def test_map_value(field_mapping_file, value_mapping_file):
    """Test mapping values using controlled vocabulary."""
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    # Test mapping with controlled vocabulary
    assert metadata_map.map_value("scientific_name", "Undetermined species") == "Undetermined sp"
    
    # Test mapping with unknown value
    with pytest.raises(KeyError):
        metadata_map.map_value("scientific_name", "Unknown Species")
    
    # Test mapping without controlled vocabulary
    value = "test value"
    assert metadata_map.map_value("nonexistent", value) == value


def test_metadata_map_with_empty_mappings(empty_mapping_file):
    """Test MetadataMap behavior with empty mappings."""
    metadata_map = MetadataMap(empty_mapping_file, empty_mapping_file)
    
    assert metadata_map.metadata_sections == []
    assert metadata_map.expected_fields == []
    
    # Test that methods return empty values for nonexistent fields
    assert metadata_map.get_allowed_values("any") is None
    assert metadata_map.map_value("any", "value") == "value"


def test_metadata_map_with_partial_value_mapping(field_mapping_file, empty_mapping_file):
    """Test MetadataMap with field mapping but no value mapping."""
    metadata_map = MetadataMap(field_mapping_file, empty_mapping_file)
    
    # Check that fields are loaded but no controlled vocabularies
    assert len(metadata_map.expected_fields) > 0
    assert len(metadata_map.controlled_vocabularies) == 0
    
    # Test that values are kept as-is
    value = "test value"
    assert metadata_map.map_value("scientific_name", value) == value
