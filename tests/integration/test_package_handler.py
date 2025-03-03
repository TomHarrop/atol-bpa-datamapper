"""Integration tests for package_handler.py."""

import pytest
from atol_bpa_datamapper.package_handler import BpaPackage
from atol_bpa_datamapper.config_parser import MetadataMap


def test_bpa_package_initialization(sample_bpa_package):
    """Test BpaPackage initialization with sample data."""
    package = BpaPackage(sample_bpa_package)
    assert package.id == sample_bpa_package["id"]
    assert sorted(package.fields) == sorted(sample_bpa_package.keys())
    assert package.resource_ids == [r["id"] for r in sample_bpa_package["resources"]]


def test_get_resource_value(sample_bpa_package):
    """Test getting values from resources."""
    package = BpaPackage(sample_bpa_package)
    resource = sample_bpa_package["resources"][0]
    
    # Test getting resource-level field
    assert package.get_resource_value(resource, "type") == "illumina"
    assert package.get_resource_value(resource, "library_name") == "lib_001"
    
    # Test getting non-existent field
    assert package.get_resource_value(resource, "nonexistent") is None


def test_map_metadata_basic(sample_bpa_package, field_mapping_file, value_mapping_file):
    """Test basic metadata mapping."""
    package = BpaPackage(sample_bpa_package)
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    package.map_metadata(metadata_map)
    
    # Check that metadata was mapped correctly
    assert isinstance(package.mapped_metadata, dict)
    assert "organism" in package.mapped_metadata
    assert "reads" in package.mapped_metadata


def test_map_metadata_empty_resources(field_mapping_file, value_mapping_file):
    """Test mapping package with no resources."""
    package_data = {
        "id": "empty_package",
        "resources": [],
        "scientific_name": "Undetermined species",  # Use a value that exists in the mapping
        "project_aim": "genome_assembly"  # Use a value that exists in the mapping
    }
    package = BpaPackage(package_data)
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    package.map_metadata(metadata_map)
    assert package.mapped_metadata["reads"] == []


def test_map_metadata_missing_fields(sample_bpa_package, field_mapping_file, value_mapping_file):
    """Test mapping package with missing fields."""
    # Remove some fields that are required for mapping
    del sample_bpa_package["scientific_name"]
    
    package = BpaPackage(sample_bpa_package)
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    # This should not raise a KeyError
    # Not implemented but perhaps we should notraise a KeyError if one of the required fields is missing - and instead just log the output
    package.map_metadata(metadata_map)
    
    # Check that missing fields are mapped to None
    assert "organism" in package.mapped_metadata
    assert "scientific_name" in package.mapped_metadata["organism"]
    assert package.mapped_metadata["organism"]["scientific_name"] is None
    
    # Check that the mapping log reflects the missing field
    assert any(
        log["atol_field"] == "scientific_name" and
        log["value"] is None and
        log["mapped_value"] is None
        for log in package.mapping_log
    )


def test_map_metadata_value_mapping(sample_bpa_package, field_mapping_file, value_mapping_file):
    """Test value mapping during metadata mapping."""
    package = BpaPackage(sample_bpa_package)
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    package.map_metadata(metadata_map)
    
    # Check that values were mapped correctly
    assert package.mapped_metadata["reads"][0]["platform"] == "illumina_genomic"


def test_map_metadata_key_error(sample_bpa_package, field_mapping_file, value_mapping_file):
    """Test that KeyError is handled correctly during mapping."""
    # Modify a field to have a value that doesn't exist in the mapping
    sample_bpa_package["scientific_name"] = "Unknown Species"
    
    package = BpaPackage(sample_bpa_package)
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    # This should raise a KeyError since the value is not in the mapping
    with pytest.raises(KeyError):
        package.map_metadata(metadata_map)


def test_filter_basic(sample_bpa_package, field_mapping_file, value_mapping_file):
    """Test basic filtering of a BPA package."""
    package = BpaPackage(sample_bpa_package)
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    package.filter(metadata_map)
    
    # Check that filtering decisions were made
    assert isinstance(package.decisions, dict)
    assert isinstance(package.bpa_fields, dict)
    assert isinstance(package.bpa_values, dict)
    assert isinstance(package.keep, bool)
    
    # Check that decisions were made for controlled vocabulary fields
    for field in metadata_map.controlled_vocabularies:
        assert f"{field}_accepted" in package.decisions
        assert field in package.decisions
        assert field in package.bpa_fields
        assert field in package.bpa_values


def test_filter_genome_data_override(field_mapping_file, value_mapping_file):
    """Test the special case for genome_data field."""
    package_data = {
        "id": "test_package",
        "genome_data": "yes",
        "resources": []
    }
    package = BpaPackage(package_data)
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    package.filter(metadata_map)
    
    # Check that the genome_data override was applied
    assert package.decisions["data_context_accepted"] is True
    assert package.decisions["data_context"] == "yes"
    assert package.bpa_fields["data_context"] == "genome_data"
    assert package.bpa_values["data_context"] == "yes"


def test_map_metadata_sections(sample_bpa_package, field_mapping_file, value_mapping_file):
    """Test that metadata is mapped to correct sections."""
    package = BpaPackage(sample_bpa_package)
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    package.map_metadata(metadata_map)
    
    # Check that all sections are present
    for section in metadata_map.metadata_sections:
        assert section in package.mapped_metadata
    
    # Check that reads section is a list and others are dicts
    assert isinstance(package.mapped_metadata["reads"], list)
    for section in metadata_map.metadata_sections:
        if section != "reads":
            assert isinstance(package.mapped_metadata[section], dict)


def test_map_metadata_field_mapping(sample_bpa_package, field_mapping_file, value_mapping_file):
    """Test that field mapping is recorded correctly."""
    package = BpaPackage(sample_bpa_package)
    metadata_map = MetadataMap(field_mapping_file, value_mapping_file)
    
    package.map_metadata(metadata_map)
    
    # Check that field mapping is recorded
    assert isinstance(package.field_mapping, dict)
    for atol_field in metadata_map.expected_fields:
        if metadata_map.get_atol_section(atol_field) != "reads":
            assert atol_field in package.field_mapping
            
    # Check that mapping log is recorded
    assert isinstance(package.mapping_log, list)
    for entry in package.mapping_log:
        assert "atol_field" in entry
        assert "bpa_field" in entry
        assert "value" in entry
        assert "mapped_value" in entry
