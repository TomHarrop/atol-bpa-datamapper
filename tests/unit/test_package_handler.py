"""Unit tests for package_handler.py."""

import pytest
from atol_bpa_datamapper.package_handler import BpaPackage


def test_bpa_package_initialization_unit():
    """Unit test for BpaPackage initialization."""
    package_data = {
        "id": "test_package",
        "resources": [
            {"id": "resource1", "type": "illumina"},
            {"id": "resource2", "type": "pacbio"}
        ]
    }
    package = BpaPackage(package_data)
    
    assert package.id == "test_package"
    assert package.fields == ["id", "resources"]
    assert package.resource_ids == ["resource1", "resource2"]


def test_get_resource_value_unit():
    """Unit test for get_resource_value method."""
    package_data = {
        "id": "test_package",
        "resources": [
            {
                "id": "resource1",
                "type": "illumina",
                "library_name": "lib_001"
            }
        ]
    }
    package = BpaPackage(package_data)
    resource = package_data["resources"][0]
    
    assert package.get_resource_value(resource, "type") == "illumina"
    assert package.get_resource_value(resource, "library_name") == "lib_001"
    assert package.get_resource_value(resource, "nonexistent") is None


def test_filter_unit(mocker):
    """Unit test for filter method."""
    # Create a mock MetadataMap with subscriptable behavior
    mock_metadata_map = mocker.Mock()
    mock_metadata_map.controlled_vocabularies = ["scientific_name"]
    mock_metadata_map.get_allowed_values.return_value = ["Undetermined sp"]
    mock_metadata_map.get_bpa_fields.return_value = ["scientific_name"]
    
    # Mock the __getitem__ method
    mock_field_data = {
        "scientific_name": {
            "bpa_fields": ["scientific_name"],
            "value_mapping": {"Undetermined sp": "Undetermined sp"}
        }
    }
    mock_metadata_map.__getitem__ = lambda self, key: mock_field_data[key]
    
    # Create a test package
    package_data = {
        "id": "test_package",
        "scientific_name": "Undetermined sp",
        "resources": []
    }
    package = BpaPackage(package_data)
    
    # Test filtering
    package.filter(mock_metadata_map)
    
    assert package.decisions["scientific_name_accepted"] is True
    assert package.keep is True


def test_map_metadata_unit(mocker):
    """Unit test for map_metadata method."""
    # Create a mock MetadataMap with subscriptable behavior
    mock_metadata_map = mocker.Mock()
    mock_metadata_map.metadata_sections = ["organism"]
    mock_metadata_map.expected_fields = ["scientific_name"]
    mock_metadata_map.get_atol_section.return_value = "organism"
    mock_metadata_map.get_bpa_fields.return_value = ["scientific_name"]
    mock_metadata_map.map_value.return_value = "Undetermined sp"
    mock_metadata_map.get_allowed_values.return_value = ["Undetermined sp"]
    
    # Create a test package
    package_data = {
        "id": "test_package",
        "scientific_name": "Undetermined species",
        "resources": []
    }
    package = BpaPackage(package_data)
    
    # Test mapping
    package.map_metadata(mock_metadata_map)
    
    assert package.mapped_metadata["organism"]["scientific_name"] == "Undetermined sp"
    assert isinstance(package.mapping_log, list)
    assert len(package.mapping_log) > 0
