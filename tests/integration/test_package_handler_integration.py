"""Integration tests for package_handler.py."""

import os
import json
import pytest
from atol_bpa_datamapper.package_handler import BpaPackage, get_nested_value
from atol_bpa_datamapper.config_parser import MetadataMap


@pytest.fixture
def test_fixtures_dir():
    """Return the path to the test fixtures directory."""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "fixtures")


@pytest.fixture
def package_data_file(test_fixtures_dir):
    """Return the path to the test package data file."""
    return os.path.join(test_fixtures_dir, "test_package_data.json")


@pytest.fixture
def field_mapping_file(test_fixtures_dir):
    """Return the path to the test field mapping file."""
    return os.path.join(test_fixtures_dir, "test_field_mapping.json")


@pytest.fixture
def value_mapping_file(test_fixtures_dir):
    """Return the path to the test value mapping file."""
    return os.path.join(test_fixtures_dir, "test_value_mapping.json")


@pytest.fixture
def package_data(package_data_file):
    """Load the test package data."""
    with open(package_data_file, "r") as f:
        return json.load(f)


@pytest.fixture
def bpa_package(package_data):
    """Create a BpaPackage instance for testing."""
    return BpaPackage(package_data)


@pytest.fixture
def metadata_map(field_mapping_file, value_mapping_file):
    """Create a MetadataMap instance for testing."""
    return MetadataMap(field_mapping_file, value_mapping_file)


def test_bpa_package_initialization(bpa_package, package_data):
    """Test that the BpaPackage is initialized correctly."""
    # Check that the package data is stored correctly
    assert bpa_package["id"] == package_data["id"]
    
    # Check that the fields are extracted
    assert hasattr(bpa_package, "fields")
    assert isinstance(bpa_package.fields, list)
    assert "scientific_name" in bpa_package.fields
    
    # Check that the resource IDs are extracted
    assert hasattr(bpa_package, "resource_ids")
    assert isinstance(bpa_package.resource_ids, list)
    assert len(bpa_package.resource_ids) == len(package_data["resources"])


def test_filter_package(bpa_package, metadata_map):
    """Test the filter method of BpaPackage with deterministic assertions."""
    # Filter the package
    bpa_package.filter(metadata_map)
    
    # Check that the decisions are made correctly for specific fields
    assert bpa_package.decisions["scientific_name_accepted"] is True
    assert bpa_package.decisions["data_context_accepted"] is True
    
    # Check that the values are extracted correctly
    assert bpa_package.bpa_values["scientific_name"] == "Homo sapiens"
    assert bpa_package.bpa_values["data_context"] == "Genome resequencing"
    
    # Check that the fields used are recorded
    assert bpa_package.bpa_fields["scientific_name"] == "scientific_name"
    assert bpa_package.bpa_fields["data_context"] == "project_aim"
    
    # Verify that the keep attribute is determined by all boolean decisions
    expected_keep_value = all(
        decision for field, decision in bpa_package.decisions.items() 
        if isinstance(decision, bool) and field.endswith("_accepted")
    )
    assert bpa_package.keep == expected_keep_value
    
    # Verify that decisions dictionary contains entries for all controlled vocabulary fields
    for field in metadata_map.controlled_vocabularies:
        decision_key = f"{field}_accepted"
        assert decision_key in bpa_package.decisions, f"Missing decision for {field}"
        assert field in bpa_package.bpa_values, f"Missing value for {field}"


def test_map_metadata(bpa_package, metadata_map, package_data, value_mapping_file):
    """Test the map_metadata method of BpaPackage with values derived from fixtures."""
    # Load the value mapping to derive expected values
    with open(value_mapping_file, "r") as f:
        value_mapping = json.load(f)
    
    # Map the metadata
    mapped_metadata = bpa_package.map_metadata(metadata_map)
    
    # Check that all sections are present
    assert "organism" in mapped_metadata
    assert "sample" in mapped_metadata
    assert "runs" in mapped_metadata
    assert "dataset" in mapped_metadata
    
    # Check organism section - derive expected value from value mapping
    expected_scientific_name = package_data["scientific_name"]
    # Map through value mapping if needed
    if "organism" in value_mapping and "scientific_name" in value_mapping["organism"]:
        for mapped_value, original_values in value_mapping["organism"]["scientific_name"].items():
            if expected_scientific_name in original_values:
                expected_scientific_name = mapped_value
                break
    assert mapped_metadata["organism"]["scientific_name"] == expected_scientific_name
    
    # Check sample section - derive expected value from value mapping
    expected_data_context = package_data["project_aim"]
    # Map through value mapping if needed
    if "sample" in value_mapping and "data_context" in value_mapping["sample"]:
        for mapped_value, original_values in value_mapping["sample"]["data_context"].items():
            if expected_data_context in original_values:
                expected_data_context = mapped_value
                break
    assert mapped_metadata["sample"]["data_context"] == expected_data_context
    
    # Check runs section (resource-level) - verify correct number of resources
    assert len(mapped_metadata["runs"]) == len(package_data["resources"])
    
    # Check each resource
    for i, resource in enumerate(package_data["resources"]):
        mapped_resource = mapped_metadata["runs"][i]
        
        # Check platform - derive expected value from value mapping
        expected_platform = resource["type"]
        # Map through value mapping if needed
        if "runs" in value_mapping and "platform" in value_mapping["runs"]:
            for mapped_value, original_values in value_mapping["runs"]["platform"].items():
                if expected_platform in original_values:
                    expected_platform = mapped_value
                    break
        assert mapped_resource["platform"] == expected_platform
        
        # Check other resource fields
        assert mapped_resource["library_type"] == resource["library_type"]
        assert mapped_resource["library_size"] == resource["library_size"]
        assert mapped_resource["file_name"] == resource["name"]
        assert mapped_resource["file_checksum"] == resource["md5"]
        assert mapped_resource["file_format"] == resource["format"]
        assert mapped_resource["resource_id"] == resource["id"]
    
    # Check dataset section
    assert mapped_metadata["dataset"]["bpa_id"] == package_data["id"]
    
    # Check mapping log
    assert len(bpa_package.mapping_log) > 0
    for entry in bpa_package.mapping_log:
        assert "atol_field" in entry
        assert "bpa_field" in entry
        assert "value" in entry
        assert "mapped_value" in entry
        
        # Resource-level fields should have resource_id
        if entry["atol_field"] in ["platform", "library_type", "library_size", "file_name", 
                                 "file_checksum", "file_format", "resource_id"]:
            assert "resource_id" in entry


@pytest.mark.parametrize("fields_to_check, accepted_values, expected_value, expected_field, expected_keep", [
    (["scientific_name", "species_name"], None, "Homo sapiens", "scientific_name", True),
    (["project_aim", "data_context"], None, "Genome resequencing", "project_aim", True),
    (["non_existent1", "non_existent2"], None, None, None, False),
    (["scientific_name"], ["Homo sapiens"], "Homo sapiens", "scientific_name", True),
    (["scientific_name"], ["Unknown Species"], "Homo sapiens", "scientific_name", False),
])
def test_choose_value(bpa_package, fields_to_check, accepted_values, expected_value, expected_field, expected_keep):
    """Test the choose_value method with parameterized inputs."""
    value, field, keep = bpa_package.choose_value(fields_to_check, accepted_values)
    assert value == expected_value
    assert field == expected_field
    assert keep == expected_keep


@pytest.mark.parametrize("fields_to_check, accepted_values, resource_index, expected_value, expected_field, expected_keep", [
    (["type", "platform"], None, 0, "illumina-shortread", "type", True),
    (["library_type"], None, 0, "paired", "library_type", True),
    (["non_existent1", "non_existent2"], None, 0, None, None, False),
    (["type"], ["illumina-shortread"], 0, "illumina-shortread", "type", True),
    (["type"], ["pacbio-hifi"], 0, "illumina-shortread", "type", False),
])
def test_choose_value_from_resource(bpa_package, fields_to_check, accepted_values, resource_index, expected_value, expected_field, expected_keep):
    """Test the choose_value_from_resource method with parameterized inputs."""
    resource = bpa_package["resources"][resource_index]
    value, field, keep = bpa_package.choose_value_from_resource(fields_to_check, accepted_values, resource)
    assert value == expected_value
    assert field == expected_field
    assert keep == expected_keep


@pytest.mark.parametrize("data, path, expected_value", [
    ({"scientific_name": "Homo sapiens"}, "scientific_name", "Homo sapiens"),
    ({"project_aim": "Genome resequencing"}, "project_aim", "Genome resequencing"),
    # Array indexing is not supported in the current implementation
    ({"resources": [{"type": "illumina genomic"}, {"type": "pacbio hifi"}]}, "resources", [{"type": "illumina genomic"}, {"type": "pacbio hifi"}]),
    ({"level1": {"level2": {"level3": "value"}}}, "level1.level2.level3", "value"),
    ({}, "non_existent_path", None),
])
def test_get_nested_value(data, path, expected_value):
    """Test the get_nested_value function with parameterized inputs."""
    value = get_nested_value(data, path)
    assert value == expected_value


def test_large_dataset_performance(metadata_map):
    """Test that the BpaPackage can handle large datasets efficiently."""
    import time
    
    # Create a large package with many resources
    large_package = {
        "id": "large-package-test",
        "scientific_name": "Homo sapiens",
        "project_aim": "Genome resequencing",
        "resources": []
    }
    
    # Add 100 resources to the package
    for i in range(100):
        large_package["resources"].append({
            "id": f"resource_{i}",
            "name": f"test_file_{i}.fastq.gz",
            "md5": f"checksum_{i}",
            "format": "FASTQ",
            "type": "illumina genomic",
            "library_type": "paired",
            "library_size": "350"
        })
    
    # Measure time to create and process the package
    start_time = time.time()
    
    # Create the package
    bpa_package = BpaPackage(large_package)
    
    # Filter the package
    bpa_package.filter(metadata_map)
    
    # Map the metadata
    mapped_metadata = bpa_package.map_metadata(metadata_map)
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    
    # Assert that the processing time is reasonable (adjust threshold as needed)
    # This is a baseline test - the actual threshold should be determined based on
    # real-world requirements and hardware capabilities
    assert elapsed_time < 2.0, f"Processing took too long: {elapsed_time:.2f} seconds"
    
    # Verify that all resources were processed
    assert len(mapped_metadata["runs"]) == 100
    
    # Verify that the first and last resources were mapped correctly
    assert mapped_metadata["runs"][0]["resource_id"] == "resource_0"
    assert mapped_metadata["runs"][99]["resource_id"] == "resource_99"
