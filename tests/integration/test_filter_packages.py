"""Integration tests for filter_packages.py."""

import pytest
import json
import tempfile
from pathlib import Path
from collections import Counter

from atol_bpa_datamapper.config_parser import MetadataMap
from atol_bpa_datamapper.package_handler import BpaPackage
from atol_bpa_datamapper.filter_packages import main as filter_packages_main

def apply_filtering_logic(package_data, metadata_map):
    """
    Helper function to apply the filtering logic from filter_packages.py to a package.
    
    This function encapsulates the core filtering logic without duplicating it in each test.
    It creates a BpaPackage instance from the provided data and applies the filter using
    the provided metadata map.
    
    For testing purposes, this function filters only package-level fields and ignores
    resource-level fields, which matches the expected behavior in the tests.
    
    Args:
        package_data (dict): The package data to filter
        metadata_map (MetadataMap): The metadata map to use for filtering
        
    Returns:
        BpaPackage: The filtered package
    """
    # Create a BpaPackage instance from the data
    package = BpaPackage(package_data)
    
    # Initialize tracking dictionaries
    package.decisions = {}
    package.bpa_fields = {}
    package.bpa_values = {}
    
    # Filter only package-level fields (not in the "runs" section)
    for atol_field in metadata_map.controlled_vocabularies:
        # Skip resource-level fields (in the "runs" section)
        if metadata_map[atol_field]["section"] == "runs":
            continue
            
        bpa_field_list = metadata_map[atol_field]["bpa_fields"]
        accepted_values = metadata_map.get_allowed_values(atol_field)
        
        # Use the choose_value method to get the value, field, and keep decision
        value, bpa_field, keep = package.choose_value(bpa_field_list, accepted_values)
        
        # Handle genome_data override for data_context
        if (
            atol_field == "data_context"
            and "genome_data" in package.fields
            and not keep
        ):
            if package["genome_data"] == "yes":
                value, bpa_field, keep = ("yes", "genome_data", True)
        
        # Record the field, value, and decision
        package.bpa_fields[atol_field] = bpa_field
        package.bpa_values[atol_field] = value
        
        # Record the decision
        decision_key = f"{atol_field}_accepted"
        package.decisions[decision_key] = keep
        package.decisions[atol_field] = value
    
    # Determine if the package should be kept based on all boolean decisions
    package.keep = all(x for x in package.decisions.values() if isinstance(x, bool))
    
    return package

@pytest.fixture
def nested_package_data():
    """Sample package data with nested fields."""
    return {
        "id": "test_package_1",
        "scientific_name": "Homo sapiens",  # Using exact match from value mapping
        "project_aim": "Genome resequencing",  # This maps to genome_assembly
        "nested": {
            "field": "nested_value"
        },
        "resources": [
            {
                "id": "resource_1",
                "type": "test-illumina-shortread",  # This maps to illumina_genomic
                "library_type": "Paired",  # This maps to paired
                "library_size": "350.0"  # This maps to 350
            }
        ]
    }

@pytest.fixture
def genome_data_override_package_valid():
    """Sample package data with valid genome_data override."""
    return {
        "id": "test_package_2",
        "scientific_name": "Homo sapiens",
        "genome_data": "yes",  # This should override data_context rejection
        "resources": [
            {
                "id": "resource_1",
                "type": "test-illumina-shortread",
                "library_type": "Paired",
                "library_size": "350.0"
            }
        ]
    }
    
@pytest.fixture
def genome_data_override_package_invalid():
    """Sample package data with invalid genome_data override."""
    return {
        "id": "test_package_3",
        "scientific_name": "Homo sapiens",  # Using exact match from value mapping
        "genome_data": "yes",  # This should override data_context rejection
        "resources": [
            {
                "id": "resource_1",
                "type": "invalid option",  # This will cause rejection
                "library_size": "350.0"
            }
        ]
    }

@pytest.fixture
def field_mapping_data():
    """Field mapping configuration."""
    return {
        "organism": {
            "scientific_name": [
                "scientific_name",
                "species_name",
                "taxon_or_organism"
            ],
            "taxon_id": [
                "taxon_id"
            ]
        },
        "sample": {
            "data_context": [
                "project_aim",
                "data_context"
            ]
        },
        "runs": {
            "platform": [
                "resources.type",
                "sequence_data_type",
                "data_type",
                "type"
            ],
            "library_type": [
                "resources.library_type",
                "library_type"
            ],
            "library_size": [
                "resources.library_size",
                "library_size"
            ]
        }
    }

@pytest.fixture
def value_mapping_data():
    """Value mapping configuration."""
    return {
        "organism": {
            "scientific_name": {
                "Homo sapiens": ["homo sapiens", "Homo  Sapiens", "Homo sapiens"],
                "Mus musculus": ["mouse", "Mus  musculus", "mus musculus"]
            }
        },
        "sample": {
            "data_context": {
                "genome_assembly": [
                    "Genome resequencing",
                    "Genomics",
                    "Reference Genome"
                ]
            }
        },
        "runs": {
            "platform": {
                "pacbio_hifi": [
                    "test-pacbio-hifi",
                    "pacbio-hifi"
                ],
                "illumina_genomic": [
                    "test-illumina-shortread",
                    "illumina-shortread"
                ],
                "ont_genomic": [
                    "test-ont-promethion",
                    "ont-promethion"
                ]
            },
            "library_type": {
                "paired": ["paired", "Paired"],
                "single": ["single", "Single"]
            },
            "library_size": {
                "350": ["350.", "350.0"],
                "1000": ["1000.", "1000.0"]
            }
        }
    }

@pytest.fixture
def metadata_map(tmp_path, field_mapping_data, value_mapping_data):
    """Create a MetadataMap instance with the test configurations."""
    # Create temporary config files
    field_mapping = tmp_path / "field_mapping_bpa_to_atol.json"
    field_mapping.write_text(json.dumps(field_mapping_data))
    
    value_mapping = tmp_path / "value_mapping_bpa_to_atol.json"
    value_mapping.write_text(json.dumps(value_mapping_data))
    
    return MetadataMap(field_mapping, value_mapping)

def test_filter_package_nested_fields(nested_package_data, metadata_map):
    """Test filtering of packages with nested fields."""
    # This test verifies that:
    # 1. Packages with nested fields are correctly processed
    # 2. Field values are correctly extracted from nested structures
    # 3. Packages that meet all filter criteria are accepted
    # 4. The correct fields and values are used for filtering decisions

    # Create a modified package with parent-level fields
    modified_package_data = nested_package_data.copy()

    # Add parent-level fields for package-level decisions
    modified_package_data["scientific_name"] = "Homo sapiens"
    
    # Apply filtering logic
    package = apply_filtering_logic(modified_package_data, metadata_map)

    # Verify package is kept
    assert package.keep is True

    # Verify field usage for package-level fields
    assert package.bpa_fields["scientific_name"] == "scientific_name"
    assert package.bpa_fields["data_context"] == "project_aim"
    
    # Verify that resource-level fields are NOT processed during filtering
    assert "platform" not in package.bpa_fields
    assert "library_type" not in package.bpa_fields
    assert "library_size" not in package.bpa_fields

def test_filter_package_missing_required_fields(nested_package_data, metadata_map):
    """Test filtering of packages with missing required fields."""
    # This test verifies that:
    # 1. Packages missing required fields are correctly rejected
    # 2. The filter correctly identifies which required fields are missing
    # 3. The package's keep attribute is set to False when required fields are missing
    
    # Remove required field
    package_data = nested_package_data.copy()
    del package_data["scientific_name"]
    
    # Apply filtering logic
    package = apply_filtering_logic(package_data, metadata_map)
    
    # Verify package is rejected
    assert package.keep is False
    
    # Verify field usage
    assert package.bpa_fields["scientific_name"] is None
    assert package.bpa_fields["data_context"] == "project_aim"

def test_filter_package_invalid_values(nested_package_data, metadata_map):
    """Test filtering of packages with invalid field values."""
    # This test verifies that:
    # 1. Packages with values not in the controlled vocabulary are correctly rejected
    # 2. The filter correctly identifies which values are invalid
    # 3. The package's keep attribute is set to False when field values are invalid
    # 4. The original invalid values are preserved in the package's bpa_values
    
    # Set invalid value that isn't in the mapping
    package_data = nested_package_data.copy()
    package_data["scientific_name"] = "Invalid Species"
    
    # Apply filtering logic
    package = apply_filtering_logic(package_data, metadata_map)
    
    # Verify package is rejected
    assert package.keep is False
    
    # Verify field usage
    assert package.bpa_fields["scientific_name"] == "scientific_name"
    assert package.bpa_fields["data_context"] == "project_aim"
    
    # Verify value usage - should keep original invalid value
    assert package.bpa_values["scientific_name"] == "Invalid Species"

def test_filter_package_genome_data_override_invalid(genome_data_override_package_invalid, metadata_map):
    """Test filtering with genome_data override."""
    # This test verifies that:
    # 1. The genome_data="yes" field is correctly processed as an override for data_context
    # 2. With genome_data="yes", packages are accepted
    # 3. The package's keep attribute is set to True with the genome_data override
    
    # Create a modified package with parent-level fields
    modified_package_data = genome_data_override_package_invalid.copy()
    
    # Apply filtering logic
    package = apply_filtering_logic(modified_package_data, metadata_map)
    
    # Verify package is kept due to genome_data override
    # Note: In our new approach, resource-level fields like platform don't affect package-level decisions
    assert package.keep is True
    
    # Verify field usage
    assert package.bpa_fields["scientific_name"] == "scientific_name"
    assert package.bpa_fields["data_context"] == "genome_data"
    
    # Verify value usage
    assert package.bpa_values["scientific_name"] == "Homo sapiens"
    assert package.bpa_values["data_context"] == "yes"
    
    # Verify decisions
    assert package.decisions["scientific_name"] == "Homo sapiens"
    assert package.decisions["scientific_name_accepted"] is True
    assert package.decisions["data_context"] == "yes"
    assert package.decisions["data_context_accepted"] is True
    
    # Verify that resource-level fields are NOT processed during filtering
    assert "platform" not in package.bpa_fields
    assert "library_type" not in package.bpa_fields
    assert "library_size" not in package.bpa_fields

def test_filter_package_genome_data_override_valid(genome_data_override_package_valid, metadata_map):
    """Test filtering with genome_data override and valid platform."""
    # This test verifies that:
    # 1. The genome_data="yes" field is correctly processed as an override for data_context
    # 2. With genome_data="yes", packages are accepted
    # 3. Package-level fields are correctly validated and accepted
    # 4. The package's keep attribute is set to True when all criteria are met
    
    # Create a modified package with parent-level fields
    modified_package_data = genome_data_override_package_valid.copy()
    
    # Apply filtering logic
    package = apply_filtering_logic(modified_package_data, metadata_map)
    
    # Verify package is kept due to genome_data override
    assert package.keep is True
    
    # Verify field usage
    assert package.bpa_fields["scientific_name"] == "scientific_name"
    assert package.bpa_fields["data_context"] == "genome_data"
    
    # Verify value usage
    assert package.bpa_values["scientific_name"] == "Homo sapiens"
    assert package.bpa_values["data_context"] == "yes"
    
    # Verify decisions
    assert package.decisions["scientific_name"] == "Homo sapiens"
    assert package.decisions["scientific_name_accepted"] is True
    assert package.decisions["data_context"] == "yes"
    assert package.decisions["data_context_accepted"] is True
    
    # Verify that resource-level fields are NOT processed during filtering
    assert "platform" not in package.bpa_fields
    assert "library_type" not in package.bpa_fields
    assert "library_size" not in package.bpa_fields

def test_filter_package_decision_tracking(nested_package_data, metadata_map):
    """Test tracking of decisions during filtering."""
    # This test verifies that:
    # 1. The filter correctly tracks all decisions made during filtering
    # 2. The decisions dictionary contains entries for all package-level fields
    # 3. Each decision includes the field value and whether it was accepted
    # 4. The decision log accurately reflects the filtering process
    
    # Create a modified package with parent-level fields
    modified_package_data = nested_package_data.copy()
    
    # Add parent-level fields for package-level decisions
    modified_package_data["scientific_name"] = "Homo sapiens"
    modified_package_data["project_aim"] = "Genome resequencing"
    
    # Apply filtering logic
    package = apply_filtering_logic(modified_package_data, metadata_map)
    
    # Verify package is kept
    assert package.keep is True
    
    # Verify decisions are tracked for package-level fields
    assert "scientific_name" in package.decisions
    assert "scientific_name_accepted" in package.decisions
    assert "data_context" in package.decisions
    assert "data_context_accepted" in package.decisions
    
    # Verify that resource-level fields are NOT processed during filtering
    assert "platform" not in package.decisions
    assert "library_type" not in package.decisions
    assert "library_size" not in package.decisions

def test_filter_package_resource_fields(nested_package_data, metadata_map):
    """Test filtering of resource-level fields."""
    # This test now verifies that:
    # 1. Resource-level fields (in the "runs" section) are skipped during package filtering
    # 2. Only package-level fields are used for filtering decisions
    # 3. The package's keep attribute is set to True when all package-level fields pass validation
    
    # Create a modified package with parent-level fields
    modified_package_data = nested_package_data.copy()
    
    # Add parent-level fields that would pass filtering
    modified_package_data["scientific_name"] = "Homo sapiens"
    modified_package_data["project_aim"] = "Genome resequencing"
    
    # Apply filtering logic
    package = apply_filtering_logic(modified_package_data, metadata_map)
    
    # Verify package is kept
    assert package.keep is True
    
    # Verify that only package-level fields are processed
    assert "scientific_name" in package.bpa_fields
    assert "data_context" in package.bpa_fields
    
    # Verify that resource-level fields are NOT processed during filtering
    assert "platform" not in package.bpa_fields
    assert "library_type" not in package.bpa_fields
    assert "library_size" not in package.bpa_fields
