import pytest
import json
from collections import Counter
from atol_bpa_datamapper.config_parser import MetadataMap
from atol_bpa_datamapper.package_handler import BpaPackage

@pytest.fixture
def nested_package_data():
    return {
        "id": "test_package_1",
        "scientific_name": "Homo  Sapiens",  # This maps to ["homo sapiens", "Homo  Sapiens"]
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
    return {
        "id": "test_package_2",
        "scientific_name": "Homo  sapiens",
        "genome_data": "yes",  # This should override data_context rejection
        "resources": [
            {
                "id": "resource_1",
                "type": "invalid option",
                "library_size": "350.0"
            }
        ]
    }

@pytest.fixture
def field_mapping_data():
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
        "reads": {
            "platform": [
                "resources.type",
                "sequence_data_type",
                "data_type"
            ],
            "library_type": [
                "resources.library_type"
            ],
            "library_size": [
                "resources.library_size"
            ]
        }
    }

@pytest.fixture
def value_mapping_data():
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
        "reads": {
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
    # Create temporary config files
    field_mapping = tmp_path / "field_mapping_bpa_to_atol.json"
    field_mapping.write_text(json.dumps(field_mapping_data))
    
    value_mapping = tmp_path / "value_mapping_bpa_to_atol.json"
    value_mapping.write_text(json.dumps(value_mapping_data))
    
    return MetadataMap(field_mapping, value_mapping)

def test_filter_package_nested_fields(nested_package_data, metadata_map):
    # Test filtering of packages with nested fields.
    package = BpaPackage(nested_package_data)
    
    # Filter package
    keep_package = package.filter(metadata_map)
    
    # Verify package is kept
    assert keep_package is True
    
    # Verify field usage
    assert package.bpa_fields["scientific_name"] == "scientific_name"
    assert package.bpa_fields["data_context"] == "project_aim"
    
    # Verify value usage and mapping
    assert package.bpa_values["scientific_name"] == "Homo  Sapiens"  # Original BPA value
    assert package.decisions["scientific_name"] == "Homo sapiens"   # Mapped AToL value
    assert package.bpa_values["data_context"] == "Genome resequencing"
    assert package.decisions["data_context"] == "genome_assembly"

def test_filter_package_missing_required_fields(nested_package_data, metadata_map):
    #Test filtering of packages with missing required fields.
    # Remove required field
    del nested_package_data["scientific_name"]
    package = BpaPackage(nested_package_data)
    
    # Filter package
    keep_package = package.filter(metadata_map)
    
    # Verify package is rejected
    assert keep_package is False
    
    # Verify field usage
    assert package.bpa_fields["scientific_name"] is None
    assert package.bpa_fields["data_context"] == "project_aim"



def test_filter_package_invalid_values(nested_package_data, metadata_map):
    #Test filtering of packages with invalid field values.
    # Set invalid value that isn't in the mapping
    nested_package_data["scientific_name"] = "Invalid Species"
    package = BpaPackage(nested_package_data)
    
    # Filter package
    keep_package = package.filter(metadata_map)
    
    # Verify package is rejected since value isn't in mapping
    assert keep_package is False
    
    # Verify field usage
    assert package.bpa_fields["scientific_name"] == "scientific_name"
    assert package.bpa_fields["data_context"] == "project_aim"
    
    # Verify value usage - should keep original invalid value
    assert package.bpa_values["scientific_name"] == "Invalid Species"
    assert package.decisions["scientific_name"] == "Invalid Species"

def test_filter_package_genome_data_override_invalid(genome_data_override_package_invalid, metadata_map):
    #Test the special case where genome_data: yes overrides data_context rejection.
    package = BpaPackage(genome_data_override_package_invalid)
    
    # Debug: print allowed values for each field
    for field in metadata_map.controlled_vocabularies:
        print(f"\nAllowed values for {field}:")
        print(metadata_map.get_allowed_values(field))
    # Debug: print allowed values for each field
    # Filter package
    keep_package = package.filter(metadata_map)
    print(f"\nBPA VALUESXX: {package.bpa_values}")
    print(f"\nBPA DECISIONSXX: {package.decisions}")
    
    # Verify package is kept despite invalid scientific_name
    assert keep_package is False


    
    # Verify field usage
    # TODO: dot notation not happening here
    assert package.bpa_fields["scientific_name"] == "scientific_name"
    assert package.bpa_fields["platform"] == "resources.type"
    assert package.bpa_fields["data_context"] == "genome_data"
    
    # Verify value usage
    assert package.bpa_values["scientific_name"] == "Homo  sapiens"
    assert package.bpa_values["platform"] == "invalid option"
    assert package.bpa_values["data_context"] == "yes"
    
    # Verify decisions
    assert package.decisions["platform"] == "invalid option"
    assert package.decisions["platform_accepted"] is False
    assert package.decisions["data_context"] == "genome_assembly"
    assert package.decisions["data_context_accepted"] is True


def test_filter_package_genome_data_override_valid(genome_data_override_package_valid, metadata_map):
    #Test the special case where genome_data: yes overrides data_context rejection.
    package = BpaPackage(genome_data_override_package_valid)
    
    # Debug: print allowed values for each field
    for field in metadata_map.controlled_vocabularies:
        print(f"\nAllowed values for {field}:")
        print(metadata_map.get_allowed_values(field))
    
    # Filter package
    keep_package = package.filter(metadata_map)
    
    # Verify package is kept despite invalid scientific_name
    assert keep_package is True
    
    # Verify field usage
    assert package.bpa_fields["scientific_name"] == "scientific_name"
    assert package.bpa_fields["platform"] == "resources.type"
    assert package.bpa_fields["data_context"] == "genome_data"
    
    # Verify value usage
    assert package.bpa_values["scientific_name"] == "Homo sapiens"
    assert package.bpa_values["platform"] == "test-illumina-shortread"
    assert package.bpa_values["data_context"] == "yes"
    
    # Verify decisions
    assert package.decisions["scientific_name"] == "Homo sapiens"
    assert package.decisions["scientific_name_accepted"] is True
    assert package.decisions["platform"] == "illumina_genomic"
    assert package.decisions["platform_accepted"] is True
    assert package.decisions["data_context"] == "genome_assembly"
    assert package.decisions["data_context_accepted"] is True

def test_filter_package_decision_tracking(nested_package_data, metadata_map):
    #Test that all decisions are properly tracked during filtering.
    nested_package_data["project_aim"] = "xx"
    package = BpaPackage(nested_package_data)
    keep_package = package.filter(metadata_map)
    print(f"\nBPA VALUESXX: {package.bpa_values}")
    print(f"\nBPA DECISIONSXX: {package.decisions}")
    
    # Check all controlled vocabulary fields have decisions tracked
    for field in metadata_map.controlled_vocabularies:
        # Every field should have an acceptance decision
        assert f"{field}_accepted" in package.decisions
        # Every field should have a value decision
        assert field in package.decisions
        # If we found a BPA value, it should be stored in bpa_values
        if field in package.bpa_values:
            # The decision should be either:
            # 1. The mapped AToL value if the BPA value was in the mapping
            # 2. The original BPA value if it wasn't in the mapping
            if package.decisions[f"{field}_accepted"]:
                # If accepted, the BPA value was in the mapping
                # So the decision should be the mapped AToL value
                if field == "scientific_name":
                    assert package.decisions[field] == "Homo sapiens"
                elif field == "data_context":
                    assert package.decisions[field] == "genome_assembly"
                elif field == "platform":
                    assert package.decisions[field] == "illumina_genomic"
            else:
                # If not accepted, the BPA value wasn't in the mapping
                # So the decision should be the original BPA value
                assert package.decisions[field] == package.bpa_values[field]

    # Verify specific decisions
    assert package.decisions["scientific_name_accepted"] is True
    assert package.decisions["data_context_accepted"] is False
    assert package.decisions["platform_accepted"] is True
    assert package.decisions["library_type_accepted"] is True
    assert package.decisions["library_size_accepted"] is True

    # Verify package is discarded
    assert keep_package is False

def test_filter_package_resource_fields(nested_package_data, metadata_map):
    #Test filtering of fields within resources.
    package = BpaPackage(nested_package_data)
    
    # Filter package
    keep_package = package.filter(metadata_map)
    
    # Verify package is kept
    assert keep_package is True
    
    # Verify resource fields are properly tracked
    assert package.bpa_fields["platform"] == "resources.type"
    assert package.bpa_fields["library_type"] == "resources.library_type"
    assert package.bpa_fields["library_size"] == "resources.library_size"
    
    # Verify resource values are properly tracked
    assert package.bpa_values["platform"] == "test-illumina-shortread"
    assert package.bpa_values["library_type"] == "Paired"
    assert package.bpa_values["library_size"] == "350.0"
    
    # Verify resource decisions
    assert package.decisions["platform"] == "illumina_genomic"
    assert package.decisions["platform_accepted"] is True
    assert package.decisions["library_type"] == "paired"
    assert package.decisions["library_type_accepted"] is True
    assert package.decisions["library_size"] == "350"
    assert package.decisions["library_size_accepted"] is True
