import pytest
import json
from atol_bpa_datamapper.config_parser import MetadataMap
from atol_bpa_datamapper.package_handler import BpaPackage

@pytest.fixture
def nested_package_data():
    return {
        "id": "test_package_1",
        "scientific_name": "Homo sapiens",  
        "project_aim": "Genome resequencing",  
        "nested": {
            "field": "nested_value"
        },
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
            ],
            "flowcell_type": [
                "flowcell_type"
            ],
            "insert_size": [
                "insert_size_range"
            ],
            "library_construction_protocol": [
                "library_construction_protocol"
            ],
            "library_source": [
                "library_source"
            ],
            "instrument_model": [
                "sequencing_platform"
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
def sanitization_config_data():
    return {
        "organism": {
            "scientific_name": ["text_sanitization"]
        },
        "reads": {
            "library_type": ["text_sanitization"],
            "library_size": ["integer_sanitization"]
        },
        "sanitization_rules": {
            "text_sanitization": {
                "description": "Remove extra whitespace"
            },
            "integer_sanitization": {
                "description": "Ensure integer values, remove decimals"
            }
        }
    }



@pytest.fixture
def metadata_map(tmp_path, field_mapping_data, value_mapping_data, sanitization_config_data):
    # Create temporary config files
    field_mapping = tmp_path / "field_mapping_bpa_to_atol.json"
    field_mapping.write_text(json.dumps(field_mapping_data))
    
    value_mapping = tmp_path / "value_mapping_bpa_to_atol.json"
    value_mapping.write_text(json.dumps(value_mapping_data))
    
    sanitization_config = tmp_path / "sanitization_config.json"
    sanitization_config.write_text(json.dumps(sanitization_config_data))
    
    metadata_map = MetadataMap(field_mapping, value_mapping, sanitization_config)
    
    # Verify metadata sections
    assert metadata_map.metadata_sections == ["organism", "reads", "sample"]
    assert metadata_map.expected_fields == ["data_context", "flowcell_type", "insert_size", "instrument_model", "library_construction_protocol", "library_size", "library_source", "library_type", "platform", "scientific_name", "taxon_id"]
    assert metadata_map.controlled_vocabularies == ["data_context", "library_size", "library_type", "platform", "scientific_name"]
    
    return metadata_map

def test_map_metadata_nested_fields(nested_package_data, metadata_map):
    """Test mapping of metadata with nested fields."""
    package = BpaPackage(nested_package_data)
    
    # Map metadata
    mapped_metadata = package.map_metadata(metadata_map)
    print(f"Mapped metadata: {mapped_metadata}")
    
    # Verify organism section
    assert mapped_metadata["organism"]["scientific_name"] == "Homo sapiens"
    
    # Verify sample section
    assert mapped_metadata["sample"]["data_context"] == "genome_assembly"
    
    # Verify reads section
    assert len(mapped_metadata["reads"]) == 1  # One resource
    reads = mapped_metadata["reads"][0]
    assert reads["platform"] == "illumina_genomic"
    assert reads["library_type"] == "paired"
    assert reads["library_size"] == "350"
    
    # Verify mapping log
    assert len(package.mapping_log) == 5  # scientific_name, data_context, platform, library_type, library_size
    for entry in package.mapping_log:
        assert all(k in entry for k in ["atol_field", "bpa_field", "value", "mapped_value"])
        if entry["atol_field"] in ["platform", "library_type", "library_size"]:
            assert "resource_id" in entry
            assert entry["resource_id"] == "resource_1"
    
    # Verify field mapping
    assert package.field_mapping == {
        "scientific_name": "scientific_name",
        "data_context": "project_aim",
        "platform": "resources.type",
        "library_type": "resources.library_type",
        "library_size": "resources.library_size"
    }
    
    # Verify unused fields
    # TODO FIX THIS
    # assert set(package.unused_fields) == {"id", "nested.field"}
    
    # Verify sanitization changes (none in this case)
    print(f"Sanitization changes: {package.sanitization_changes}")
    assert len(package.sanitization_changes) == 1

def test_map_metadata_sanitization_tracking(nested_package_data, metadata_map):
    """Test tracking of sanitization changes during metadata mapping."""
    # Add values that need sanitization
    nested_package_data["scientific_name"] = "  Homo   sapiens  "  # Extra spaces
    nested_package_data["resources"][0]["library_size"] = " 350.0 "  # Extra spaces
    package = BpaPackage(nested_package_data)
    
    # Map metadata
    mapped_metadata = package.map_metadata(metadata_map)
    
    print(f"Sanitization changes: {package.sanitization_changes}")
    # Verify sanitization changes
    assert len(package.sanitization_changes) == 2
    
    # Check scientific_name sanitization
    name_change = next(c for c in package.sanitization_changes if c["field"] == "scientific_name")
    assert name_change["bpa_id"] == "test_package_1"
    assert name_change["original_value"] == "  Homo   sapiens  "
    assert name_change["sanitized_value"] == "Homo sapiens"
    
    # Check library_size sanitization in resource
    size_change = next(c for c in package.sanitization_changes if c["field"] == "library_size")
    assert size_change["bpa_id"] == "test_package_1"
    assert size_change["original_value"] == " 350.0 "
    assert size_change["sanitized_value"] == "350"
    assert size_change["resource_id"] == "resource_1"

def test_map_metadata_field_mapping_tracking(nested_package_data, metadata_map):
    """Test tracking of field mapping during metadata mapping."""
    package = BpaPackage(nested_package_data)
    
    # Map metadata
    mapped_metadata = package.map_metadata(metadata_map)
    
    # Verify field mappings
    expected_mapping = {
        "scientific_name": "scientific_name",
        "data_context": "project_aim",
        "platform": "resources.type",
        "library_type": "resources.library_type",
        "library_size": "resources.library_size"
    }
    assert package.field_mapping == expected_mapping

def test_map_metadata_mapping_log(nested_package_data, metadata_map):
    """Test logging of mapping operations."""
    package = BpaPackage(nested_package_data)
    
    # Map metadata
    mapped_metadata = package.map_metadata(metadata_map)
    
    # Verify mapping log structure
    assert len(package.mapping_log) == 5  # All fields mapped
    
    # Check non-resource fields
    name_log = next(l for l in package.mapping_log if l["atol_field"] == "scientific_name")
    assert name_log["bpa_field"] == "scientific_name"
    assert name_log["value"] == "Homo sapiens"  # Original value
    assert name_log["mapped_value"] == "Homo sapiens"  # Mapped value
    assert "resource_id" not in name_log
    
    context_log = next(l for l in package.mapping_log if l["atol_field"] == "data_context")
    assert context_log["bpa_field"] == "project_aim"
    assert context_log["value"] == "Genome resequencing"
    assert context_log["mapped_value"] == "genome_assembly"
    assert "resource_id" not in context_log
    
    # Check resource fields
    platform_log = next(l for l in package.mapping_log if l["atol_field"] == "platform")
    assert platform_log["bpa_field"] == "resources.type"
    assert platform_log["value"] == "test-illumina-shortread"
    assert platform_log["mapped_value"] == "illumina_genomic"
    assert platform_log["resource_id"] == "resource_1"

def test_map_metadata_missing_values(nested_package_data, metadata_map):
    """Test handling of missing values during metadata mapping."""
    # Remove values
    del nested_package_data["scientific_name"]
    del nested_package_data["resources"][0]["library_type"]
    package = BpaPackage(nested_package_data)
    
    # Map metadata
    mapped_metadata = package.map_metadata(metadata_map)
    
    # Verify missing values
    assert "scientific_name" not in mapped_metadata["organism"]
    assert mapped_metadata["reads"][0]["library_type"] is None
    
    # Verify mapping log excludes missing values
    mapped_fields = [l["atol_field"] for l in package.mapping_log]
    assert "scientific_name" not in mapped_fields
    assert "library_type" not in mapped_fields

def test_map_metadata_invalid_values(nested_package_data, metadata_map):
    """Test handling of invalid values during metadata mapping."""
    # Set invalid values
    nested_package_data["scientific_name"] = "Invalid Species"
    nested_package_data["resources"][0]["library_type"] = "Invalid Type"
    package = BpaPackage(nested_package_data)
    
    # Map metadata
    mapped_metadata = package.map_metadata(metadata_map)
    
    # Verify invalid values
    assert mapped_metadata["organism"]["scientific_name"] == "Invalid Species"  # Kept as-is
    assert mapped_metadata["reads"][0]["library_type"] == "Invalid Type"  # Kept as-is
    
    # Verify mapping log shows unmapped values
    name_log = next(l for l in package.mapping_log if l["atol_field"] == "scientific_name")
    assert name_log["value"] == "Invalid Species"
    assert name_log["mapped_value"] == "Invalid Species"
    
    type_log = next(l for l in package.mapping_log if l["atol_field"] == "library_type")
    assert type_log["value"] == "Invalid Type"
    assert type_log["mapped_value"] == "Invalid Type"

def test_map_metadata_multiple_resources(nested_package_data, metadata_map):
    """Test mapping of metadata with multiple resources."""
    # Add another resource
    nested_package_data["resources"].append({
        "id": "resource_2",
        "type": "test-pacbio-hifi",
        "library_type": "Single",
        "library_size": "1000.0"
    })
    package = BpaPackage(nested_package_data)
    
    # Map metadata
    mapped_metadata = package.map_metadata(metadata_map)
    
    # Verify reads section has two resources
    assert len(mapped_metadata["reads"]) == 2
    
    # Verify first resource
    reads1 = mapped_metadata["reads"][0]
    assert reads1["platform"] == "illumina_genomic"
    assert reads1["library_type"] == "paired"
    assert reads1["library_size"] == "350"
    
    # Verify second resource
    reads2 = mapped_metadata["reads"][1]
    assert reads2["platform"] == "pacbio_hifi"
    assert reads2["library_type"] == "single"
    assert reads2["library_size"] == "1000"
    
    # Verify mapping log includes both resources
    platform_logs = [l for l in package.mapping_log if l["atol_field"] == "platform"]
    assert len(platform_logs) == 2
    assert {"resource_1", "resource_2"} == {l["resource_id"] for l in platform_logs}

def test_map_metadata_parent_fields_to_resources(nested_package_data, metadata_map):
    """Test mapping of parent-level fields to resource objects in the reads section."""
    # Add parent-level fields that should be mapped to resources
    nested_package_data["flowcell_type"] = "10B-300"
    nested_package_data["insert_size_range"] = "588.0"
    nested_package_data["library_construction_protocol"] = "Illumina DNA Prep M"
    nested_package_data["library_source"] = "DNA"
    nested_package_data["sequencing_platform"] = "NovaSeq"
    
    # Add a resource with minimal information
    nested_package_data["resources"] = [{
        "id": "resource_1",
        "type": "test-illumina-shortread",
        "file_name": "test_file.fastq.gz",
        "md5": "test_md5"
    }]
    
    package = BpaPackage(nested_package_data)
    
    # Map metadata
    mapped_metadata = package.map_metadata(metadata_map)
    
    # Verify reads section has the resource
    assert len(mapped_metadata["reads"]) == 1
    
    # Verify parent-level fields are correctly mapped to the resource
    reads = mapped_metadata["reads"][0]
    assert reads["platform"] == "illumina_genomic"  # From resource.type
    assert reads["flowcell_type"] == "10B-300"  # From parent
    assert reads["insert_size"] == "588.0"  # From parent
    assert reads["library_construction_protocol"] == "Illumina DNA Prep M"  # From parent
    assert reads["library_source"] == "DNA"  # From parent
    assert reads["instrument_model"] == "NovaSeq"  # From parent sequencing_platform
    
    # Verify the mapping log contains entries for parent-level fields
    parent_field_logs = [log for log in package.mapping_log 
                         if log["atol_field"] in ["flowcell_type", "insert_size", 
                                                 "library_construction_protocol", 
                                                 "library_source", "instrument_model"]]
    
    assert len(parent_field_logs) == 5
    
    # Verify field mapping contains correct source fields
    assert package.field_mapping["flowcell_type"] == "flowcell_type"
    assert package.field_mapping["insert_size"] == "insert_size_range"
    assert package.field_mapping["library_construction_protocol"] == "library_construction_protocol"
    assert package.field_mapping["library_source"] == "library_source"
    assert package.field_mapping["instrument_model"] == "sequencing_platform"

def test_map_metadata_skip_empty_strings(nested_package_data, metadata_map):
    """Test that empty strings are skipped in favor of non-empty values lower in the field list."""
    # Add a field with empty string at higher priority and meaningful value at lower priority
    nested_package_data["empty_field"] = ""
    nested_package_data["meaningful_field"] = "Meaningful Value"
    
    # Create a new package with the modified data
    package = BpaPackage(nested_package_data)
    
    # Manually add a test field to the metadata map that checks empty_field first, then meaningful_field
    metadata_map["test_field"] = {
        "bpa_fields": ["empty_field", "meaningful_field"],
        "section": "sample"
    }
    metadata_map.expected_fields.append("test_field")
    
    # Map metadata
    mapped_metadata = package.map_metadata(metadata_map)
    
    # Verify that the meaningful value was used instead of the empty string
    assert mapped_metadata["sample"]["test_field"] == "Meaningful Value"
    
    # Verify the field mapping contains the correct source field
    assert package.field_mapping["test_field"] == "meaningful_field"
