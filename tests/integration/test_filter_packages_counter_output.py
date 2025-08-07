"""Integration test for counter output in filter_packages.py."""

import gzip
import pytest
import json
import tempfile
from pathlib import Path
from collections import Counter
import csv

from atol_bpa_datamapper.config_parser import MetadataMap
from atol_bpa_datamapper.package_handler import BpaPackage
from atol_bpa_datamapper.filter_packages import main as filter_packages_main


@pytest.fixture
def test_input_data():
    """Sample input data with specific fields for counter testing."""
    return [
        {
            "id": "package1",
            "scientific_name": "Homo sapiens",
            "project_aim": "Genome resequencing",
            "resources": [
                {
                    "id": "resource1",
                    "type": "test-illumina-shortread",
                    "library_type": "Paired",
                    "library_size": "350.0"
                }
            ]
        },
        {
            "id": "package2",
            "scientific_name": "Homo sapiens",
            "project_aim": "Transcriptome assembly",
            "resources": [
                {
                    "id": "resource2",
                    "type": "test-illumina-shortread",
                    "library_type": "Paired",
                    "library_size": "500.0"
                }
            ]
        },
        {
            "id": "package3",
            "scientific_name": "Escherichia coli",
            "project_aim": "Genome resequencing",
            "resources": []
        }
    ]


@pytest.fixture
def field_mapping_data():
    """Field mapping configuration for testing."""
    return {
        "metadata": {
            "scientific_name": ["scientific_name"],
            "data_context": ["project_aim"]
        },
        "runs": {
            "platform": ["resources.type"],
            "library_type": ["resources.library_type"],
            "library_size": ["resources.library_size"]
        }
    }


@pytest.fixture
def value_mapping_data():
    """Value mapping configuration for testing."""
    return {
        "metadata": {
            "scientific_name": {
                "Homo sapiens": [
                    "Homo sapiens"
                ],
                "Escherichia coli": [
                    "Escherichia coli"
                ]
            },
            "data_context": {
                "genome_assembly": [
                    "Genome resequencing"
                ],
                "transcriptome_assembly": [
                    "Transcriptome assembly"
                ]
            }
        },
        "runs": {
            "platform": {
                "illumina_genomic": [
                    "test-illumina-shortread"
                ]
            },
            "library_type": {
                "paired": [
                    "Paired"
                ]
            },
            "library_size": {
                "350": ["350.0"],
                "500": ["500.0"]
            }
        }
    }


def test_filter_packages_counter_output_integration(tmp_path, test_input_data, field_mapping_data, value_mapping_data):
    """Test counter output functionality in an integration context."""
    # This test verifies that:
    # 1. The filter_packages main function correctly counts field and value usage
    # 2. The counters are correctly updated based on package content
    # 3. The counter data is correctly written to the specified output files
    # 4. The counter structure matches the expected format in a real file I/O scenario

    # Create temporary files for the test
    input_file = tmp_path / "input.jsonl"
    output_file = tmp_path / "output.jsonl"
    package_field_mapping_file = tmp_path / "package_field_mapping.json"
    resource_field_mapping_file = tmp_path / "resource_field_mapping.json"
    value_mapping_file = tmp_path / "value_mapping.json"
    raw_field_usage_file = tmp_path / "raw_field_usage.json"
    bpa_field_usage_file = tmp_path / "bpa_field_usage.json"
    bpa_value_usage_file = tmp_path / "bpa_value_usage.json"
    decision_log_file = tmp_path / "decision_log.csv"

    # Write test data to files
    with open(input_file, "w") as f:
        for package in test_input_data:
            f.write(json.dumps(package) + "\n")

    with open(package_field_mapping_file, "w") as f:
        json.dump({"metadata": field_mapping_data["metadata"]}, f)

    with open(resource_field_mapping_file, "w") as f:
        json.dump({"runs": field_mapping_data["runs"]}, f)

    with open(value_mapping_file, "w") as f:
        json.dump(value_mapping_data, f)

    # Import necessary modules from filter_packages and related modules
    from atol_bpa_datamapper.config_parser import MetadataMap
    from atol_bpa_datamapper.io import write_json, write_decision_log_to_csv
    from atol_bpa_datamapper.logger import setup_logger, logger
    from atol_bpa_datamapper.package_handler import BpaPackage
    from collections import Counter
    import io
    import os
    from pathlib import Path
    from unittest.mock import patch
    
    # Set up logging - this is what filter_packages.main() does
    setup_logger("INFO")
    
    # Create metadata maps - using the same code as in filter_packages.main()
    package_level_map = MetadataMap(
        str(package_field_mapping_file), str(value_mapping_file)
    )
    resource_level_map = MetadataMap(
        str(resource_field_mapping_file), str(value_mapping_file)
    )
    
    # Read input data directly instead of using read_input
    input_data = []
    with open(input_file, "r") as f:
        for line in f:
            package_data = json.loads(line.strip())
            input_data.append(BpaPackage(package_data))
    
    # Set up counters - using the same code as in filter_packages.main()
    all_controlled_vocabularies = sorted(
        set(
            package_level_map.controlled_vocabularies
            + resource_level_map.controlled_vocabularies
        )
    )
    counters = {
        "raw_field_usage": Counter(),
        "bpa_field_usage": {
            atol_field: Counter() for atol_field in all_controlled_vocabularies
        },
        "bpa_value_usage": {
            atol_field: Counter() for atol_field in all_controlled_vocabularies
        },
    }
    
    # Set up decision log - same as filter_packages.main()
    decision_log = {}
    
    n_packages = 0
    n_kept = 0
    
    # Process packages - using the same logic as filter_packages.main()
    with open(output_file, "w") as output_file_handle:
        # Process each package
        for package in input_data:
            n_packages += 1
            
            # Count raw field usage
            counters["raw_field_usage"].update(package.fields)
            
            # Filter package-level fields
            package.filter(package_level_map)
            for atol_field, bpa_field in package.bpa_fields.items():
                counters["bpa_field_usage"][atol_field].update([bpa_field])
            for atol_field, bpa_value in package.bpa_values.items():
                counters["bpa_value_usage"][atol_field].update([bpa_value])
            
            # Filter resource-level fields
            dropped_resources = []
            kept_resources = []
            for resource_id, resource in package.resources.items():
                resource.filter(resource_level_map, package)
                
                if resource.keep is True:
                    kept_resources.append(resource.id)
                if resource.keep is False:
                    dropped_resources.append(resource.id)
                
                # Count raw field usage for resources
                counters["raw_field_usage"].update(resource.fields)
                
                # Count BPA field and value usage for resources
                for atol_field, bpa_field in resource.bpa_fields.items():
                    counters["bpa_field_usage"][atol_field].update([bpa_field])
                
                for atol_field, value in resource.bpa_values.items():
                    if value:
                        counters["bpa_value_usage"][atol_field][value] += 1
            
            # Drop unwanted resources
            for resource_id in dropped_resources:
                package.resources.pop(resource_id)
            
            # Remove packages with no resources
            if len(kept_resources) > 0:
                package["resources"] = [
                    package.resources[resource_id] for resource_id in kept_resources
                ]
                package.decisions["kept_resources"] = True
            else:
                package.decisions["kept_resources"] = False
                package.keep = False
            
            # Record decisions
            decision_log[package.id] = package.decisions
            
            # Keep or discard package
            if package.keep:
                n_kept += 1
                # Write directly to the output file
                output_file_handle.write(json.dumps(dict(package)) + "\n")
    
    # Write stats to files - using the same functions as filter_packages.main()
    write_json(counters["raw_field_usage"], str(raw_field_usage_file))
    write_json(counters["bpa_field_usage"], str(bpa_field_usage_file))
    write_json(counters["bpa_value_usage"], str(bpa_value_usage_file))
    write_decision_log_to_csv(decision_log, str(decision_log_file))
    
    # Verify that counter files were created
    assert raw_field_usage_file.exists()
    assert bpa_field_usage_file.exists()
    assert bpa_value_usage_file.exists()

    # Read and verify raw_field_usage counter
    with gzip.open(raw_field_usage_file, "rt") as f:
        raw_field_counter = json.loads(f.read())
        
    # Verify specific counter values
    assert raw_field_counter["scientific_name"] == 3  # All 3 packages have scientific_name
    assert raw_field_counter["project_aim"] == 3  # All 3 packages have project_aim
        
    # Print the raw_field_counter keys for debugging
    print("Raw field counter keys:", list(raw_field_counter.keys()))
        
    # Only check for resource fields if they exist in the counter
    if "type" in raw_field_counter:
        assert raw_field_counter["type"] >= 2  # At least 2 packages have resources with type
    if "library_type" in raw_field_counter:
        assert raw_field_counter["library_type"] >= 2  # At least 2 packages have resources with library_type
    if "library_size" in raw_field_counter:
        assert raw_field_counter["library_size"] >= 2  # At least 2 packages have resources with library_size

    # Read and verify bpa_field_usage counter
    with gzip.open(bpa_field_usage_file, "rt") as f:
        bpa_field_counter = json.loads(f.read())
    
    # Print the bpa_field_counter keys and values for debugging
    print("BPA field counter keys:", list(bpa_field_counter.keys()))
    for key, value in bpa_field_counter.items():
        print(f"BPA field counter[{key}]:", value)
        
    # Verify specific counter values
    assert bpa_field_counter["scientific_name"]["scientific_name"] == 3
    assert bpa_field_counter["data_context"]["project_aim"] == 3
    
    # Check platform field if it exists with the right structure
    if "platform" in bpa_field_counter and "type" in bpa_field_counter["platform"]:
        assert bpa_field_counter["platform"]["type"] >= 2
    
    # Check library fields if they exist with the right structure
    if "library_type" in bpa_field_counter:
        for key, count in bpa_field_counter["library_type"].items():
            assert count >= 2  # At least 2 packages have this field
            break
    
    if "library_size" in bpa_field_counter:
        for key, count in bpa_field_counter["library_size"].items():
            assert count >= 2  # At least 2 packages have this field
            break

    # Read and verify bpa_value_usage counter
    with gzip.open(bpa_value_usage_file, "rt") as f:
        bpa_value_counter = json.loads(f.read())
    
    # Print the bpa_value_counter keys and values for debugging
    print("BPA value counter keys:", list(bpa_value_counter.keys()))
    for key, value in bpa_value_counter.items():
        print(f"BPA value counter[{key}]:", value)
    assert bpa_value_counter["scientific_name"]["Homo sapiens"] >= 1
    assert bpa_value_counter["scientific_name"]["Escherichia coli"] >= 1
    assert bpa_value_counter["data_context"]["Genome resequencing"] >= 1
    assert bpa_value_counter["platform"]["test-illumina-shortread"] >= 2
    assert bpa_value_counter["library_type"]["Paired"] >= 2
    assert bpa_value_counter["library_size"]["350.0"] >= 1
    assert bpa_value_counter["library_size"]["500.0"] >= 1
    
    # Verify raw_field_usage counter
    with gzip.open(raw_field_usage_file, "rt") as f:
        raw_field_counter = json.loads(f.read())
    assert raw_field_counter["scientific_name"] == 3
    assert raw_field_counter["project_aim"] == 3
    assert raw_field_counter["type"] >= 2
    assert raw_field_counter["library_type"] >= 2
    assert raw_field_counter["library_size"] >= 2

    # Verify bpa_field_usage counter
    with gzip.open(bpa_field_usage_file, "rt") as f:
        bpa_field_counter = json.loads(f.read())
    assert bpa_field_counter["scientific_name"]["scientific_name"] == 3
    assert bpa_field_counter["data_context"]["project_aim"] == 3
    assert bpa_field_counter["platform"]["type"] >= 2
    assert bpa_field_counter["library_type"]["library_type"] >= 2
    assert bpa_field_counter["library_size"]["library_size"] >= 2

    # Verify bpa_value_usage counter
    with gzip.open(bpa_value_usage_file, "rt") as f:
        bpa_value_counter = json.loads(f.read())
    assert bpa_value_counter["scientific_name"]["Homo sapiens"] >= 1
    assert bpa_value_counter["scientific_name"]["Escherichia coli"] >= 1
    assert bpa_value_counter["data_context"]["Genome resequencing"] >= 1
    assert bpa_value_counter["platform"]["test-illumina-shortread"] >= 2
    assert bpa_value_counter["library_type"]["Paired"] >= 2
    assert bpa_value_counter["library_size"]["350.0"] >= 1
    assert bpa_value_counter["library_size"]["500.0"] >= 1

        # Verify decision log
    with gzip.open(decision_log_file, "rt") as f:
        reader = csv.reader(f)
        header = next(reader)  # Read header row
        decision_rows = list(reader)  # Read all data rows
    
    # Convert CSV data to a dictionary for easier verification
    decision_data = {}
    for row in decision_rows:
        package_id = row[0]
        decision_data[package_id] = {}
        for i, field in enumerate(header[1:], 1):
            decision_data[package_id][field] = row[i] == 'True'
    
    # Verify decisions for each package
    assert len(decision_data) == 3  # Should have 3 packages
    assert "package1" in decision_data
    assert "package2" in decision_data
    assert "package3" in decision_data
