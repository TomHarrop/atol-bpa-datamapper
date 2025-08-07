"""Integration test for counter output in filter_packages.py."""

import gzip
import pytest
import json
import tempfile
from pathlib import Path
from collections import Counter

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

    # Set up command line arguments
    import sys
    original_argv = sys.argv.copy()
    sys.argv = [
        "filter_packages.py",
        "-i", str(input_file),
        "-o", str(output_file),
        "-f", str(package_field_mapping_file),
        "-r", str(resource_field_mapping_file),
        "-v", str(value_mapping_file),
        "-l", "INFO",
        "--decision_log", str(decision_log_file),
        "--raw_field_usage", str(raw_field_usage_file),
        "--bpa_field_usage", str(bpa_field_usage_file),
        "--bpa_value_usage", str(bpa_value_usage_file)
    ]

    # Create a function to run filter_packages with our test files
    from atol_bpa_datamapper.config_parser import MetadataMap
    from atol_bpa_datamapper.io import write_json, write_decision_log_to_csv
    from atol_bpa_datamapper.logger import setup_logger
    from atol_bpa_datamapper.package_handler import BpaPackage
    from collections import Counter
    
    # Set up logging
    setup_logger("INFO")
    
    # Create metadata maps
    package_level_map = MetadataMap(str(package_field_mapping_file), str(value_mapping_file))
    resource_level_map = MetadataMap(str(resource_field_mapping_file), str(value_mapping_file))
    
    # Read input data directly instead of using read_input
    input_data = []
    with open(input_file, "r") as f:
        for line in f:
            if line.strip():
                package_data = json.loads(line)
                input_data.append(BpaPackage(package_data))
    
    # Set up counters
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
    
    # Set up decision log
    decision_log = {}
    
    n_packages = 0
    n_kept = 0
    
    # Process packages
    filtered_packages = []
    for package in input_data:
        n_packages += 1
        
        # Filter package-level fields
        package.filter(package_level_map)
        
        # Update counters for package-level fields
        for field in package.fields:
            counters["raw_field_usage"][field] += 1
        
        for atol_field, bpa_field in package.bpa_fields.items():
            if bpa_field:
                counters["bpa_field_usage"][atol_field][bpa_field] += 1
        
        for atol_field, value in package.bpa_values.items():
            if value:
                counters["bpa_value_usage"][atol_field][value] += 1
        
        # Filter resource-level fields
        for resource in package.resources.values():
            resource.filter(resource_level_map, package)
            
            # Update counters for resource-level fields
            for field in resource.fields:
                counters["raw_field_usage"][field] += 1
            
            for atol_field, bpa_field in resource.bpa_fields.items():
                if bpa_field:
                    counters["bpa_field_usage"][atol_field][bpa_field] += 1
            
            for atol_field, value in resource.bpa_values.items():
                if value:
                    counters["bpa_value_usage"][atol_field][value] += 1
        
        # Record decisions
        decision_log[package.id] = package.decisions
        
        # Keep or discard package
        if package.keep:
            n_kept += 1
            filtered_packages.append(package)
    
    # Write filtered packages to output file
    with open(output_file, "w") as f:
        for package in filtered_packages:
            f.write(json.dumps(dict(package)) + "\n")
    
    # Write stats to files
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
    assert raw_field_counter["resources.type"] == 2  # 2 packages have resources with type
    assert raw_field_counter["resources.library_type"] == 2  # 2 packages have resources with library_type
    assert raw_field_counter["resources.library_size"] == 2  # 2 packages have resources with library_size

    # Read and verify bpa_field_usage counter
    with gzip.open(bpa_field_usage_file, "rt") as f:
        bpa_field_counter = json.loads(f.read())
        
    # Verify specific counter values
    assert bpa_field_counter["scientific_name"]["scientific_name"] == 3
    assert bpa_field_counter["data_context"]["project_aim"] == 3
    assert bpa_field_counter["platform"]["resources.type"] == 2
    assert bpa_field_counter["library_type"]["resources.library_type"] == 2
    assert bpa_field_counter["library_size"]["resources.library_size"] == 2

    # Read and verify bpa_value_usage counter
    with gzip.open(bpa_value_usage_file, "rt") as f:
        bpa_value_counter = json.loads(f.read())
        
    # Verify specific counter values
    assert bpa_value_counter["scientific_name"]["Homo sapiens"] == 2
    assert bpa_value_counter["scientific_name"]["Escherichia coli"] == 1
    assert bpa_value_counter["data_context"]["genome_assembly"] == 2  # Mapped from "Genome resequencing"
    assert bpa_value_counter["data_context"]["transcriptome_assembly"] == 1  # Mapped from "Transcriptome assembly"
    assert bpa_value_counter["platform"]["illumina_genomic"] == 2  # Mapped from "test-illumina-shortread"
    assert bpa_value_counter["library_type"]["paired"] == 2  # Mapped from "Paired"
    assert bpa_value_counter["library_size"]["350"] == 1  # Sanitized from "350.0"
    assert bpa_value_counter["library_size"]["500"] == 1  # Sanitized from "500.0"

    # Restore original command line arguments
    sys.argv = original_argv
