"""
Integration tests for the transform_data module's main function.
"""

import os
import json
import gzip
import pytest
import tempfile
from unittest.mock import patch, MagicMock
from atol_bpa_datamapper.transform_data import main


@pytest.fixture
def test_input_data():
    """Create test input data for transform_data."""
    return [
        {
            "id": "package1",
            "experiment": {
                "bpa_package_id": "package1",
                "experiment_type": "genome"
            },
            "sample": {
                "bpa_sample_id": "sample1",
                "collection_date": "2023-01-01",
                "sample_access_date": "2023-02-01"
            },
            "organism": {
                "organism_grouping_key": "org1",
                "scientific_name": "Homo sapiens",
                "common_name": "Human"
            },
            "runs": [
                {"run_id": "run1"}
            ]
        },
        {
            "id": "package2",
            "experiment": {
                "bpa_package_id": "package2",
                "experiment_type": "transcriptome"
            },
            "sample": {
                "bpa_sample_id": "sample2",
                "collection_date": "2023-03-01",
                "sample_access_date": "2023-04-01"
            },
            "organism": {
                "organism_grouping_key": "org2",
                "scientific_name": "Mus musculus",
                "common_name": "Mouse"
            }
        },
        {
            "id": "package3",
            "experiment": {
                "bpa_package_id": "package3",
                "experiment_type": "genome"
            },
            "sample": {
                "bpa_sample_id": "sample1",  # Same sample as package1
                "collection_date": "2023-02-15",  # Different collection date to create a conflict
                "sample_access_date": "2023-05-01"  # Different access date
            },
            "organism": {
                "organism_grouping_key": "org1",  # Same organism as package1
                "scientific_name": "Homo sapiens",  # Same scientific name to avoid non-ignored conflicts
                "common_name": "Human variant"  # Different common name to create a conflict
            }
        }
    ]


@pytest.fixture
def input_file(test_input_data):
    """Create a temporary input file with test data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for item in test_input_data:
            f.write(json.dumps(item) + '\n')
    
    yield f.name
    
    # Cleanup
    if os.path.exists(f.name):
        os.remove(f.name)


def read_gzipped_json(file_path):
    """Read a gzipped JSON file and return the parsed content."""
    with gzip.open(file_path, 'rt') as f:
        return json.load(f)


def test_transform_data_main(test_input_data, input_file):
    """Test the main function of transform_data."""
    # Create temporary output files
    with tempfile.TemporaryDirectory() as temp_dir:
        samples_output = os.path.join(temp_dir, "samples.json")
        sample_conflicts = os.path.join(temp_dir, "sample_conflicts.json")
        sample_package_map = os.path.join(temp_dir, "sample_package_map.json")
        organisms_output = os.path.join(temp_dir, "organisms.json")
        organism_conflicts = os.path.join(temp_dir, "organism_conflicts.json")
        organism_package_map = os.path.join(temp_dir, "organism_package_map.json")
        experiments_output = os.path.join(temp_dir, "experiments.json")
        transformation_changes = os.path.join(temp_dir, "transformation_changes.json")
        
        # Mock the argument parser
        mock_args = type('Args', (), {
            'input': input_file,
            'output': samples_output,
            'sample_conflicts': sample_conflicts,
            'sample_package_map': sample_package_map,
            'unique_organisms': organisms_output,
            'organism_conflicts': organism_conflicts,
            'organism_package_map': organism_package_map,
            'experiments_output': experiments_output,
            'transformation_changes': transformation_changes,
            'dry_run': False,
            'log_level': 'INFO',
            'sample_ignored_fields': None,
            'organism_ignored_fields': None
        })
        
        # Run the main function with mocked arguments
        with patch('atol_bpa_datamapper.transform_data.parse_args_for_transform', return_value=mock_args), \
             patch('atol_bpa_datamapper.transform_data.read_jsonl_file', return_value=test_input_data):
            main()
        
        # Verify output files exist
        assert os.path.exists(samples_output)
        assert os.path.exists(sample_conflicts)
        assert os.path.exists(sample_package_map)
        assert os.path.exists(organisms_output)
        assert os.path.exists(organism_conflicts)
        assert os.path.exists(organism_package_map)
        assert os.path.exists(experiments_output)
        # print("length:::")
        
        # Verify content of output files
        samples = read_gzipped_json(samples_output)
        print("samples:::")
        print(samples)
        assert len(samples) == 1  # Should have 2 unique samples (the sample with a conflict with no ignored fields should not be in unique_samples)
        assert "sample1" not in samples
        assert "sample2" in samples
        assert samples["sample2"]["sample_access_date"] == "2023-04-01"
        
        # Check sample conflicts
        sample_conflicts_data = read_gzipped_json(sample_conflicts)
        assert "sample1" in sample_conflicts_data
        assert "collection_date" in sample_conflicts_data["sample1"]
        assert len(sample_conflicts_data["sample1"]["collection_date"]) == 2
        assert "2023-01-01" in sample_conflicts_data["sample1"]["collection_date"]
        assert "2023-02-15" in sample_conflicts_data["sample1"]["collection_date"]
        
        organisms = read_gzipped_json(organisms_output)
        assert len(organisms) == 1  # Should have 1 unique organism (no ignored fields, and there is a conflict for common_name)
        assert "org2" in organisms
        
        # Check organism conflicts
        organism_conflicts_data = read_gzipped_json(organism_conflicts)
        assert "org1" in organism_conflicts_data
        assert "common_name" in organism_conflicts_data["org1"]
        assert len(organism_conflicts_data["org1"]["common_name"]) == 2
        assert "Human" in organism_conflicts_data["org1"]["common_name"]
        assert "Human variant" in organism_conflicts_data["org1"]["common_name"]
        
        experiments = read_gzipped_json(experiments_output)
        assert len(experiments) == 3  # Should have 3 experiments
        assert "package1" in experiments
        assert "package2" in experiments
        assert "package3" in experiments


def test_transform_data_main_with_ignored_fields(test_input_data, input_file):
    """Test the main function with ignored fields."""
    # Create temporary output files
    with tempfile.TemporaryDirectory() as temp_dir:
        samples_output = os.path.join(temp_dir, "samples.json")
        sample_conflicts = os.path.join(temp_dir, "sample_conflicts.json")
        organisms_output = os.path.join(temp_dir, "organisms.json")
        organism_conflicts = os.path.join(temp_dir, "organism_conflicts.json")
        
        # Mock the argument parser with ignored fields
        mock_args = type('Args', (), {
            'input': input_file,
            'output': samples_output,
            'sample_conflicts': sample_conflicts,
            'sample_package_map': None,
            'unique_organisms': organisms_output,
            'organism_conflicts': organism_conflicts,
            'organism_package_map': None,
            'experiments_output': None,
            'transformation_changes': None,
            'dry_run': False,
            'log_level': 'INFO',
            'sample_ignored_fields': 'collection_date',  # Ignore collection_date field
            'organism_ignored_fields': 'common_name'     # Ignore common_name field
        })
        
        # Run the main function with mocked arguments
        with patch('atol_bpa_datamapper.transform_data.parse_args_for_transform', return_value=mock_args), \
             patch('atol_bpa_datamapper.transform_data.read_jsonl_file', return_value=test_input_data):
            main()
        
        # Verify output files exist
        assert os.path.exists(samples_output)
        assert os.path.exists(sample_conflicts)
        assert os.path.exists(organisms_output)
        assert os.path.exists(organism_conflicts)
        
        # Verify content of output files with ignored fields
        samples = read_gzipped_json(samples_output)
        assert len(samples) == 2  # Should have 2 unique samples
        assert "sample1" in samples
        # Check that collection_date is None due to being ignored and having conflicts
        assert samples["sample1"]["collection_date"] is None
        
        organisms = read_gzipped_json(organisms_output)
        assert len(organisms) == 2  # Should have 2 unique organisms
        assert "org1" in organisms
        # Check that common_name is None due to being ignored and having conflicts
        assert organisms["org1"]["common_name"] is None


def test_transform_data_main_dry_run(test_input_data, input_file):
    """Test the main function with dry_run=True."""
    # Create temporary output files
    with tempfile.TemporaryDirectory() as temp_dir:
        samples_output = os.path.join(temp_dir, "samples.json")
        
        # Mock the argument parser with dry_run=True
        mock_args = type('Args', (), {
            'input': input_file,
            'output': samples_output,
            'sample_conflicts': None,
            'sample_package_map': None,
            'unique_organisms': None,
            'organism_conflicts': None,
            'organism_package_map': None,
            'experiments_output': None,
            'transformation_changes': None,
            'dry_run': True,
            'log_level': 'INFO',
            'sample_ignored_fields': None,
            'organism_ignored_fields': None
        })
        
        # Run the main function with mocked arguments
        with patch('atol_bpa_datamapper.transform_data.parse_args_for_transform', return_value=mock_args), \
             patch('atol_bpa_datamapper.transform_data.read_jsonl_file', return_value=test_input_data):
            main()
        
        # Verify output files don't exist (dry run)
        assert not os.path.exists(samples_output)
