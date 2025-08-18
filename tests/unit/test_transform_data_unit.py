"""
Unit tests for the transform_data module.
"""

import pytest
from atol_bpa_datamapper.transform_data import OrganismTransformer, SampleTransformer


class TestOrganismTransformer:
    """Tests for the OrganismTransformer class."""

    def test_init_with_ignored_fields(self):
        """Test initializing with ignored fields."""
        ignored_fields = ["common_name", "description"]
        transformer = OrganismTransformer(ignored_fields=ignored_fields)
        assert transformer.ignored_fields == ignored_fields

    def test_process_package_with_ignored_fields(self):
        """Test processing a package with ignored fields."""
        # Create a transformer with ignored fields
        transformer = OrganismTransformer(ignored_fields=["common_name"])
        
        # Create two packages with the same organism_grouping_key but different common_name
        package1 = {
            "experiment": {"bpa_package_id": "package1"},
            "organism": {
                "organism_grouping_key": "key1",
                "scientific_name": "Species name",
                "common_name": "Common name 1",
                "taxon_id": "12345"
            }
        }
        
        package2 = {
            "experiment": {"bpa_package_id": "package2"},
            "organism": {
                "organism_grouping_key": "key1",
                "scientific_name": "Species name",
                "common_name": "Common name 2",
                "taxon_id": "12345"
            }
        }
        
        # Process both packages
        transformer.process_package(package1)
        transformer.process_package(package2)
        
        # Get results
        results = transformer.get_results()
        
        # Check that the organism is in the unique organisms list despite the conflict in common_name
        assert "key1" in results["unique_organisms"]
        # Check that the common_name field is set to None due to the conflict
        assert results["unique_organisms"]["key1"]["common_name"] is None
        # Check that the conflict is still recorded
        assert "key1" in results["organism_conflicts"]
        assert "common_name" in results["organism_conflicts"]["key1"]
        assert len(results["organism_conflicts"]["key1"]["common_name"]) == 2
        assert "Common name 1" in results["organism_conflicts"]["key1"]["common_name"]
        assert "Common name 2" in results["organism_conflicts"]["key1"]["common_name"]

    def test_critical_conflicts_exclude_organism(self):
        """Test that critical conflicts (non-ignored fields) exclude the organism from results."""
        # Create a transformer with ignored fields
        transformer = OrganismTransformer(ignored_fields=["common_name"])
        
        # Create two packages with the same organism_grouping_key but different scientific_name (critical field)
        package1 = {
            "experiment": {"bpa_package_id": "package1"},
            "organism": {
                "organism_grouping_key": "key1",
                "scientific_name": "Species name 1",
                "common_name": "Common name",
                "taxon_id": "12345"
            }
        }
        
        package2 = {
            "experiment": {"bpa_package_id": "package2"},
            "organism": {
                "organism_grouping_key": "key1",
                "scientific_name": "Species name 2",
                "common_name": "Common name",
                "taxon_id": "12345"
            }
        }
        
        # Process both packages
        transformer.process_package(package1)
        transformer.process_package(package2)
        
        # Get results
        results = transformer.get_results()
        
        # Check that the organism is NOT in the unique organisms list due to critical conflict
        assert "key1" not in results["unique_organisms"]
        # Check that the conflict is recorded
        assert "key1" in results["organism_conflicts"]
        assert "scientific_name" in results["organism_conflicts"]["key1"]


class TestSampleTransformer:
    """Tests for the SampleTransformer class."""

    def test_init_with_ignored_fields(self):
        """Test initializing with ignored fields."""
        ignored_fields = ["collection_date", "description"]
        transformer = SampleTransformer(ignored_fields=ignored_fields)
        assert transformer.ignored_fields == ignored_fields

    def test_process_package_with_ignored_fields(self):
        """Test processing a package with ignored fields."""
        # Create a transformer with ignored fields
        transformer = SampleTransformer(ignored_fields=["collection_date"])
        
        # Create two packages with the same bpa_sample_id but different collection_date
        package1 = {
            "experiment": {"bpa_package_id": "package1"},
            "sample": {
                "bpa_sample_id": "sample1",
                "collection_date": "2023-01-01",
                "location": "Location A"
            }
        }
        
        package2 = {
            "experiment": {"bpa_package_id": "package2"},
            "sample": {
                "bpa_sample_id": "sample1",
                "collection_date": "2023-02-01",
                "location": "Location A"
            }
        }
        
        # Process both packages
        transformer.process_package(package1)
        transformer.process_package(package2)
        
        # Get results
        results = transformer.get_results()
        
        # Check that the sample is in the unique samples list despite the conflict in collection_date
        assert "sample1" in results["unique_samples"]
        # Check that the collection_date field is set to None due to the conflict
        assert results["unique_samples"]["sample1"]["collection_date"] is None
        # Check that the conflict is still recorded
        assert "sample1" in results["sample_conflicts"]
        assert "collection_date" in results["sample_conflicts"]["sample1"]
        assert len(results["sample_conflicts"]["sample1"]["collection_date"]) == 2
        assert "2023-01-01" in results["sample_conflicts"]["sample1"]["collection_date"]
        assert "2023-02-01" in results["sample_conflicts"]["sample1"]["collection_date"]

    def test_critical_conflicts_exclude_sample(self):
        """Test that critical conflicts (non-ignored fields) exclude the sample from results."""
        # Create a transformer with ignored fields
        transformer = SampleTransformer(ignored_fields=["collection_date"])
        
        # Create two packages with the same bpa_sample_id but different location (critical field)
        package1 = {
            "experiment": {"bpa_package_id": "package1"},
            "sample": {
                "bpa_sample_id": "sample1",
                "collection_date": "2023-01-01",
                "location": "Location A"
            }
        }
        
        package2 = {
            "experiment": {"bpa_package_id": "package2"},
            "sample": {
                "bpa_sample_id": "sample1",
                "collection_date": "2023-01-01",
                "location": "Location B"
            }
        }
        
        # Process both packages
        transformer.process_package(package1)
        transformer.process_package(package2)
        
        # Get results
        results = transformer.get_results()
        
        # Check that the sample is NOT in the unique samples list due to critical conflict
        assert "sample1" not in results["unique_samples"]
        # Check that the conflict is recorded
        assert "sample1" in results["sample_conflicts"]
        assert "location" in results["sample_conflicts"]["sample1"]

    def test_sample_to_organism_mapping(self):
        # Create a sample transformer
        sample_transformer = SampleTransformer()
        
        # Create a package with both sample and organism data
        package1 = {
            "experiment": {"bpa_package_id": "package1"},
            "sample": {
                "bpa_sample_id": "sample1",
                "field1": "value1"
            },
            "organism": {
                "organism_grouping_key": "organism1",
                "field1": "value1"
            }
        }
        
        package2 = {
            "experiment": {"bpa_package_id": "package2"},
            "sample": {
                "bpa_sample_id": "sample2",
                "field1": "value2"
            },
            "organism": {
                "organism_grouping_key": "organism2",
                "field1": "value2"
            }
        }
        
        # Process the packages
        sample_transformer.process_package(package1)
        sample_transformer.process_package(package2)
        
        # Get the results
        results = sample_transformer.get_results()
        
        # Check that the organism_grouping_key is added to the sample data
        assert results["unique_samples"]["sample1"]["organism_grouping_key"] == "organism1"
        assert results["unique_samples"]["sample2"]["organism_grouping_key"] == "organism2"

    def test_sample_with_multiple_organisms(self):
        # Create a sample transformer with organism_grouping_key as an ignored field
        # This prevents the sample from being excluded due to organism conflicts
        sample_transformer = SampleTransformer(ignored_fields=["organism_grouping_key"])
        
        # Create packages with the same sample but different organisms
        package1 = {
            "experiment": {"bpa_package_id": "package1"},
            "sample": {
                "bpa_sample_id": "sample1",
                "field1": "value1"
            },
            "organism": {
                "organism_grouping_key": "organism1",
                "field1": "value1"
            }
        }
        
        package2 = {
            "experiment": {"bpa_package_id": "package2"},
            "sample": {
                "bpa_sample_id": "sample1",
                "field1": "value1"
            },
            "organism": {
                "organism_grouping_key": "organism2",
                "field1": "value2"
            }
        }
        
        # Process the packages
        sample_transformer.process_package(package1)
        sample_transformer.process_package(package2)
        
        # Get the results
        results = sample_transformer.get_results()
        
        # Check that the organism_grouping_key is null in the sample data
        assert results["unique_samples"]["sample1"]["organism_grouping_key"] is None
        
        # Check that the conflict is recorded
        assert "sample1" in results["sample_conflicts"]
        assert "organism_grouping_key" in results["sample_conflicts"]["sample1"]
        
        # Check that both organism keys are in the conflicts
        organism_conflicts = results["sample_conflicts"]["sample1"]["organism_grouping_key"]
        assert "organism1" in organism_conflicts
        assert "organism2" in organism_conflicts
