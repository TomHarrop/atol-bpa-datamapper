"""Test data factory functions for generating test data."""

from unittest.mock import MagicMock


class ResourceWithId:
    """Wrapper class for resource dictionaries to provide an id attribute."""
    
    def __init__(self, resource_dict, resource_id="test_resource_id"):
        """Initialize with a resource dictionary and optional id."""
        self.data = resource_dict
        self.id = resource_id
        
    def __getitem__(self, key):
        """Allow dictionary-style access to the underlying data."""
        return self.data[key]
        
    def __contains__(self, key):
        """Support 'in' operator for the underlying dictionary."""
        return key in self.data
        
    def get(self, key, default=None):
        """Mimic dictionary get method."""
        return self.data.get(key, default)
        
    def keys(self):
        """Return keys of the underlying dictionary."""
        return self.data.keys()
        
    def items(self):
        """Return items of the underlying dictionary."""
        return self.data.items()
        
    def values(self):
        """Return values of the underlying dictionary."""
        return self.data.values()


def create_test_package(package_id="test_package", scientific_name="Homo sapiens", 
                        project_aim="genome_assembly", resources=None):
    """Create a test BPA package with configurable fields."""
    package = {
        "id": package_id,
        "scientific_name": scientific_name,
        "project_aim": project_aim,
    }
    
    if resources:
        package["resources"] = resources
    
    return package


def create_test_resource(resource_type="illumina", library_type="paired", 
                         file_format="FASTQ", resource_id="test_resource_id"):
    """Create a test resource dictionary with configurable fields."""
    resource = {
        "shortread-type": resource_type,
        "library_type": library_type,
        "file_format": file_format
    }
    
    # Wrap the resource in ResourceWithId to provide the id attribute
    return ResourceWithId(resource, resource_id)


def create_large_test_dataset(num_packages=100, resources_per_package=5):
    """Create a large test dataset with many packages and resources."""
    packages = []
    
    for i in range(num_packages):
        resources = []
        for j in range(resources_per_package):
            resource = create_test_resource(
                resource_id=f"resource_{i}_{j}",
                resource_type="illumina" if j % 2 == 0 else "pacbio",
                library_type="paired" if j % 3 == 0 else "single",
                file_format="FASTQ" if j % 4 == 0 else "BAM"
            )
            resources.append(resource)
            
        package = create_test_package(
            package_id=f"package_{i}",
            scientific_name=f"Species {i}",
            project_aim="genome_assembly" if i % 2 == 0 else "transcriptome_assembly",
            resources=resources
        )
        packages.append(package)
        
    return packages


def create_invalid_mapping_structure():
    """Create an invalid mapping structure for testing validation."""
    return {
        "invalid_section": {
            "field1": "This should be a list, not a string"
        },
        "circular_reference": {
            "field1": {"refers_to": "field2"},
            "field2": {"refers_to": "field1"}
        },
        "missing_required": {
            # Missing required fields
        }
    }


def create_legacy_mapping():
    """Create a legacy format mapping for backward compatibility testing."""
    return {
        "legacy_section": {
            "field1": {
                "old_format_key": "old_value",
                "deprecated_key": True
            }
        }
    }
