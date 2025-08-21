"""Unit tests for package_handler.py."""

from atol_bpa_datamapper.package_handler import BpaPackage, get_nested_value


def test_bpa_package_initialization():
    """Test BpaPackage initialization."""
    # This test verifies that:
    # 1. The BpaPackage class initializes correctly with package data
    # 2. The fields attribute is populated with all field names from the package
    # 3. The resource_ids attribute is populated with the IDs of all resources
    
    # Create a simple package
    package_data = {
        "id": "test-package-123",
        "field1": "value1",
        "field2": "value2",
        "resources": [
            {"id": "resource1", "type": "fastq"},
            {"id": "resource2", "type": "bam"}
        ]
    }
    package = BpaPackage(package_data)
    
    # Check that the package was initialized correctly
    assert package.id == "test-package-123"
    assert package["field1"] == "value1"
    assert package["field2"] == "value2"
    assert sorted(package.fields) == sorted(["field1", "field2", "id", "resources"])
    # resource_ids is now a set rather than a list
    assert package.resource_ids == {"resource1", "resource2"}


def test_choose_value_with_no_fields():
    """Test _choose_value with no fields to check."""
    # This test verifies that:
    # 1. When no fields are provided to check, the method returns (None, None, False)
    # 2. The keep decision is False when no fields are provided
    
    package = BpaPackage({"id": "test-package-123"})
    value, bpa_field, keep = package._choose_value([], None)
    assert value is None
    assert bpa_field is None
    assert keep is False


def test_choose_value_with_missing_fields():
    """Test _choose_value with fields that don't exist in the package."""
    # This test verifies that:
    # 1. When the specified fields don't exist in the package, the method returns (None, None, False)
    # 2. The keep decision is False when no matching fields are found
    
    package = BpaPackage({"id": "test-package-123"})
    value, bpa_field, keep = package._choose_value(["field1", "field2"], None)
    assert value is None
    assert bpa_field is None
    assert keep is False


def test_choose_value_with_no_controlled_vocabulary():
    """Test _choose_value with no controlled vocabulary."""
    # This test verifies that:
    # 1. When a field exists in the package and no controlled vocabulary is provided,
    #    the method returns the value, field name, and True
    # 2. The keep decision is True when no controlled vocabulary constraints are applied
    
    package = BpaPackage({"id": "test-package-123", "field1": "value1"})
    value, bpa_field, keep = package._choose_value(["field1"], None)
    assert value == "value1"
    assert bpa_field == "field1"
    assert keep is True


def test_choose_value_with_controlled_vocabulary_match():
    """Test _choose_value with a controlled vocabulary that matches."""
    # This test verifies that:
    # 1. When a field value matches an entry in the controlled vocabulary,
    #    the method returns the value, field name, and True
    # 2. The keep decision is True when the value is in the controlled vocabulary
    
    package = BpaPackage({"id": "test-package-123", "field1": "value1"})
    value, bpa_field, keep = package._choose_value(["field1"], ["value1", "value2"])
    assert value == "value1"
    assert bpa_field == "field1"
    assert keep is True


def test_choose_value_with_controlled_vocabulary_no_match():
    """Test _choose_value with a controlled vocabulary that doesn't match."""
    # This test verifies that:
    # 1. When a field value does not match any entry in the controlled vocabulary,
    #    the method returns the value, field name, and False
    # 2. The keep decision is False when the value is not in the controlled vocabulary
    
    package = BpaPackage({"id": "test-package-123", "field1": "value1"})
    value, bpa_field, keep = package._choose_value(["field1"], ["value2", "value3"])
    assert value == "value1"
    assert bpa_field == "field1"
    assert keep is False


def test_choose_value_with_multiple_fields():
    """Test _choose_value with multiple fields to check."""
    # This test verifies that:
    # 1. When multiple fields are provided, the method checks them in order
    # 2. The first field with a value is used, regardless of subsequent fields
    # 3. The keep decision is True when no controlled vocabulary is provided
    
    package = BpaPackage({
        "id": "test-package-123", 
        "field1": "value1",
        "field2": "value2"
    })
    value, bpa_field, keep = package._choose_value(["field1", "field2"], None)
    assert value == "value1"
    assert bpa_field == "field1"
    assert keep is True


def test_get_nested_value():
    """Test get_nested_value function."""
    # This test verifies that:
    # 1. The get_nested_value function correctly extracts values from nested dictionaries
    # 2. Dot notation is correctly interpreted to access nested dictionary values
    # 3. The function returns None when the specified path doesn't exist
    # 4. The function handles edge cases like None inputs gracefully
    
    # Create a dictionary with nested values
    data = {
        "field1": "value1",
        "nested": {
            "field2": "value2",
            "deeply": {
                "field3": "value3"
            }
        },
        "list": [
            {"id": "item1", "value": "value4"},
            {"id": "item2", "value": "value5"}
        ]
    }
    
    # Test getting simple values
    assert get_nested_value(data, "field1") == "value1"
    
    # Test getting nested values
    assert get_nested_value(data, "nested.field2") == "value2"
    assert get_nested_value(data, "nested.deeply.field3") == "value3"
    
    # Test getting values from non-existent paths
    assert get_nested_value(data, "field2") is None
    assert get_nested_value(data, "nested.field3") is None
    assert get_nested_value(data, "nested.deeply.field4") is None
    
    # Test with None input
    assert get_nested_value(None, "field1") is None
    assert get_nested_value(data, None) is None
