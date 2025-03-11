import pytest
from atol_bpa_datamapper.config_parser import MetadataMap

@pytest.fixture
def sanitization_config():
    return {
        "organism": {
            "scientific_name": ["text_sanitization", "empty_string_sanitization"],
            "taxon_id": ["integer_sanitization"],
            "name": ["text_sanitization"]  # organism.name
        },
        "sample": {
            "data_context": ["empty_string_sanitization"],
            "name": ["empty_string_sanitization"]  # sample.name
        },
        "sanitization_rules": {
            "text_sanitization": {
                "description": "Strip double whitespace, unicode whitespace characters"
            },
            "empty_string_sanitization": {
                "description": "Convert empty strings to null"
            },
            "integer_sanitization": {
                "description": "Ensure integer values, remove decimals"
            }
        }
    }

@pytest.fixture
def metadata_map(tmp_path):
    # Create temporary config files
    field_mapping = tmp_path / "field_mapping.json"
    field_mapping.write_text("{}")
    
    value_mapping = tmp_path / "value_mapping.json"
    value_mapping.write_text("{}")
    
    sanitization_config = tmp_path / "sanitization_config.json"
    sanitization_config.write_text("""
    {
        "organism": {
            "scientific_name": ["text_sanitization", "empty_string_sanitization"],
            "taxon_id": ["integer_sanitization"],
            "name": ["text_sanitization"]
        },
        "sample": {
            "data_context": ["empty_string_sanitization"],
            "name": ["empty_string_sanitization"]
        },
        "sanitization_rules": {
            "text_sanitization": {
                "description": "Strip double whitespace, unicode whitespace characters"
            },
            "empty_string_sanitization": {
                "description": "Convert empty strings to null"
            },
            "integer_sanitization": {
                "description": "Ensure integer values, remove decimals"
            }
        }
    }
    """)
    
    return MetadataMap(
        field_mapping,
        value_mapping,
        sanitization_config
    )

def test_text_sanitization(metadata_map):
    """Test text sanitization rules."""
    # Test double whitespace removal
    assert metadata_map._sanitize_value(
        "organism", "scientific_name", 
        "Homo    sapiens"
    ) == "Homo sapiens"
    
    # Test unicode whitespace removal
    assert metadata_map._sanitize_value(
        "organism", "scientific_name", 
        "Homo\u2003sapiens"  # Unicode em space
    ) == "Homo sapiens"
    
    # Test leading/trailing whitespace
    assert metadata_map._sanitize_value(
        "organism", "scientific_name", 
        "  Homo sapiens  "
    ) == "Homo sapiens"

def test_empty_string_sanitization(metadata_map):
    """Test empty string sanitization rules."""
    # Test empty string
    assert metadata_map._sanitize_value(
        "organism","scientific_name", 
        ""
    ) is None
    
    # Test whitespace string
    assert metadata_map._sanitize_value(
        "organism", "scientific_name", 
        "   "
    ) is None
    
    # Test non-empty string
    assert metadata_map._sanitize_value(
        "organism", "scientific_name", 
        "Homo sapiens"
    ) == "Homo sapiens"

def test_integer_sanitization(metadata_map):
    """Test integer sanitization rules."""
    # Test integer
    assert metadata_map._sanitize_value(
        "organism", "taxon_id",
        42
    ) == 42
    
    # Test float
    assert metadata_map._sanitize_value(
        "organism", "taxon_id",
        42.0
    ) == 42
    
    # Test string number
    assert metadata_map._sanitize_value(
        "organism", "taxon_id",
        "42"
    ) == 42
    
    # Test invalid number
    assert metadata_map._sanitize_value(
        "organism", "taxon_id",
        "not a number"
    ) is None

def test_multiple_rules(metadata_map):
    """Test multiple rules applied in sequence."""
    # Test empty string with multiple rules
    assert metadata_map._sanitize_value(
        "organism", "scientific_name", 
        "   "
    ) is None
    
    # Test text sanitization followed by empty string check
    assert metadata_map._sanitize_value(
        "organism", "scientific_name", 
        "  Homo    sapiens  "
    ) == "Homo sapiens"

def test_no_sanitization(metadata_map):
    """Test cases where no sanitization should be applied."""
    # Test field without rules
    assert metadata_map._sanitize_value(
        "organism", "no_rules", 
        "test"
    ) == "test"
    
    # Test non-existent section
    assert metadata_map._sanitize_value(
        "nonexistent", "field", 
        "test"
    ) == "test"
    
    # Test None value
    assert metadata_map._sanitize_value(
        "organism", "scientific_name", 
        None
    ) is None

def test_same_field_different_sections(metadata_map):
    """Test fields with same name in different sections get different rules applied."""
    # Test organism.name (should apply text_sanitization)
    assert metadata_map._sanitize_value(
        "organism", "name", 
        "  Test    Name  "
    ) == "Test Name"
    
    # Test sample.name (should apply empty_string_sanitization)
    assert metadata_map._sanitize_value(
        "sample", "name", 
        "  "
    ) is None
    
    # Test sample.name with non-empty value
    assert metadata_map._sanitize_value(
        "sample", "name", 
        "Test Name"
    ) == "Test Name"
    
    # Verify organism.name doesn't apply empty_string_sanitization
    assert metadata_map._sanitize_value(
        "organism", "name", 
        "  "
    ) == ""  # Should only remove double spaces, not convert to None
