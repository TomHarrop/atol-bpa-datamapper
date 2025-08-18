"""Helper functions for tests."""

from unittest.mock import MagicMock


def create_mock_metadata_map(
    metadata_sections=None,
    expected_fields=None,
    get_atol_section_func=None,
    get_bpa_fields_func=None,
    get_allowed_values_func=None,
    map_value_func=None,
    sanitize_value_func=None,
):
    """Create a mock metadata map with all required methods."""
    metadata_map = MagicMock()
    
    # Set default values for metadata sections and expected fields
    metadata_map.metadata_sections = metadata_sections or ["dataset"]
    metadata_map.expected_fields = expected_fields or []
    metadata_map.sanitization_config = {"null_values": [""]}
    
    # Define default mock functions
    def default_get_atol_section(field):
        return "dataset"
    
    def default_get_bpa_fields(field):
        return []
    
    def default_get_allowed_values(field):
        return None
    
    def default_map_value(field, value):
        return value
    
    def default_sanitize_value(section, field, value):
        return (value, [])
    
    def default_check_default_value(field):
        return (False, None)
    
    # Define a mock map_metadata_result function
    def mock_map_metadata_result(package):
        result = {}
        for section in metadata_map.metadata_sections:
            result[section] = {}
        
        # Add field_mapping attribute to package for tests that check it
        if not hasattr(package, 'field_mapping'):
            package.field_mapping = {}
        
        # Process each expected field
        for field in metadata_map.expected_fields:
            atol_section = metadata_map.get_atol_section(field)
            if atol_section not in result:
                result[atol_section] = {}
            
            bpa_fields = metadata_map.get_bpa_fields(field)
            value = None
            used_field = None
            
            # Check if any of the fields are resource fields
            is_resource_field = any("." in f and f.startswith("resources.") for f in bpa_fields)
            
            if is_resource_field:
                # Handle resource fields
                if "resources" in package and package["resources"]:
                    # For resource fields, create a list of resources in the section
                    if not isinstance(result[atol_section], list):
                        result[atol_section] = []
                    
                    for resource in package["resources"]:
                        resource_entry = {"resource_id": resource["id"]}
                        
                        # Extract the field value from the resource
                        for bpa_field in bpa_fields:
                            if "." in bpa_field and bpa_field.startswith("resources."):
                                resource_field = bpa_field.split(".", 1)[1]
                                if resource_field in resource and resource[resource_field] is not None:
                                    value = resource[resource_field]
                                    # Apply value mapping
                                    mapped_value = metadata_map.map_value(field, value)
                                    resource_entry[field] = mapped_value
                                    package.field_mapping[field] = bpa_field
                                    break
                        
                        result[atol_section].append(resource_entry)
            else:
                # Handle regular fields
                for bpa_field in bpa_fields:
                    if "." in bpa_field:
                        # Handle nested fields
                        parts = bpa_field.split(".")
                        obj = package
                        for part in parts:
                            if obj is None or part not in obj:
                                value = None
                                break
                            obj = obj[part]
                        value = obj
                        used_field = bpa_field
                    else:
                        # Handle regular fields
                        value = package.get(bpa_field)
                        used_field = bpa_field
                    
                    if value is not None:
                        # Record which field was used for mapping
                        package.field_mapping[field] = used_field
                        
                        # Apply value mapping
                        mapped_value = metadata_map.map_value(field, value)
                        
                        # Apply sanitization
                        sanitized_value, _ = metadata_map._sanitize_value(atol_section, field, mapped_value)
                        
                        result[atol_section][field] = sanitized_value
                        break
        
        return result
    
    # Set mock methods with provided functions or defaults
    metadata_map.get_atol_section = get_atol_section_func or default_get_atol_section
    metadata_map.get_bpa_fields = get_bpa_fields_func or default_get_bpa_fields
    metadata_map.get_allowed_values = get_allowed_values_func or default_get_allowed_values
    metadata_map.map_value = map_value_func or default_map_value
    metadata_map._sanitize_value = sanitize_value_func or default_sanitize_value
    metadata_map.check_default_value = default_check_default_value
    metadata_map.mock_map_metadata_result = mock_map_metadata_result
    
    # Set up the __getitem__ method to return a dictionary with bpa_fields, section, and value_mapping
    def getitem(self, key):
        return {
            "bpa_fields": metadata_map.get_bpa_fields(key),
            "section": metadata_map.get_atol_section(key),
            "value_mapping": metadata_map.get_allowed_values(key)
        }
    
    metadata_map.__getitem__ = getitem
    
    return metadata_map
