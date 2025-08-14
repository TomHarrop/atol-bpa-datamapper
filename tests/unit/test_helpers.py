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
            for field in metadata_map.expected_fields:
                atol_section = metadata_map.get_atol_section(field)
                if atol_section != section:
                    continue
                
                bpa_fields = metadata_map.get_bpa_fields(field)
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
                    else:
                        # Handle regular fields
                        value = package.get(bpa_field)
                    
                    if value is not None:
                        result[section][field] = value
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
    
    # Set up the __getitem__ method to return a dictionary with bpa_fields
    def getitem(self, key):
        return {"bpa_fields": metadata_map.get_bpa_fields(key)}
    
    metadata_map.__getitem__ = getitem
    
    return metadata_map
