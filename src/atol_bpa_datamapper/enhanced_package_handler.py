"""
Enhanced BpaBase class with improved field processing.
"""

from .logger import logger
from abc import ABC, abstractmethod


class FieldProcessor(ABC):
    """Abstract base class for field processing strategies."""
    
    @abstractmethod
    def process_field(self, atol_field, value, bpa_field, metadata_map):
        """Process a field value according to the specific strategy."""
        pass


class FilteringFieldProcessor(FieldProcessor):
    """Field processor for filtering operations."""
    
    def process_field(self, atol_field, value, bpa_field, metadata_map):
        """Simply return the value for filtering purposes."""
        return value


class MappingFieldProcessor(FieldProcessor):
    """Field processor for mapping operations."""
    
    def __init__(self, package_instance):
        self.package = package_instance
    
    def process_field(self, atol_field, value, bpa_field, metadata_map):
        """Apply sanitization and mapping for mapping operations."""
        if value is None or bpa_field is None:
            return None
            
        # Get section for sanitization
        section = metadata_map.get_atol_section(atol_field)
        
        # Apply sanitization
        sanitized_value = self.package._apply_sanitization(
            metadata_map, section, atol_field, value
        )
        
        # Apply value mapping
        try:
            mapped_value = metadata_map.map_value(atol_field, sanitized_value)
        except KeyError:
            mapped_value = sanitized_value
            
        return mapped_value


class EnhancedBpaBase(dict):
    """Enhanced BpaBase with improved field processing."""
    
    def __init__(self, data):
        super().__init__()
        self.update(data)
        self.fields = sorted(set(self.keys()))
        self.id = self.get("id")
        self["bpa_id"] = self.id
        
    def _check_atol_field_with_processor(
        self, atol_field, metadata_map, field_processor, parent_package=None
    ):
        """
        Unified field checking method that uses a processor strategy.
        
        This method contains the common logic for checking AToL fields,
        while delegating the specific processing to the field_processor.
        """
        null_values = metadata_map.sanitization_config.get("null_values", [])
        logger.debug(f"Checking field {atol_field}...")

        bpa_field_list = metadata_map[atol_field]["bpa_fields"]
        accepted_values = metadata_map.get_allowed_values(atol_field)

        logger.debug(f"  for values {accepted_values}...")
        logger.debug(f"  in BPA fields {bpa_field_list}.")

        # Check for default value
        has_default, default_value = metadata_map.check_default_value(atol_field)
        if has_default:
            logger.debug(f"  Default is {default_value}.")

        # Get the raw value
        raw_value, bpa_field, keep = self._choose_value(
            bpa_field_list, accepted_values, parent_package, null_values
        )

        # Apply default if needed
        if has_default and not keep:
            if raw_value is not None:
                logger.warning(f"Field {atol_field} has value {raw_value}. Using default {default_value}.")
            else:
                logger.debug(f"Field {atol_field} has no value. Using default {default_value}.")
            raw_value, bpa_field, keep = (default_value, "default_value", True)

        # Special handling for genome_data
        if atol_field == "data_context" and "genome_data" in self.fields and not keep:
            logger.debug("Checking genome_data field")
            if self["genome_data"] == "yes":
                logger.debug("Setting keep to True")
                raw_value, bpa_field, keep = ("yes", "genome_data", True)

        # Process the value using the strategy
        if keep:
            processed_value = field_processor.process_field(
                atol_field, raw_value, bpa_field, metadata_map
            )
        else:
            processed_value = raw_value

        logger.debug(f"Found value {processed_value} for atol_field {atol_field} in bpa_field {bpa_field}. Keep is {keep}.")
        
        return (processed_value, bpa_field, keep)
        
    # Keep the original _choose_value method unchanged
    def _choose_value(self, fields_to_check, accepted_values, parent_package=None, null_values=None):
        """Original _choose_value method - unchanged."""
        if null_values is None:
            null_values = []
        # Implementation would be copied from the original BpaBase class
        # For now, returning placeholder values
        return (None, None, False)


# Usage example showing how the enhanced class could be used:
class ExampleUsage(EnhancedBpaBase):
    """Example of how to use the enhanced BpaBase class."""
    
    def filter_with_enhanced_base(self, metadata_map, parent_package=None):
        """Example filtering using the enhanced base class."""
        processor = FilteringFieldProcessor()
        
        self.decisions = {}
        self.bpa_fields = {}
        self.bpa_values = {}
        
        for atol_field in metadata_map.controlled_vocabularies:
            value, bpa_field, keep = self._check_atol_field_with_processor(
                atol_field, metadata_map, processor, parent_package
            )
            
            self.bpa_fields[atol_field] = bpa_field
            self.bpa_values[atol_field] = value
            
            decision_key = f"{atol_field}_accepted"
            self.decisions[decision_key] = keep
            self.decisions[atol_field] = value
            
        self.keep = all(x for x in self.decisions.values() if isinstance(x, bool))
        
    def map_with_enhanced_base(self, metadata_map, parent_package=None):
        """Example mapping using the enhanced base class."""
        processor = MappingFieldProcessor(self)
        
        mapped_metadata = {k: {} for k in metadata_map.metadata_sections}
        self.mapping_log = []
        self.field_mapping = {}
        
        for atol_field in metadata_map.expected_fields:
            section = metadata_map.get_atol_section(atol_field)
            value, bpa_field, keep = self._check_atol_field_with_processor(
                atol_field, metadata_map, processor, parent_package
            )
            
            if keep and bpa_field is not None:
                mapped_metadata[section][atol_field] = value
                self.field_mapping[atol_field] = bpa_field
                
                self.mapping_log.append({
                    "atol_field": atol_field,
                    "bpa_field": bpa_field,
                    "mapped_value": value,
                })
                
        self.mapped_metadata = mapped_metadata
        return mapped_metadata
