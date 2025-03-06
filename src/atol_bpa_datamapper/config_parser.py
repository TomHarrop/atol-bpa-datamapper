from .logger import logger
import json
import re
import unicodedata
from typing import Any, Dict, Optional


class MetadataMap(dict):
    def __init__(self, field_mapping_file, value_mapping_file, sanitization_config_file=None):
        super().__init__()
        logger.info(f"Reading field mapping from {field_mapping_file}")
        with open(field_mapping_file, "rt") as f:
            field_mapping = json.load(f)
        logger.info(f"Reading value mapping from {value_mapping_file}")
        with open(value_mapping_file, "rt") as f:
            value_mapping = json.load(f)
            
        # Load sanitization config if provided
        self.sanitization_config = None
        if sanitization_config_file:
            logger.info(f"Reading sanitization config from {sanitization_config_file}")
            with open(sanitization_config_file, "rt") as f:
                self.sanitization_config = json.load(f)
        
        # Map the expected AToL fields to fields in the BPA data
        for atol_section, mapping_dict in field_mapping.items():
            for atol_field, bpa_field_list in mapping_dict.items():
                self[atol_field] = {}
                self[atol_field]["bpa_fields"] = bpa_field_list
                self[atol_field]["section"] = atol_section
                
        # Generate a value_mapping dict for each AToL field
        for atol_section, mapping_dict in value_mapping.items():
            for atol_field, value_mapping_dict in mapping_dict.items():
                try:
                    bpa_value_to_atol_value = {}
                    for atol_value, list_of_bpa_values in value_mapping_dict.items():
                        for value in list_of_bpa_values:
                            bpa_value_to_atol_value[value] = atol_value
                    self[atol_field]["value_mapping"] = bpa_value_to_atol_value
                except KeyError as e:
                    print(
                        "\n".join(
                            [
                                f"Field {atol_field} isn't defined in field_mapping.",
                                f"The following fields were parsed from {field_mapping_file}:",
                                f"{sorted(set(self.keys()))}",
                            ]
                        )
                    )
                    raise e
                    
        # We iterate over the expected keys during mapping
        setattr(self, "expected_fields", list(self.keys()))
        logger.debug(f"expected_fields:\n{self.expected_fields}")

        setattr(
            self, "metadata_sections", sorted(set(x["section"] for x in self.values()))
        )
        logger.debug(f"metadata_sections:\n{self.metadata_sections}")

        setattr(
            self,
            "controlled_vocabularies",
            [k for k in self.keys() if "value_mapping" in self[k]],
        )
        logger.debug(f"controlled_vocabularies:\n{self.controlled_vocabularies}")

    def _text_sanitization(self, value: str) -> str:
        """Sanitize text by removing double spaces and trimming whitespace."""
        if not isinstance(value, str):
            return value
        
        # Remove double spaces
        while "  " in value:
            value = value.replace("  ", " ")
            
        # Trim leading/trailing whitespace
        return value.strip()

    def _empty_string_sanitization(self, value: Any) -> Optional[str]:
        """Convert empty strings to None."""
        if isinstance(value, str) and not value.strip():
            return None
        return value

    def _integer_sanitization(self, value: Any) -> str:
        """Convert float or integer to string, removing decimal places if float."""
        if isinstance(value, (int, float)):
            # Convert to integer string, removing decimal places
            return str(int(value))
        elif isinstance(value, str):
            try:
                # Try to convert to float first to handle both integers and floats
                num = float(value)
                # Convert to integer string, removing decimal places
                return str(int(num))
            except ValueError:
                # If conversion fails, return original string
                return value
        return str(value)

    def _sanitize_value(self, field: str, value: Any) -> Any:
        """Apply sanitization rules to a value based on field name."""
        if not self.sanitization_config or value is None:
            return value
            
        # Apply rules in specific order
        original_value = value
        sanitized_value = value
        
        # Convert field name to match sanitization config format
        field_parts = field.split(".")
        if len(field_parts) > 1:
            field = field_parts[-1]  # Use last part of field name
        
        # Add "organism." prefix for organism fields
        if field in ["scientific_name", "common_name", "infraspecific_epithet", 
                    "family", "genus", "species", "order_or_group", "taxon_id"]:
            field = f"organism.{field}"
        
        if field in self.sanitization_config["rules"]["empty_string_sanitization"]["fields"]:
            sanitized_value = self._empty_string_sanitization(sanitized_value)
            
        if field in self.sanitization_config["rules"]["text_sanitization"]["fields"]:
            sanitized_value = self._text_sanitization(sanitized_value)
            
        if field in self.sanitization_config["rules"]["integer_sanitization"]["fields"]:
            sanitized_value = self._integer_sanitization(sanitized_value)
            
        return sanitized_value

    def map_value(self, atol_field, bpa_value):
        """Map a BPA value to an AToL value."""
        if bpa_value is None:
            return None

        # First apply sanitization if configured
        sanitized_value = self._sanitize_value(atol_field, bpa_value)
        
        # Map the sanitized value to AToL value
        try:
            return self[atol_field]["value_mapping"][sanitized_value]
        except KeyError as e:
            if atol_field == "data_context" and bpa_value == "yes":
                logger.warning(f"Value of {atol_field} is {bpa_value}.")
                return "genome_assembly"
            else:
                logger.warning(f"Value {sanitized_value} not found in mapping for {atol_field}")
                return sanitized_value

    def get_allowed_values(self, atol_field):
        try:
            return sorted(set(self[atol_field]["value_mapping"].keys()))
        except KeyError as e:
            return None

    def get_bpa_fields(self, atol_field):
        return self[atol_field]["bpa_fields"]

    def get_atol_section(self, atol_field):
        return self[atol_field]["section"]

    def keep_value(self, atol_field, bpa_value):
        allowed_values = self.get_allowed_values(atol_field)
        # If there is no list of allowed values, then we don't have a
        # controlled vocabulary for this field, so we keep anything.
        if allowed_values is None:
            return True
        else:
            return bpa_value in allowed_values
