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
        setattr(self, "expected_fields", sorted(self.keys()))
        logger.debug(f"expected_fields:\n{self.expected_fields}")

        setattr(
            self, "metadata_sections", sorted(set(x["section"] for x in self.values()))
        )
        logger.debug(f"metadata_sections:\n{self.metadata_sections}")

        setattr(
            self,
            "controlled_vocabularies",
            sorted([k for k in self.keys() if "value_mapping" in self[k]])
        )
        logger.debug(f"controlled_vocabularies:\n{self.controlled_vocabularies}")

    def _sanitize_value(self, section: str, field: str, value: Any):
        logger.info(f"Sanitizing {field} with value {value}")
        """Apply sanitization rules to a value based on field name."""
        if value is None:
            return None

        if not self.sanitization_config:
            return value

        # Get sanitization rules for this field
        section_config = self.sanitization_config.get(section, {})
        rules_to_apply = section_config.get(field, [])
        
        # Apply each rule in sequence
        sanitized_value = value
        for rule in rules_to_apply:
            rule_config = self.sanitization_config["sanitization_rules"].get(rule)
            if not rule_config:
                logger.warning(f"Unknown sanitization rule: {rule}")
                continue
                
            if rule == "text_sanitization":
                if isinstance(sanitized_value, str):
                    # Remove double whitespace and unicode whitespace
                    sanitized_value = ' '.join(sanitized_value.split())
                    
            elif rule == "empty_string_sanitization":
                if isinstance(sanitized_value, str) and not sanitized_value.strip():
                    sanitized_value = None
                    
            elif rule == "integer_sanitization":
                try:
                    if isinstance(sanitized_value, (int, float, str)):
                        sanitized_value = str(int(float(str(sanitized_value))))
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert {sanitized_value} to integer")
                    sanitized_value = None

        return sanitized_value

    def map_value(self, atol_field, bpa_value):
        """Map a BPA value to an AToL value."""
        allowed_values = self.get_allowed_values(atol_field)
        # If there is no list of allowed values, then we don't have a
        # controlled vocabulary for this field, so we keep anything.
        if allowed_values is None:
            return bpa_value
        if bpa_value is None:
            logger.warning(f"Bpa value {bpa_value} not found in controlled vocabulary.")
            return None


        # First apply sanitization if configured
        
        # Map the sanitized value to AToL value
        try:
            return self[atol_field]["value_mapping"][bpa_value]
        except KeyError as e:
            if atol_field == "data_context" and bpa_value == "yes":
                logger.warning(f"Value of {atol_field} is {bpa_value}.")
                return "genome_assembly"
            else:
                logger.warning(f"Value {bpa_value} not found in mapping for {atol_field}")
                return bpa_value

    def get_allowed_values(self, atol_field):
        try:
            return self[atol_field]["value_mapping"]
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
