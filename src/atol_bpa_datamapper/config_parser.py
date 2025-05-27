from .logger import logger
import json
import os


class MetadataMap(dict):
    def __init__(self, field_mapping_file, value_mapping_file):
        super().__init__()
        logger.info(f"Reading field mapping from {field_mapping_file}")
        with open(field_mapping_file, "rt") as f:
            field_mapping = json.load(f)
        logger.info(f"Reading value mapping from {value_mapping_file}")
        with open(value_mapping_file, "rt") as f:
            value_mapping = json.load(f)

        # Load sanitization config if it exists
        self.sanitization_config = {}
        sanitization_config_path = os.path.join(
            os.path.dirname(field_mapping_file), "sanitization_config.json"
        )
        if os.path.exists(sanitization_config_path):
            logger.info(f"Reading sanitization config from {sanitization_config_path}")
            with open(sanitization_config_path, "rt") as f:
                self.sanitization_config = json.load(f)
        else:
            logger.warning(
                f"Sanitization config not found at {sanitization_config_path}"
            )

        # Debug: Print the sections in field_mapping
        logger.debug(f"Field mapping sections: {list(field_mapping.keys())}")

        # Map the expected AToL fields to fields in the BPA data
        for atol_section, mapping_dict in field_mapping.items():
            logger.debug(f"Processing section: {atol_section}")
            for atol_field, bpa_field_list in mapping_dict.items():
                logger.debug(f"  Field: {atol_field}, BPA fields: {bpa_field_list}")
                self[atol_field] = {}
                self[atol_field]["bpa_fields"] = bpa_field_list
                self[atol_field]["section"] = atol_section

        # Debug: Print specific fields we're interested in
        for field in ["package_id", "bioplatforms_dataset_url"]:
            if field in self:
                logger.debug(f"Field {field} is in section {self[field]['section']}")

        # Generate a value_mapping dict for each AToL field
        for atol_section, mapping_dict in value_mapping.items():
            if atol_section not in field_mapping.keys():
                logger.debug(
                    (
                        f"Skipping value_mapping section {atol_section} "
                        "because it's not a section in the field_mapping"
                    )
                )
                continue
            logger.debug(f"Processing value mapping section: {atol_section}")
            for atol_field, value_mapping_dict in mapping_dict.items():
                try:
                    bpa_value_to_atol_value = {}
                    for atol_value, list_of_bpa_values in value_mapping_dict.items():
                        for value in list_of_bpa_values:
                            bpa_value_to_atol_value[value] = atol_value
                    self[atol_field]["value_mapping"] = bpa_value_to_atol_value
                except KeyError as e:
                    logger.error(
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

    def map_value(self, atol_field, bpa_value):
        allowed_values = self.get_allowed_values(atol_field)
        # If there is no list of allowed values, then we don't have a
        # controlled vocabulary for this field, so we keep anything.
        if allowed_values is None:
            return bpa_value
        try:
            return self[atol_field]["value_mapping"][bpa_value]
        # This is a manual override for the pesky genome_data key. If the
        # package has no context_keys whose value is in accepted_data_context,
        # but it does have a key called "genome_data" with value "yes",
        # mapped_value is "genome_assembly".
        except KeyError as e:
            if atol_field == "data_context" and bpa_value == "yes":
                logger.debug("Value of {atol_field} is {bpa_value}.")
                return "genome_assembly"
            else:
                raise e

    def _sanitize_value(self, section, atol_field, value):
        """
        Apply sanitization rules to a value based on the sanitization config.

        Args:
            section (str): The section of the metadata (e.g., "organism", "experiment", "runs")
            atol_field (str): The AToL field name
            value: The value to sanitize

        Returns:
            tuple: (sanitized_value, applied_rules) where applied_rules is a list of rules that were actually applied
        """
        # If no sanitization config or section not in config, return original value
        if not self.sanitization_config or section not in self.sanitization_config:
            return value, []

        # If field not in section config, return original value
        if atol_field not in self.sanitization_config[section]:
            return value, []

        # Get sanitization rules for this field
        sanitization_rules = self.sanitization_config[section][atol_field]

        # If value is None, no sanitization needed
        if value is None:
            return value, []

        # Apply each sanitization rule in order
        sanitized_value = value
        applied_rules = []

        for rule in sanitization_rules:
            original_value_str = (
                str(sanitized_value) if sanitized_value is not None else None
            )

            if rule == "text_sanitization":
                # Strip double whitespace, unicode whitespace characters
                if isinstance(sanitized_value, str):
                    import re

                    # Replace multiple spaces with a single space
                    sanitized_value = re.sub(r"\s+", " ", sanitized_value)
                    # Strip leading/trailing whitespace
                    sanitized_value = sanitized_value.strip()

            elif rule == "empty_string_sanitization":
                # Convert empty strings to null
                if (
                    isinstance(sanitized_value, str)
                    and sanitized_value.strip().upper()
                    in self.sanitization_config["null_values"]
                ):
                    logger.debug(f"value {value} mapped to None")
                    sanitized_value = None

            elif rule == "integer_sanitization":
                # Ensure integer values, remove decimals
                if isinstance(sanitized_value, str) and sanitized_value.strip():
                    try:
                        # Try to convert to float first, then to int
                        sanitized_value = str(int(float(sanitized_value)))
                    except (ValueError, TypeError):
                        # If conversion fails, keep original value
                        pass

            # Check if this rule actually changed the value
            sanitized_value_str = (
                str(sanitized_value) if sanitized_value is not None else None
            )
            if original_value_str != sanitized_value_str:
                applied_rules.append(rule)

        return sanitized_value, applied_rules
