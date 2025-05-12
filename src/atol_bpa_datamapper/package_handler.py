from .logger import logger


class BpaBase(dict):
    def __init__(self, data):
        super().__init__()
        self.update(data)
        self.fields = sorted(set(self.keys()))
        self.id = self.get("id")
        # add bpa_id as a field
        self["bpa_id"] = self.id

    def choose_value(self, fields_to_check, accepted_values, parent_package=None):
        """
        Returns a tuple of (value, bpa_field, keep).

        fields_to_check is an ordered list.

        If accepted_values is None, then we don't have a controlled vocabulary
        for this field, and keep will always be True.

        If accepted_values is a list, then keep will be True if the value of
        the selected bpa_field is in the list of accepted_values.

        If package has any fields_to_check whose value is in accepted_values,
        the value and the bpa_field are returned and accept_value is True.

        If the package has no fields_to_check whose value is in
        accepted_values, the first bpa_field and its value are returned.

        If the package has no bpa_fields matching fields_to_check, the value
        and bpa_field is None.

        If the parent_package is not None, this is a Resource (not a Package),
        and we have to strip the `resource` prefix from the field in the
        metadata schema. We also have to check the parent object for the
        required metadata.
        """
        logger.debug(
            f"choose_value for field {fields_to_check}. Controlled vocab: {accepted_values}"
        )

        # if there is a parent package, this is a resource, and we need to strip the prefixes
        if parent_package is not None:
            fields_to_check = [x.split(".")[-1] for x in fields_to_check]
            parent_values = {
                key: get_nested_value(parent_package, key) for key in fields_to_check
            }

        values = {key: get_nested_value(self, key) for key in fields_to_check}

        # if we have values from the parent, we have to combine them
        if parent_package is not None and parent_values:
            my_values = {}
            for k, v in values.items():
                my_values[k] = None

                if (v is not None) and not (v.strip == ""):
                    my_values[k] = v
                    continue

                if (parent_values[k] is not None) and not (
                    parent_values[k].strip == ""
                ):
                    my_values[k] = parent_values[k]

            values = my_values

        first_value = None
        first_key = None

        for key, value in values.items():
            # Skip None values and empty strings
            if value is None or (isinstance(value, str) and value.strip() == ""):
                continue

            if not accepted_values or value in accepted_values:
                return (value, key, True)
            if first_value is None:
                first_value = value
                first_key = key

        return (first_value, first_key, False)

    def filter(self, metadata_map: "MetadataMap", parent_package=None):
        logger.debug(f"Filtering {type(self).__name__} {self.id}")
        self.decisions = {}
        self.bpa_fields = {}
        self.bpa_values = {}

        for atol_field in metadata_map.controlled_vocabularies:

            logger.debug(f"Checking field {atol_field}...")

            bpa_field_list = metadata_map[atol_field]["bpa_fields"]
            accepted_values = metadata_map.get_allowed_values(atol_field)

            logger.debug(f"  for values {accepted_values}...")
            logger.debug(f"  in BPA fields {bpa_field_list}.")

            value, bpa_field, keep = self.choose_value(
                bpa_field_list, accepted_values, parent_package
            )

            # This is a manual override for the pesky genome_data key. If the
            # package has no context_keys whose value is in
            # accepted_data_context, but it does have a key called
            # "genome_data" with value "yes", keep_package is True.
            if (
                atol_field == "data_context"
                and "genome_data" in self.fields
                and not keep
            ):
                logger.debug("Checking genome_data field")
                if self["genome_data"] == "yes":
                    logger.debug("Setting keep to True")
                    value, bpa_field, keep = ("yes", "genome_data", True)

            # Summarise the value choice
            logger.debug(
                (
                    f"Found value {value} "
                    f"for atol_field {atol_field} "
                    f"in bpa_field {bpa_field}. "
                    f"Keep is {keep}."
                )
            )

            # record the field that was used in the bpa data
            self.bpa_fields[atol_field] = bpa_field
            self.bpa_values[atol_field] = value

            # record the decision for this field
            decision_key = f"{atol_field}_accepted"
            self.decisions[decision_key] = keep
            self.decisions[atol_field] = value

        # summarise the decision for this package
        logger.debug(f"Decisions: {self.decisions}")
        self.keep = all(x for x in self.decisions.values() if isinstance(x, bool))
        logger.debug(f"Keep: {self.keep}")

    def map_metadata(self, metadata_map: "MetadataMap", parent_package=None):
        """Map BPA package metadata to AToL format, handling resources properly."""
        logger.debug(f"Mapping BpaPackage {self.id}")

        mapped_metadata = {k: {} for k in metadata_map.metadata_sections}

        self.mapping_log = []
        self.field_mapping = {}
        self.sanitization_changes = []

        for atol_field in metadata_map.expected_fields:
            section = metadata_map.get_atol_section(atol_field)
            value, bpa_field, keep = self.choose_value(
                metadata_map.get_bpa_fields(atol_field),
                metadata_map.get_allowed_values(atol_field),
                parent_package,
            )

            # Summarise the value choice
            logger.debug(
                (
                    f"Found value {value} "
                    f"for atol_field {atol_field} "
                    f"in bpa_field {bpa_field}. "
                    f"Keep is {keep}."
                )
            )

            if value is not None and bpa_field is not None:
                # Get the original value directly from package
                original_value = get_nested_value(self, bpa_field)

                if isinstance(original_value, list) and len(original_value) > 1:
                    raise NotImplementedError(
                        (
                            f"Found different values for bpa_field {bpa_field} "
                            f"when trying to map atol_field {atol_field} for Package {self.id}. "
                            "Choosing between different values for the same field is not implemented.\n"
                            f"{self}"
                        )
                    )

                # Apply sanitization rules
                sanitized_value = self._apply_sanitization(
                    metadata_map, section, atol_field, original_value
                )

                # Map the sanitized value
                try:
                    mapped_value = metadata_map.map_value(atol_field, sanitized_value)
                except KeyError as e:
                    # Handle invalid values gracefully
                    logger.warning(
                        f"Invalid value '{sanitized_value}' for field '{atol_field}': {e}"
                    )
                    mapped_value = sanitized_value

                mapped_metadata[section][atol_field] = mapped_value
                self.field_mapping[atol_field] = bpa_field

                self.mapping_log.append(
                    {
                        "atol_field": atol_field,
                        "bpa_field": bpa_field,
                        "value": original_value,
                        "sanitized_value": sanitized_value,
                        "mapped_value": mapped_value,
                    }
                )

        # Store the mapped metadata
        self.mapped_metadata = mapped_metadata

        # Track fields that weren't used
        self.unused_fields = [
            field
            for field in self.fields
            if field
            not in [
                self.field_mapping.get(atol_field) for atol_field in self.field_mapping
            ]
        ]

        return mapped_metadata

    def _apply_sanitization(self, metadata_map, section, atol_field, original_value):
        """
        Apply sanitization rules to a value and record changes if any.

        Args:
            metadata_map: The MetadataMap instance
            section: The section of the metadata (e.g., "organism", "reads")
            atol_field: The AToL field name
            original_value: The value to sanitize

        Returns:
            The sanitized value
        """
        # Apply sanitization rules
        original_str = str(original_value) if original_value is not None else None
        sanitized_value, applied_rules = metadata_map._sanitize_value(
            section, atol_field, original_value
        )
        sanitized_str = str(sanitized_value) if sanitized_value is not None else None

        # Record sanitization if the value was changed during sanitization
        if original_str != sanitized_str:
            sanitization_record = {
                "bpa_id": self.id,
                "field": atol_field,
                "original_value": original_value,
                "sanitized_value": sanitized_value,
                "applied_rules": applied_rules,
            }

            self.sanitization_changes.append(sanitization_record)

        return sanitized_value


class BpaResource(BpaBase):
    def __init__(self, resource_data):
        logger.debug("Initialising BpaResource")
        super().__init__(resource_data)


class BpaPackage(BpaBase):
    def __init__(self, package_data):
        logger.debug("Initialising BpaPackage")
        super().__init__(package_data)

        # Generate a list of Resources for this Package
        self.resources = {}
        self.resource_ids = set()
        for resource in self.get("resources"):
            self.resources[resource["id"]] = BpaResource(resource)
            self.resource_ids.add(resource["id"])

        logger.debug(self.id)
        logger.debug(self.fields)
        logger.debug(self.resource_ids)


def get_nested_value(d, key):
    """
    Retrieve the value from a nested dictionary or list using a dot-notated
    key.

    This function traverses a nested data structure (dictionaries and lists) to
    retrieve the value corresponding to the specified dot-notated key.

    If the key points to a list of dictionaries, the function will attempt to
    extract the value from each dictionary in the list. Values of None will be
    removed.

    If multiple values are found, a warning is logged, and a list of values is
    returned.

    Args:
        d (dict or list): The nested data structure to search. key (str): The
        dot-notated key specifying the path to the desired value.

    Returns:
        Any: The value corresponding to the key, or None if the key is not
        found.

    Behaviour:
        - If `d` or `key` is None, the function returns None.
        - If the key points to a dictionary, the function retrieves the value
          directly.
        - If the key points to a list of dictionaries, the function iterates
          over the list and retrieves the value from each dictionary.
        - If multiple non-None values are found in a list, a warning is logged,
          and a list of values is returned.
        - If no value is found, the function returns None.

    """
    if d is None or key is None:
        return None

    keys = key.split(".", 1)

    if len(keys) > 1:
        logger.debug(f"Potential nested key {key}")
        logger.debug(d[keys[0]])

    current_key = keys[0]
    remaining_keys = keys[1] if len(keys) > 1 else None

    if isinstance(d, dict):
        if current_key in d and remaining_keys is None:
            return d[current_key]
        if current_key in d and remaining_keys:
            return get_nested_value(d[current_key], remaining_keys)
    # Iterate over lists (e.g. list of resources)
    elif isinstance(d, list):
        results = [
            get_nested_value(item, remaining_keys)
            for item in d
            if isinstance(item, dict)
        ]
        filtered_results = sorted(set(x for x in results if x is not None))
        if len(filtered_results) > 1:
            logger.warning("Resources have different values for key {current_key}")
            logger.warning(filtered_results)
        return filtered_results if filtered_results else None

    return None
