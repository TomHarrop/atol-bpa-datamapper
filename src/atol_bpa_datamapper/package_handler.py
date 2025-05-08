from .logger import logger


class BpaBase(dict):
    def __init__(self, data):
        super().__init__()
        self.update(data)
        self.fields = sorted(set(self.keys()))
        self.id = self.get("id")
        # add bpa_id as a field
        self["bpa_id"] = self.id

    def filter(self, metadata_map: "MetadataMap"):
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

            value, bpa_field, keep = self.choose_value(bpa_field_list, accepted_values)

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

    def choose_value(self, fields_to_check, accepted_values):
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
        """
        values = {key: get_nested_value(self, key) for key in fields_to_check}

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


class BpaResource(BpaBase):
    def __init__(self, resource_data):
        logger.debug("Initialising BpaResource")
        super().__init__(resource_data)

    # We can handle parent lookups here
    def filter(self, metadata_map: "MetadataMap"):
        raise NotImplementedError("Called the BpaResource filter method")
        super().filter(metadata_map)


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

    def map_metadata(self, metadata_map: "MetadataMap"):
        """Map BPA package metadata to AToL format, handling resources properly."""
        logger.debug(f"Mapping BpaPackage {self.id}")

        # Define sections that should be treated as resource-level (list type)
        resource_sections = ["runs"]

        # Initialize metadata sections
        # Use a list for resource-level sections, dictionaries for other sections
        mapped_metadata = {
            section: [] if section in resource_sections else {}
            for section in metadata_map.metadata_sections
        }

        self.mapping_log = []
        self.field_mapping = {}
        self.sanitization_changes = []

        # First handle non-resource sections
        for atol_field in metadata_map.expected_fields:
            section = metadata_map.get_atol_section(atol_field)
            if section not in resource_sections:
                value, bpa_field, keep = self.choose_value(
                    metadata_map.get_bpa_fields(atol_field),
                    metadata_map.get_allowed_values(atol_field),
                )

                if value is not None and bpa_field is not None:
                    # Get the original value directly from package
                    original_value = get_nested_value(self, bpa_field)

                    # Apply sanitization rules
                    sanitized_value = self._apply_sanitization(
                        metadata_map, section, atol_field, original_value
                    )

                    # Map the sanitized value
                    try:
                        mapped_value = metadata_map.map_value(
                            atol_field, sanitized_value
                        )
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

        # Handle resource-level sections - map each resource to an entry in the appropriate list
        if "resources" in self and self["resources"]:
            for resource in self["resources"]:
                resource_id = resource.get("id")
                if not resource_id:
                    continue

                # Create a dictionary for each resource section
                resource_metadata = {section: {} for section in resource_sections}

                # Track if this resource has any invalid controlled vocabulary values
                has_invalid_cv_value = False

                # Process each field for this resource
                for atol_field in metadata_map.expected_fields:
                    section = metadata_map.get_atol_section(atol_field)
                    if section in resource_sections:
                        # Get the field list and separate resource and parent fields
                        bpa_field_list = metadata_map.get_bpa_fields(atol_field)
                        resource_fields = [
                            f.replace("resources.", "")
                            for f in bpa_field_list
                            if f.startswith("resources.")
                        ]
                        parent_fields = [
                            f for f in bpa_field_list if not f.startswith("resources.")
                        ]

                        # First try to get value from resource fields
                        value, bpa_field, keep = self.choose_value(
                            resource_fields,
                            metadata_map.get_allowed_values(atol_field),
                            resource,
                        )

                        # If no value found in resource, try parent fields
                        if value is None and parent_fields:
                            value, bpa_field, keep = self.choose_value(
                                parent_fields,
                                metadata_map.get_allowed_values(atol_field),
                            )
                            # Record that this value came from parent
                            source = "parent"
                            # Get the original value directly from package
                            original_value = (
                                get_nested_value(self, bpa_field) if bpa_field else None
                            )
                        else:
                            # Record that this value came from resource
                            source = "resource"
                            # Get the original value directly from resource
                            original_value = (
                                get_nested_value(resource, bpa_field)
                                if bpa_field
                                else None
                            )

                        # Check if this is a controlled vocabulary field
                        is_cv_field = (
                            metadata_map.get_allowed_values(atol_field) is not None
                        )

                        # Map the value if found
                        if value is not None and bpa_field is not None:
                            # Apply sanitization rules
                            sanitized_value = self._apply_sanitization(
                                metadata_map,
                                section,
                                atol_field,
                                original_value,
                                resource_id,
                            )

                            # Map the sanitized value
                            try:
                                mapped_value = metadata_map.map_value(
                                    atol_field, sanitized_value
                                )
                            except KeyError as e:
                                # Handle invalid values gracefully
                                logger.warning(
                                    f"Invalid value '{sanitized_value}' for field '{atol_field}': {e}"
                                )
                                mapped_value = sanitized_value
                                # If this is a controlled vocabulary field and the value is invalid,
                                # mark this resource as having an invalid CV value
                                if is_cv_field:
                                    has_invalid_cv_value = True

                            resource_metadata[section][atol_field] = mapped_value

                            # Record the correct field mapping path
                            if source == "resource":
                                self.field_mapping[atol_field] = (
                                    f"resources.{bpa_field}"
                                )
                            else:
                                self.field_mapping[atol_field] = bpa_field

                            # Add to mapping log with resource information
                            self.mapping_log.append(
                                {
                                    "atol_field": atol_field,
                                    "bpa_field": bpa_field,
                                    "value": original_value,
                                    "sanitized_value": sanitized_value,
                                    "mapped_value": mapped_value,
                                    "resource_id": resource_id,
                                    "source": source,
                                }
                            )

                # Skip resources with invalid controlled vocabulary values
                if has_invalid_cv_value:
                    logger.debug(
                        f"Skipping resource {resource_id} as it has invalid controlled vocabulary values"
                    )
                    continue

                # Add the resource metadata to the appropriate section lists if not empty
                for section in resource_sections:
                    if resource_metadata[section]:
                        # Add resource_id to the metadata
                        resource_metadata[section]["resource_id"] = resource_id
                        mapped_metadata[section].append(resource_metadata[section])

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

    def _apply_sanitization(
        self, metadata_map, section, atol_field, original_value, resource_id=None
    ):
        """
        Apply sanitization rules to a value and record changes if any.

        Args:
            metadata_map: The MetadataMap instance
            section: The section of the metadata (e.g., "organism", "reads")
            atol_field: The AToL field name
            original_value: The value to sanitize
            resource_id: Optional resource ID for resource-level fields

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

            # Add resource_id if provided
            if resource_id is not None:
                sanitization_record["resource_id"] = resource_id

            self.sanitization_changes.append(sanitization_record)

        return sanitized_value


def get_nested_value(d, key):
    """
    Retrieve the value from a nested dictionary using a dot-notated key.
    """
    if d is None or key is None:
        return None

    keys = key.split(".")
    current = d

    for k in keys:
        if isinstance(current, dict) and k in current:
            current = current[k]
        else:
            return None

    return current
