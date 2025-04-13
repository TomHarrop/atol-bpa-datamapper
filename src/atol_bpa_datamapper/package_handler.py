from .logger import logger


class BpaPackage(dict):
    def __init__(self, package_data):
        super().__init__()
        logger.debug("Initialising BpaPackage")
        self.update(package_data)
        self.id = self.get("id")
        # Add bpa_id to the package data
        self["bpa_id"] = self.id
        self.fields = sorted(set(self.keys()))
        self.resource_ids = sorted(set(x["id"] for x in self.get("resources")))
        logger.debug(self.id)
        logger.debug(self.fields)
        logger.debug(self.resource_ids)

    def filter(self, metadata_map: "MetadataMap"):
        logger.debug(f"Filtering BpaPackage {self.id}")
        self.decisions = {}
        self.bpa_fields = {}
        self.bpa_values = {}
        for atol_field in metadata_map.controlled_vocabularies:
            logger.debug(f"Checking field {atol_field}")
            bpa_field_list = metadata_map[atol_field]["bpa_fields"]
            accepted_values = metadata_map.get_allowed_values(atol_field)
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
                    value, bpa_field, keep = ("genome_assembly", "genome_data", True)

            # record the field that was used in the bpa data
            self.bpa_fields[atol_field] = bpa_field
            # record the original BPA value
            self.bpa_values[atol_field] = self.get_nested_value(self, bpa_field) if bpa_field else None

            # record the decision for this field
            decision_key = f"{atol_field}_accepted"
            self.decisions[decision_key] = keep
            self.decisions[atol_field] = value

        logger.info(f"Decisions: {self.decisions}")
        self.keep = all(self.decisions[x] for x in self.decisions if x.endswith('_accepted'))
        logger.debug(f"Keep: {self.keep}")
        return self.keep

    def map_metadata(self, metadata_map: "MetadataMap"):
        """Map BPA package metadata to AToL format."""
        logger.debug(f"Mapping BpaPackage {self.id}")
        
        # Initialize metadata sections
        mapped_metadata = {section: [] if section == "reads" else {} 
                         for section in metadata_map.metadata_sections}
        
        self.mapping_log = []
        self.field_mapping = {}
        self.sanitization_changes = []

        # First handle non-reads sections
        for atol_field in metadata_map.expected_fields:
            section = metadata_map.get_atol_section(atol_field)
            if section != "reads":
                value, bpa_field, _ = self.choose_value(
                    metadata_map.get_bpa_fields(atol_field),
                    metadata_map.get_allowed_values(atol_field)
                )
                if value is not None:
                    # Get the original value directly from package
                    original_value = self.get_nested_value(self, bpa_field)
                    
                    # Track sanitization changes
                    original_str = str(original_value) if original_value is not None else None
                    sanitized_value = metadata_map._sanitize_value(section, atol_field, original_value)
                    sanitized_str = str(sanitized_value) if sanitized_value is not None else None
                    
                    # Record sanitization if the value was changed during sanitization
                    if original_str != sanitized_str:
                        self.sanitization_changes.append({
                            "bpa_id": self.id,
                            "field": atol_field,
                            "original_value": original_value,
                            "sanitized_value": sanitized_value
                        })
                    
                    # Store the mapped value
                    mapped_metadata[section][atol_field] = metadata_map.map_value(atol_field, sanitized_value)
                    self.field_mapping[atol_field] = bpa_field
                    
                    self.mapping_log.append({
                        "atol_field": atol_field,
                        "bpa_field": bpa_field,
                        "value": original_value,
                        "mapped_value": metadata_map.map_value(atol_field, sanitized_value)
                    })
        
        # Handle reads section
        if "resources" in self:
            for resource in self["resources"]:
                resource_id = resource["id"]
                resource_metadata = {}
                
                # Initialize all reads fields to None
                for atol_field in metadata_map.expected_fields:
                    if metadata_map.get_atol_section(atol_field) == "reads":
                        resource_metadata[atol_field] = None
                
                for atol_field in metadata_map.expected_fields:
                    section = metadata_map.get_atol_section(atol_field)
                    if section == "reads":
                        bpa_field_list = metadata_map.get_bpa_fields(atol_field)
                        # Remove "resources." prefix since we're already in a resource
                        bpa_field_list = [f.replace("resources.", "") for f in bpa_field_list]
                        value, bpa_field, _ = self.choose_value(
                            bpa_field_list,
                            metadata_map.get_allowed_values(atol_field),
                            resource
                        )
                        
                        if value is not None:
                            # Get the original value directly from resource
                            resource_field = bpa_field.replace("resources.", "")
                            original_value = self.get_nested_value(resource, resource_field)
                            
                            # Track sanitization changes
                            original_str = str(original_value) if original_value is not None else None
                            sanitized_value = metadata_map._sanitize_value(section, atol_field, original_value)
                            sanitized_str = str(sanitized_value) if sanitized_value is not None else None
                            
                            # Record sanitization if the value was changed during sanitization
                            if original_str != sanitized_str:
                                self.sanitization_changes.append({
                                    "bpa_id": self.id,
                                    "field": atol_field,
                                    "original_value": original_value,
                                    "sanitized_value": sanitized_value,
                                    "resource_id": resource_id
                                })
                            
                            # Store the mapped value
                            resource_metadata[atol_field] = metadata_map.map_value(atol_field, sanitized_value)
                            self.field_mapping[atol_field] = f"resources.{resource_field}"
                            
                            self.mapping_log.append({
                                "atol_field": atol_field,
                                "bpa_field": f"resources.{resource_field}",
                                "value": original_value,
                                "mapped_value": metadata_map.map_value(atol_field, sanitized_value),
                                "resource_id": resource_id
                            })
                
                # Always append resource metadata since we initialized all fields
                mapped_metadata["reads"].append(resource_metadata)

        self.mapped_metadata = mapped_metadata
        self.unused_fields = [f for f in self.fields if f not in self.field_mapping.values()]
        
        logger.debug(f"Field mapping: {self.field_mapping}")
        logger.debug(f"Data mapping: {self.mapping_log}")
        logger.debug(f"Unused fields: {self.unused_fields}")
        logger.debug(f"Sanitization changes: {self.sanitization_changes}")
        
        return mapped_metadata

    def get_nested_value(self, obj, field):
        """Get a value from a nested field."""
        if field is None:
            return None
            
        parts = field.split(".")
        current = obj
        
        for part in parts:
            if current is None:
                return None
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                # Try to get value from each item in the list
                for item in current:
                    if isinstance(item, dict):
                        value = item.get(part)
                        if value is not None:
                            current = value
                            break
                else:
                    return None
            else:
                return None
        return current

    def get_resource_value(self, resource, field):
        """Get a value from a specific resource."""
        if "." in field:
            # Handle nested fields
            parts = field.split(".")
            if parts[0] == "resources":
                # Remove "resources" prefix since we're already in a resource
                field = ".".join(parts[1:])
        return self.get_nested_value(resource, field)

    def choose_value(self, fields_to_check, accepted_values, resource=None):
        """
        Returns a tuple of (value, bpa_field, keep).

        fields_to_check is an ordered list of BPA fields to check.
        accepted_values is a dict mapping BPA values to AToL values.

        If accepted_values is None, then we don't have a controlled vocabulary
        for this field, and keep will always be True.

        If accepted_values is a dict, then:
        1. We look for any BPA field whose value appears as a key in accepted_values
        2. If found, we return the corresponding AToL value
        3. If not found, we return the first value we found, with keep=False

        If the package has no bpa_fields matching fields_to_check, the value
        and bpa_field is None.
        """
        if resource is None:
            values = {key: self.get_nested_value(self, key) for key in fields_to_check}
        else:
            values = {key: self.get_nested_value(resource, key) for key in fields_to_check}

        first_value = None
        first_key = None

        for key, value in values.items():
            if value is not None:
                # If no accepted values, accept anything
                if not accepted_values:
                    return (value, key, True)
                # Check if our value is in the accepted values dict
                if value in accepted_values:
                    return (accepted_values[value], key, True)
                # Keep track of first value for fallback
                if first_value is None:
                    first_value = value
                    first_key = key

        return (first_value, first_key, False)


def get_nested_value(d, key):
    """
    Retrieve the value from a nested dictionary using a dot-notated key.
    For resources, returns a list of all matching values from all resources.
    """
    # logger.info(f"Getting nested value for key {key} from {d}")
    keys = key.split(".")
    
    # Special case for resources - we want to check all resources
    if keys[0] == "resources" and len(keys) > 1:
        if not isinstance(d.get("resources"), list):
            return None
            
        # Get the nested key we want from each resource
        nested_key = keys[1]
        values = []
        for resource in d["resources"]:
            if isinstance(resource, dict) and nested_key in resource:
                values.append(resource[nested_key])
        
        # logger.info(f"Found resource values for {nested_key}: {values}")
        return values[0] if values else None  # Return first match if any
    
    # Normal case - traverse the dictionary
    for k in keys:
        if isinstance(d, dict):
            if k in d:
                d = d[k]
                #logger.info(f"Found dict value for {k}: {d}")
            else:
                #logger.info(f"Could not find key {k} in dict")
                return None
        else:
            #logger.info(f"Could not traverse {k}, parent is not a dict")
            return None
    return d
