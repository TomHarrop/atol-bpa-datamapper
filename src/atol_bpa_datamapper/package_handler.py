from .logger import logger


class BpaPackage(dict):
    def __init__(self, package_data):
        super().__init__()
        logger.debug("Initialising BpaPackage")
        self.update(package_data)
        self.fields = sorted(set(self.keys()))
        self.id = self.get("id")
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
                    value, bpa_field, keep = ("yes", "genome_data", True)

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

    def map_metadata(self, metadata_map: "MetadataMap"):
        """Map BPA package metadata to AToL format."""
        logger.debug(f"Mapping BpaPackage {self.id}")
        
        # Initialize metadata sections
        mapped_metadata = {section: [] if section == "reads" else {} 
                         for section in metadata_map.metadata_sections}
        
        self.mapping_log = []
        self.field_mapping = {}

        # First handle non-reads sections
        for atol_field in metadata_map.expected_fields:
            section = metadata_map.get_atol_section(atol_field)
            if section != "reads":
                value, bpa_field, _ = self.choose_value(
                    metadata_map.get_bpa_fields(atol_field),
                    metadata_map.get_allowed_values(atol_field)
                )
                mapped_value = metadata_map.map_value(atol_field, value)
                mapped_metadata[section][atol_field] = mapped_value
                self.field_mapping[atol_field] = bpa_field
                
                self.mapping_log.append({
                    "atol_field": atol_field,
                    "bpa_field": bpa_field,
                    "value": value,
                    "mapped_value": mapped_value
                })
        
        # Handle reads section - group by resource
        resources = self.get("resources", [])
        reads_fields = [f for f in metadata_map.expected_fields 
                       if metadata_map.get_atol_section(f) == "reads"]
        
        # Create one reads object per resource
        for resource in resources:
            reads_obj = {}
            resource_id = resource.get("id")
            
            # Map all reads fields for this resource
            for atol_field in reads_fields:
                bpa_fields = metadata_map.get_bpa_fields(atol_field)
                value = None
                bpa_field = None
                
                # Try to get value from resource or package level
                for field in bpa_fields:
                    val = self.get_resource_value(resource, field)
                    if val is not None:
                        value = val
                        bpa_field = field
                        break
                    val = self.get(field)
                    if val is not None:
                        value = val
                        bpa_field = field
                        break
                
                # Map the value if found
                if value is not None:
                    mapped_value = metadata_map.map_value(atol_field, value)
                    reads_obj[atol_field] = mapped_value
                    self.field_mapping[atol_field] = bpa_field
                    
                    self.mapping_log.append({
                        "atol_field": atol_field,
                        "bpa_field": bpa_field,
                        "value": value,
                        "mapped_value": mapped_value,
                        "resource_id": resource_id
                    })
                else:
                    # Include the field with a null value
                    reads_obj[atol_field] = None
            
            # Add the complete reads object for this resource
            mapped_metadata["reads"].append(reads_obj)

        self.mapped_metadata = mapped_metadata
        self.unused_fields = [f for f in self.fields if f not in self.field_mapping.values()]
        
        logger.debug(f"Field mapping: {self.field_mapping}")
        logger.debug(f"Data mapping: {self.mapping_log}")
        logger.debug(f"Unused fields: {self.unused_fields}")

    def get_resource_value(self, resource, field):
        """Get a value from a specific resource."""
        return resource.get(field)

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
            if value is not None:
                if not accepted_values or value in accepted_values:
                    return (value, key, True)
                if first_value is None:
                    first_value = value
                    first_key = key

        return (first_value, first_key, False)


def get_nested_value(d, key):
    """
    Retrieve the value from a nested dictionary using a dot-notated key.
    """
    keys = key.split(".")
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return None
    return d
