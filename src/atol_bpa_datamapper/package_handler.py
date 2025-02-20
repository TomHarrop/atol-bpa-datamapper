from .logger import logger


class BpaPackage(dict):
    def __init__(self, package_data):
        super().__init__()
        self.update(package_data)
        self.fields = sorted(set(self.keys()))

    def filter(self, metadata_map: "MetadataMap"):
        self.decisions = {}
        self.bpa_fields = {}
        self.bpa_values = {}
        for atol_field in metadata_map.controlled_vocabularies:
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
        self.keep = all(x for x in self.decisions.values() if isinstance(x, bool))

    def choose_value(self, fields_to_check, accepted_values):
        """
        Returns a tuple of (chosen_value, accept_value).

        Package is a dict parsed from json.

        keys_to_check is an ordered list.

        If package has any keys_to_check whose value is in accepted_values, the
        value of that keys_to_check is returned and accept_value is True.

        If the package has no keys_to_check whose value is in accepted_values, the
        value of the first keys_to_check is returned.

        If the package has keys_to_check at all, the value is None.

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
