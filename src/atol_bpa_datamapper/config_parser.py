import json


class MetadataMap(dict):
    def __init__(self, field_mapping_file, value_mapping_file):
        super().__init__()
        with open(field_mapping_file, "rt") as f:
            field_mapping = json.load(f)
        with open(value_mapping_file, "rt") as f:
            value_mapping = json.load(f)
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
        setattr(
            self, "metadata_sections", sorted(set(x["section"] for x in self.values()))
        )
        setattr(
            self,
            "controlled_vocabularies",
            [k for k in self.keys() if "value_mapping" in self[k]],
        )

    def get_allowed_values(self, atol_field):
        try:
            return sorted(set(self[atol_field]["value_mapping"].keys()))
        except KeyError as e:
            return None

    def get_bpa_fields(self, atol_field):
        return self[atol_field]["bpa_fields"]

    def keep_value(self, atol_field, bpa_value):
        allowed_values = self.get_allowed_values(atol_field)
        # If there is no list of allowed values, then we don't have a
        # controlled vocabulary for this field, so we keep anything.
        if not allowed_values:
            return True
        else:
            return bpa_value in allowed_values

    def map_value(self, atol_field, bpa_value):
        raise NotImplementedError("TODO: map_value(self, atol_field, bpa_value)")
