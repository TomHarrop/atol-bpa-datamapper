import json


class ConfigParser(dict):
    def __init__(self, field_mapping_file, value_mapping_file):
        super().__init__()
        with open(field_mapping_file, "rt") as f:
            field_mapping = json.load(f)
        with open(value_mapping_file, "rt") as f:
            value_mapping = json.load(f)

        for atol_section, mapping_dict in field_mapping.items():
            for atol_field, bpa_field_list in mapping_dict.items():
                self[atol_field] = {}
                self[atol_field]["bpa_fields"] = bpa_field_list
                self[atol_field]["section"] = atol_section

        for atol_section, mapping_dict in value_mapping.items():
            for atol_field, value_mapping_dict in mapping_dict.items():
                print(atol_field)
                print(value_mapping_dict)
                print(self[atol_field])
                # TODO: add value mapping to self[atol_field]["value_mapping"]
                quit(1)
            print(mapping_dict)
            quit(1)

        print(field_mapping.keys())
        print(field_mapping["organism"])
        print(value_mapping.keys())

        quit(1)
