#!/usr/bin/env python

from .utils.common import parse_taxon_id


class OrganismSection(dict):

    def __init__(self, package_data, null_values=None):

        super().__init__()
        self.update(package_data)
        self.has_taxid = self.get("taxon_id") not in null_values + ["0", "0.0"]

        # get the taxon_id
        self.raw_taxon_id = self.get("taxon_id") if self.has_taxid else None
        self.taxon_id = None

        if self.raw_taxon_id is not None:
            self.format_taxon_id()

        if self.taxon_id is not None:
            self.organism_grouping_key = f"taxid{self.taxon_id}"

    def format_taxon_id(self):
        try:
            self.taxon_id = parse_taxon_id(self.raw_taxon_id)
        except (TypeError, ValueError):
            self.taxon_id = None
