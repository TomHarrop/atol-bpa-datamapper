"""
Transform data from mapped metadata packages.

This script processes mapped metadata packages to:
1. Extract unique samples based on sample_name
2. Detect and report conflicts in sample attributes
3. Track which packages (bpa_package_id) relate to each unique sample
"""

import logging
import json
import gzip
import sys
import jsonlines
import os
from collections import defaultdict

from atol_bpa_datamapper.arg_parser import parse_args_for_transform


class SampleTransformer:
    def __init__(self):
        self.unique_samples = {}
        self.sample_conflicts = {}
        self.package_map = defaultdict(list)

    def process_package(self, package):
        if "sample" not in package:
            return
        
        sample = package.get("sample")
        if not sample:
            return
        
        sample_name = sample.get("sample_name")
        if not sample_name:
            return
        
        package_id = None
        if "experiment" in package and "bpa_package_id" in package["experiment"]:
            package_id = package["experiment"]["bpa_package_id"]
        else:
            package_id = package.get("id")
        
        if package_id:
            if sample_name not in self.package_map:
                self.package_map[sample_name] = []
            if package_id not in self.package_map[sample_name]:
                self.package_map[sample_name].append(package_id)
        
        if sample_name not in self.unique_samples:
            self.unique_samples[sample_name] = sample
            return
            
        existing_sample = self.unique_samples[sample_name]
        conflicts = self._check_conflicts(sample_name, existing_sample, sample)
        
        if conflicts:
            if sample_name not in self.sample_conflicts:
                self.sample_conflicts[sample_name] = []
            self.sample_conflicts[sample_name].extend(conflicts)
            
        return True
    
    def _check_conflicts(self, sample_name, existing_sample, new_sample):
        conflicts = []
        
        for field, new_value in new_sample.items():
            if field == "sample_name":
                continue
                
            if field in existing_sample:
                existing_value = existing_sample[field]
                
                if existing_value != new_value:
                    conflicts.append({
                        "sample_name": sample_name,
                        "field": field,
                        "existing_value": existing_value,
                        "new_value": new_value
                    })
        
        return conflicts
    
    def get_unique_samples(self):
        return self.unique_samples
    
    def get_conflicts(self):
        return self.sample_conflicts
    
    def get_package_map(self):
        return dict(self.package_map)


def read_mapped_data(input_source):
    with gzip.open(input_source, "rt") as f:
        reader = jsonlines.Reader(f)
        for obj in reader:
            yield obj

def main():
    args = parse_args_for_transform()
    
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("atol_bpa_datamapper")
    
    transformer = SampleTransformer()
    
    logger.info(f"Reading input from {args.input}")
    input_data = read_mapped_data(args.input)
    
    for package in input_data:
        transformer.process_package(package)
    
    unique_samples = transformer.get_unique_samples()
    conflicts = transformer.get_conflicts()
    package_map = transformer.get_package_map()
    
    if not args.dry_run:
        logger.info(f"Writing {len(unique_samples)} unique samples to {args.output}")
        with gzip.open(args.output, "wt") as f:
            json.dump(unique_samples, f)
        
        if args.conflicts:
            logger.info(f"Writing {len(conflicts)} conflicts to {args.conflicts}")
            with gzip.open(args.conflicts, "wt") as f:
                json.dump(conflicts, f)
        
        if args.package_map:
            logger.info(f"Writing package map to {args.package_map}")
            with gzip.open(args.package_map, "wt") as f:
                json.dump(package_map, f)
    else:
        logger.info("Dry run, not writing output files")
        logger.info(f"Would write {len(unique_samples)} unique samples to {args.output}")
        logger.info(f"Would write {len(conflicts)} conflicts to {args.conflicts}")
        logger.info(f"Would write package map to {args.package_map}")


if __name__ == "__main__":
    main()
