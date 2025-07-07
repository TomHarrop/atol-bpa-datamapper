"""
Transform data from mapped metadata packages.

This script processes mapped metadata packages to:
1. Extract unique samples based on sample_name
2. Detect and report conflicts in sample attributes
3. Track which packages (bpa_package_id) relate to each unique sample
"""

from .arg_parser import parse_args_for_transform
from .io import read_input, write_json
from .logger import logger, setup_logger
import json
import os
from collections import defaultdict


class SampleTransformer:
    def __init__(self):
        self.unique_samples = {}
        self.sample_conflicts = {}
        self.package_to_sample_map = defaultdict(list)

    def process_package(self, package):
        """
        Process a single package to extract sample information.
        
        Args:
            package: A package object with mapped_metadata
            
        Returns:
            bool: True if the package was processed successfully
        """

        if "sample" not in package.mapped_metadata:
            logger.warning(f"Package {package.id} has no sample section")
            return False
            
        sample_data = package.mapped_metadata["sample"]
        
        if "sample_name" not in sample_data:
            logger.warning(f"Package {package.id} has no sample_name")
            return False
            
        sample_name = sample_data["sample_name"]
        
        package_id = package.id
        if "experiment" in package.mapped_metadata:
            if "bpa_package_id" in package.mapped_metadata["experiment"]:
                package_id = package.mapped_metadata["experiment"]["bpa_package_id"]
        
        # Track which package relates to this sample
        self.package_to_sample_map[sample_name].append(package_id)
        
        # If this is a new sample, add it to unique samples
        if sample_name not in self.unique_samples:
            self.unique_samples[sample_name] = sample_data
            return True
            
        # If the sample already exists, check for conflicts
        existing_sample = self.unique_samples[sample_name]
        conflicts = self._check_conflicts(sample_name, existing_sample, sample_data)
        
        if conflicts:
            if sample_name not in self.sample_conflicts:
                self.sample_conflicts[sample_name] = []
            self.sample_conflicts[sample_name].extend(conflicts)
            
        return True
    
    def _check_conflicts(self, sample_name, existing_sample, new_sample):
        """
        Check for conflicts between existing and new sample data.
        
        Args:
            sample_name: The name of the sample
            existing_sample: The existing sample data
            new_sample: The new sample data
            
        Returns:
            list: A list of conflict records
        """
        conflicts = []
        
        for field, new_value in new_sample.items():
            if field == "sample_name":
                continue
                
            # If the field exists in the existing sample, check for conflicts
            if field in existing_sample:
                existing_value = existing_sample[field]
                
                # If the values are different, record a conflict
                if existing_value != new_value:
                    conflicts.append({
                        "sample_name": sample_name,
                        "field": field,
                        "existing_value": existing_value,
                        "new_value": new_value
                    })
        
        return conflicts
    
    def get_results(self):
        return {
            "unique_samples": self.unique_samples,
            "sample_conflicts": self.sample_conflicts,
            "package_to_sample_map": dict(self.package_to_sample_map)
        }


def main():
    """Main function to transform mapped metadata."""
    args = parse_args_for_transform()
    setup_logger(args.log_level)
    
    transformer = SampleTransformer()
    
    input_data = read_input(args.input)
    n_packages = 0
    n_processed = 0
    
    for package in input_data:
        logger.debug(f"Processing package {package.id}")
        n_packages += 1
        
        if transformer.process_package(package):
            n_processed += 1
    
    logger.info(f"Processed {n_processed} of {n_packages} packages")
    
    results = transformer.get_results()
    
    if not args.dry_run:
        if args.output:
            logger.info(f"Writing unique samples to {args.output}")
            write_json(results["unique_samples"], args.output)
        
        if args.conflicts:
            logger.info(f"Writing sample conflicts to {args.conflicts}")
            write_json(results["sample_conflicts"], args.conflicts)
        
        if args.package_map:
            logger.info(f"Writing package to sample map to {args.package_map}")
            write_json(results["package_to_sample_map"], args.package_map)
    
    n_unique = len(results["unique_samples"])
    n_conflicts = len(results["sample_conflicts"])
    logger.info(f"Found {n_unique} unique samples")
    logger.info(f"Found {n_conflicts} samples with conflicts")


if __name__ == "__main__":
    main()
