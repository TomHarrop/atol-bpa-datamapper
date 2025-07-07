"""
Transform data from mapped metadata packages.

This script processes mapped metadata packages to:
1. Extract unique samples based on sample_name
2. Detect and report conflicts in sample attributes
3. Track which packages (bpa_package_id) relate to each unique sample
"""

from .arg_parser import parse_args_for_transform
from .io import read_mapped_data, write_json
from .logger import logger, setup_logger
import json
import os
from collections import defaultdict


class SampleTransformer:
    def __init__(self):
        self.unique_samples = {}
        self.sample_conflicts = {}
        self.package_to_sample_map = defaultdict(list)
        self.transformation_changes = []

    def process_package(self, package):
        """
        Process a single package to extract sample information.
        
        Args:
            package: A raw package dictionary with sample data
            
        Returns:
            bool: True if the package was processed successfully
        """
        # Get package ID from experiment section if available
        package_id = 'unknown'
        if "experiment" in package and "bpa_package_id" in package["experiment"]:
            package_id = package["experiment"]["bpa_package_id"]
        
        # Check if sample section exists
        if "sample" not in package:
            logger.warning(f"Package {package_id} has no sample section")
            return False
            
        sample_data = package["sample"]
        
        # Check if sample_name exists
        if "sample_name" not in sample_data:
            logger.warning(f"Package {package_id} has no sample_name")
            return False
            
        sample_name = sample_data["sample_name"]
        
        # Track which package relates to this sample
        self.package_to_sample_map[sample_name].append(package_id)
        
        # If this is a new sample, add it to unique samples
        if sample_name not in self.unique_samples:
            self.unique_samples[sample_name] = sample_data
            return True
            
        # If the sample already exists, check for conflicts
        existing_sample = self.unique_samples[sample_name]
        conflicts = self._check_conflicts(sample_name, existing_sample, sample_data)
        
        # Track the transformation change (sample merging)
        transformation_change = {
            "sample_name": sample_name,
            "package_id": package_id,
            "action": "merge",
            "conflicts": len(conflicts) > 0
        }
        self.transformation_changes.append(transformation_change)
        
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
            "package_to_sample_map": dict(self.package_to_sample_map),
            "transformation_changes": self.transformation_changes
        }


def main():
    """Main function to transform mapped metadata."""
    args = parse_args_for_transform()
    setup_logger(args.log_level)
    
    transformer = SampleTransformer()
    
    input_data = read_mapped_data(args.input)
    n_packages = 0
    n_processed = 0
    
    for package in input_data:
        logger.debug(f"Processing package {package.get('id', 'unknown')}")
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
        
        if args.transformation_changes:
            logger.info(f"Writing transformation changes to {args.transformation_changes}")
            write_json(results["transformation_changes"], args.transformation_changes)
    
    n_unique = len(results["unique_samples"])
    n_conflicts = len(results["sample_conflicts"])
    logger.info(f"Found {n_unique} unique samples")
    logger.info(f"Found {n_conflicts} samples with conflicts")


if __name__ == "__main__":
    main()
