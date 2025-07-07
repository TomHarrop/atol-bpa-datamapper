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
from datetime import datetime


class SampleTransformer:
    """
    Transform sample data from multiple packages into unique samples.
    """
    
    def __init__(self):
        """
        Initialize the SampleTransformer.
        """
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
        # Extract package ID and validate required fields
        package_id = package.get("experiment", {}).get("bpa_package_id", "unknown")
        
        if "sample" not in package:
            logger.warning(f"Package {package_id} has no sample section")
            return False
            
        sample_data = package["sample"]
        
        if "sample_name" not in sample_data:
            logger.warning(f"Package {package_id} has no sample_name")
            return False
            
        sample_name = sample_data["sample_name"]
        
        # Track package to sample map
        self.package_to_sample_map[package_id].append(sample_name)
        
        # Create transformation change record
        has_conflicts = False
        
        # Check if this is a new sample or if we need to check for conflicts
        if sample_name not in self.unique_samples:
            # New sample - just add it
            self.unique_samples[sample_name] = sample_data.copy()
        else:
            # Existing sample - check for conflicts
            existing_sample = self.unique_samples[sample_name]
            field_conflicts = self._check_conflicts(sample_name, existing_sample, sample_data)
            
            # If we found conflicts, add them to our conflicts dictionary
            if field_conflicts:
                self.sample_conflicts[sample_name] = field_conflicts
                has_conflicts = True
        
        # Add transformation change record
        transformation_change = {
            "sample_name": sample_name,
            "package_id": package_id,
            "action": "merge",
            "conflicts": has_conflicts
        }
        self.transformation_changes.append(transformation_change)
        
        return True
    def _check_conflicts(self, sample_name, existing_sample, new_sample):
        """
        Check for conflicts between existing and new sample data.
        
        Args:
            sample_name: The name of the sample
            existing_sample: The existing sample data
            new_sample: The new sample data
            
        Returns:
            dict: A dictionary of conflicts grouped by field
        """
        conflicts = {}
        
        # Skip sample_name field and only check fields that exist in both samples
        common_fields = set(new_sample.keys()) & set(existing_sample.keys()) - {'sample_name'}
        
        for field in common_fields:
            existing_value = existing_sample[field]
            new_value = new_sample[field]
            
            if existing_value != new_value:
                # Special handling for sample_access_date
                if field == "sample_access_date" and self._update_access_date(existing_sample, field, existing_value, new_value):
                    continue
                    
                # Add the conflict
                if field not in conflicts:
                    conflicts[field] = []
                
                # Add both values to the list of conflicting values if not already present
                for value in (existing_value, new_value):
                    if value not in conflicts[field]:
                        conflicts[field].append(value)
        
        return conflicts

    def _update_access_date(self, existing_sample, field, existing_value, new_value):
        """Helper method to handle sample_access_date special case"""
        try:
            # Try to parse the dates
            existing_date = datetime.fromisoformat(existing_value.split('T')[0] if 'T' in existing_value else existing_value)
            new_date = datetime.fromisoformat(new_value.split('T')[0] if 'T' in new_value else new_value)
            
            # Update to the most recent date
            if new_date > existing_date:
                logger.info(f"Updating sample_access_date from {existing_value} to {new_value}")
                existing_sample[field] = new_value
            
            # Successfully handled the date conflict
            return True
        except (ValueError, TypeError):
            # If we can't parse the dates, treat it as a normal conflict
            logger.warning(f"Could not parse dates for sample_access_date: {existing_value} and {new_value}")
            return False
    
    def get_results(self):
        """
        Get the results of the transformation.
        
        Returns:
            dict: A dictionary containing unique samples, conflicts, and package to sample map
        """
        # Remove samples with conflicts from unique_samples
        unique_samples_without_conflicts = {}
        for sample_name, sample_data in self.unique_samples.items():
            if sample_name not in self.sample_conflicts:
                unique_samples_without_conflicts[sample_name] = sample_data
            else:
                logger.info(f"Removing sample {sample_name} from output due to conflicts")
                
        return {
            "unique_samples": unique_samples_without_conflicts,
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
