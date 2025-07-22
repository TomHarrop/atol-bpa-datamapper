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


class OrganismTransformer:
    """
    Transform organism data from multiple packages into unique organisms.
    Organisms are identified by their organism_grouping_key, which is generated
    in the organism_mapper.py module.
    """
    
    def __init__(self, ignored_fields=None):
        self.unique_organisms = {}
        self.organism_conflicts = {}
        self.organism_to_package_map = defaultdict(list)
        self.transformation_changes = []
        self.ignored_fields = ignored_fields or []
    
    def process_package(self, package):
        """
        Process a single package to extract organism information.
        
        Args:
            package: A raw package dictionary with organism data
        
        Returns:
            bool: True if the package was processed successfully
        """
        package_id = package.get("experiment", {}).get("bpa_package_id", "unknown")
        
        if "organism" not in package:
            logger.warning(f"Package {package_id} has no organism section")
            return False
            
        organism_data = package["organism"]
        organism_key = organism_data.get("organism_grouping_key")
        
        if not organism_key:
            logger.warning(f"Package {package_id} has no organism_grouping_key")
            return False
            
        self.organism_to_package_map[organism_key].append(package_id)
        
        if organism_key in self.unique_organisms:
            conflicts, critical_conflicts = self._detect_conflicts(
                organism_key, self.unique_organisms[organism_key], organism_data, package_id
            )
            if conflicts:
                if organism_key not in self.organism_conflicts:
                    self.organism_conflicts[organism_key] = {}
                
                for field, conflict_values in conflicts.items():
                    if field not in self.organism_conflicts[organism_key]:
                        self.organism_conflicts[organism_key][field] = []
                    
                    for value in conflict_values:
                        if value not in self.organism_conflicts[organism_key][field]:
                            self.organism_conflicts[organism_key][field].append(value)
        else:
            self.unique_organisms[organism_key] = organism_data
            
            self.transformation_changes.append({
                "package_id": package_id,
                "organism_key": organism_key,
                "action": "add_organism",
                "data": organism_data
            })
            
        return True
        
    def _detect_conflicts(self, organism_key, existing_organism, new_organism, package_id):
        """
        Detect conflicts between existing and new organism data.
        
        Args:
            organism_key: The organism_grouping_key
            existing_organism: The existing organism data
            new_organism: The new organism data
            package_id: The package ID of the new data
            
        Returns:
            dict: A dictionary of conflicts, or empty dict if no conflicts
        """
        conflicts = {}
        critical_conflicts = False
        
        exclude_fields = ["organism_grouping_key"]
        
        for field, new_value in new_organism.items():
            if field in exclude_fields:
                continue
                
            if field in existing_organism:
                existing_value = existing_organism[field]
                
                if existing_value != new_value:
                    if field not in conflicts:
                        conflicts[field] = []
                    
                    if existing_value not in conflicts[field]:
                        conflicts[field].append(existing_value)
                    if new_value not in conflicts[field]:
                        conflicts[field].append(new_value)
                    
                    # If this field is not in the ignored list, mark as a critical conflict
                    if field not in self.ignored_fields:
                        critical_conflicts = True
                    else:
                        # For ignored fields with conflicts, set the value to null in the unique organism
                        existing_organism[field] = None
        
        # Return conflicts dictionary and whether there are critical conflicts
        return conflicts, critical_conflicts
        
    def get_results(self):
        """
        Get the results of the organism transformation.
        
        Returns:
            dict: A dictionary containing unique organisms, conflicts, and package to organism map
        """
        unique_organisms_without_critical_conflicts = {}
        for organism_key, organism_data in self.unique_organisms.items():
            # Check if this organism has any non-ignored conflicts
            has_critical_conflicts = False
            if organism_key in self.organism_conflicts:
                for field in self.organism_conflicts[organism_key]:
                    if field not in self.ignored_fields:
                        has_critical_conflicts = True
                        break
            
            if not has_critical_conflicts:
                unique_organisms_without_critical_conflicts[organism_key] = organism_data
            else:
                logger.info(f"Removing organism {organism_key} from output due to critical conflicts")
                
        return {
            "unique_organisms": unique_organisms_without_critical_conflicts,
            "organism_conflicts": self.organism_conflicts,
            "organism_package_map": dict(self.organism_to_package_map),
            "organism_transformation_changes": self.transformation_changes
        }


class SampleTransformer:
    """
    Transform sample data from multiple packages into unique samples.
    """
    
    def __init__(self, ignored_fields=None):
        """
        Initialize the SampleTransformer.
        
        Args:
            ignored_fields: List of field names that should be ignored when determining uniqueness
                           (conflicts in these fields will still be reported but won't prevent
                           inclusion in the unique samples list)
        """
        self.unique_samples = {}
        self.sample_conflicts = {}
        self.sample_to_package_map = defaultdict(list)
        self.transformation_changes = []
        self.ignored_fields = ignored_fields or []

    def process_package(self, package):
        """
        Process a single package to extract sample information.
        
        Args:
            package: A raw package dictionary with sample data
        
        Returns:
            bool: True if the package was processed successfully
        """
        package_id = package.get("experiment", {}).get("bpa_package_id", "unknown")
        
        if "sample" not in package:
            logger.warning(f"Package {package_id} has no sample section")
            return False
            
        sample_data = package["sample"]
        
        if "sample_name" not in sample_data:
            logger.warning(f"Package {package_id} has no sample_name")
            return False
            
        sample_name = sample_data["sample_name"]
        
        # Track sample to package map
        self.sample_to_package_map[sample_name].append(package_id)
        
        # Create transformation change record
        has_conflicts = False
        has_critical_conflicts = False
        
        # Check if this is a new sample or if we need to check for conflicts
        if sample_name not in self.unique_samples:
            # New sample - just add it
            self.unique_samples[sample_name] = sample_data.copy()
        else:
            # Existing sample - check for conflicts
            existing_sample = self.unique_samples[sample_name]
            field_conflicts, has_critical_conflicts = self._check_conflicts(sample_name, existing_sample, sample_data)
            
            # If we found conflicts, add them to our conflicts dictionary
            if field_conflicts:
                self.sample_conflicts[sample_name] = field_conflicts
                has_conflicts = True
        
        # Add transformation change record
        transformation_change = {
            "sample_name": sample_name,
            "package_id": package_id,
            "action": "merge",
            "conflicts": has_conflicts,
            "critical_conflicts": has_critical_conflicts
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
            tuple: (conflicts_dict, has_critical_conflicts)
                  conflicts_dict: A dictionary of conflicts grouped by field
                  has_critical_conflicts: Boolean indicating if there are conflicts in non-ignored fields
        """
        conflicts = {}
        has_critical_conflicts = False
        
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
                
                # Check if this is a critical conflict (not in ignored fields)
                if field not in self.ignored_fields:
                    has_critical_conflicts = True
                else:
                    # For ignored fields with conflicts, set the value to null in the existing sample
                    existing_sample[field] = None
        
        return conflicts, has_critical_conflicts

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
        # Remove samples with critical conflicts from unique_samples
        unique_samples_without_critical_conflicts = {}
        for sample_name, sample_data in self.unique_samples.items():
            # Check if this sample has any non-ignored conflicts
            has_critical_conflicts = False
            if sample_name in self.sample_conflicts:
                for field in self.sample_conflicts[sample_name]:
                    if field not in self.ignored_fields:
                        has_critical_conflicts = True
                        break
            
            if not has_critical_conflicts:
                unique_samples_without_critical_conflicts[sample_name] = sample_data
            else:
                logger.info(f"Removing sample {sample_name} from output due to critical conflicts")
                
        return {
            "unique_samples": unique_samples_without_critical_conflicts,
            "sample_conflicts": self.sample_conflicts,
            "package_map": dict(self.sample_to_package_map),
            "transformation_changes": self.transformation_changes
        }


def main():
    """Main function to transform mapped metadata."""
    args = parse_args_for_transform()
    setup_logger(args.log_level)
    
    # Parse ignored fields if provided
    sample_ignored_fields = []
    organism_ignored_fields = []
    if hasattr(args, 'sample_ignored_fields') and args.sample_ignored_fields:
        sample_ignored_fields = args.sample_ignored_fields.split(',')
        logger.info(f"Ignoring sample fields: {sample_ignored_fields}")
    
    if hasattr(args, 'organism_ignored_fields') and args.organism_ignored_fields:
        organism_ignored_fields = args.organism_ignored_fields.split(',')
        logger.info(f"Ignoring organism fields: {organism_ignored_fields}")
    
    sample_transformer = SampleTransformer(ignored_fields=sample_ignored_fields)
    organism_transformer = OrganismTransformer(ignored_fields=organism_ignored_fields)
    
    input_data = read_mapped_data(args.input)
    n_packages = 0
    n_processed_samples = 0
    n_processed_organisms = 0
    
    for package in input_data:
        package_id = package.get('id', 'unknown')
        logger.debug(f"Processing package {package_id}")
        n_packages += 1
        
        if sample_transformer.process_package(package):
            n_processed_samples += 1
        
        if organism_transformer.process_package(package):
            n_processed_organisms += 1
    
    logger.info(f"Processed {n_packages} packages")
    logger.info(f"Extracted sample data from {n_processed_samples} packages")
    logger.info(f"Extracted organism data from {n_processed_organisms} packages")
    
    sample_results = sample_transformer.get_results()
    organism_results = organism_transformer.get_results()
    
    if not args.dry_run:
        # Write sample outputs
        if args.output:
            logger.info(f"Writing unique samples to {args.output}")
            write_json(sample_results["unique_samples"], args.output)
        
        if args.sample_conflicts:
            logger.info(f"Writing sample conflicts to {args.sample_conflicts}")
            write_json(sample_results["sample_conflicts"], args.sample_conflicts)
        
        if args.sample_package_map:
            logger.info(f"Writing sample to package map to {args.sample_package_map}")
            write_json(sample_results["package_map"], args.sample_package_map)
        
        if args.transformation_changes:
            logger.info(f"Writing transformation changes to {args.transformation_changes}")
            write_json(sample_results["transformation_changes"], args.transformation_changes)
        
        # Write organism outputs
        if args.unique_organisms:
            logger.info(f"Writing unique organisms to {args.unique_organisms}")
            write_json(organism_results["unique_organisms"], args.unique_organisms)
        
        if args.organism_conflicts:
            logger.info(f"Writing organism conflicts to {args.organism_conflicts}")
            write_json(organism_results["organism_conflicts"], args.organism_conflicts)
        
        if args.organism_package_map:
            logger.info(f"Writing organism to package map to {args.organism_package_map}")
            write_json(organism_results["organism_package_map"], args.organism_package_map)
    
    # Log summary statistics
    n_unique_samples = len(sample_results["unique_samples"])
    n_sample_conflicts = len(sample_results["sample_conflicts"])
    n_unique_organisms = len(organism_results["unique_organisms"])
    n_organism_conflicts = len(organism_results["organism_conflicts"])
    
    logger.info(f"Found {n_unique_samples} unique samples")
    logger.info(f"Found {n_sample_conflicts} samples with conflicts")
    logger.info(f"Found {n_unique_organisms} unique organisms")
    logger.info(f"Found {n_organism_conflicts} organisms with conflicts")


if __name__ == "__main__":
    main()
