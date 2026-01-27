"""
Transform data from mapped metadata packages.

This script processes mapped metadata packages to:
1. Extract unique samples based on bpa_sample_id
2. Detect and report conflicts in sample attributes
3. Track which packages (bpa_package_id) relate to each unique sample
4. Extract unique organisms based on organism_grouping_key
"""

from .arg_parser import parse_args_for_transform
from .io import read_jsonl_file, write_json
from .logger import logger, setup_logger
import json
import os
import gzip
from collections import defaultdict
from datetime import datetime
from abc import ABC, abstractmethod


class EntityTransformer(ABC):
    """
    Abstract base class for transforming entity data from multiple packages
    into unique entities.
    """

    def __init__(self, entity_type, key_fields, ignored_fields=None):
        """
        Args:
            entity_type: Logical entity type label (e.g. 'organism', 'sample', 'specimen')
            key_fields: Field name OR list/tuple of field names that make up the unique identifier
        """
        self.entity_type = entity_type

        # Handle multiple keys e.g. for specimen (taxon_id, specimen_id)
        if isinstance(key_fields, (list, tuple)):
            self.key_fields = list(key_fields)
        else:
            self.key_fields = [key_fields]

        self.unique_entities = {}
        self.entity_conflicts = {}
        self.entity_to_package_map = defaultdict(list)
        self.transformation_changes = []
        self.ignored_fields = ignored_fields or []

        self.exclude_fields = list(self.key_fields)

    def _get_entity_data(self, package):
        """
        For extracting entity data from a package.

        Default behavior expects a top-level section matching self.entity_type.
        Override when entity data is derived (e.g. specimen derived from
        sample+organism).
        """
        data = package.get(self.entity_type)
        return data if isinstance(data, dict) else None

    def _normalize_entity_key(self, raw_key):
        """
        Normalize entity keys for internal storage:
          - str -> stripped str
          - (a, b) / [a, b] -> tuple(a, b) (with stripped strings)
        Returns None when invalid.
        """
        if raw_key is None:
            return None

        if isinstance(raw_key, str):
            key = raw_key.strip()
            return key or None

        if isinstance(raw_key, (list, tuple)):
            parts = []
            for p in raw_key:
                if p is None:
                    return None
                if isinstance(p, str):
                    sp = p.strip()
                    if not sp:
                        return None
                    parts.append(sp)
                else:
                    parts.append(p)
            return tuple(parts)

        # Fallback: ensure hashable
        try:
            hash(raw_key)
        except TypeError:
            return None
        return raw_key

    def process_package(self, package):
        """
        Process a single package to extract entity information.
        """
        package_id = package.get("experiment", {}).get("bpa_package_id", "unknown")

        # Get entity data (may be derived)
        entity_data = self._get_entity_data(package)
        if not entity_data:
            logger.warning(
                f"No {self.entity_type} data found/derived in package {package_id}, skipping"
            )
            return False

        # Get and normalize entity key (string or tuple)
        raw_key = self._get_entity_key(entity_data)
        entity_key = self._normalize_entity_key(raw_key)
        if entity_key is None:
            logger.warning(
                f"No valid {self.entity_type} key found in package {package_id}, skipping"
            )
            return False

        # Track entity to package map
        self.entity_to_package_map[entity_key].append(package_id)

        # Process entity-specific data before conflict detection
        self._pre_process_entity(entity_key, entity_data, package_id)

        # Check for conflicts or add as new entity
        has_conflicts = False
        has_critical_conflicts = False

        if entity_key in self.unique_entities:
            existing_entity = self.unique_entities[entity_key]
            conflicts, has_critical_conflicts = self._detect_conflicts(
                entity_key, existing_entity, entity_data, package_id
            )

            if conflicts:
                has_conflicts = True
                if entity_key not in self.entity_conflicts:
                    self.entity_conflicts[entity_key] = {}

                for field, conflict_values in conflicts.items():
                    if field not in self.entity_conflicts[entity_key]:
                        self.entity_conflicts[entity_key][field] = []

                    for value in conflict_values:
                        if value not in self.entity_conflicts[entity_key][field]:
                            self.entity_conflicts[entity_key][field].append(value)
        else:
            self.unique_entities[entity_key] = entity_data.copy()
            self._record_new_entity(entity_key, entity_data, package_id)

        if entity_key in self.unique_entities and entity_key != package_id:
            self._record_entity_change(
                entity_key, package_id, has_conflicts, has_critical_conflicts
            )

        return True

    def _detect_conflicts(self, entity_key, existing_entity, new_entity, package_id):
        """
        Detect conflicts between existing and new entity data.

        Args:
            entity_key: The entity key (identifier)
            existing_entity: The existing entity data
            new_entity: The new entity data
            package_id: The package ID of the new data

        Returns:
            tuple: (conflicts_dict, has_critical_conflicts)
                  conflicts_dict: A dictionary of conflicts grouped by field
                  has_critical_conflicts: Boolean indicating if there are conflicts in non-ignored fields
        """
        conflicts = {}
        has_critical_conflicts = False

        # Find common fields, excluding the key field
        common_fields = set(new_entity.keys()) & set(existing_entity.keys())
        common_fields = common_fields - set(self.exclude_fields)

        for field in common_fields:
            existing_value = existing_entity[field]
            new_value = new_entity[field]

            if existing_value != new_value:
                # Check for field-specific handling
                if self._handle_special_field(
                    existing_entity, field, existing_value, new_value
                ):
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
                    # For ignored fields with conflicts, set the value to null in the existing entity
                    existing_entity[field] = None

        return conflicts, has_critical_conflicts

    def get_results(self):
        """
        Get the results of the entity transformation.

        Returns:
            dict: A dictionary containing unique entities, conflicts, and package to entity map
        """
        # Remove entities with critical conflicts
        unique_entities_without_critical_conflicts = {}
        for entity_key, entity_data in self.unique_entities.items():
            # Check if this entity has any non-ignored conflicts
            has_critical_conflicts = False
            if entity_key in self.entity_conflicts:
                for field in self.entity_conflicts[entity_key]:
                    if field not in self.ignored_fields:
                        has_critical_conflicts = True
                        break

            if not has_critical_conflicts:
                unique_entities_without_critical_conflicts[entity_key] = entity_data
            else:
                logger.info(
                    f"Removing {self.entity_type} {entity_key} from output due to critical conflicts"
                )

        # Build the results dictionary with entity-specific keys
        results = self._build_results(unique_entities_without_critical_conflicts)
        return results

    @abstractmethod
    def _get_entity_key(self, entity_data):
        """
        Extract the entity key from the entity data.

        Args:
            entity_data: The entity data dictionary

        Returns:
            str: The entity key (identifier)
        """
        pass

    @abstractmethod
    def _build_results(self, unique_entities):
        """
        Build the results dictionary with entity-specific keys.

        Args:
            unique_entities: Dictionary of unique entities without critical conflicts

        Returns:
            dict: Results dictionary with entity-specific keys
        """
        pass

    def _pre_process_entity(self, entity_key, entity_data, package_id):
        """
        Perform any entity-specific pre-processing before conflict detection.

        Args:
            entity_key: The entity key (identifier)
            entity_data: The entity data dictionary
            package_id: The package ID
        """
        # Default implementation does nothing
        pass

    def _handle_special_field(self, existing_entity, field, existing_value, new_value):
        """
        Handle special fields that require custom conflict resolution.

        Args:
            existing_entity: The existing entity data
            field: The field name
            existing_value: The existing value
            new_value: The new value

        Returns:
            bool: True if the conflict was handled, False otherwise
        """
        # Default implementation does not handle any special fields
        return False

    def _record_new_entity(self, entity_key, entity_data, package_id):
        """
        Record a transformation change for a new entity.

        Args:
            entity_key: The entity key (identifier)
            entity_data: The entity data dictionary
            package_id: The package ID
        """
        # Default implementation adds a generic record
        self.transformation_changes.append(
            {
                "package_id": package_id,
                f"{self.entity_type}_key": entity_key,
                "action": f"add_{self.entity_type}",
                "data": entity_data,
            }
        )

    def _record_entity_change(
        self, entity_key, package_id, has_conflicts, has_critical_conflicts
    ):
        """
        Record a transformation change for an existing entity.

        Args:
            entity_key: The entity key (identifier)
            package_id: The package ID
            has_conflicts: Whether there are any conflicts
            has_critical_conflicts: Whether there are any critical conflicts
        """
        # Default implementation adds a generic record
        self.transformation_changes.append(
            {
                f"{self.entity_type}_key": entity_key,
                "package_id": package_id,
                "action": "merge",
                "conflicts": has_conflicts,
                "critical_conflicts": has_critical_conflicts,
            }
        )


class OrganismTransformer(EntityTransformer):
    """
    Transform organism data from multiple packages into unique organisms.
    Organisms are identified by their organism_grouping_key, which is generated
    in the organism_mapper.py module.
    """

    def __init__(self, ignored_fields=None):
        super().__init__("organism", "organism_grouping_key", ignored_fields)

    def _get_entity_key(self, entity_data):
        return entity_data.get("organism_grouping_key")

    def _build_results(self, unique_entities):
        return {
            "unique_organisms": unique_entities,
            "organism_conflicts": self.entity_conflicts,
            "organism_package_map": dict(self.entity_to_package_map),
            "organism_transformation_changes": self.transformation_changes,
        }


class SampleTransformer(EntityTransformer):
    """
    Transform sample data from multiple packages into unique samples.
    Samples are identified by their bpa_sample_id.
    """

    def __init__(self, ignored_fields=None):
        super().__init__("sample", "bpa_sample_id", ignored_fields)

    def _get_entity_key(self, entity_data):
        return entity_data.get("bpa_sample_id")

    def _pre_process_entity(self, entity_key, entity_data, package_id):
        """
        Pre-process the entity data before adding it to unique entities.
        This is where we add the organism_grouping_key to the sample data.
        """
        # Check if there's an organism section in the package
        package = None
        for pkg in self._get_package_by_id(package_id):
            package = pkg
            break

        if (
            package
            and "organism" in package
            and "organism_grouping_key" in package["organism"]
        ):
            organism_key = package["organism"]["organism_grouping_key"]

            # If this is a new sample, add the organism key directly
            if entity_key not in self.unique_entities:
                entity_data["organism_grouping_key"] = organism_key
            else:
                # If the sample already exists, check if we need to handle a conflict
                existing_entity = self.unique_entities[entity_key]
                if "organism_grouping_key" in existing_entity:
                    existing_key = existing_entity["organism_grouping_key"]
                    if existing_key != organism_key:
                        # We have a conflict - record it
                        if entity_key not in self.entity_conflicts:
                            self.entity_conflicts[entity_key] = {}

                        if (
                            "organism_grouping_key"
                            not in self.entity_conflicts[entity_key]
                        ):
                            self.entity_conflicts[entity_key][
                                "organism_grouping_key"
                            ] = []

                        for key in [existing_key, organism_key]:
                            if (
                                key
                                not in self.entity_conflicts[entity_key][
                                    "organism_grouping_key"
                                ]
                            ):
                                self.entity_conflicts[entity_key][
                                    "organism_grouping_key"
                                ].append(key)

                        # Set to None if there's a conflict and organism_grouping_key is in ignored fields
                        # Otherwise, it will be treated as a critical conflict and the sample will be excluded
                        if "organism_grouping_key" in self.ignored_fields:
                            existing_entity["organism_grouping_key"] = None
                        logger.warning(
                            f"Sample {entity_key} is associated with multiple organisms: {existing_key} and {organism_key}"
                        )
                else:
                    # No organism key yet, add it
                    existing_entity["organism_grouping_key"] = organism_key

    def _get_package_by_id(self, package_id):
        """
        Helper method to find a package by its ID.
        This is used to get the organism data for a sample.
        """
        # This is a generator that yields packages with the given ID
        # In a real implementation, you would have a way to look up packages by ID
        # For now, we'll use a simple approach that works with our test cases
        from inspect import currentframe

        frame = currentframe()
        while frame:
            if "package" in frame.f_locals and isinstance(
                frame.f_locals["package"], dict
            ):
                pkg = frame.f_locals["package"]
                pkg_id = pkg.get("experiment", {}).get("bpa_package_id", None)
                if pkg_id == package_id:
                    yield pkg
            frame = frame.f_back

    def _handle_special_field(self, existing_entity, field, existing_value, new_value):
        # Special handling for sample_access_date
        if field == "sample_access_date":
            return self._update_access_date(
                existing_entity, field, existing_value, new_value
            )
        return False

    def _update_access_date(self, existing_entity, field, existing_value, new_value):
        """Helper method to handle sample_access_date special case"""
        try:
            # Try to parse the dates
            existing_date = datetime.fromisoformat(
                existing_value.split("T")[0]
                if "T" in existing_value
                else existing_value
            )
            new_date = datetime.fromisoformat(
                new_value.split("T")[0] if "T" in new_value else new_value
            )

            # Update to the most recent date
            if new_date > existing_date:
                logger.info(
                    f"Updating sample_access_date from {existing_value} to {new_value}"
                )
                existing_entity[field] = new_value

            # Successfully handled the date conflict
            return True
        except (ValueError, TypeError):
            # If we can't parse the dates, treat it as a normal conflict
            logger.warning(
                f"Could not parse dates for sample_access_date: {existing_value} and {new_value}"
            )
            return False

    def _record_entity_change(
        self, entity_key, package_id, has_conflicts, has_critical_conflicts
    ):
        # Override to use bpa_sample_id instead of sample_key
        self.transformation_changes.append(
            {
                "bpa_sample_id": entity_key,
                "package_id": package_id,
                "action": "merge",
                "conflicts": has_conflicts,
                "critical_conflicts": has_critical_conflicts,
            }
        )

    def _build_results(self, unique_entities):
        return {
            "unique_samples": unique_entities,
            "sample_conflicts": self.entity_conflicts,
            "package_map": dict(self.entity_to_package_map),
            "transformation_changes": self.transformation_changes,
        }


class SpecimenTransformer(EntityTransformer):
    """
    Transform specimen data derived from packages into unique specimens.

    Key: (taxon_id, specimen_id)
    Source fields:
      - specimen_id from package['sample']
      - taxon_id from package['organism']
    Metadata: for now, use the entire sample section (agnostic / TBD subset later).
    """

    def __init__(self, ignored_fields=None):
        super().__init__("specimen", ["taxon_id", "specimen_id"], ignored_fields)

    def _get_entity_data(self, package):
        sample = package.get("sample")
        organism = package.get("organism")

        if not isinstance(sample, dict) or not isinstance(organism, dict):
            return None

        taxon_id = organism.get("taxon_id")
        specimen_id = sample.get("specimen_id")

        if not taxon_id or not specimen_id:
            return None

        merged = sample.copy()
        merged["taxon_id"] = taxon_id
        return merged

    def _get_entity_key(self, entity_data):
        return (entity_data.get("taxon_id"), entity_data.get("specimen_id"))

    def _record_new_entity(self, entity_key, entity_data, package_id):
        taxon_id, specimen_id = entity_key
        self.transformation_changes.append(
            {
                "package_id": package_id,
                "taxon_id": taxon_id,
                "specimen_id": specimen_id,
                "action": "add_specimen",
                "data": entity_data,
            }
        )

    def _record_entity_change(
        self, entity_key, package_id, has_conflicts, has_critical_conflicts
    ):
        taxon_id, specimen_id = entity_key
        self.transformation_changes.append(
            {
                "package_id": package_id,
                "taxon_id": taxon_id,
                "specimen_id": specimen_id,
                "action": "merge",
                "conflicts": has_conflicts,
                "critical_conflicts": has_critical_conflicts,
            }
        )

    def _build_results(self, unique_entities):
        # JSON-safe nested dicts: taxon_id -> specimen_id -> data
        unique_specimens = {}
        specimen_conflicts = {}
        specimen_package_map = {}

        for (taxon_id, specimen_id), data in unique_entities.items():
            tkey = str(taxon_id)
            skey = str(specimen_id)
            unique_specimens.setdefault(tkey, {})[skey] = data

        for (taxon_id, specimen_id), conflicts in self.entity_conflicts.items():
            tkey = str(taxon_id)
            skey = str(specimen_id)
            specimen_conflicts.setdefault(tkey, {})[skey] = conflicts

        for (taxon_id, specimen_id), packages in self.entity_to_package_map.items():
            tkey = str(taxon_id)
            skey = str(specimen_id)
            specimen_package_map.setdefault(tkey, {})[skey] = packages

        return {
            "unique_specimens": unique_specimens,
            "specimen_conflicts": specimen_conflicts,
            "specimen_package_map": specimen_package_map,
            "specimen_transformation_changes": self.transformation_changes,
        }


def extract_experiment(experiments_data, package):

    logger.debug(f"Processing package: {package}")
    try:
        # Skip if no experiment section
        if "experiment" not in package:
            logger.warning(f"No experiment section found in package, skipping")
            return

        # Create experiment object with all experiment fields
        experiment = package["experiment"].copy()

        # Skip if no bpa_sample_id in sample section
        if "sample" not in package or "bpa_sample_id" not in package["sample"]:
            logger.warning(f"No bpa_sample_id found in package, skipping")
            return

        bpa_sample_id = package["sample"]["bpa_sample_id"]

        # Skip if no bpa_package_id
        if "bpa_package_id" not in experiment:
            logger.warning(f"No bpa_package_id found in experiment, skipping")
            return

        # Get the bpa_package_id to use as key
        bpa_package_id = experiment["bpa_package_id"]

        # Add runs if present
        if "runs" in package:
            experiment["runs"] = package["runs"]
        else:
            experiment["runs"] = []

        # Add bpa_sample_id to experiment for linking in database
        experiment["bpa_sample_id"] = bpa_sample_id

        # Add to dictionary with bpa_package_id as key
        experiments_data[bpa_package_id] = experiment
    except json.JSONDecodeError:
        logger.error(f"Line {line_count}: Invalid JSON, skipping")
    except Exception as e:
        logger.error(f"Error processing package: {str(e)}")


def get_transformer(transformer_type, args, ignored_fields):

    user_ignored_fields = vars(args).get(ignored_fields, None)
    ignored_fields_list = []

    if user_ignored_fields is not None:
        ignored_fields_list = user_ignored_fields.split(",")
        logger.info(f"Ignoring fields in {transformer_type}: {ignored_fields_list}")

    return transformer_type(ignored_fields=ignored_fields_list)


def main():
    """Main function to transform mapped metadata."""
    args = parse_args_for_transform()
    setup_logger(args.log_level)

    sample_transformer = get_transformer(
        SampleTransformer,
        args,
        "sample_ignored_fields",
    )
    organism_transformer = get_transformer(
        OrganismTransformer, args, "organism_ignored_fields"
    )
    specimen_transformer = get_transformer(
        SpecimenTransformer, args, "specimen_ignored_fields"
    )

    input_data = read_jsonl_file(args.input)
    n_packages = 0
    n_processed_samples = 0
    n_processed_organisms = 0
    n_processed_experiments = 0
    n_processed_specimens = 0

    experiments_data = {}

    for package in input_data:
        package_id = package.get("id", "unknown")
        logger.debug(f"Processing package {package_id}")
        n_packages += 1

        if sample_transformer.process_package(package):
            n_processed_samples += 1

        if organism_transformer.process_package(package):
            n_processed_organisms += 1

        if specimen_transformer.process_package(package):
            n_processed_specimens += 1

        extract_experiment(experiments_data, package)
        n_processed_experiments += 1

    logger.info(f"Processed {n_packages} packages")
    logger.info(f"Extracted sample data from {n_processed_samples} packages")
    logger.info(f"Extracted organism data from {n_processed_organisms} packages")
    logger.info(f"Extracted experiment data from {n_processed_experiments} packages")

    sample_results = sample_transformer.get_results()
    organism_results = organism_transformer.get_results()
    specimen_results = specimen_transformer.get_results()

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
            logger.info(
                f"Writing transformation changes to {args.transformation_changes}"
            )
            write_json(
                sample_results["transformation_changes"], args.transformation_changes
            )

        # Write organism outputs
        if args.unique_organisms:
            logger.info(f"Writing unique organisms to {args.unique_organisms}")
            write_json(organism_results["unique_organisms"], args.unique_organisms)

        if args.organism_conflicts:
            logger.info(f"Writing organism conflicts to {args.organism_conflicts}")
            write_json(organism_results["organism_conflicts"], args.organism_conflicts)

        if args.organism_package_map:
            logger.info(
                f"Writing organism to package map to {args.organism_package_map}"
            )
            write_json(
                organism_results["organism_package_map"], args.organism_package_map
            )
        if args.experiments_output:
            logger.info(f"Writing experiments data to {args.experiments_output}")
            write_json(experiments_data, args.experiments_output)

    # Log summary statistics
    n_unique_samples = len(sample_results["unique_samples"])
    n_sample_conflicts = len(sample_results["sample_conflicts"])
    n_unique_organisms = len(organism_results["unique_organisms"])
    n_organism_conflicts = len(organism_results["organism_conflicts"])

    logger.info(f"Found {n_unique_samples} unique samples")
    logger.info(f"Found {n_sample_conflicts} samples with conflicts")
    logger.info(f"Found {n_unique_organisms} unique organisms")
    logger.info(f"Found {n_organism_conflicts} organisms with conflicts")
    logger.info(f"Found {len(experiments_data)} experiments")


if __name__ == "__main__":
    main()
