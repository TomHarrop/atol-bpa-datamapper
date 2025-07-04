"""
Base framework for processing BPA packages with shared functionality.
"""

from abc import ABC, abstractmethod
from collections import Counter
from .config_parser import MetadataMap
from .io import read_input, OutputWriter
from .logger import logger


class BaseProcessor(ABC):
    """Base class for processing BPA packages."""
    
    def __init__(self, args):
        self.args = args
        self.setup_metadata_maps()
        self.setup_counters()
        self.setup_logs()
        
    def setup_metadata_maps(self):
        """Set up metadata mapping configurations."""
        self.package_level_map = MetadataMap(
            self.args.package_field_mapping_file, 
            self.args.value_mapping_file
        )
        self.resource_level_map = MetadataMap(
            self.args.resource_field_mapping_file, 
            self.args.value_mapping_file
        )
        
    @abstractmethod
    def setup_counters(self):
        """Set up counters specific to the processor type."""
        pass
        
    @abstractmethod
    def setup_logs(self):
        """Set up logging structures specific to the processor type."""
        pass
        
    @abstractmethod
    def process_package(self, package):
        """Process a single package. Returns True if package should be kept."""
        pass
        
    @abstractmethod
    def process_resource(self, resource, package):
        """Process a single resource within a package."""
        pass
        
    @abstractmethod
    def write_optional_outputs(self):
        """Write optional output files specific to the processor."""
        pass
        
    def run(self):
        """Main processing loop."""
        input_data = read_input(self.args.input)
        n_packages = 0
        n_kept = 0
        
        with OutputWriter(self.args.output, self.args.dry_run) as output_writer:
            for package in input_data:
                n_packages += 1
                logger.debug(f"Processing package {package.id}")
                
                # Track raw field usage
                self.counters["raw_field_usage"].update(package.fields)
                
                # Process package-level metadata
                keep_package = self.process_package(package)
                
                # Process resources
                kept_resources = self.process_resources(package)
                
                # Handle package-level decisions
                if self.should_keep_package(package, kept_resources):
                    if hasattr(self, 'output_processor'):
                        output_data = self.output_processor(package)
                    else:
                        output_data = package
                    output_writer.write_data(output_data)
                    n_kept += 1
                    
                # Check iteration limits
                if hasattr(self.args, 'max_iterations') and self.args.max_iterations:
                    if n_packages >= self.args.max_iterations:
                        break
                        
        logger.info(f"Processed {n_packages} packages")
        logger.info(f"Kept {n_kept} packages")
        
        # Write optional outputs
        if not self.args.dry_run:
            self.write_optional_outputs()
            
    def process_resources(self, package):
        """Process all resources for a package."""
        kept_resources = []
        dropped_resources = []
        
        for resource_id, resource in package.resources.items():
            self.process_resource(resource, package)
            
            if resource.keep is True:
                kept_resources.append(resource.id)
            elif resource.keep is False:
                dropped_resources.append(resource.id)
                
        # Clean up dropped resources
        for resource_id in dropped_resources:
            package.resources.pop(resource_id)
            
        return kept_resources
        
    def should_keep_package(self, package, kept_resources):
        """Determine if a package should be kept based on processing results."""
        if len(kept_resources) > 0:
            package["resources"] = [
                package.resources[resource_id] for resource_id in kept_resources
            ]
            if hasattr(package, 'decisions'):
                package.decisions["kept_resources"] = True
            return getattr(package, 'keep', True)
        else:
            if hasattr(package, 'decisions'):
                package.decisions["kept_resources"] = False
            package.keep = False
            return False


class FilterProcessor(BaseProcessor):
    """Processor for filtering packages based on controlled vocabularies."""
    
    def setup_counters(self):
        all_controlled_vocabularies = sorted(
            set(
                self.package_level_map.controlled_vocabularies
                + self.resource_level_map.controlled_vocabularies
            )
        )
        self.counters = {
            "raw_field_usage": Counter(),
            "bpa_field_usage": {
                atol_field: Counter() for atol_field in all_controlled_vocabularies
            },
            "bpa_value_usage": {
                atol_field: Counter() for atol_field in all_controlled_vocabularies
            },
        }
        
    def setup_logs(self):
        self.decision_log = {}
        
    def process_package(self, package):
        """Filter package-level metadata."""
        package.filter(self.package_level_map)
        
        # Update counters
        for atol_field, bpa_field in package.bpa_fields.items():
            self.counters["bpa_field_usage"][atol_field].update([bpa_field])
        for atol_field, bpa_value in package.bpa_values.items():
            self.counters["bpa_value_usage"][atol_field].update([bpa_value])
            
        return package.keep
        
    def process_resource(self, resource, package):
        """Filter resource-level metadata."""
        resource.filter(self.resource_level_map, package)
        
    def should_keep_package(self, package, kept_resources):
        """Override to handle decision logging."""
        keep = super().should_keep_package(package, kept_resources)
        self.decision_log[package.id] = package.decisions
        return keep
        
    def write_optional_outputs(self):
        """Write filtering-specific output files."""
        from .io import write_decision_log_to_csv, write_json
        
        if self.args.decision_log:
            logger.info(f"Writing decision log to {self.args.decision_log}")
            write_decision_log_to_csv(self.decision_log, self.args.decision_log)
        if self.args.raw_field_usage:
            logger.info(f"Writing field usage counts to {self.args.raw_field_usage}")
            write_json(self.counters["raw_field_usage"], self.args.raw_field_usage)
        if self.args.bpa_field_usage:
            logger.info(f"Writing BPA key usage counts to {self.args.bpa_field_usage}")
            write_json(self.counters["bpa_field_usage"], self.args.bpa_field_usage)
        if self.args.bpa_value_usage:
            logger.info(f"Writing BPA value usage counts to {self.args.bpa_value_usage}")
            write_json(self.counters["bpa_value_usage"], self.args.bpa_value_usage)


class MappingProcessor(BaseProcessor):
    """Processor for mapping packages to AToL format."""
    
    def setup_counters(self):
        all_fields = sorted(
            set(self.package_level_map.expected_fields + self.resource_level_map.expected_fields)
        )
        self.counters = {
            "raw_field_usage": Counter(),
            "raw_value_usage": {},
            "mapped_field_usage": {atol_field: Counter() for atol_field in all_fields},
            "mapped_value_usage": {atol_field: Counter() for atol_field in all_fields},
            "unused_field_counts": Counter(),
        }
        
    def setup_logs(self):
        self.mapping_log = {}
        self.grouping_log = {}
        self.grouped_packages = {}
        self.sanitization_changes = {}
        
        # Set up taxonomy data for organism mapping
        from .organism_mapper import NcbiTaxdump
        self.ncbi_taxdump = NcbiTaxdump(
            self.args.nodes,
            self.args.names,
            self.args.cache_dir,
            resolve_to_rank="species",
        )
        self.null_values = self.package_level_map.sanitization_config.get("null_values")
        
    def process_package(self, package):
        """Map package-level metadata."""
        # Track raw value usage
        for bpa_field in package.fields:
            if bpa_field not in self.counters["raw_value_usage"]:
                self.counters["raw_value_usage"][bpa_field] = Counter()
            try:
                self.counters["raw_value_usage"][bpa_field].update([package[bpa_field]])
            except TypeError:
                pass
                
        # Map metadata
        package.map_metadata(self.package_level_map)
        self.mapping_log[package.id] = package.mapping_log
        
        # Handle organism mapping
        self.process_organism_mapping(package)
        
        return True  # Mapping doesn't filter packages
        
    def process_organism_mapping(self, package):
        """Handle organism-specific mapping logic."""
        from .organism_mapper import OrganismSection
        
        organism_section = OrganismSection(
            package.id,
            package.mapped_metadata["organism"],
            self.ncbi_taxdump,
            self.null_values,
        )
        
        self.grouping_log[package.id] = [organism_section.mapped_metadata]
        grouping_key = organism_section.organism_grouping_key
        
        if grouping_key is not None:
            self.grouped_packages.setdefault(grouping_key, []).append(package.id)
            
        # Update organism metadata
        for key, value in organism_section.mapped_metadata.items():
            if key in self.package_level_map.expected_fields:
                try:
                    current_value = package.mapped_metadata["organism"][key]
                except KeyError:
                    current_value = None
                    
                if not value == current_value:
                    logger.debug(f"Updating organism key {key} from {current_value} to {value}")
                    package.mapped_metadata["organism"][key] = value
                    
    def process_resource(self, resource, package):
        """Map resource-level metadata."""
        resource.map_metadata(self.resource_level_map, package)
        
        # Collect resource metadata by section
        if not hasattr(self, '_resource_metadata_buffer'):
            self._resource_metadata_buffer = {}
            
        package_id = package.id
        if package_id not in self._resource_metadata_buffer:
            self._resource_metadata_buffer[package_id] = {
                section: [] for section in self.resource_level_map.metadata_sections
            }
            
        for section in self.resource_level_map.metadata_sections:
            self._resource_metadata_buffer[package_id][section].append(
                resource.mapped_metadata[section]
            )
            
    def should_keep_package(self, package, kept_resources):
        """Override to handle resource metadata consolidation."""
        keep = super().should_keep_package(package, kept_resources)
        
        # Consolidate resource metadata
        if hasattr(self, '_resource_metadata_buffer') and package.id in self._resource_metadata_buffer:
            for section, resource_metadata in self._resource_metadata_buffer[package.id].items():
                package.mapped_metadata[section] = resource_metadata
                
        # Track additional metrics
        if hasattr(package, "sanitization_changes") and package.sanitization_changes:
            self.sanitization_changes[package.id] = package.sanitization_changes
            
        self.counters["unused_field_counts"].update(package.unused_fields)
        
        # Update field/value usage counters
        self.update_usage_counters(package)
        
        return keep
        
    def update_usage_counters(self, package):
        """Update field and value usage counters."""
        for section_name, section in package.mapped_metadata.items():
            if isinstance(section, list):
                section = section[0] if section else {}
                
            for atol_field, mapped_value in section.items():
                self.counters["mapped_value_usage"][atol_field].update([mapped_value])
                
                # Track field mapping
                bpa_field = None
                if atol_field in package.field_mapping:
                    bpa_field = package.field_mapping[atol_field]
                else:
                    bpa_fields = {
                        x.field_mapping[atol_field]
                        for x in package.resources.values()
                        if atol_field in x.field_mapping
                    }
                    if bpa_fields:
                        bpa_field = sorted(bpa_fields)[0]
                        
                if bpa_field is not None:
                    self.counters["mapped_field_usage"][atol_field].update([bpa_field])
                    
    def output_processor(self, package):
        """Return the mapped metadata for output."""
        return package.mapped_metadata
        
    def write_optional_outputs(self):
        """Write mapping-specific output files."""
        from .io import write_mapping_log_to_csv, write_json
        
        if self.args.mapping_log:
            logger.info(f"Writing mapping log to {self.args.mapping_log}")
            write_mapping_log_to_csv(self.mapping_log, self.args.mapping_log)
        if self.args.grouping_log:
            logger.info(f"Writing grouping log to {self.args.grouping_log}")
            write_mapping_log_to_csv(self.grouping_log, self.args.grouping_log)
        if self.args.raw_field_usage:
            logger.info(f"Writing field usage counts to {self.args.raw_field_usage}")
            write_json(self.counters["raw_field_usage"], self.args.raw_field_usage)
        if self.args.raw_value_usage:
            logger.info(f"Writing raw value usage to {self.args.raw_value_usage}")
            write_json(self.counters["raw_value_usage"], self.args.raw_value_usage)
        if self.args.mapped_field_usage:
            logger.info(f"Writing mapped field usage to {self.args.mapped_field_usage}")
            write_json(self.counters["mapped_field_usage"], self.args.mapped_field_usage)
        if self.args.mapped_value_usage:
            logger.info(f"Writing mapped value usage to {self.args.mapped_value_usage}")
            write_json(self.counters["mapped_value_usage"], self.args.mapped_value_usage)
        if self.args.grouped_packages:
            logger.info(f"Writing grouped packages to {self.args.grouped_packages}")
            write_json(self.grouped_packages, self.args.grouped_packages)
        if self.args.unused_field_counts:
            logger.info(f"Writing unused field counts to {self.args.unused_field_counts}")
            write_json(self.counters["unused_field_counts"], self.args.unused_field_counts)
        if self.args.sanitization_changes and self.sanitization_changes:
            logger.info(f"Writing sanitization changes to {self.args.sanitization_changes}")
            write_json(self.sanitization_changes, self.args.sanitization_changes)
