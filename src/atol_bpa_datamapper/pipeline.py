"""
Unified data processing pipeline that combines filtering, mapping, and transformation.

This module consolidates the functionality from filter_packages.py, map_metadata.py, 
and transform_data.py into a single configurable pipeline.
"""

from .arg_parser import parse_args_for_filtering, parse_args_for_mapping, parse_args_for_transform
from .config_parser import MetadataMap
from .io import read_input, OutputWriter, write_json, write_decision_log_to_csv
from .logger import logger, setup_logger
from .transform_data import SampleTransformer, OrganismTransformer, extract_experiment
from .organism_mapper import OrganismMapper
from collections import Counter
import argparse


class DataProcessingPipeline:
    """
    Unified pipeline for processing BPA data through filtering, mapping, and transformation steps.
    """
    
    def __init__(self, config):
        """
        Initialize the pipeline with configuration.
        
        Args:
            config: Configuration object containing all pipeline parameters
        """
        self.config = config
        self.counters = {}
        self.decision_log = {}
        self.stats = {
            'n_packages': 0,
            'n_filtered': 0,
            'n_mapped': 0,
            'n_transformed_samples': 0,
            'n_transformed_organisms': 0
        }
        
        # Initialize components based on configuration
        if config.enable_filtering or config.enable_mapping:
            self.package_metadata_map = MetadataMap(
                config.package_field_mapping_file, 
                config.value_mapping_file, 
                config.sanitization_config_file
            )
            self.resource_metadata_map = MetadataMap(
                config.resource_field_mapping_file, 
                config.value_mapping_file, 
                config.sanitization_config_file
            )
            
        if config.enable_mapping:
            self.organism_mapper = OrganismMapper(
                config.nodes_file, 
                config.names_file,
                config.taxids_to_busco_dataset_mapping,
                config.cache_dir
            )
            
        if config.enable_transformation:
            self.sample_transformer = SampleTransformer(
                ignored_fields=config.sample_ignored_fields
            )
            self.organism_transformer = OrganismTransformer(
                ignored_fields=config.organism_ignored_fields
            )
            
        self._setup_counters()

    def _setup_counters(self):
        """Initialize counters for tracking field and value usage."""
        if self.config.enable_filtering or self.config.enable_mapping:
            all_controlled_vocabularies = sorted(
                set(
                    self.package_metadata_map.controlled_vocabularies
                    + self.resource_metadata_map.controlled_vocabularies
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

    def process_package_filtering(self, package):
        """Apply filtering logic to a package."""
        # Filter on Package-level fields
        package.filter(self.package_metadata_map)
        
        # Update counters
        for atol_field, bpa_field in package.bpa_fields.items():
            self.counters["bpa_field_usage"][atol_field].update([bpa_field])
        for atol_field, bpa_value in package.bpa_values.items():
            self.counters["bpa_value_usage"][atol_field].update([bpa_value])

        # Check Resources for this Package
        dropped_resources = []
        kept_resources = []
        for resource_id, resource in package.resources.items():
            resource.filter(self.resource_metadata_map, package)
            if resource.keep:
                kept_resources.append(resource.id)
            else:
                dropped_resources.append(resource.id)

        # Drop unwanted resources
        for resource_id in dropped_resources:
            package.resources.pop(resource_id)

        # Remove packages with no resources
        if len(kept_resources) > 0:
            package["resources"] = [
                package.resources[resource_id] for resource_id in kept_resources
            ]
            package.decisions["kept_resources"] = True
        else:
            package.decisions["kept_resources"] = False
            package.keep = False

        self.decision_log[package.id] = package.decisions
        return package.keep

    def process_package_mapping(self, package):
        """Apply mapping logic to a package."""
        # Map package-level metadata
        package.map_metadata(self.package_metadata_map)
        
        # Map organism information
        self.organism_mapper.process_package(package)
        
        # Map resource-level metadata
        resource_mapped_metadata = {
            section: [] for section in self.resource_metadata_map.metadata_sections
        }
        for resource_id, resource in package.resources.items():
            resource.map_metadata(self.resource_metadata_map, package)
            for section in resource_mapped_metadata:
                if section in resource.mapped_metadata:
                    resource_mapped_metadata[section].append(
                        resource.mapped_metadata[section]
                    )

        # Merge resource metadata into package metadata
        for section, resource_metadata in resource_mapped_metadata.items():
            package.mapped_metadata[section] = resource_metadata
            
        return True

    def process_package_transformation(self, package):
        """Apply transformation logic to a package."""
        sample_processed = self.sample_transformer.process_package(package)
        organism_processed = self.organism_transformer.process_package(package)
        
        # Extract experiment data
        if hasattr(self, 'experiments_data'):
            extract_experiment(self.experiments_data, package)
        else:
            self.experiments_data = {}
            extract_experiment(self.experiments_data, package)
            
        return sample_processed or organism_processed

    def run(self):
        """Execute the complete pipeline."""
        logger.info(f"Starting pipeline with steps: "
                   f"filter={self.config.enable_filtering}, "
                   f"map={self.config.enable_mapping}, "
                   f"transform={self.config.enable_transformation}")

        input_data = read_input(self.config.input)
        
        with OutputWriter(self.config.output, self.config.dry_run) as output_writer:
            for package in input_data:
                # Debug limits
                if (hasattr(self.config, 'max_iterations') and 
                    self.config.max_iterations and 
                    self.stats['n_packages'] >= self.config.max_iterations):
                    break
                if (hasattr(self.config, 'manual_record') and 
                    self.config.manual_record and 
                    package.id != self.config.manual_record):
                    continue

                self.stats['n_packages'] += 1
                logger.debug(f"Processing package {package.id}")
                
                # Update raw field usage counter
                if hasattr(self, 'counters'):
                    self.counters["raw_field_usage"].update(package.fields)

                should_output = True
                
                # Step 1: Filtering
                if self.config.enable_filtering:
                    should_output = self.process_package_filtering(package)
                    if should_output:
                        self.stats['n_filtered'] += 1

                # Step 2: Mapping
                if should_output and self.config.enable_mapping:
                    self.process_package_mapping(package)
                    self.stats['n_mapped'] += 1

                # Step 3: Transformation
                if should_output and self.config.enable_transformation:
                    if self.process_package_transformation(package):
                        self.stats['n_transformed_samples'] += 1

                # Output the package if it should be included
                if should_output:
                    output_writer.write_data(package)

        self._write_outputs()
        self._log_statistics()

    def _write_outputs(self):
        """Write all output files based on configuration."""
        if self.config.dry_run:
            return
            
        # Filtering outputs
        if self.config.enable_filtering:
            if hasattr(self.config, 'decision_log') and self.config.decision_log:
                logger.info(f"Writing decision log to {self.config.decision_log}")
                write_decision_log_to_csv(self.decision_log, self.config.decision_log)
                
        # Counter outputs (shared between filtering and mapping)
        if hasattr(self, 'counters'):
            if hasattr(self.config, 'raw_field_usage') and self.config.raw_field_usage:
                write_json(self.counters["raw_field_usage"], self.config.raw_field_usage)
            if hasattr(self.config, 'bpa_field_usage') and self.config.bpa_field_usage:
                write_json(self.counters["bpa_field_usage"], self.config.bpa_field_usage)
            if hasattr(self.config, 'bpa_value_usage') and self.config.bpa_value_usage:
                write_json(self.counters["bpa_value_usage"], self.config.bpa_value_usage)
                
        # Mapping outputs
        if self.config.enable_mapping and hasattr(self, 'organism_mapper'):
            self.organism_mapper.write_outputs(self.config)
            
        # Transformation outputs
        if self.config.enable_transformation:
            sample_results = self.sample_transformer.get_results()
            organism_results = self.organism_transformer.get_results()
            
            if hasattr(self.config, 'sample_output') and self.config.sample_output:
                write_json(sample_results["unique_samples"], self.config.sample_output)
            if hasattr(self.config, 'organism_output') and self.config.organism_output:
                write_json(organism_results["unique_organisms"], self.config.organism_output)
            if hasattr(self.config, 'experiments_output') and self.config.experiments_output:
                write_json(self.experiments_data, self.config.experiments_output)

    def _log_statistics(self):
        """Log processing statistics."""
        logger.info(f"Processed {self.stats['n_packages']} packages")
        if self.config.enable_filtering:
            logger.info(f"Kept {self.stats['n_filtered']} packages after filtering")
        if self.config.enable_mapping:
            logger.info(f"Mapped {self.stats['n_mapped']} packages")
        if self.config.enable_transformation:
            logger.info(f"Processed {self.stats['n_transformed_samples']} samples for transformation")


class PipelineConfig:
    """Configuration class for the unified pipeline."""
    
    def __init__(self, **kwargs):
        # Pipeline control
        self.enable_filtering = kwargs.get('enable_filtering', True)
        self.enable_mapping = kwargs.get('enable_mapping', True) 
        self.enable_transformation = kwargs.get('enable_transformation', True)
        
        # Common parameters
        self.input = kwargs.get('input')
        self.output = kwargs.get('output')
        self.dry_run = kwargs.get('dry_run', False)
        self.log_level = kwargs.get('log_level', 'INFO')
        
        # Configuration files
        self.package_field_mapping_file = kwargs.get('package_field_mapping_file')
        self.resource_field_mapping_file = kwargs.get('resource_field_mapping_file')
        self.value_mapping_file = kwargs.get('value_mapping_file')
        self.sanitization_config_file = kwargs.get('sanitization_config_file')
        
        # Mapping-specific parameters
        self.nodes_file = kwargs.get('nodes_file')
        self.names_file = kwargs.get('names_file')
        self.taxids_to_busco_dataset_mapping = kwargs.get('taxids_to_busco_dataset_mapping')
        self.cache_dir = kwargs.get('cache_dir')
        
        # Transformation-specific parameters
        self.sample_ignored_fields = kwargs.get('sample_ignored_fields', [])
        self.organism_ignored_fields = kwargs.get('organism_ignored_fields', [])
        
        # Output files
        for key, value in kwargs.items():
            if not hasattr(self, key):
                setattr(self, key, value)


def create_unified_args_parser():
    """Create argument parser that combines all three pipeline steps."""
    parser = argparse.ArgumentParser(
        description="Unified BPA data processing pipeline"
    )
    
    # Pipeline control
    pipeline_group = parser.add_argument_group("Pipeline control")
    pipeline_group.add_argument(
        '--steps', 
        choices=['filter', 'map', 'transform', 'filter,map', 'map,transform', 'filter,map,transform'],
        default='filter,map,transform',
        help='Which pipeline steps to run (default: all steps)'
    )
    
    # Input/Output
    io_group = parser.add_argument_group("Input/Output")
    io_group.add_argument('-i', '--input', help='Input file (default: stdin)')
    io_group.add_argument('-o', '--output', help='Output file (default: stdout)')
    
    # Configuration files
    config_group = parser.add_argument_group("Configuration")
    config_group.add_argument('-f', '--package_field_mapping_file', 
                             help='Package-level field mapping file in json')
    config_group.add_argument('-r', '--resource_field_mapping_file',
                             help='Resource-level field mapping file in json')
    config_group.add_argument('-v', '--value_mapping_file',
                             help='Value mapping file in json')
    config_group.add_argument('--sanitization_config_file',
                             help='Sanitization configuration file')
    
    # General options
    general_group = parser.add_argument_group("General options")
    general_group.add_argument('-l', '--log-level', 
                              choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                              default='INFO', help='Set the logging level')
    general_group.add_argument('-n', '--dry-run', action='store_true',
                              help='Test mode. Output will be uncompressed jsonlines.')
    
    # Add specific arguments for each step
    # ...existing argument definitions from each original parser...
    
    return parser


def main():
    """Main entry point for the unified pipeline."""
    parser = create_unified_args_parser()
    args = parser.parse_args()
    
    # Setup logging
    setup_logger(args.log_level)
    
    # Parse which steps to run
    steps = args.steps.split(',')
    config_dict = vars(args)
    config_dict.update({
        'enable_filtering': 'filter' in steps,
        'enable_mapping': 'map' in steps, 
        'enable_transformation': 'transform' in steps
    })
    
    # Create configuration
    config = PipelineConfig(**config_dict)
    
    # Create and run pipeline
    pipeline = DataProcessingPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
