"""
Single-pass data processing pipeline that combines filtering, mapping, and transformation.

This pipeline processes each package sequentially through all three stages:
1. Filter packages based on controlled vocabularies
2. Map metadata to AToL schema format  
3. Transform data to extract unique entities

This approach is much more efficient than the multi-pass approach as it:
- Reads input data only once
- Processes packages in memory without intermediate files
- Reduces I/O overhead significantly
"""

from .arg_parser import parse_args_for_filtering, parse_args_for_mapping, parse_args_for_transform
from .config_parser import MetadataMap
from .io import read_input, OutputWriter, write_json, write_decision_log_to_csv
from .logger import logger, setup_logger
from .transform_data import SampleTransformer, OrganismTransformer, extract_experiment
from collections import Counter
import argparse


class SinglePassPipeline:
    """
    Single-pass pipeline that processes packages through filter → map → transform sequentially.
    """
    
    def __init__(self, args):
        """Initialize pipeline with command-line arguments."""
        self.args = args
        setup_logger(args.log_level)
        
        # Initialize metadata maps
        self.package_metadata_map = MetadataMap(
            args.package_field_mapping_file, 
            args.value_mapping_file, 
            getattr(args, 'sanitization_config_file', None)
        )
        self.resource_metadata_map = MetadataMap(
            args.resource_field_mapping_file, 
            args.value_mapping_file, 
            getattr(args, 'sanitization_config_file', None)
        )
        
        # Initialize NCBI taxonomy for organism mapping if needed
        self.ncbi_taxdump = None
        if hasattr(args, 'nodes') and args.nodes and hasattr(args, 'names') and args.names:
            from .organism_mapper import NcbiTaxdump
            self.ncbi_taxdump = NcbiTaxdump(
                args.nodes, 
                args.names,
                getattr(args, 'taxids_to_busco_dataset_mapping', None),
                getattr(args, 'cache_dir', './cache')
            )
            
        # Initialize transformers for transformation stage
        sample_ignored_fields = []
        organism_ignored_fields = []
        if hasattr(args, 'sample_ignored_fields') and args.sample_ignored_fields:
            sample_ignored_fields = args.sample_ignored_fields.split(',')
        if hasattr(args, 'organism_ignored_fields') and args.organism_ignored_fields:
            organism_ignored_fields = args.organism_ignored_fields.split(',')
            
        self.sample_transformer = SampleTransformer(ignored_fields=sample_ignored_fields)
        self.organism_transformer = OrganismTransformer(ignored_fields=organism_ignored_fields)
        
        # Initialize counters and tracking
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
        
        self.decision_log = {}
        self.experiments_data = {}
        self.stats = {
            'n_packages': 0,
            'n_filtered': 0,
            'n_mapped': 0,
            'n_transformed': 0
        }

    def process_package(self, package):
        """
        Process a single package through all three stages sequentially.
        
        Returns:
            bool: True if package should be included in final output
        """
        self.stats['n_packages'] += 1
        logger.debug(f"Processing package {package.id}")
        
        # Update raw field usage counter
        self.counters["raw_field_usage"].update(package.fields)
        
        # STAGE 1: FILTERING
        should_continue = self._filter_package(package)
        if not should_continue:
            return False
            
        self.stats['n_filtered'] += 1
        
        # STAGE 2: MAPPING  
        self._map_package(package)
        self.stats['n_mapped'] += 1
        
        # STAGE 3: TRANSFORMATION
        self._transform_package(package)
        self.stats['n_transformed'] += 1
        
        return True

    def _filter_package(self, package):
        """Apply filtering logic to package and resources."""
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

        # Log decisions
        self.decision_log[package.id] = package.decisions
        
        return package.keep

    def _map_package(self, package):
        """Apply mapping logic to package and resources."""
        # Map package-level metadata
        package.map_metadata(self.package_metadata_map)
        
        # Map organism information if NCBI taxonomy is available
        if self.ncbi_taxdump and 'organism' in package.mapped_metadata:
            self._map_organism_section(package)
        
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

    def _map_organism_section(self, package):
        """Map organism section using NCBI taxonomy."""
        from .organism_mapper import OrganismSection
        from .utils.common import safe_get
        
        # Get null values for organism mapping
        null_values = safe_get(
            lambda: self.package_metadata_map.sanitization_config.null_values, 
            []
        )
        
        # Create organism section with NCBI lookup
        organism_section = OrganismSection(
            package.id,
            package.mapped_metadata.get('organism', {}),
            self.ncbi_taxdump,
            null_values
        )
        
        # Update the package's organism metadata
        package.mapped_metadata['organism'].update(organism_section.mapped_metadata)

    def _transform_package(self, package):
        """Apply transformation logic to extract entities."""
        # Process for sample extraction
        self.sample_transformer.process_package(package)
        
        # Process for organism extraction  
        self.organism_transformer.process_package(package)
        
        # Extract experiment data
        extract_experiment(self.experiments_data, package)

    def run(self):
        """Execute the complete single-pass pipeline."""
        logger.info("Starting single-pass pipeline: filter → map → transform")
        
        input_data = read_input(self.args.input)
        
        with OutputWriter(self.args.output, self.args.dry_run) as output_writer:
            for package in input_data:
                # Apply debug limits if specified
                if (hasattr(self.args, 'max_iterations') and 
                    self.args.max_iterations and 
                    self.stats['n_packages'] >= self.args.max_iterations):
                    break
                if (hasattr(self.args, 'manual_record') and 
                    self.args.manual_record and 
                    package.id != self.args.manual_record):
                    continue

                # Process package through all stages
                if self.process_package(package):
                    output_writer.write_data(package)

        self._write_outputs()
        self._log_statistics()

    def _write_outputs(self):
        """Write all output files."""
        if self.args.dry_run:
            return

        # Write filtering outputs
        if hasattr(self.args, 'decision_log') and self.args.decision_log:
            logger.info(f"Writing decision log to {self.args.decision_log}")
            write_decision_log_to_csv(self.decision_log, self.args.decision_log)
            
        # Write counter outputs
        if hasattr(self.args, 'raw_field_usage') and self.args.raw_field_usage:
            logger.info(f"Writing raw field usage to {self.args.raw_field_usage}")
            write_json(self.counters["raw_field_usage"], self.args.raw_field_usage)
        if hasattr(self.args, 'bpa_field_usage') and self.args.bpa_field_usage:
            logger.info(f"Writing BPA field usage to {self.args.bpa_field_usage}")
            write_json(self.counters["bpa_field_usage"], self.args.bpa_field_usage)
        if hasattr(self.args, 'bpa_value_usage') and self.args.bpa_value_usage:
            logger.info(f"Writing BPA value usage to {self.args.bpa_value_usage}")
            write_json(self.counters["bpa_value_usage"], self.args.bpa_value_usage)
            
        # Write transformation outputs
        sample_results = self.sample_transformer.get_results()
        organism_results = self.organism_transformer.get_results()
        
        # Use the same output paths as the individual scripts would use
        if hasattr(self.args, 'sample_conflicts') and getattr(self.args, 'sample_conflicts', None):
            logger.info(f"Writing sample conflicts to {self.args.sample_conflicts}")
            write_json(sample_results["sample_conflicts"], self.args.sample_conflicts)
        if hasattr(self.args, 'unique_organisms') and getattr(self.args, 'unique_organisms', None):
            logger.info(f"Writing unique organisms to {self.args.unique_organisms}")
            write_json(organism_results["unique_organisms"], self.args.unique_organisms)
        if hasattr(self.args, 'experiments_output') and getattr(self.args, 'experiments_output', None):
            logger.info(f"Writing experiments to {self.args.experiments_output}")
            write_json(self.experiments_data, self.args.experiments_output)

    def _log_statistics(self):
        """Log processing statistics."""
        logger.info(f"Single-pass pipeline completed:")
        logger.info(f"  Processed {self.stats['n_packages']} packages")
        logger.info(f"  Filtered {self.stats['n_filtered']} packages")
        logger.info(f"  Mapped {self.stats['n_mapped']} packages") 
        logger.info(f"  Transformed {self.stats['n_transformed']} packages")
        
        # Log transformation results
        sample_results = self.sample_transformer.get_results()
        organism_results = self.organism_transformer.get_results()
        
        logger.info(f"  Found {len(sample_results['unique_samples'])} unique samples")
        logger.info(f"  Found {len(sample_results['sample_conflicts'])} samples with conflicts")
        logger.info(f"  Found {len(organism_results['unique_organisms'])} unique organisms")
        logger.info(f"  Found {len(organism_results['organism_conflicts'])} organisms with conflicts")
        logger.info(f"  Found {len(self.experiments_data)} experiments")


def create_combined_args_parser():
    """Create argument parser that combines arguments from all three original scripts."""
    parser = argparse.ArgumentParser(
        description="Single-pass BPA data processing pipeline (filter → map → transform)"
    )
    
    # Input/Output (from all scripts)
    io_group = parser.add_argument_group("Input/Output")
    io_group.add_argument('-i', '--input', help='Input file (default: stdin)')
    io_group.add_argument('-o', '--output', help='Output file (default: stdout)')
    
    # General options (from all scripts)
    general_group = parser.add_argument_group("General options")
    general_group.add_argument('-f', '--package_field_mapping_file',
                              help='Package-level field mapping file in json')
    general_group.add_argument('-r', '--resource_field_mapping_file',
                              help='Resource-level field mapping file in json')
    general_group.add_argument('-v', '--value_mapping_file',
                              help='Value mapping file in json')
    general_group.add_argument('--sanitization_config_file',
                              help='Sanitization configuration file',
                              default=None)
    general_group.add_argument('-l', '--log-level',
                              choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                              default='INFO', help='Set the logging level')
    general_group.add_argument('-n', '--dry-run', action='store_true',
                              help='Test mode. Output will be uncompressed jsonlines.')

    # Mapping-specific arguments - make optional
    mapping_group = parser.add_argument_group("Mapping options")
    mapping_group.add_argument('--nodes', help='NCBI nodes.dmp file from taxdump', default=None)
    mapping_group.add_argument('--names', help='NCBI names.dmp file from taxdump', default=None)
    mapping_group.add_argument('--taxids_to_busco_dataset_mapping',
                              help='BUSCO dataset mapping file', default=None)
    mapping_group.add_argument('--cache_dir', help='Directory to cache NCBI taxonomy', 
                              default='./cache')
    
    # Transformation-specific arguments
    transform_group = parser.add_argument_group("Transform options")
    transform_group.add_argument('--sample-ignored-fields',
                                help='Comma-separated list of sample fields to ignore for conflicts')
    transform_group.add_argument('--organism-ignored-fields', 
                                help='Comma-separated list of organism fields to ignore for conflicts')
    
    # Output file arguments (from all scripts)
    output_group = parser.add_argument_group("Output files")
    
    # Filtering outputs
    output_group.add_argument('--decision_log', help='Decision log CSV file')
    output_group.add_argument('--raw_field_usage', help='Raw field usage JSON file')
    output_group.add_argument('--bpa_field_usage', help='BPA field usage JSON file')
    output_group.add_argument('--bpa_value_usage', help='BPA value usage JSON file')
    
    # Mapping outputs  
    output_group.add_argument('--mapping_log', help='Mapping log CSV file')
    output_group.add_argument('--grouping_log', help='Organism grouping log CSV file')
    output_group.add_argument('--grouped_packages', help='Grouped packages JSON file')
    output_group.add_argument('--sanitization_changes', help='Sanitization changes JSON file')
    
    # Transformation outputs
    output_group.add_argument('--sample-conflicts', help='Sample conflicts JSON file')
    output_group.add_argument('--unique-organisms', help='Unique organisms JSON file')
    output_group.add_argument('--experiments-output', help='Experiments JSON file')
    output_group.add_argument('--sample-package-map', help='Sample to package mapping JSON file')
    output_group.add_argument('--transformation-changes', help='Transformation changes JSON file')
    output_group.add_argument('--organism-package-map', help='Organism to package mapping JSON file')
    
    return parser


def main():
    """Main entry point for single-pass pipeline."""
    parser = create_combined_args_parser()
    args = parser.parse_args()
    
    # Create and run pipeline
    pipeline = SinglePassPipeline(args)
    pipeline.run()


if __name__ == "__main__":
    main()
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
