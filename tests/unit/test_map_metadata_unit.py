"""Unit tests for map_metadata.py."""

import pytest
from unittest.mock import patch, MagicMock, mock_open, ANY

from atol_bpa_datamapper.map_metadata import main
from atol_bpa_datamapper.package_handler import BpaPackage


@patch('atol_bpa_datamapper.map_metadata.NcbiTaxdump')
@patch('atol_bpa_datamapper.map_metadata.MetadataMap')
@patch('atol_bpa_datamapper.map_metadata.read_input')
@patch('atol_bpa_datamapper.map_metadata.OutputWriter')
@patch('atol_bpa_datamapper.map_metadata.parse_args_for_mapping')
@patch('atol_bpa_datamapper.organism_mapper.compute_sha256')
@patch('atol_bpa_datamapper.organism_mapper.read_taxdump_file')
def test_map_metadata_basic(mock_read_taxdump_file, mock_compute_sha256, mock_parse_args, mock_output_writer, mock_read_input, mock_metadata_map, mock_ncbi_taxdump):
    # Configure the NcbiTaxdump mock to prevent file I/O operations
    mock_ncbi_taxdump.return_value = MagicMock()
    """Test basic functionality of map_metadata."""
    # This test verifies that:
    # 1. The map_metadata main function correctly processes input packages
    # 2. Each package's map_metadata method is called with the correct metadata map
    # 3. Resource-level metadata is correctly mapped and added to the package
    # 4. The mapped metadata is correctly written to the output file
    # 5. Statistics are correctly calculated and displayed
    # 6. The function handles all required command line arguments
    
    # Set up mock packages and resources
    resource1 = MagicMock()
    resource1.id = "resource1"
    resource1.mapped_metadata = {
        "dataset": {},
        "organism": {},
        "runs": {"field3": "value3"}
    }
    
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.mapped_metadata = {
        "dataset": {"field1": "value1"},
        "organism": {"field2": "value2"},
        "runs": []
    }
    package1.resources = {"resource1": resource1}
    package1.field_mapping = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.mapping_log = [
        {"atol_field": "field1", "bpa_field": "bpa_field1", "value": "raw1", "mapped_value": "value1"},
        {"atol_field": "field2", "bpa_field": "bpa_field2", "value": "raw2", "mapped_value": "value2"}
    ]
    package1.unused_fields = ["unused1", "unused2"]
    package1.fields = ["bpa_field1", "bpa_field2", "unused1", "unused2"]
    package1.sanitization_changes = []
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    # Create two separate metadata map instances for package and resource level
    mock_package_metadata_map = MagicMock()
    mock_package_metadata_map.expected_fields = ["field1", "field2"]
    mock_package_metadata_map.metadata_sections = ["dataset", "organism", "runs"]
    
    mock_resource_metadata_map = MagicMock()
    mock_resource_metadata_map.expected_fields = ["field3"]
    mock_resource_metadata_map.metadata_sections = ["dataset", "organism", "runs"]
    
    # Configure the MetadataMap mock to return different instances based on arguments
    def metadata_map_side_effect(field_mapping_file, value_mapping_file, sanitization_config_file):
        if field_mapping_file == "package_field_mapping.json":
            return mock_package_metadata_map
        elif field_mapping_file == "resource_field_mapping.json":
            return mock_resource_metadata_map
    
    mock_metadata_map.side_effect = metadata_map_side_effect
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.package_field_mapping_file = "package_field_mapping.json"
    args.resource_field_mapping_file = "resource_field_mapping.json"
    args.value_mapping_file = "value_mapping.json"
    args.sanitization_config_file = "sanitization_config.json"
    args.log_level = "INFO"
    args.dry_run = False
    args.mapping_log = None
    args.raw_field_usage = None
    args.raw_value_usage = None
    args.mapped_field_usage = None
    args.mapped_value_usage = None
    args.unused_field_counts = None
    args.sanitization_changes = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Add mock values for NcbiTaxdump initialization to prevent file I/O
    args.nodes = MagicMock()
    args.names = MagicMock()
    args.cache_dir = "MagicMock"
    
    # Add mock values for output files to prevent file I/O errors
    args.grouped_packages = None
    args.grouping_log = None
    
    # Configure NcbiTaxdump mock
    mock_ncbi_taxdump_instance = MagicMock()
    mock_ncbi_taxdump.return_value = mock_ncbi_taxdump_instance
    
    # Call the function
    main()
    
    # Verify the function behavior
    assert mock_metadata_map.call_count == 2
    mock_metadata_map.assert_any_call(args.package_field_mapping_file, args.value_mapping_file, args.sanitization_config_file)
    mock_metadata_map.assert_any_call(args.resource_field_mapping_file, args.value_mapping_file, args.sanitization_config_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that map_metadata was called on the package with package-level map
    package1.map_metadata.assert_called_once_with(mock_package_metadata_map)
    
    # Verify that map_metadata was called on the resource with resource-level map
    resource1.map_metadata.assert_called_once_with(mock_resource_metadata_map, package1)
    
    # Verify that the mapped metadata was written to output
    mock_output_writer_instance.write_data.assert_called_once()


@patch('atol_bpa_datamapper.map_metadata.write_mapping_log_to_csv')
@patch('atol_bpa_datamapper.map_metadata.write_json')
@patch('atol_bpa_datamapper.map_metadata.NcbiTaxdump')
@patch('atol_bpa_datamapper.map_metadata.MetadataMap')
@patch('atol_bpa_datamapper.map_metadata.read_input')
@patch('atol_bpa_datamapper.map_metadata.OutputWriter')
@patch('atol_bpa_datamapper.map_metadata.parse_args_for_mapping')
@patch('atol_bpa_datamapper.organism_mapper.compute_sha256')
@patch('atol_bpa_datamapper.organism_mapper.read_taxdump_file')
def test_map_metadata_dry_run(mock_read_taxdump_file, mock_compute_sha256, mock_parse_args, mock_output_writer, mock_read_input, mock_metadata_map, mock_ncbi_taxdump, mock_write_json, mock_write_mapping_log):
    """Test map_metadata with dry_run=True."""
    # This test verifies that:
    # 1. The map_metadata function correctly handles dry run mode
    # 2. In dry run mode, packages are processed but no output is written
    # 3. Statistics are still calculated and displayed
    # 4. The write_json function is not called in dry run mode
    # 5. The mapping log is still written if specified
    
    # Set up mock packages and resources
    resource1 = MagicMock()
    resource1.id = "resource1"
    resource1.mapped_metadata = {
        "dataset": {},
        "organism": {},
        "runs": {"field3": "value3"}
    }
    
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.mapped_metadata = {
        "dataset": {"field1": "value1"},
        "organism": {"field2": "value2"},
        "runs": []
    }
    package1.resources = {"resource1": resource1}
    package1.field_mapping = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.mapping_log = []
    package1.unused_fields = []
    package1.fields = ["bpa_field1", "bpa_field2"]
    package1.sanitization_changes = []
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    # Create two separate metadata map instances for package and resource level
    mock_package_metadata_map = MagicMock()
    mock_package_metadata_map.expected_fields = ["field1", "field2"]
    mock_package_metadata_map.metadata_sections = ["dataset", "organism", "runs"]
    
    mock_resource_metadata_map = MagicMock()
    mock_resource_metadata_map.expected_fields = ["field3"]
    mock_resource_metadata_map.metadata_sections = ["dataset", "organism", "runs"]
    
    # Configure the MetadataMap mock to return different instances based on arguments
    def metadata_map_side_effect(field_mapping_file, value_mapping_file, sanitization_config_file):
        if field_mapping_file == "package_field_mapping.json":
            return mock_package_metadata_map
        elif field_mapping_file == "resource_field_mapping.json":
            return mock_resource_metadata_map
    
    mock_metadata_map.side_effect = metadata_map_side_effect
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args with dry_run=True
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.package_field_mapping_file = "package_field_mapping.json"
    args.resource_field_mapping_file = "resource_field_mapping.json"
    args.value_mapping_file = "value_mapping.json"
    args.sanitization_config_file = "sanitization_config.json"
    args.log_level = "INFO"
    args.dry_run = True
    args.mapping_log = None
    args.raw_field_usage = None
    args.raw_value_usage = None
    args.mapped_field_usage = None
    args.mapped_value_usage = None
    args.unused_field_counts = None
    args.sanitization_changes = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Add mock values for NcbiTaxdump initialization to prevent file I/O
    args.log_level = "INFO"
    args.nodes = MagicMock()
    args.names = MagicMock()
    args.cache_dir = "MagicMock"
    
    # Add mock values for output files to prevent file I/O errors
    args.grouped_packages = None
    args.grouping_log = None
    
    # Configure NcbiTaxdump mock
    mock_ncbi_taxdump_instance = MagicMock()
    mock_ncbi_taxdump.return_value = mock_ncbi_taxdump_instance
    
    # Call the function
    main()
    
    # Verify the function behavior
    assert mock_metadata_map.call_count == 2
    mock_metadata_map.assert_any_call(args.package_field_mapping_file, args.value_mapping_file, args.sanitization_config_file)
    mock_metadata_map.assert_any_call(args.resource_field_mapping_file, args.value_mapping_file, args.sanitization_config_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that map_metadata was called on the package with package-level map
    package1.map_metadata.assert_called_once_with(mock_package_metadata_map)
    
    # Verify that map_metadata was called on the resource with resource-level map
    resource1.map_metadata.assert_called_once_with(mock_resource_metadata_map, package1)
    
    # Verify that no output files were written due to dry_run=True
    mock_write_json.assert_not_called()
    mock_write_mapping_log.assert_not_called()


@patch('atol_bpa_datamapper.map_metadata.NcbiTaxdump')
@patch('atol_bpa_datamapper.map_metadata.MetadataMap')
@patch('atol_bpa_datamapper.map_metadata.read_input')
@patch('atol_bpa_datamapper.map_metadata.OutputWriter')
@patch('atol_bpa_datamapper.map_metadata.write_mapping_log_to_csv')
@patch('atol_bpa_datamapper.map_metadata.write_json')
@patch('atol_bpa_datamapper.map_metadata.parse_args_for_mapping')
def test_map_metadata_with_output_files(mock_parse_args, mock_write_json, mock_write_mapping_log, mock_output_writer, mock_read_input, mock_metadata_map, mock_ncbi_taxdump):
    """Test map_metadata with output files."""
    # This test verifies that:
    # 1. The map_metadata function correctly handles output file arguments
    # 2. Statistics files are written when specified in the arguments
    # 3. Mapping log files are written when specified in the arguments
    # 4. The write_json and write_mapping_log_to_csv functions are called with correct arguments
    # 5. The function correctly processes multiple packages and aggregates their statistics
    # 6. Sanitization changes are correctly tracked and written to output
    
    # Set up mock packages and resources
    resource1 = MagicMock()
    resource1.id = "resource1"
    resource1.mapped_metadata = {
        "dataset": {},
        "organism": {},
        "runs": {"field3": "value3"}
    }
    
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.mapped_metadata = {
        "dataset": {"field1": "value1"},
        "organism": {"field2": "value2"},
        "runs": []
    }
    package1.resources = {"resource1": resource1}
    package1.field_mapping = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.mapping_log = [
        {"atol_field": "field1", "bpa_field": "bpa_field1", "value": "raw1", "mapped_value": "value1"},
        {"atol_field": "field2", "bpa_field": "bpa_field2", "value": "raw2", "mapped_value": "value2"}
    ]
    package1.unused_fields = ["unused1", "unused2"]
    package1.fields = ["bpa_field1", "bpa_field2", "unused1", "unused2"]
    package1.sanitization_changes = [
        {"bpa_id": "package1", "field": "field1", "original_value": "raw1", "sanitized_value": "raw1", "applied_rules": []}
    ]
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    # Create two separate metadata map instances for package and resource level
    mock_package_metadata_map = MagicMock()
    mock_package_metadata_map.expected_fields = ["field1", "field2"]
    mock_package_metadata_map.metadata_sections = ["dataset", "organism", "runs"]
    
    mock_resource_metadata_map = MagicMock()
    mock_resource_metadata_map.expected_fields = ["field3"]
    mock_resource_metadata_map.metadata_sections = ["dataset", "organism", "runs"]
    
    # Configure the MetadataMap mock to return different instances based on arguments
    def metadata_map_side_effect(field_mapping_file, value_mapping_file, sanitization_config_file):
        if field_mapping_file == "package_field_mapping.json":
            return mock_package_metadata_map
        elif field_mapping_file == "resource_field_mapping.json":
            return mock_resource_metadata_map
    
    mock_metadata_map.side_effect = metadata_map_side_effect
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args with output file paths
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.package_field_mapping_file = "package_field_mapping.json"
    args.resource_field_mapping_file = "resource_field_mapping.json"
    args.value_mapping_file = "value_mapping.json"
    args.sanitization_config_file = "sanitization_config.json"
    args.log_level = "INFO"
    args.dry_run = False
    args.mapping_log = "mapping_log.csv"
    args.raw_field_usage = "raw_field_usage.json"
    args.raw_value_usage = "raw_value_usage.json"
    args.mapped_field_usage = "mapped_field_usage.json"
    args.mapped_value_usage = "mapped_value_usage.json"
    args.unused_field_counts = "unused_field_counts.json"
    args.sanitization_changes = "sanitization_changes.json"
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Add mock values for NcbiTaxdump initialization to prevent file I/O
    args.log_level = "INFO"
    mock_ncbi_taxdump.return_value = MagicMock()
    
    # Add mock values for output files to prevent file I/O errors
    args.grouped_packages = None
    args.grouping_log = None
    
    # Call the function
    main()
    
    # Verify the function behavior
    assert mock_metadata_map.call_count == 2
    mock_metadata_map.assert_any_call(args.package_field_mapping_file, args.value_mapping_file, args.sanitization_config_file)
    mock_metadata_map.assert_any_call(args.resource_field_mapping_file, args.value_mapping_file, args.sanitization_config_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that map_metadata was called on the package with package-level map
    package1.map_metadata.assert_called_once_with(mock_package_metadata_map)
    
    # Verify that map_metadata was called on the resource with resource-level map
    resource1.map_metadata.assert_called_once_with(mock_resource_metadata_map, package1)
    
    # Verify that the mapped metadata was written to output
    mock_output_writer_instance.write_data.assert_called_once()


@patch('atol_bpa_datamapper.map_metadata.NcbiTaxdump')
@patch('atol_bpa_datamapper.map_metadata.MetadataMap')
@patch('atol_bpa_datamapper.map_metadata.read_input')
@patch('atol_bpa_datamapper.map_metadata.OutputWriter')
@patch('atol_bpa_datamapper.map_metadata.parse_args_for_mapping')
def test_map_metadata_with_different_section_types(mock_parse_args, mock_output_writer, mock_read_input, mock_metadata_map, mock_ncbi_taxdump):
    # Configure the NcbiTaxdump mock to prevent file I/O operations
    mock_ncbi_taxdump.return_value = MagicMock()
    """Test map_metadata with different section types (dict and list)."""
    # This test verifies that:
    # 1. The map_metadata function correctly handles different section types
    # 2. Dictionary sections are correctly processed and written to output
    # 3. List sections are correctly processed and written to output
    # 4. The function correctly handles packages with mixed section types
    # 5. Statistics are correctly calculated for all section types
    
    # Set up mock resources
    resource1 = MagicMock()
    resource1.id = "resource1"
    resource1.mapped_metadata = {
        "dataset": {},
        "organism": {},
        "runs": {"field3": "value3", "field4": "value4"}
    }
    # Add field_mapping to resource1
    resource1.field_mapping = {"field3": "bpa_field3", "field4": "bpa_field4"}
    
    resource2 = MagicMock()
    resource2.id = "resource2"
    resource2.mapped_metadata = {
        "dataset": {},
        "organism": {},
        "runs": {"field3": "value5", "field4": "value6"}
    }
    # Add field_mapping to resource2
    resource2.field_mapping = {"field3": "bpa_field3", "field4": "bpa_field4"}
    
    # Set up mock package
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.mapped_metadata = {
        "dataset": {"field1": "value1"},  # Dictionary section
        "organism": {"field2": "value2"},  # Dictionary section
        "runs": []  # List section to be filled with resource data
    }
    package1.resources = {"resource1": resource1, "resource2": resource2}
    package1.field_mapping = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.mapping_log = [
        {"atol_field": "field1", "bpa_field": "bpa_field1", "value": "raw1", "mapped_value": "value1"},
        {"atol_field": "field2", "bpa_field": "bpa_field2", "value": "raw2", "mapped_value": "value2"}
    ]
    package1.unused_fields = []
    package1.fields = ["bpa_field1", "bpa_field2"]
    package1.sanitization_changes = []
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    # Create two separate metadata map instances for package and resource level
    mock_package_metadata_map = MagicMock()
    mock_package_metadata_map.expected_fields = ["field1", "field2"]
    mock_package_metadata_map.metadata_sections = ["dataset", "organism", "runs"]
    
    mock_resource_metadata_map = MagicMock()
    mock_resource_metadata_map.expected_fields = ["field3", "field4"]
    mock_resource_metadata_map.metadata_sections = ["dataset", "organism", "runs"]
    
    # Configure the MetadataMap mock to return different instances based on arguments
    def metadata_map_side_effect(field_mapping_file, value_mapping_file, sanitization_config_file):
        if field_mapping_file == "package_field_mapping.json":
            return mock_package_metadata_map
        elif field_mapping_file == "resource_field_mapping.json":
            return mock_resource_metadata_map
    
    mock_metadata_map.side_effect = metadata_map_side_effect
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.package_field_mapping_file = "package_field_mapping.json"
    args.resource_field_mapping_file = "resource_field_mapping.json"
    args.value_mapping_file = "value_mapping.json"
    args.sanitization_config_file = "sanitization_config.json"
    args.log_level = "INFO"
    args.dry_run = False
    args.mapping_log = None
    args.raw_field_usage = None
    args.raw_value_usage = None
    args.mapped_field_usage = None
    args.mapped_value_usage = None
    args.unused_field_counts = None
    args.sanitization_changes = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Add mock values for NcbiTaxdump initialization to prevent file I/O
    args.log_level = "INFO"
    args.nodes = MagicMock()
    args.names = MagicMock()
    args.cache_dir = "MagicMock"
    
    # Add mock values for output files to prevent file I/O errors
    args.grouped_packages = None
    args.grouping_log = None
    
    # Configure NcbiTaxdump mock
    mock_ncbi_taxdump_instance = MagicMock()
    mock_ncbi_taxdump.return_value = mock_ncbi_taxdump_instance
    
    # Call the function
    main()
    
    # Verify the function behavior
    assert mock_metadata_map.call_count == 2
    mock_metadata_map.assert_any_call(args.package_field_mapping_file, args.value_mapping_file, args.sanitization_config_file)
    mock_metadata_map.assert_any_call(args.resource_field_mapping_file, args.value_mapping_file, args.sanitization_config_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that map_metadata was called on the package with package-level map
    package1.map_metadata.assert_called_once_with(mock_package_metadata_map)
    
    # Verify that map_metadata was called on the resources with resource-level map
    resource1.map_metadata.assert_called_once_with(mock_resource_metadata_map, package1)
    resource2.map_metadata.assert_called_once_with(mock_resource_metadata_map, package1)
    
    # Verify that the mapped metadata was written to output
    mock_output_writer_instance.write_data.assert_called_once()
