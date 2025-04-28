"""Unit tests for map_metadata.py."""

import pytest
from unittest.mock import patch, MagicMock, mock_open

from atol_bpa_datamapper.map_metadata import main
from atol_bpa_datamapper.package_handler import BpaPackage


@patch('atol_bpa_datamapper.map_metadata.MetadataMap')
@patch('atol_bpa_datamapper.map_metadata.read_input')
@patch('atol_bpa_datamapper.map_metadata.OutputWriter')
@patch('atol_bpa_datamapper.map_metadata.parse_args_for_mapping')
def test_map_metadata_basic(mock_parse_args, mock_output_writer, mock_read_input, mock_metadata_map):
    """Test basic functionality of map_metadata."""
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.mapped_metadata = {
        "dataset": {"field1": "value1"},
        "organism": {"field2": "value2"},
        "runs": [{"field3": "value3"}]
    }
    package1.field_mapping = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.mapping_log = [
        {"atol_field": "field1", "bpa_field": "bpa_field1", "value": "raw1", "mapped_value": "value1"},
        {"atol_field": "field2", "bpa_field": "bpa_field2", "value": "raw2", "mapped_value": "value2"},
        {"atol_field": "field3", "bpa_field": "bpa_field3", "value": "raw3", "mapped_value": "value3", "resource_id": "res1"}
    ]
    package1.unused_fields = ["unused1", "unused2"]
    package1.fields = ["bpa_field1", "bpa_field2", "bpa_field3", "unused1", "unused2"]
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    mock_metadata_map_instance = MagicMock()
    mock_metadata_map_instance.expected_fields = ["field1", "field2", "field3"]
    mock_metadata_map.return_value = mock_metadata_map_instance
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.field_mapping_file = "field_mapping.json"
    args.value_mapping_file = "value_mapping.json"
    args.log_level = "INFO"
    args.dry_run = False
    args.mapping_log = None
    args.raw_field_usage = None
    args.raw_value_usage = None
    args.mapped_field_usage = None
    args.mapped_value_usage = None
    args.unused_field_counts = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Call the function
    main()
    
    # Verify the function behavior
    mock_metadata_map.assert_called_once_with(args.field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that map_metadata was called on the package
    package1.map_metadata.assert_called_once_with(mock_metadata_map_instance)
    
    # Verify that the mapped metadata was written to output
    mock_output_writer_instance.write_data.assert_called_once_with(package1.mapped_metadata)


@patch('atol_bpa_datamapper.map_metadata.write_mapping_log_to_csv')
@patch('atol_bpa_datamapper.map_metadata.write_json')
@patch('atol_bpa_datamapper.map_metadata.MetadataMap')
@patch('atol_bpa_datamapper.map_metadata.read_input')
@patch('atol_bpa_datamapper.map_metadata.OutputWriter')
@patch('atol_bpa_datamapper.map_metadata.parse_args_for_mapping')
def test_map_metadata_dry_run(mock_parse_args, mock_output_writer, mock_read_input, mock_metadata_map, 
                             mock_write_json, mock_write_mapping_log):
    """Test map_metadata with dry_run=True."""
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.mapped_metadata = {
        "dataset": {"field1": "value1"},
        "organism": {"field2": "value2"},
        "runs": [{"field3": "value3"}]
    }
    package1.field_mapping = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.mapping_log = []
    package1.unused_fields = []
    package1.fields = ["bpa_field1", "bpa_field2", "bpa_field3"]
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    mock_metadata_map_instance = MagicMock()
    mock_metadata_map_instance.expected_fields = ["field1", "field2", "field3"]
    mock_metadata_map.return_value = mock_metadata_map_instance
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args with dry_run=True
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.field_mapping_file = "field_mapping.json"
    args.value_mapping_file = "value_mapping.json"
    args.log_level = "INFO"
    args.dry_run = True
    args.mapping_log = None
    args.raw_field_usage = None
    args.raw_value_usage = None
    args.mapped_field_usage = None
    args.mapped_value_usage = None
    args.unused_field_counts = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Call the function
    main()
    
    # Verify the function behavior
    mock_metadata_map.assert_called_once_with(args.field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that map_metadata was called on the package
    package1.map_metadata.assert_called_once_with(mock_metadata_map_instance)
    
    # In dry run mode, data is still written to the output writer
    # but no stats files are written
    mock_output_writer_instance.write_data.assert_called_once_with(package1.mapped_metadata)
    
    # Verify that no stats files were written
    mock_write_json.assert_not_called()
    mock_write_mapping_log.assert_not_called()


@patch('atol_bpa_datamapper.map_metadata.MetadataMap')
@patch('atol_bpa_datamapper.map_metadata.read_input')
@patch('atol_bpa_datamapper.map_metadata.OutputWriter')
@patch('atol_bpa_datamapper.map_metadata.write_mapping_log_to_csv')
@patch('atol_bpa_datamapper.map_metadata.write_json')
@patch('atol_bpa_datamapper.map_metadata.parse_args_for_mapping')
def test_map_metadata_with_output_files(mock_parse_args, mock_write_json, mock_write_mapping_log, mock_output_writer, mock_read_input, mock_metadata_map):
    """Test map_metadata with output files."""
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.mapped_metadata = {
        "dataset": {"field1": "value1"},
        "organism": {"field2": "value2"},
        "runs": [{"field3": "value3"}]
    }
    package1.field_mapping = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.mapping_log = [
        {"atol_field": "field1", "bpa_field": "bpa_field1", "value": "raw1", "mapped_value": "value1"},
        {"atol_field": "field2", "bpa_field": "bpa_field2", "value": "raw2", "mapped_value": "value2"},
        {"atol_field": "field3", "bpa_field": "bpa_field3", "value": "raw3", "mapped_value": "value3", "resource_id": "res1"}
    ]
    package1.unused_fields = ["unused1", "unused2"]
    package1.fields = ["bpa_field1", "bpa_field2", "bpa_field3", "unused1", "unused2"]
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    mock_metadata_map_instance = MagicMock()
    mock_metadata_map_instance.expected_fields = ["field1", "field2", "field3"]
    mock_metadata_map.return_value = mock_metadata_map_instance
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args with output files
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.field_mapping_file = "field_mapping.json"
    args.value_mapping_file = "value_mapping.json"
    args.log_level = "INFO"
    args.dry_run = False
    args.mapping_log = "mapping_log.csv"
    args.raw_field_usage = "raw_field_usage.json"
    args.raw_value_usage = "raw_value_usage.json"
    args.mapped_field_usage = "mapped_field_usage.json"
    args.mapped_value_usage = "mapped_value_usage.json"
    args.unused_field_counts = "unused_field_counts.json"
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Call the function
    main()
    
    # Verify the function behavior
    mock_metadata_map.assert_called_once_with(args.field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that map_metadata was called on the package
    package1.map_metadata.assert_called_once_with(mock_metadata_map_instance)
    
    # Verify that the mapped metadata was written to output
    mock_output_writer_instance.write_data.assert_called_once_with(package1.mapped_metadata)
    
    # Verify that the mapping log was written
    mock_write_mapping_log.assert_called_once()
    
    # Verify that the other output files were written
    assert mock_write_json.call_count == 5  # Called for all 5 JSON output files


@patch('atol_bpa_datamapper.map_metadata.MetadataMap')
@patch('atol_bpa_datamapper.map_metadata.read_input')
@patch('atol_bpa_datamapper.map_metadata.OutputWriter')
@patch('atol_bpa_datamapper.map_metadata.parse_args_for_mapping')
def test_map_metadata_with_different_section_types(mock_parse_args, mock_output_writer, mock_read_input, mock_metadata_map):
    """Test map_metadata with different section types (dict and list)."""
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.mapped_metadata = {
        "dataset": {"field1": "value1"},  # Dictionary section
        "runs": [  # List section
            {"field3": "value3", "field4": "value4"},
            {"field3": "value5", "field4": "value6"}
        ]
    }
    package1.field_mapping = {"field1": "bpa_field1"}
    package1.mapping_log = [
        {"atol_field": "field1", "bpa_field": "bpa_field1", "value": "raw1", "mapped_value": "value1"},
        {"atol_field": "field3", "bpa_field": "bpa_field3", "value": "raw3", "mapped_value": "value3", "resource_id": "res1"},
        {"atol_field": "field4", "bpa_field": "bpa_field4", "value": "raw4", "mapped_value": "value4", "resource_id": "res1"},
        {"atol_field": "field3", "bpa_field": "bpa_field3", "value": "raw5", "mapped_value": "value5", "resource_id": "res2"},
        {"atol_field": "field4", "bpa_field": "bpa_field4", "value": "raw6", "mapped_value": "value6", "resource_id": "res2"}
    ]
    package1.unused_fields = []
    package1.fields = ["bpa_field1", "bpa_field3", "bpa_field4"]
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    mock_metadata_map_instance = MagicMock()
    mock_metadata_map_instance.expected_fields = ["field1", "field3", "field4"]
    mock_metadata_map.return_value = mock_metadata_map_instance
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.field_mapping_file = "field_mapping.json"
    args.value_mapping_file = "value_mapping.json"
    args.log_level = "INFO"
    args.dry_run = False
    args.mapping_log = None
    args.raw_field_usage = None
    args.raw_value_usage = None
    args.mapped_field_usage = None
    args.mapped_value_usage = None
    args.unused_field_counts = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Call the function
    main()
    
    # Verify the function behavior
    mock_metadata_map.assert_called_once_with(args.field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that map_metadata was called on the package
    package1.map_metadata.assert_called_once_with(mock_metadata_map_instance)
    
    # Verify that the mapped metadata was written to output
    mock_output_writer_instance.write_data.assert_called_once_with(package1.mapped_metadata)
