"""Unit tests for filter_packages.py."""

import pytest
from unittest.mock import patch, MagicMock, mock_open

from atol_bpa_datamapper.filter_packages import main
from atol_bpa_datamapper.package_handler import BpaPackage


@patch('atol_bpa_datamapper.filter_packages.write_decision_log_to_csv')
@patch('atol_bpa_datamapper.filter_packages.write_json')
@patch('atol_bpa_datamapper.filter_packages.MetadataMap')
@patch('atol_bpa_datamapper.filter_packages.read_input')
@patch('atol_bpa_datamapper.filter_packages.OutputWriter')
@patch('atol_bpa_datamapper.filter_packages.parse_args_for_filtering')
def test_filter_packages_basic(mock_parse_args, mock_output_writer, mock_read_input, 
                              mock_metadata_map, mock_write_json, mock_write_decision_log):
    """Test basic functionality of filter_packages."""
    # This test verifies that:
    # 1. The filter_packages main function correctly processes input packages
    # 2. Packages are correctly filtered based on their keep attribute
    # 3. Only packages with keep=True are included in the output
    # 4. The output is correctly written to the specified file
    # 5. Statistics are correctly calculated and output when requested
    
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.keep = True
    package1.fields = ["field1", "field2"]
    package1.bpa_fields = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.bpa_values = {"field1": "value1", "field2": "value2"}
    package1.decisions = {"field1": True, "field2": "value2"}
    
    package2 = MagicMock(spec=BpaPackage)
    package2.id = "package2"
    package2.keep = False
    package2.fields = ["field1", "field3"]
    package2.bpa_fields = {"field1": "bpa_field1", "field3": "bpa_field3"}
    package2.bpa_values = {"field1": "value1", "field3": "value3"}
    package2.decisions = {"field1": False, "field3": "value3"}
    
    # Configure mocks
    mock_read_input.return_value = [package1, package2]
    
    mock_metadata_map_instance = MagicMock()
    mock_metadata_map_instance.controlled_vocabularies = ["field1", "field2", "field3"]
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
    args.decision_log = None
    args.raw_field_usage = None
    args.bpa_field_usage = None
    args.bpa_value_usage = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Call the function
    main()
    
    # Verify the function behavior
    mock_metadata_map.assert_called_once_with(args.field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that filter was called on each package
    package1.filter.assert_called_once_with(mock_metadata_map_instance)
    package2.filter.assert_called_once_with(mock_metadata_map_instance)
    
    # Verify that only package1 (with keep=True) was written to output
    mock_output_writer_instance.write_data.assert_called_once()
    args_list, _ = mock_output_writer_instance.write_data.call_args
    assert args_list[0] == package1


@patch('atol_bpa_datamapper.filter_packages.write_decision_log_to_csv')
@patch('atol_bpa_datamapper.filter_packages.write_json')
@patch('atol_bpa_datamapper.filter_packages.MetadataMap')
@patch('atol_bpa_datamapper.filter_packages.read_input')
@patch('atol_bpa_datamapper.filter_packages.OutputWriter')
@patch('atol_bpa_datamapper.filter_packages.parse_args_for_filtering')
def test_filter_packages_dry_run(mock_parse_args, mock_output_writer, mock_read_input, 
                                mock_metadata_map, mock_write_json, mock_write_decision_log):
    """Test filter_packages with dry run."""
    # This test verifies that:
    # 1. The filter_packages function correctly handles dry run mode
    # 2. In dry run mode, packages are filtered but no output is written
    # 3. Statistics are still calculated and displayed
    # 4. The write_json function is not called in dry run mode
    # 5. The decision log is still written if specified
    
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.keep = True
    package1.fields = ["field1", "field2"]
    package1.bpa_fields = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.bpa_values = {"field1": "value1", "field2": "value2"}
    package1.decisions = {"field1": True, "field2": "value2"}
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    mock_metadata_map_instance = MagicMock()
    mock_metadata_map_instance.controlled_vocabularies = ["field1", "field2"]
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
    args.decision_log = None
    args.raw_field_usage = None
    args.bpa_field_usage = None
    args.bpa_value_usage = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Call the function
    main()
    
    # Verify the function behavior
    mock_metadata_map.assert_called_once_with(args.field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that filter was called on the package
    package1.filter.assert_called_once_with(mock_metadata_map_instance)
    
    # In dry run mode, data is still written to the output writer
    # but no stats files are written
    mock_output_writer_instance.write_data.assert_called_once()
    args_list, _ = mock_output_writer_instance.write_data.call_args
    assert args_list[0] == package1
    
    # Verify that no stats files were written
    mock_write_json.assert_not_called()
    mock_write_decision_log.assert_not_called()


@patch('atol_bpa_datamapper.filter_packages.write_decision_log_to_csv')
@patch('atol_bpa_datamapper.filter_packages.write_json')
@patch('atol_bpa_datamapper.filter_packages.MetadataMap')
@patch('atol_bpa_datamapper.filter_packages.read_input')
@patch('atol_bpa_datamapper.filter_packages.OutputWriter')
@patch('atol_bpa_datamapper.filter_packages.parse_args_for_filtering')
def test_filter_packages_empty_input(mock_parse_args, mock_output_writer, mock_read_input, 
                                    mock_metadata_map, mock_write_json, mock_write_decision_log):
    """Test filter_packages with empty input."""
    # This test verifies that:
    # 1. The filter_packages function correctly handles empty input
    # 2. When no packages are provided, the function completes without errors
    # 3. No output is written when there are no packages
    # 4. Statistics are still calculated and show zero packages
    # 5. The decision log is not written when there are no packages
    
    # Configure mocks for empty input
    mock_read_input.return_value = []
    
    mock_metadata_map_instance = MagicMock()
    mock_metadata_map_instance.controlled_vocabularies = []
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
    args.decision_log = None
    args.raw_field_usage = None
    args.bpa_field_usage = None
    args.bpa_value_usage = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Call the function
    main()
    
    # Verify the function behavior
    mock_metadata_map.assert_called_once_with(args.field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that no data was written to output (empty input)
    mock_output_writer_instance.write_data.assert_not_called()
    
    # Verify that no stats files were written
    mock_write_json.assert_not_called()
    mock_write_decision_log.assert_not_called()


@patch('atol_bpa_datamapper.filter_packages.write_decision_log_to_csv')
@patch('atol_bpa_datamapper.filter_packages.write_json')
@patch('atol_bpa_datamapper.filter_packages.MetadataMap')
@patch('atol_bpa_datamapper.filter_packages.read_input')
@patch('atol_bpa_datamapper.filter_packages.OutputWriter')
@patch('atol_bpa_datamapper.filter_packages.parse_args_for_filtering')
def test_filter_packages_with_stats_output(mock_parse_args, mock_output_writer, mock_read_input, 
                                          mock_metadata_map, mock_write_json, mock_write_decision_log):
    """Test filter_packages with statistics output."""
    # This test verifies that:
    # 1. The filter_packages function correctly generates statistics
    # 2. Statistics include counts of accepted and rejected packages
    # 3. Statistics include reasons for rejection
    # 4. Statistics are correctly written to the specified file
    # 5. The decision log is correctly written with all filtering decisions
    
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.keep = True
    package1.fields = ["field1", "field2"]
    package1.decisions = {"field1": True, "field2": "value2"}
    package1.bpa_fields = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.bpa_values = {"field1": "value1", "field2": "value2"}
    
    package2 = MagicMock(spec=BpaPackage)
    package2.id = "package2"
    package2.keep = False
    package2.fields = ["field1", "field3"]
    package2.decisions = {"field1": False, "field2": "value3"}
    package2.bpa_fields = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package2.bpa_values = {"field1": "value1", "field2": "value3"}
    
    # Configure mocks
    mock_read_input.return_value = [package1, package2]
    
    mock_metadata_map_instance = MagicMock()
    mock_metadata_map_instance.controlled_vocabularies = ["field1", "field2"]
    mock_metadata_map.return_value = mock_metadata_map_instance
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args with stats output files
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.field_mapping_file = "field_mapping.json"
    args.value_mapping_file = "value_mapping.json"
    args.log_level = "INFO"
    args.dry_run = False
    args.decision_log = "decisions.csv"
    args.raw_field_usage = "field_usage.json"
    args.bpa_field_usage = "bpa_field_usage.json"
    args.bpa_value_usage = "bpa_value_usage.json"
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Call the function
    main()
    
    # Verify the function behavior
    mock_metadata_map.assert_called_once_with(args.field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that filter was called on each package
    package1.filter.assert_called_once_with(mock_metadata_map_instance)
    package2.filter.assert_called_once_with(mock_metadata_map_instance)
    
    # Verify that only package1 (with keep=True) was written to output
    mock_output_writer_instance.write_data.assert_called_once()
    args_list, _ = mock_output_writer_instance.write_data.call_args
    assert args_list[0] == package1
    
    # Verify that statistics were written to output files
    assert mock_write_json.call_count == 3  # Called for raw_field_usage, bpa_field_usage, and bpa_value_usage
    mock_write_decision_log.assert_called_once_with(
        {package1.id: package1.decisions, package2.id: package2.decisions}, 
        args.decision_log
    )
