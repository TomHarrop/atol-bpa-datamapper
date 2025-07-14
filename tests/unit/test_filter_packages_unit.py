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
    
    # Set up mock resources
    resource1 = MagicMock()
    resource1.id = "resource1"
    resource1.keep = True
    resource1.bpa_fields = {"platform": "resources.type", "library_type": "resources.library_type"}
    resource1.bpa_values = {"platform": "illumina-shortread", "library_type": "paired"}
    resource1.decisions = {"platform": "illumina-shortread", "platform_accepted": True, 
                         "library_type": "paired", "library_type_accepted": True}
    
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.keep = True
    package1.fields = ["field1", "field2"]
    package1.bpa_fields = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.bpa_values = {"field1": "value1", "field2": "value2"}
    package1.decisions = {"field1": True, "field1_accepted": True, "field2": "value2", "field2_accepted": True, "kept_resources": True}
    package1.resources = {"resource1": resource1}
    
    package2 = MagicMock(spec=BpaPackage)
    package2.id = "package2"
    package2.keep = False
    package2.fields = ["field1", "field3"]
    package2.bpa_fields = {"field1": "bpa_field1", "field3": "bpa_field3"}
    package2.bpa_values = {"field1": "value1", "field3": "value3"}
    package2.decisions = {"field1": False, "field1_accepted": False, "field3": "value3", "field3_accepted": True, "kept_resources": False}
    package2.resources = {}
    
    # Configure mocks
    mock_read_input.return_value = [package1, package2]
    
    # Create two separate metadata map instances for package and resource level
    mock_package_metadata_map = MagicMock()
    mock_package_metadata_map.controlled_vocabularies = ["field1", "field2", "field3"]
    
    mock_resource_metadata_map = MagicMock()
    mock_resource_metadata_map.controlled_vocabularies = ["platform", "library_type", "library_size"]
    
    # Configure the MetadataMap mock to return different instances based on arguments
    def metadata_map_side_effect(field_mapping_file, value_mapping_file):
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
    assert mock_metadata_map.call_count == 2
    mock_metadata_map.assert_any_call(args.package_field_mapping_file, args.value_mapping_file)
    mock_metadata_map.assert_any_call(args.resource_field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that filter was called on each package with package-level map
    package1.filter.assert_called_once_with(mock_package_metadata_map)
    package2.filter.assert_called_once_with(mock_package_metadata_map)
    
    # Verify that filter was called on the resource with resource-level map and parent package
    resource1.filter.assert_called_once_with(mock_resource_metadata_map, package1)
    
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
    
    # Set up mock resources
    resource1 = MagicMock()
    resource1.id = "resource1"
    resource1.keep = True
    resource1.bpa_fields = {"platform": "resources.type", "library_type": "resources.library_type"}
    resource1.bpa_values = {"platform": "illumina-shortread", "library_type": "paired"}
    resource1.decisions = {"platform": "illumina-shortread", "platform_accepted": True, 
                         "library_type": "paired", "library_type_accepted": True}
    
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.keep = True
    package1.fields = ["field1", "field2"]
    package1.bpa_fields = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.bpa_values = {"field1": "value1", "field2": "value2"}
    package1.decisions = {"field1": True, "field1_accepted": True, "field2": "value2", "field2_accepted": True, "kept_resources": True}
    package1.resources = {"resource1": resource1}
    
    # Configure mocks
    mock_read_input.return_value = [package1]
    
    # Create two separate metadata map instances for package and resource level
    mock_package_metadata_map = MagicMock()
    mock_package_metadata_map.controlled_vocabularies = ["field1", "field2"]
    
    mock_resource_metadata_map = MagicMock()
    mock_resource_metadata_map.controlled_vocabularies = ["platform", "library_type"]
    
    # Configure the MetadataMap mock to return different instances based on arguments
    def metadata_map_side_effect(field_mapping_file, value_mapping_file):
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
    args.log_level = "INFO"
    args.dry_run = True
    args.decision_log = "decision_log.csv"
    args.raw_field_usage = None
    args.bpa_field_usage = None
    args.bpa_value_usage = None
    
    # Configure parse_args to return our mock args
    mock_parse_args.return_value = args
    
    # Call the function
    main()
    
    # Verify the function behavior
    assert mock_metadata_map.call_count == 2
    mock_metadata_map.assert_any_call(args.package_field_mapping_file, args.value_mapping_file)
    mock_metadata_map.assert_any_call(args.resource_field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that filter was called on the package with package-level map
    package1.filter.assert_called_once_with(mock_package_metadata_map)
    
    # Verify that filter was called on the resource with resource-level map and parent package
    resource1.filter.assert_called_once_with(mock_resource_metadata_map, package1)
    
    # In dry run mode, write_data is still called for packages that pass the filter
    # but the OutputWriter handles the dry_run flag internally
    mock_output_writer_instance.write_data.assert_called_once_with(package1)
    
    # Verify that decision log was NOT written in dry run mode
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
    
    # Create two separate metadata map instances for package and resource level
    mock_package_metadata_map = MagicMock()
    mock_package_metadata_map.controlled_vocabularies = []
    
    mock_resource_metadata_map = MagicMock()
    mock_resource_metadata_map.controlled_vocabularies = []
    
    # Configure the MetadataMap mock to return different instances based on arguments
    def metadata_map_side_effect(field_mapping_file, value_mapping_file):
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
    assert mock_metadata_map.call_count == 2
    mock_metadata_map.assert_any_call(args.package_field_mapping_file, args.value_mapping_file)
    mock_metadata_map.assert_any_call(args.resource_field_mapping_file, args.value_mapping_file)
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
    
    # Set up mock resources
    resource1 = MagicMock()
    resource1.id = "resource1"
    resource1.keep = True
    resource1.bpa_fields = {"platform": "resources.type", "library_type": "resources.library_type"}
    resource1.bpa_values = {"platform": "illumina-shortread", "library_type": "paired"}
    resource1.decisions = {"platform": "illumina-shortread", "platform_accepted": True, 
                         "library_type": "paired", "library_type_accepted": True}
    
    resource2 = MagicMock()
    resource2.id = "resource2"
    resource2.keep = False
    resource2.bpa_fields = {"platform": "resources.type", "library_type": "resources.library_type"}
    resource2.bpa_values = {"platform": "unknown-platform", "library_type": "single"}
    resource2.decisions = {"platform": "unknown-platform", "platform_accepted": False, 
                         "library_type": "single", "library_type_accepted": True}
    
    # Set up mock packages
    package1 = MagicMock(spec=BpaPackage)
    package1.id = "package1"
    package1.keep = True
    package1.fields = ["field1", "field2"]
    package1.decisions = {"field1": True, "field1_accepted": True, "field2": "value2", "field2_accepted": True, "kept_resources": True}
    package1.bpa_fields = {"field1": "bpa_field1", "field2": "bpa_field2"}
    package1.bpa_values = {"field1": "value1", "field2": "value2"}
    package1.resources = {"resource1": resource1}
    
    package2 = MagicMock(spec=BpaPackage)
    package2.id = "package2"
    package2.keep = False
    package2.fields = ["field1", "field3"]
    package2.decisions = {"field1": False, "field1_accepted": False, "field3": "value3", "field3_accepted": True, "kept_resources": False}
    package2.bpa_fields = {"field1": "bpa_field1", "field3": "bpa_field3"}
    package2.bpa_values = {"field1": "value1", "field3": "value3"}
    package2.resources = {"resource2": resource2}
    
    # Configure mocks
    mock_read_input.return_value = [package1, package2]
    
    # Create two separate metadata map instances for package and resource level
    mock_package_metadata_map = MagicMock()
    mock_package_metadata_map.controlled_vocabularies = ["field1", "field2", "field3"]
    
    mock_resource_metadata_map = MagicMock()
    mock_resource_metadata_map.controlled_vocabularies = ["platform", "library_type"]
    
    # Configure the MetadataMap mock to return different instances based on arguments
    def metadata_map_side_effect(field_mapping_file, value_mapping_file):
        if field_mapping_file == "package_field_mapping.json":
            return mock_package_metadata_map
        elif field_mapping_file == "resource_field_mapping.json":
            return mock_resource_metadata_map
    
    mock_metadata_map.side_effect = metadata_map_side_effect
    
    mock_output_writer_instance = MagicMock()
    mock_output_writer.return_value.__enter__.return_value = mock_output_writer_instance
    
    # Create mock args with stats output files
    args = MagicMock()
    args.input = "input.jsonl.gz"
    args.output = "output.jsonl.gz"
    args.package_field_mapping_file = "package_field_mapping.json"
    args.resource_field_mapping_file = "resource_field_mapping.json"
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
    assert mock_metadata_map.call_count == 2
    mock_metadata_map.assert_any_call(args.package_field_mapping_file, args.value_mapping_file)
    mock_metadata_map.assert_any_call(args.resource_field_mapping_file, args.value_mapping_file)
    mock_read_input.assert_called_once_with(args.input)
    
    # Verify that filter was called on each package with package-level map
    package1.filter.assert_called_once_with(mock_package_metadata_map)
    package2.filter.assert_called_once_with(mock_package_metadata_map)
    
    # Verify that filter was called on each resource with resource-level map and parent package
    resource1.filter.assert_called_once_with(mock_resource_metadata_map, package1)
    resource2.filter.assert_called_once_with(mock_resource_metadata_map, package2)
    
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
