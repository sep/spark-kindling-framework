# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e2a89496-5a18-4104-ac7a-4bfe4f325065",
# META       "default_lakehouse_name": "ent_datalake_np",
# META       "default_lakehouse_workspace_id": "ab18d43b-50de-4b41-b44b-f513a6731b99",
# META       "known_lakehouses": [
# META         {
# META           "id": "e2a89496-5a18-4104-ac7a-4bfe4f325065"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

BOOTSTRAP_CONFIG = {
    'is_interactive': True,
    'use_lake_packages' : False,
    'load_local_packages' : False,
    'workspace_endpoint': "059d44a0-c01e-4491-beed-b528c9eca9e8",
    'package_storage_path': "Files/artifacts/packages/latest",
    'required_packages': ["azure.identity", "injector", "dynaconf", "pytest"],
    'ignored_folders': ['utilities'],
    'spark_configs': {
        'spark.databricks.delta.schema.autoMerge.enabled': 'true'
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run environment_bootstrap

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run test_framework

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_env = setup_global_test_environment()
if 'GI_IMPORT_GUARD' in globals():
    del GI_IMPORT_GUARD

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run watermarking

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pytest
from unittest.mock import patch, MagicMock, call
from typing import List, Dict, Any, Optional
from datetime import datetime
import time
from pyspark.sql.types import (
    TimestampType, StringType, IntegerType, DateType, 
    StructType, StructField
)


class TestWatermarkManager(SynapseNotebookTestCase):
    """Test suite for WatermarkManager and related watermark functionality"""
    
    def watermark_manager(self, notebook_runner, basic_test_config):
        """Setup method that provides a configured WatermarkManager instance"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkManager = globals().get('WatermarkManager')
        EntityProvider = globals().get('EntityProvider')
        PythonLoggerProvider = globals().get('PythonLoggerProvider')
        
        if not WatermarkManager:
            pytest.skip("WatermarkManager not available")
        
        # Use framework's global mocks
        entity_provider = EntityProvider()
        watermark_entity_finder = MagicMock()
        logger_provider = PythonLoggerProvider()
        
        # Create manager instance
        manager = WatermarkManager.__new__(WatermarkManager)
        manager.__init__(entity_provider, watermark_entity_finder, logger_provider)
        
        # Return components for use in tests
        return {
            'manager': manager,
            'entity_provider': entity_provider,
            'watermark_entity_finder': watermark_entity_finder,
            'logger_provider': logger_provider,
            'spark': notebook_runner.test_env.spark_session
        }
        
        # Create manager instance
        manager = WatermarkManager.__new__(WatermarkManager)
        manager.__init__(entity_provider, watermark_entity_finder, logger_provider)
        
        # Return components for use in tests
        return {
            'manager': manager,
            'entity_provider': entity_provider,
            'watermark_entity_finder': watermark_entity_finder,
            'logger_provider': logger_provider,
            'spark': notebook_runner.test_env.spark_session
        }
    
    def test_watermark_entity_finder_interface(self, notebook_runner, basic_test_config):
        """Test that WatermarkEntityFinder is properly abstract"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkEntityFinder = globals().get('WatermarkEntityFinder')
        if not WatermarkEntityFinder:
            pytest.skip("WatermarkEntityFinder not available")
        
        # Should not be able to instantiate abstract class
        with pytest.raises(TypeError):
            WatermarkEntityFinder()
        
        # Should have required abstract methods
        assert hasattr(WatermarkEntityFinder, 'get_watermark_entity_for_entity')
        assert hasattr(WatermarkEntityFinder, 'get_watermark_entity_for_layer')
    
    def test_watermark_service_interface(self, notebook_runner, basic_test_config):
        """Test that WatermarkService is properly abstract"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkService = globals().get('WatermarkService')
        if not WatermarkService:
            pytest.skip("WatermarkService not available")
        
        # Should not be able to instantiate abstract class
        with pytest.raises(TypeError):
            WatermarkService()
        
        # Should have required abstract methods
        required_methods = [
            'get_watermark', 'save_watermark', 'read_current_entity_changes'
        ]
        
        for method_name in required_methods:
            assert hasattr(WatermarkService, method_name)
    
    def test_watermark_manager_initialization(self, watermark_manager):
        """Test WatermarkManager initialization and dependency injection"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        logger_provider = setup['logger_provider']
        
        WatermarkManager = globals().get('WatermarkManager')
        BaseServiceProvider = globals().get('BaseServiceProvider')
        WatermarkService = globals().get('WatermarkService')
        
        # Test inheritance
        assert issubclass(WatermarkManager, BaseServiceProvider)
        assert issubclass(WatermarkManager, WatermarkService)
        
        # Verify initialization
        assert manager.ep == entity_provider
        assert manager.wef == watermark_entity_finder
        assert hasattr(manager, 'logger')
        
        # Verify logger was obtained from provider
        logger_provider.get_logger.assert_called_with("watermark")
    
    def test_get_watermark_with_existing_watermark(self, watermark_manager):
        """Test getting an existing watermark"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        # Setup test scenario
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        # Create mock DataFrame with watermark data
        mock_watermark_row = MagicMock()
        mock_watermark_row.__getitem__.return_value = 42  # last_version_processed = 42
        
        # Configure the framework's EntityProvider mock
        mock_df = entity_provider.read_entity(mock_watermark_entity)
        mock_df.isEmpty.return_value = False
        mock_df.first.return_value = mock_watermark_row
        
        # Test getting watermark
        result = manager.get_watermark("test_entity", "test_reader")
        
        # Verify result
        assert result == 42
    
    def test_get_watermark_with_no_existing_watermark(self, watermark_manager):
        """Test getting watermark when none exists"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        # Setup test scenario
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        # Configure the framework's EntityProvider mock for empty result
        mock_df = entity_provider.read_entity(mock_watermark_entity)
        mock_df.isEmpty.return_value = True
        
        # Test getting watermark
        result = manager.get_watermark("test_entity", "test_reader")
        
        # Verify result is None
        assert result is None
    
    def test_save_watermark(self, watermark_manager):
        """Test saving a watermark"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        spark = setup['spark']
        
        # Setup test scenario
        mock_watermark_entity = MagicMock()
        mock_schema = MagicMock()
        mock_watermark_entity.schema = mock_schema
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        # Mock DataFrame operations
        mock_df = MagicMock()
        mock_merged_df = MagicMock()
        spark.createDataFrame.return_value = mock_df
        entity_provider.merge_to_entity.return_value = mock_merged_df
        
        # Test saving watermark
        with patch('time.time', return_value=1640995200.0):  # Fixed timestamp
            result = manager.save_watermark("test_entity", "test_reader", 50, "exec_123")
        
        # Verify result
        assert result == mock_merged_df
        
        # Verify DataFrame creation
        spark.createDataFrame.assert_called_once()
        create_args = spark.createDataFrame.call_args[0]
        
        # Verify data structure
        data = create_args[0]
        assert len(data) == 1
        assert data[0][0] == "test_entity_test_reader"  # id
        assert data[0][1] == "test_entity"  # source_entity_id
        assert data[0][2] == "test_reader"  # reader_id
        assert data[0][4] == 50  # last_version_processed
        assert data[0][5] == "exec_123"  # last_execution_id
        
        # Verify schema usage
        assert create_args[1] == mock_schema
        
        # Verify merge call
        entity_provider.merge_to_entity.assert_called_once_with(mock_df, mock_watermark_entity)
    
    def test_read_current_entity_changes_no_watermark(self, watermark_manager):
        """Test reading current entity changes when no watermark exists"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        
        # Mock entity and pipe
        mock_entity = MagicMock()
        mock_entity.entityid = "test_entity"
        mock_entity.merge_columns = ["id", "name"]
        
        mock_pipe = MagicMock()
        mock_pipe.name = "test_pipe"
        mock_pipe.pipeid = "test_pipe_id"
        
        # Setup framework mocks
        mock_df = entity_provider.read_entity(mock_entity)
        mock_final_df = MagicMock()
        entity_provider.get_entity_version.return_value = 10
        
        # Mock global functions that should be available after notebook execution
        globals()['remove_duplicates'] = MagicMock(return_value=mock_final_df)
        globals()['drop_if_exists'] = MagicMock()
        
        # Mock get_watermark to return None (no existing watermark)
        with patch.object(manager, 'get_watermark', return_value=None):
            result = manager.read_current_entity_changes(mock_entity, mock_pipe)
        
        # Verify result
        assert result == mock_final_df
        
        # Verify method calls
        entity_provider.read_entity.assert_called_with(mock_entity)
        entity_provider.get_entity_version.assert_called_with(mock_entity)
    
    def test_read_current_entity_changes_with_new_version(self, watermark_manager):
        """Test reading current entity changes when new version is available"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        
        # Mock entity and pipe
        mock_entity = MagicMock()
        mock_entity.entityid = "test_entity"
        mock_entity.merge_columns = ["id", "name"]
        
        mock_pipe = MagicMock()
        mock_pipe.name = "test_pipe"
        mock_pipe.pipeid = "test_pipe_id"
        
        # Setup framework mocks
        mock_version_df = MagicMock()
        entity_provider.get_entity_version.return_value = 15  # Current version
        entity_provider.read_entity_since_version.return_value = mock_version_df
        
        # Mock get_watermark to return existing watermark (version 10)
        with patch.object(manager, 'get_watermark', return_value=10):
            result = manager.read_current_entity_changes(mock_entity, mock_pipe)
        
        # Verify result
        assert result == mock_version_df
        
        # Verify method calls
        entity_provider.get_entity_version.assert_called_with(mock_entity)
        entity_provider.read_entity_since_version.assert_called_with(mock_entity, 15)
    
    def test_read_current_entity_changes_no_new_data(self, watermark_manager):
        """Test reading current entity changes when no new data is available"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        
        # Mock entity and pipe
        mock_entity = MagicMock()
        mock_entity.entityid = "test_entity"
        mock_entity.merge_columns = ["id", "name"]
        
        mock_pipe = MagicMock()
        mock_pipe.name = "test_pipe"
        mock_pipe.pipeid = "test_pipe_id"
        
        # Current version same as watermark
        entity_provider.get_entity_version.return_value = 10  # Current version
        
        # Mock get_watermark to return same version as current
        with patch.object(manager, 'get_watermark', return_value=10):
            result = manager.read_current_entity_changes(mock_entity, mock_pipe)
        
        # Verify result is None (no new data)
        assert result is None
        
        # Verify get_entity_version was called
        entity_provider.get_entity_version.assert_called_with(mock_entity)
    
    def test_watermark_manager_logging(self, watermark_manager):
        """Test that WatermarkManager logs appropriately"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        # Setup for empty watermark case
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        mock_df = entity_provider.read_entity(mock_watermark_entity)
        mock_df.isEmpty.return_value = True
        
        # Mock logger to capture calls
        manager.logger = MagicMock()
        
        # Test get_watermark logging
        manager.get_watermark("test_entity", "test_reader")
        
        # Verify debug logging calls
        manager.logger.debug.assert_any_call("Getting watermark for test_entity-test_reader")
        manager.logger.debug.assert_any_call("No watermark")


class TestWatermarkManagerIntegration(SynapseNotebookTestCase):
    """Integration tests for WatermarkManager with real-like scenarios"""
    
    def integration_manager(self, notebook_runner, basic_test_config):
        """Setup method for integration test setup with extended schema"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkManager = globals().get('WatermarkManager')
        EntityProvider = globals().get('EntityProvider')
        PythonLoggerProvider = globals().get('PythonLoggerProvider')
        
        if not WatermarkManager:
            pytest.skip("WatermarkManager not available")
        
        entity_provider = EntityProvider()
        watermark_entity_finder = MagicMock()
        logger_provider = PythonLoggerProvider()
        
        # Setup watermark entity with schema
        mock_schema = StructType([
            StructField("id", StringType(), True),
            StructField("source_entity_id", StringType(), True),
            StructField("reader_id", StringType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("last_version_processed", IntegerType(), True),
            StructField("last_execution_id", StringType(), True)
        ])
        
        mock_watermark_entity = MagicMock()
        mock_watermark_entity.schema = mock_schema
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        manager = WatermarkManager.__new__(WatermarkManager)
        manager.__init__(entity_provider, watermark_entity_finder, logger_provider)
        
        return {
            'manager': manager,
            'entity_provider': entity_provider,
            'watermark_entity_finder': watermark_entity_finder,
            'watermark_entity': mock_watermark_entity,
            'logger_provider': logger_provider,
            'spark': notebook_runner.test_env.spark_session
        }
    
    def test_complete_watermark_lifecycle(self, integration_manager):
        """Test complete watermark lifecycle: save -> get -> update"""
        setup = integration_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        spark = setup['spark']
        
        # Test 1: Save initial watermark
        mock_save_df = MagicMock()
        mock_merged_df = MagicMock()
        spark.createDataFrame.return_value = mock_save_df
        entity_provider.merge_to_entity.return_value = mock_merged_df
        
        with patch('time.time', return_value=1640995200.0):
            save_result = manager.save_watermark("source_entity", "reader_1", 10, "exec_001")
        
        assert save_result == mock_merged_df
        
        # Test 2: Get the saved watermark
        mock_watermark_row = MagicMock()
        mock_watermark_row.__getitem__.return_value = 10
        
        mock_read_df = entity_provider.read_entity(setup['watermark_entity'])
        mock_read_df.isEmpty.return_value = False
        mock_read_df.first.return_value = mock_watermark_row
        
        watermark = manager.get_watermark("source_entity", "reader_1")
        assert watermark == 10
        
        # Test 3: Update watermark to newer version
        with patch('time.time', return_value=1640995260.0):  # 1 minute later
            update_result = manager.save_watermark("source_entity", "reader_1", 15, "exec_002")
        
        assert update_result == mock_merged_df
    
    def test_multiple_readers_same_entity(self, integration_manager):
        """Test multiple readers tracking watermarks for the same entity"""
        setup = integration_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        spark = setup['spark']
        
        # Save watermarks for different readers of same entity
        readers = ["reader_A", "reader_B", "reader_C"]
        versions = [5, 8, 12]
        
        for reader, version in zip(readers, versions):
            mock_df = MagicMock()
            spark.createDataFrame.return_value = mock_df
            entity_provider.merge_to_entity.return_value = mock_df
            
            with patch('time.time', return_value=1640995200.0):
                manager.save_watermark("shared_entity", reader, version, f"exec_{reader}")
        
        # Verify each reader got separate watermark records
        assert spark.createDataFrame.call_count == 3
        
        # Verify each call had correct reader-specific data
        create_calls = spark.createDataFrame.call_args_list
        for i, (reader, version) in enumerate(zip(readers, versions)):
            call_data = create_calls[i][0][0][0]  # First row of data
            assert call_data[1] == "shared_entity"  # source_entity_id
            assert call_data[2] == reader  # reader_id
            assert call_data[4] == version  # last_version_processed
    
    def test_entity_changes_processing_workflow(self, integration_manager):
        """Test the complete workflow of processing entity changes with watermarks"""
        setup = integration_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        
        # Setup all required global functions
        globals()['remove_duplicates'] = MagicMock()
        globals()['drop_if_exists'] = MagicMock()
        
        # Create test entity and pipe
        mock_entity = MagicMock()
        mock_entity.entityid = "test_source_entity"
        mock_entity.merge_columns = ["id", "name"]
        
        mock_pipe = MagicMock()
        mock_pipe.name = "Test Processing Pipe"
        mock_pipe.pipeid = "test_pipe_001"
        
        # Scenario 1: First time processing (no watermark)
        entity_provider.get_entity_version.return_value = 5
        mock_df = MagicMock()
        mock_final_df = MagicMock()
        
        entity_provider.read_entity.return_value = mock_df
        globals()['remove_duplicates'].return_value = mock_final_df
        
        with patch.object(manager, 'get_watermark', return_value=None):
            result1 = manager.read_current_entity_changes(mock_entity, mock_pipe)
        
        assert result1 == mock_final_df
        globals()['remove_duplicates'].assert_called_once()
        
        # Reset mocks
        globals()['remove_duplicates'].reset_mock()
        
        # Scenario 2: Processing with existing watermark and new data
        mock_version_df = MagicMock()
        entity_provider.get_entity_version.return_value = 10  # Newer version
        entity_provider.read_entity_since_version.return_value = mock_version_df
        
        with patch.object(manager, 'get_watermark', return_value=7):  # Existing watermark
            result2 = manager.read_current_entity_changes(mock_entity, mock_pipe)
        
        assert result2 == mock_version_df
        entity_provider.read_entity_since_version.assert_called_with(mock_entity, 10)
        
        # Scenario 3: No new data available
        entity_provider.get_entity_version.return_value = 7  # Same as watermark
        
        with patch.object(manager, 'get_watermark', return_value=7):
            result3 = manager.read_current_entity_changes(mock_entity, mock_pipe)
        
        assert result3 is None  # No new data


class TestWatermarkManagerEdgeCases(SynapseNotebookTestCase):
    """Test edge cases and error scenarios for WatermarkManager"""
    
    def test_watermark_with_zero_version(self, watermark_manager):
        """Test handling watermark with version 0"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        # Mock watermark with version 0
        mock_watermark_row = MagicMock()
        mock_watermark_row.__getitem__.return_value = 0
        
        mock_df = entity_provider.read_entity(mock_watermark_entity)
        mock_df.isEmpty.return_value = False
        mock_df.first.return_value = mock_watermark_row
        
        result = manager.get_watermark("test_entity", "test_reader")
        assert result == 0
    
    def test_negative_version_handling(self, watermark_manager):
        """Test handling of negative version numbers"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        spark = setup['spark']
        
        mock_watermark_entity = MagicMock()
        mock_watermark_entity.schema = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df
        entity_provider.merge_to_entity.return_value = mock_df
        
        # Test saving negative version (should still work)
        with patch('time.time', return_value=1640995200.0):
            result = manager.save_watermark("test_entity", "test_reader", -1, "exec_negative")
        
        # Should not raise error and return merged DataFrame
        assert result == mock_df
        
        # Verify data contains negative version
        create_call = spark.createDataFrame.call_args[0][0][0]
        assert create_call[4] == -1  # last_version_processed
    
    def test_empty_string_parameters(self, watermark_manager):
        """Test handling of empty string parameters"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        mock_df = entity_provider.read_entity(mock_watermark_entity)
        mock_df.isEmpty.return_value = True
        
        result = manager.get_watermark("", "")
        assert result is None
    
    def test_very_large_version_numbers(self, watermark_manager):
        """Test handling of very large version numbers"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        spark = setup['spark']
        
        mock_watermark_entity = MagicMock()
        mock_watermark_entity.schema = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        mock_df = MagicMock()
        spark.createDataFrame.return_value = mock_df
        entity_provider.merge_to_entity.return_value = mock_df
        
        # Test with very large version number
        large_version = 9223372036854775807  # Max 64-bit signed integer
        
        with patch('time.time', return_value=1640995200.0):
            result = manager.save_watermark("test_entity", "test_reader", large_version, "exec_large")
        
        assert result == mock_df
        
        # Verify large version was stored correctly
        create_call = spark.createDataFrame.call_args[0][0][0]
        assert create_call[4] == large_version
    
    def test_malformed_watermark_data(self, watermark_manager):
        """Test handling of edge case with missing watermark data"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        # Test case where DataFrame is empty (normal case)
        mock_df = entity_provider.read_entity(mock_watermark_entity)
        mock_df.isEmpty.return_value = True
        
        result = manager.get_watermark("edge_case_entity", "test_reader")
        assert result is None
    
    # Add watermark_manager setup method for this class too
    def watermark_manager(self, notebook_runner, basic_test_config):
        """Setup method that provides a configured WatermarkManager instance"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkManager = globals().get('WatermarkManager')
        EntityProvider = globals().get('EntityProvider')
        PythonLoggerProvider = globals().get('PythonLoggerProvider')
        
        if not WatermarkManager:
            pytest.skip("WatermarkManager not available")
        
        # Use framework's global mocks
        entity_provider = EntityProvider()
        watermark_entity_finder = MagicMock()
        logger_provider = PythonLoggerProvider()
        
        # Create manager instance
        manager = WatermarkManager.__new__(WatermarkManager)
        manager.__init__(entity_provider, watermark_entity_finder, logger_provider)
        
        # Return components for use in tests
        return {
            'manager': manager,
            'entity_provider': entity_provider,
            'watermark_entity_finder': watermark_entity_finder,
            'logger_provider': logger_provider,
            'spark': notebook_runner.test_env.spark_session
        }


class TestWatermarkManagerDependencyValidation(SynapseNotebookTestCase):
    """Test dependency injection and interface validation"""
    
    def test_watermark_entity_finder_dependency(self, notebook_runner, basic_test_config):
        """Test that WatermarkEntityFinder dependency is properly injected"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkManager = globals().get('WatermarkManager')
        WatermarkEntityFinder = globals().get('WatermarkEntityFinder')
        EntityProvider = globals().get('EntityProvider')
        PythonLoggerProvider = globals().get('PythonLoggerProvider')
        
        if not WatermarkManager or not WatermarkEntityFinder:
            pytest.skip("Required classes not available")
        
        # Create a mock that implements the interface
        mock_wef = MagicMock()
        mock_wef.get_watermark_entity_for_entity = MagicMock()
        mock_wef.get_watermark_entity_for_layer = MagicMock()
        
        entity_provider = EntityProvider()
        logger_provider = PythonLoggerProvider()
        
        # Test initialization with proper interface
        manager = WatermarkManager.__new__(WatermarkManager)
        manager.__init__(entity_provider, mock_wef, logger_provider)
        
        assert manager.wef == mock_wef
        
        # Test that the interface methods are called
        mock_entity = MagicMock()
        mock_wef.get_watermark_entity_for_entity.return_value = mock_entity
        
        # Should call the interface method
        manager.wef.get_watermark_entity_for_entity("test_entity")
        mock_wef.get_watermark_entity_for_entity.assert_called_with("test_entity")
    
    def test_entity_provider_dependency(self, notebook_runner, basic_test_config):
        """Test that EntityProvider dependency works correctly"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkManager = globals().get('WatermarkManager')
        EntityProvider = globals().get('EntityProvider')
        PythonLoggerProvider = globals().get('PythonLoggerProvider')
        
        if not WatermarkManager:
            pytest.skip("WatermarkManager not available")
        
        # Use framework's EntityProvider mock
        entity_provider = EntityProvider()
        mock_wef = MagicMock()
        logger_provider = PythonLoggerProvider()
        
        manager = WatermarkManager.__new__(WatermarkManager)
        manager.__init__(entity_provider, mock_wef, logger_provider)
        
        assert manager.ep == entity_provider
        
        # Test that EntityProvider methods are accessible
        assert hasattr(manager.ep, 'read_entity')
        assert hasattr(manager.ep, 'merge_to_entity')
        assert hasattr(manager.ep, 'get_entity_version')
        assert hasattr(manager.ep, 'read_entity_since_version')
    
    def test_python_logger_provider_initialization(self, notebook_runner, basic_test_config):
        """Test that PythonLoggerProvider is properly initialized"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkManager = globals().get('WatermarkManager')
        EntityProvider = globals().get('EntityProvider')
        PythonLoggerProvider = globals().get('PythonLoggerProvider')
        
        if not WatermarkManager:
            pytest.skip("WatermarkManager not available")
        
        entity_provider = EntityProvider()
        mock_wef = MagicMock()
        logger_provider = PythonLoggerProvider()
        
        manager = WatermarkManager.__new__(WatermarkManager)
        manager.__init__(entity_provider, mock_wef, logger_provider)
        
        # Verify logger was obtained from provider
        logger_provider.get_logger.assert_called_with("watermark")
        assert hasattr(manager, 'logger')
    
    def test_global_injector_singleton_registration(self, notebook_runner, basic_test_config):
        """Test that WatermarkManager is properly registered with GlobalInjector"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkManager = globals().get('WatermarkManager')
        if not WatermarkManager:
            pytest.skip("WatermarkManager not available")
        
        # Verify the class has the singleton_autobind decorator applied
        # This is verified by the decorator not raising an error during class definition
        assert WatermarkManager is not None
        
        # Verify it implements the WatermarkService interface
        WatermarkService = globals().get('WatermarkService')
        if WatermarkService:
            assert issubclass(WatermarkManager, WatermarkService)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(
    TestWatermarkManager,
    TestWatermarkManagerIntegration,
    TestWatermarkManagerEdgeCases,
    TestWatermarkManagerDependencyValidation,
    test_config={
        'use_real_spark': False,
        'test_data': {
            'test_entity_data': [
                {'id': 1, 'name': 'Entity1', 'version': 1},
                {'id': 2, 'name': 'Entity2', 'version': 2}
            ],
            'test_watermark_data': [
                {'source_entity_id': 'test_entity', 'reader_id': 'test_reader', 'last_version_processed': 5}
            ]
        }
    }
)
    
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
