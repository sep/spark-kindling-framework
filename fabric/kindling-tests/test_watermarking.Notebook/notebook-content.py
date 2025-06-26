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
    'log_level': "INFO",
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

test_config={
    'use_real_spark': True,
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
    
    def test_watermark_service_interface(self, notebook_runner, basic_test_config):
        """Test that WatermarkService is properly abstract"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        WatermarkService = globals().get('WatermarkService')
        if not WatermarkService:
            pytest.skip("WatermarkService not available")
        
        # Should have required abstract methods
        required_methods = [
            'get_watermark', 'save_watermark', 'read_current_entity_changes'
        ]
        
        for method_name in required_methods:
            assert hasattr(WatermarkService, method_name), \
                f"Required method missing: {method_name} method should exist"
    
    def test_watermark_manager_initialization(self, watermark_manager):
        """Test WatermarkManager initialization and dependency injection"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        logger_provider = setup['logger_provider']
        
        WatermarkManager = globals().get('WatermarkManager')
        WatermarkService = globals().get('WatermarkService')
        
        # Test inheritance contract
        assert issubclass(WatermarkManager, WatermarkService), \
            "Inheritance contract failed: WatermarkManager should inherit from WatermarkService"
        
        # Verify dependency injection contract
        assert manager.ep == entity_provider, \
            "Dependency injection failed: Entity provider should be correctly assigned"
        assert manager.wef == watermark_entity_finder, \
            "Dependency injection failed: Watermark entity finder should be correctly assigned"
        assert hasattr(manager, 'logger'), \
            "Logger initialization failed: Logger attribute should exist"
        
        # Verify logger was obtained from provider (contract behavior)
        logger_provider.get_logger.assert_called_with("watermark")
    
    def test_get_watermark_with_existing_watermark(self, watermark_manager):
        """Test getting an existing watermark - contract behavior"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        # Setup test scenario
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        # Create mock watermark data
        mock_watermark_row = MagicMock()
        mock_watermark_row.__getitem__ = lambda self, key: 42 if key == "last_version_processed" else None
        mock_watermark_row.last_version_processed = 42
        
        # Create a mock DataFrame that supports the query chain
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.limit.return_value = mock_df
        mock_df.isEmpty.return_value = False
        mock_df.first.return_value = mock_watermark_row
        
        entity_provider.read_entity.return_value = mock_df
        
        # Test the contract: get_watermark should return the version number
        result = manager.get_watermark("test_entity", "test_reader")
        
        # Verify contract: correct entity was requested
        watermark_entity_finder.get_watermark_entity_for_entity.assert_called_with("test_entity")
        entity_provider.read_entity.assert_called_with(mock_watermark_entity)
        
        # Verify contract: result should be the version number
        assert result == 42, \
            f"get_watermark contract failed: Expected=42, Got={result}"
        
        # Verify contract: proper filtering was applied
        mock_df.filter.assert_called_once()
        mock_df.select.assert_called_once_with("last_version_processed")
        mock_df.limit.assert_called_once_with(1)
    
    def test_get_watermark_with_no_existing_watermark(self, watermark_manager):
        """Test getting watermark when none exists - contract behavior"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        # Setup test scenario
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        # Create a mock DataFrame that returns empty
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.limit.return_value = mock_df
        mock_df.isEmpty.return_value = True
        
        entity_provider.read_entity.return_value = mock_df
        
        # Test the contract: get_watermark should return None for non-existent watermark
        result = manager.get_watermark("test_entity", "test_reader")
        
        # Verify contract: None should be returned when no watermark exists
        assert result is None, \
            f"get_watermark contract failed: Expected=None when no watermark exists, Got={result}"
        
        # Verify contract: first() should not be called on empty DataFrame
        mock_df.first.assert_not_called()
    
    def test_save_watermark_contract(self, watermark_manager):
        """Test saving a watermark - contract behavior"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        # Setup test scenario with real schema (createDataFrame requires valid schema)
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
        
        real_schema = StructType([
            StructField("id", StringType(), True),
            StructField("source_entity_id", StringType(), True),
            StructField("reader_id", StringType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("last_version_processed", IntegerType(), True),
            StructField("last_execution_id", StringType(), True)
        ])
        
        mock_watermark_entity = MagicMock()
        mock_watermark_entity.schema = real_schema  # Real schema needed for createDataFrame
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        # Mock the merge operation result
        mock_merged_df = MagicMock()
        entity_provider.merge_to_entity.return_value = mock_merged_df
        
        # Test the contract: save_watermark should create and merge watermark data
        with patch('time.time', return_value=1640995200.0):
            result = manager.save_watermark("test_entity", "test_reader", 50, "exec_123")
        
        # Verify contract: method should return result from merge operation
        assert result == mock_merged_df, \
            f"save_watermark contract failed: Expected result from merge_to_entity, Got={result}"
        
        # Verify contract: correct watermark entity was requested
        watermark_entity_finder.get_watermark_entity_for_entity.assert_called_with("test_entity")
        
        # Verify contract: merge_to_entity was called with DataFrame and entity
        entity_provider.merge_to_entity.assert_called_once()
        merge_call_args = entity_provider.merge_to_entity.call_args[0]
        
        # Verify contract: correct entity was passed to merge
        entity_used = merge_call_args[1]
        assert entity_used == mock_watermark_entity, \
            f"Entity contract failed: Expected={mock_watermark_entity}, Got={entity_used}"
        
        # Verify contract: DataFrame contains correct watermark data
        df_passed = merge_call_args[0]
        df_data = df_passed.collect()
        assert len(df_data) == 1, \
            f"Watermark record count contract failed: Expected=1 record, Got={len(df_data)}"
        
        watermark_row = df_data[0]
        
        # Verify contract: all required fields are present with correct values
        assert watermark_row.source_entity_id == "test_entity", \
            f"Source entity contract failed: Expected='test_entity', Got='{watermark_row.source_entity_id}'"
        assert watermark_row.reader_id == "test_reader", \
            f"Reader ID contract failed: Expected='test_reader', Got='{watermark_row.reader_id}'"
        assert watermark_row.last_version_processed == 50, \
            f"Version contract failed: Expected=50, Got={watermark_row.last_version_processed}"
        assert watermark_row.last_execution_id == "exec_123", \
            f"Execution ID contract failed: Expected='exec_123', Got='{watermark_row.last_execution_id}'"
        
        # Verify contract: ID follows expected format (business rule)
        assert watermark_row.id == "test_entity_test_reader", \
            f"ID format contract failed: Expected='test_entity_test_reader', Got='{watermark_row.id}'"
        
        # Verify contract: timestamp field exists
        assert hasattr(watermark_row, 'last_updated'), \
            "Timestamp contract failed: last_updated field should exist"
    
    def test_read_current_entity_changes_no_watermark(self, watermark_manager):
        """Test reading changes when no watermark exists - contract behavior"""
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
        mock_df = MagicMock()
        mock_final_df = MagicMock()
        entity_provider.read_entity.return_value = mock_df
        entity_provider.get_entity_version.return_value = 10
        
        # Mock global functions
        globals()['remove_duplicates'] = MagicMock(return_value=mock_final_df)
        globals()['drop_if_exists'] = MagicMock()
        
        # Contract: when no watermark exists, should read full entity
        with patch.object(manager, 'get_watermark', return_value=None):
            result = manager.read_current_entity_changes(mock_entity, mock_pipe)
        
        # Verify contract: should return processed DataFrame
        assert result == mock_final_df, \
            f"read_current_entity_changes contract failed: Expected={mock_final_df}, Got={result}"
        
        # Verify contract: should read full entity when no watermark
        entity_provider.read_entity.assert_called_with(mock_entity)
        entity_provider.get_entity_version.assert_called_with(mock_entity)
        
        # Verify contract: should call remove_duplicates for data processing
        globals()['remove_duplicates'].assert_called_once()
    
    def test_read_current_entity_changes_with_new_version(self, watermark_manager):
        """Test reading changes when new version is available - contract behavior"""
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
        
        # Contract: when watermark < current version, should read incremental changes
        with patch.object(manager, 'get_watermark', return_value=10):  # Existing watermark
            result = manager.read_current_entity_changes(mock_entity, mock_pipe)
        
        # Verify contract: should return changes DataFrame
        assert result == mock_version_df, \
            f"read_current_entity_changes contract failed: Expected={mock_version_df}, Got={result}"
        
        # Verify contract: should read incremental changes since current version
        entity_provider.get_entity_version.assert_called_with(mock_entity)
        entity_provider.read_entity_since_version.assert_called_with(mock_entity, 15)
    
    def test_read_current_entity_changes_no_new_data(self, watermark_manager):
        """Test reading changes when no new data is available - contract behavior"""
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
        
        # Contract: when watermark == current version, should return None (no new data)
        with patch.object(manager, 'get_watermark', return_value=10):
            result = manager.read_current_entity_changes(mock_entity, mock_pipe)
        
        # Verify contract: should return None when no new data available
        assert result is None, \
            f"read_current_entity_changes contract failed: Expected=None when no new data, Got={result}"
        
        # Verify contract: should still check entity version
        entity_provider.get_entity_version.assert_called_with(mock_entity)
    
    def test_watermark_with_zero_version(self, watermark_manager):
        """Test handling watermark with version 0 - edge case contract"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        # Mock watermark row with version 0
        mock_watermark_row = MagicMock()
        mock_watermark_row.__getitem__ = lambda self, key: 0 if key == "last_version_processed" else None
        mock_watermark_row.last_version_processed = 0
        
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.limit.return_value = mock_df
        mock_df.isEmpty.return_value = False
        mock_df.first.return_value = mock_watermark_row
        
        entity_provider.read_entity.return_value = mock_df
        
        # Contract: version 0 should be treated as valid (not falsy)
        result = manager.get_watermark("test_entity", "test_reader")
        assert result == 0, \
            f"Zero version contract failed: Expected=0, Got={result} (should not treat 0 as falsy)"
    
    def test_negative_version_handling(self, watermark_manager):
        """Test handling of negative version numbers - edge case contract"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        # Setup test scenario with real schema (createDataFrame requires valid schema)
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
        
        real_schema = StructType([
            StructField("id", StringType(), True),
            StructField("source_entity_id", StringType(), True),
            StructField("reader_id", StringType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("last_version_processed", IntegerType(), True),
            StructField("last_execution_id", StringType(), True)
        ])
        
        mock_watermark_entity = MagicMock()
        mock_watermark_entity.schema = real_schema  # Real schema needed for createDataFrame
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        mock_merged_df = MagicMock()
        entity_provider.merge_to_entity.return_value = mock_merged_df
        
        # Contract: negative versions should be handled without error
        with patch('time.time', return_value=1640995200.0):
            result = manager.save_watermark("test_entity", "test_reader", -1, "exec_negative")
        
        # Verify contract: should not raise error and return merged DataFrame
        assert result == mock_merged_df, \
            f"Negative version contract failed: Expected={mock_merged_df}, Got={result}"
        
        # Verify contract: merge_to_entity was called with DataFrame containing negative version
        entity_provider.merge_to_entity.assert_called_once()
        df_passed = entity_provider.merge_to_entity.call_args[0][0]
        watermark_data = df_passed.collect()[0]
        
        assert watermark_data.last_version_processed == -1, \
            f"Negative version preservation contract failed: Expected=-1, Got={watermark_data.last_version_processed}"
    
    def test_watermark_manager_logging_contract(self, watermark_manager):
        """Test that WatermarkManager logs appropriately - logging contract"""
        setup = watermark_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        watermark_entity_finder = setup['watermark_entity_finder']
        
        # Setup for empty watermark case
        mock_watermark_entity = MagicMock()
        watermark_entity_finder.get_watermark_entity_for_entity.return_value = mock_watermark_entity
        
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.limit.return_value = mock_df
        mock_df.isEmpty.return_value = True
        
        entity_provider.read_entity.return_value = mock_df
        
        # Mock logger to capture calls
        manager.logger = MagicMock()
        
        # Contract: should log debug messages for traceability
        manager.get_watermark("test_entity", "test_reader")
        
        # Verify logging contract: should log watermark retrieval attempt
        manager.logger.debug.assert_any_call("Getting watermark for test_entity-test_reader")
        manager.logger.debug.assert_any_call("No watermark")


class TestWatermarkManagerIntegration(SynapseNotebookTestCase):
    """Integration tests for WatermarkManager with real-like scenarios"""
    
    def integration_manager(self, notebook_runner, basic_test_config):
        """Setup method for integration test setup"""
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
    
    def test_complete_watermark_lifecycle_contract(self, integration_manager):
        """Test complete watermark lifecycle: save -> get -> update - integration contract"""
        setup = integration_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        
        # Test 1: Save initial watermark
        mock_merged_df = MagicMock()
        entity_provider.merge_to_entity.return_value = mock_merged_df
        
        with patch('time.time', return_value=1640995200.0):
            save_result = manager.save_watermark("source_entity", "reader_1", 10, "exec_001")
        
        # Verify contract: save operation should succeed
        assert save_result == mock_merged_df, \
            f"Initial save contract failed: Expected={mock_merged_df}, Got={save_result}"
        
        # Verify the save operation used correct data
        entity_provider.merge_to_entity.assert_called()
        initial_df = entity_provider.merge_to_entity.call_args_list[0][0][0]
        initial_data = initial_df.collect()[0]
        assert initial_data.source_entity_id == "source_entity", \
            f"Initial save entity contract failed: Expected='source_entity', Got='{initial_data.source_entity_id}'"
        assert initial_data.last_version_processed == 10, \
            f"Initial save version contract failed: Expected=10, Got={initial_data.last_version_processed}"
        
        # Test 2: Get the saved watermark
        mock_watermark_row = MagicMock()
        mock_watermark_row.__getitem__ = lambda self, key: 10 if key == "last_version_processed" else None
        mock_watermark_row.last_version_processed = 10
        
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.limit.return_value = mock_df
        mock_df.isEmpty.return_value = False
        mock_df.first.return_value = mock_watermark_row
        
        entity_provider.read_entity.return_value = mock_df
        
        # Contract: should retrieve the same version that was saved
        watermark = manager.get_watermark("source_entity", "reader_1")
        assert watermark == 10, \
            f"get_watermark contract failed: Expected=10, Got={watermark}"
        
        # Test 3: Update watermark to newer version
        entity_provider.merge_to_entity.reset_mock()  # Reset to isolate this call
        
        with patch('time.time', return_value=1640995260.0):  # 1 minute later
            update_result = manager.save_watermark("source_entity", "reader_1", 15, "exec_002")
        
        # Verify contract: update operation should succeed
        assert update_result == mock_merged_df, \
            f"Update save contract failed: Expected={mock_merged_df}, Got={update_result}"
        
        # Verify the update operation used correct data
        entity_provider.merge_to_entity.assert_called_once()
        update_df = entity_provider.merge_to_entity.call_args[0][0]
        update_data = update_df.collect()[0]
        assert update_data.last_version_processed == 15, \
            f"Update version contract failed: Expected=15, Got={update_data.last_version_processed}"
        assert update_data.last_execution_id == "exec_002", \
            f"Update execution ID contract failed: Expected='exec_002', Got='{update_data.last_execution_id}'"
    
    def test_multiple_readers_same_entity_contract(self, integration_manager):
        """Test multiple readers tracking watermarks for the same entity - isolation contract"""
        setup = integration_manager
        manager = setup['manager']
        entity_provider = setup['entity_provider']
        
        # Save watermarks for different readers of same entity
        readers = ["reader_A", "reader_B", "reader_C"]
        versions = [5, 8, 12]
        
        mock_merged_df = MagicMock()
        entity_provider.merge_to_entity.return_value = mock_merged_df
        
        for reader, version in zip(readers, versions):
            with patch('time.time', return_value=1640995200.0):
                result = manager.save_watermark("shared_entity", reader, version, f"exec_{reader}")
            
            # Verify contract: each save operation should succeed
            assert result == mock_merged_df, \
                f"save_watermark contract failed for {reader}: Expected={mock_merged_df}, Got={result}"
        
        # Verify contract: each reader should get separate watermark records
        assert entity_provider.merge_to_entity.call_count == 3, \
            f"Merge call count contract failed: Expected=3, Got={entity_provider.merge_to_entity.call_count}"
        
        # Verify contract: each call should have correct reader-specific data
        merge_calls = entity_provider.merge_to_entity.call_args_list
        for i, (reader, version) in enumerate(zip(readers, versions)):
            df_passed = merge_calls[i][0][0]  # First argument is the DataFrame
            watermark_data = df_passed.collect()[0]
            
            # Verify reader isolation contract
            assert watermark_data.source_entity_id == "shared_entity", \
                f"Source entity contract failed for {reader}: Expected='shared_entity', Got='{watermark_data.source_entity_id}'"
            assert watermark_data.reader_id == reader, \
                f"Reader isolation contract failed: Expected='{reader}', Got='{watermark_data.reader_id}'"
            assert watermark_data.last_version_processed == version, \
                f"Version contract failed for {reader}: Expected={version}, Got={watermark_data.last_version_processed}"
            assert watermark_data.last_execution_id == f"exec_{reader}", \
                f"Execution ID contract failed for {reader}: Expected='exec_{reader}', Got='{watermark_data.last_execution_id}'"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(
    TestWatermarkManager,
    TestWatermarkManagerIntegration,
    test_config={
        'use_real_spark': True,
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
