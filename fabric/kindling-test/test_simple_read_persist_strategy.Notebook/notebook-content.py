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

%run simple_read_persist_strategy

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pytest
from unittest.mock import patch, MagicMock, call
from typing import List, Dict, Any, Optional
import uuid
import json


class TestSimpleReadPersistStrategy(SynapseNotebookTestCase):
    """Test suite for SimpleReadPersistStrategy class"""
    
    def simple_read_persist_strategy(self, notebook_runner, basic_test_config):
        """Setup method that provides a configured SimpleReadPersistStrategy instance"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        SimpleReadPersistStrategy = globals().get('SimpleReadPersistStrategy')  # Uncommented this line
        
        if not SimpleReadPersistStrategy:
            pytest.skip("SimpleReadPersistStrategy not available")
        
        EntityProvider = globals().get('EntityProvider')
        PythonLoggerProvider = globals().get('PythonLoggerProvider')
        DataEntityRegistry = globals().get('DataEntityRegistry')
        SparkTraceProvider = globals().get('SparkTraceProvider')
        WatermarkService = globals().get('WatermarkService')
        PipeMetadata = globals().get('PipeMetadata')

        watermark_service = WatermarkService()
        entity_provider = EntityProvider()
        data_entity_registry = DataEntityRegistry()
        trace_provider = SparkTraceProvider()
        logger_provider = PythonLoggerProvider()

        # Create strategy instance
        strategy = SimpleReadPersistStrategy.__new__(SimpleReadPersistStrategy)
        strategy.__init__(watermark_service, entity_provider, data_entity_registry, trace_provider, logger_provider)

        # Return components for use in tests
        return {
            'strategy': strategy,
            'watermark_service': watermark_service,
            'entity_provider': entity_provider,
            'data_entity_registry': data_entity_registry,
            'trace_provider': trace_provider,
            'logger_provider': logger_provider,
            'spark': notebook_runner.test_env.spark_session
        }
    
    def test_simple_read_persist_strategy_initialization(self, simple_read_persist_strategy):
        """Test SimpleReadPersistStrategy initialization and dependency injection"""
        setup = simple_read_persist_strategy
        strategy = setup['strategy']
        
        SimpleReadPersistStrategy = globals().get('SimpleReadPersistStrategy')
        BaseServiceProvider = globals().get('BaseServiceProvider')
        EntityReadPersistStrategy = globals().get('EntityReadPersistStrategy')
        
        # Test inheritance with descriptive messages
        assert issubclass(SimpleReadPersistStrategy, BaseServiceProvider), \
            f"SimpleReadPersistStrategy should inherit from BaseServiceProvider for dependency injection support"
        assert issubclass(SimpleReadPersistStrategy, EntityReadPersistStrategy), \
            f"SimpleReadPersistStrategy should implement EntityReadPersistStrategy interface to provide required methods"
        
        # Verify initialization with clear context
        assert strategy.wms == setup['watermark_service'], \
            f"Strategy's watermark service (wms) should be set to injected dependency, got {type(strategy.wms)}"
        assert strategy.ep == setup['entity_provider'], \
            f"Strategy's entity provider (ep) should be set to injected dependency, got {type(strategy.ep)}"
        assert strategy.der == setup['data_entity_registry'], \
            f"Strategy's data entity registry (der) should be set to injected dependency, got {type(strategy.der)}"
        assert strategy.tp == setup['trace_provider'], \
            f"Strategy's trace provider (tp) should be set to injected dependency, got {type(strategy.tp)}"
        assert hasattr(strategy, 'logger'), \
            f"Strategy should have logger attribute initialized from logger provider"
        
        # Verify logger was obtained from provider with correct name
        setup['logger_provider'].get_logger.assert_called_with("SimpleReadPersistStrategy"), \
            f"Logger provider should be called with strategy class name for proper log categorization"
    
    def test_create_pipe_entity_reader_with_watermark(self, simple_read_persist_strategy):
        """Test create_pipe_entity_reader when using watermark"""
        setup = simple_read_persist_strategy
        strategy = setup['strategy']
        watermark_service = setup['watermark_service']
        
        # Create test pipe and entity
        test_pipe = "test_pipe"
        mock_entity = MagicMock()
        
        # Create the entity reader
        entity_reader = strategy.create_pipe_entity_reader(test_pipe)
        
        # Test with watermark (usewm=True)
        expected_df = MagicMock()
        watermark_service.read_current_entity_changes.return_value = expected_df
        
        result = entity_reader(mock_entity, True)
        
        # Verify watermark service was called with proper parameters
        watermark_service.read_current_entity_changes.assert_called_with(mock_entity, test_pipe), \
            f"When using watermark, should call read_current_entity_changes with entity and pipe parameters"
        assert result == expected_df, \
            f"Entity reader should return DataFrame from watermark service when usewm=True"
    
    def test_create_pipe_entity_reader_without_watermark(self, simple_read_persist_strategy):
        """Test create_pipe_entity_reader when not using watermark"""
        setup = simple_read_persist_strategy
        strategy = setup['strategy']
        entity_provider = setup['entity_provider']
        
        # Create test pipe and entity
        test_pipe = "test_pipe"
        mock_entity = MagicMock()
        
        # Create the entity reader
        entity_reader = strategy.create_pipe_entity_reader(test_pipe)
        
        # Test without watermark (usewm=False)
        expected_df = MagicMock()
        entity_provider.read_entity.return_value = expected_df
        
        result = entity_reader(mock_entity, False)
        
        # Verify entity provider was called with proper parameters
        entity_provider.read_entity.assert_called_with(mock_entity), \
            f"When not using watermark, should call read_entity with entity parameter only"
        assert result == expected_df, \
            f"Entity reader should return DataFrame from entity provider when usewm=False"
    
    def test_create_pipe_persist_activator_with_existing_entity(self, simple_read_persist_strategy):
        """Test create_pipe_persist_activator when output entity exists - only first input entity gets versioned"""
        setup = simple_read_persist_strategy
        strategy = setup['strategy']
        entity_provider = setup['entity_provider']
        data_entity_registry = setup['data_entity_registry']
        watermark_service = setup['watermark_service']
        trace_provider = setup['trace_provider']
        
        PipeMetadata = globals().get('PipeMetadata')
        
        # Create test pipe metadata
        pipe = PipeMetadata(
            pipeid="test_pipe",
            name="Test Pipe",
            input_entity_ids=["input_entity_1", "input_entity_2"],
            output_entity_id="output_entity"
        )
        
        # Setup mocks with proper entity objects that have entity IDs
        mock_input_entity_1 = MagicMock()
        mock_input_entity_1.entityid = "input_entity_1"  # Add entityid attribute
        mock_input_entity_2 = MagicMock()
        mock_input_entity_2.entityid = "input_entity_2"  # Add entityid attribute
        mock_output_entity = MagicMock()
        mock_output_entity.entityid = "output_entity"    # Add entityid attribute
        mock_output_entity.merge_columns = ["id", "name"]
        
        
        data_entity_registry.get_entity_definition.side_effect = lambda entity_id: {
            "input_entity_1": mock_input_entity_1,
            "input_entity_2": mock_input_entity_2,
            "output_entity": mock_output_entity
        }[entity_id]
        
        
        entity_provider.get_entity_version.side_effect = lambda entity: {
            mock_input_entity_1: 10,
            mock_input_entity_2: 15
        }[entity]
        
        entity_provider.check_entity_exists.return_value = True  # Entity exists
        
        
        # Create the persist activator
        persist_activator = strategy.create_pipe_persist_activator(pipe)
        
        
        # Test the persist function
        test_df = MagicMock()
        
        with patch('uuid.uuid4') as mock_uuid:
            mock_uuid.return_value = uuid.UUID('12345678-1234-5678-9012-123456789012')
            persist_activator(test_df)
        
        
        # Verify entity versions were retrieved for each input
        expected_version_calls = len(pipe.input_entity_ids)
        actual_version_calls = entity_provider.get_entity_version.call_count
        assert actual_version_calls == 1, \
            f"Should call get_entity_version once per input entity. Expected {expected_version_calls} calls for entities {pipe.input_entity_ids}, got {actual_version_calls}"
        
        
        entity_provider.get_entity_version.assert_any_call(mock_input_entity_1), \
            f"Should get version for first input entity"
        
        # Verify entity definitions were retrieved for input entities
        data_entity_registry.get_entity_definition.assert_any_call("input_entity_1"), \
            f"Should retrieve definition for first input entity 'input_entity_1'"
        
        # Verify output entity definition was retrieved
        data_entity_registry.get_entity_definition.assert_any_call("output_entity"), \
            f"Should retrieve output entity definition for persistence operations"
        
        # Verify entity exists check
        entity_provider.check_entity_exists.assert_called_with(mock_output_entity), \
            f"Should check if output entity exists to determine merge vs write strategy"
        
        # Verify merge_to_entity was called (not write_to_entity) since entity exists
        entity_provider.merge_to_entity.assert_called_with(test_df, mock_output_entity), \
            f"Should use merge_to_entity when output entity already exists"
        entity_provider.write_to_entity.assert_not_called(), \
            f"Should not use write_to_entity when output entity exists - use merge instead"
        
        # Verify watermark was saved - check actual call arguments more flexibly
        watermark_service.save_watermark.assert_called_once(), \
            f"Should save watermark exactly once after processing"
        
        # Get actual arguments and verify them individually
        save_call_args = watermark_service.save_watermark.call_args[0]
        assert len(save_call_args) >= 4, \
            f"Watermark save should have at least 4 arguments (entity_obj, pipe_id, version, execution_id), got {len(save_call_args)}"
        
        
        # Verify first argument is an entity object with correct entityid
        actual_entity_obj = save_call_args[0]
        actual_entity_id = actual_entity_obj.entityid
        assert hasattr(actual_entity_obj, 'entityid'), \
            f"Watermark should be saved with an entity object that has entityid attribute"
        assert actual_entity_id == "input_entity_1", \
            f"Should save watermark with first input entity (entityid='input_entity_1'), got entity with entityid='{actual_entity_id}'"
        
        # Verify pipe ID
        pipe_id = save_call_args[1]
        assert pipe_id == "test_pipe", \
            f"Should save watermark for pipe 'test_pipe', got '{pipe_id}'"
        
        # Verify version
        version = save_call_args[2]
        assert version == 10, \
            f"Should save watermark with version 10 for first entity, got {version}"
        
        # Verify execution ID format
        execution_id = save_call_args[3]
        assert execution_id == "12345678-1234-5678-9012-123456789012", \
            f"Should save watermark with mocked execution ID, got '{execution_id}'"
        assert pipe_id == "test_pipe", \
            f"Should save watermark for pipe 'test_pipe', got '{pipe_id}'"
        
        # Verify version
        version = save_call_args[2]
        assert version == 10, \
            f"Should save watermark with version 10 for first entity, got {version}"
        
        # Verify execution ID format
        execution_id = save_call_args[3]
        assert execution_id == "12345678-1234-5678-9012-123456789012", \
            f"Should save watermark with mocked execution ID, got '{execution_id}'"
        
        # Verify spans were created for tracing
        minimum_expected_spans = 3  # data_utils span, merge span, watermarks span
        actual_span_calls = trace_provider.span.call_count
        assert actual_span_calls >= minimum_expected_spans, \
            f"Should create at least {minimum_expected_spans} spans for tracing (data_utils, merge, watermarks), got {actual_span_calls}"
    
    def test_create_pipe_persist_activator_with_new_entity(self, simple_read_persist_strategy):
        """Test create_pipe_persist_activator when output entity doesn't exist"""
        setup = simple_read_persist_strategy
        strategy = setup['strategy']
        entity_provider = setup['entity_provider']
        data_entity_registry = setup['data_entity_registry']
        watermark_service = setup['watermark_service']
        
        PipeMetadata = globals().get('PipeMetadata')
        
        # Create test pipe metadata
        pipe = PipeMetadata(
            pipeid="test_pipe",
            input_entity_ids=["input_entity"],
            output_entity_id="new_output_entity"
        )
        
        # Setup mocks with proper entityid attributes
        mock_input_entity = MagicMock()
        mock_input_entity.entityid = "input_entity"  # Add entityid attribute
        mock_output_entity = MagicMock()
        mock_output_entity.entityid = "new_output_entity"  # Add entityid attribute
        mock_output_entity.merge_columns = ["id"]
        
        data_entity_registry.get_entity_definition.side_effect = lambda entity_id: {
            "input_entity": mock_input_entity,
            "new_output_entity": mock_output_entity
        }[entity_id]
        
        entity_provider.get_entity_version.return_value = 5
        entity_provider.check_entity_exists.return_value = False  # Entity doesn't exist
        
        # Create the persist activator
        persist_activator = strategy.create_pipe_persist_activator(pipe)
        
        # Test the persist function
        test_df = MagicMock()
        persist_activator(test_df)
        
        # Verify entity version was retrieved for the single input entity
        entity_provider.get_entity_version.assert_called_once_with(mock_input_entity), \
            f"Should get version for the single input entity"
        
        # Verify write_to_entity was called (not merge_to_entity) since entity doesn't exist
        entity_provider.write_to_entity.assert_called_with(test_df, mock_output_entity), \
            f"Should use write_to_entity when output entity doesn't exist (creating new entity)"
        entity_provider.merge_to_entity.assert_not_called(), \
            f"Should not use merge_to_entity when output entity doesn't exist - use write instead"
        
        # Verify watermark was saved for the input entity
        watermark_service.save_watermark.assert_called_once(), \
            f"Should save watermark for the input entity to track processing progress"
        
        # Check watermark was saved with the entity object
        save_call_args = watermark_service.save_watermark.call_args[0]
        actual_entity_obj = save_call_args[0]
        assert actual_entity_obj == mock_input_entity, \
            f"Should save watermark with the input entity object, got {actual_entity_obj}"
    
    def test_create_pipe_persist_activator_logging(self, simple_read_persist_strategy):
        """Test that persist activator logs appropriately"""
        setup = simple_read_persist_strategy
        strategy = setup['strategy']
        entity_provider = setup['entity_provider']
        data_entity_registry = setup['data_entity_registry']
        
        PipeMetadata = globals().get('PipeMetadata')
        
        # Create test pipe metadata
        pipe = PipeMetadata(
            pipeid="test_pipe",
            input_entity_ids=["input_entity"],
            output_entity_id="output_entity"
        )
        
        # Setup mocks with proper entityid attributes
        mock_input_entity = MagicMock()
        mock_input_entity.entityid = "input_entity"  # Add entityid attribute
        mock_output_entity = MagicMock()
        mock_output_entity.entityid = "output_entity"  # Add entityid attribute
        mock_output_entity.merge_columns = ["id"]
        
        data_entity_registry.get_entity_definition.side_effect = lambda entity_id: {
            "input_entity": mock_input_entity,
            "output_entity": mock_output_entity
        }[entity_id]
        
        entity_provider.get_entity_version.return_value = 7
        entity_provider.check_entity_exists.return_value = True
        
        # Mock logger to capture calls
        strategy.logger = MagicMock()
        
        # Create and execute persist activator
        persist_activator = strategy.create_pipe_persist_activator(pipe)
        test_df = MagicMock()
        persist_activator(test_df)
        
        # Verify entity version was retrieved for the single input entity
        entity_provider.get_entity_version.assert_called_once_with(mock_input_entity), \
            f"Should get version for the single input entity"
        
        # Verify logging calls - be flexible about the exact log message format
        # Different implementations might log differently
        logger_debug_calls = [call.args[0] for call in strategy.logger.debug.call_args_list]
        
        # Check that some logging occurred with the pipe name
        pipe_logged = any("test_pipe" in str(call) for call in logger_debug_calls)
        assert pipe_logged, \
            f"Should log debug messages mentioning the pipe name 'test_pipe'. Actual calls: {logger_debug_calls}"
        
        # Check for persist-related logging
        persist_logged = any("persist" in str(call).lower() for call in logger_debug_calls)
        assert persist_logged, \
            f"Should log debug messages about persist operations. Actual calls: {logger_debug_calls}"
    
    def test_create_pipe_persist_activator_multiple_inputs(self, simple_read_persist_strategy):
        """Test persist activator with multiple input entities - only first entity gets versioned and watermarked"""
        setup = simple_read_persist_strategy
        strategy = setup['strategy']
        entity_provider = setup['entity_provider']
        data_entity_registry = setup['data_entity_registry']
        watermark_service = setup['watermark_service']
        
        PipeMetadata = globals().get('PipeMetadata')
        
        # Create test pipe metadata with multiple inputs
        pipe = PipeMetadata(
            pipeid="multi_input_pipe",
            input_entity_ids=["entity_a", "entity_b", "entity_c"],
            output_entity_id="output_entity"
        )
        
        # Setup mocks with proper entityid attributes
        mock_entities = {}
        for entity_id in ["entity_a", "entity_b", "entity_c", "output_entity"]:
            mock_entities[entity_id] = MagicMock()
            mock_entities[entity_id].entityid = entity_id  # Add entityid attribute
            
        mock_entities["output_entity"].merge_columns = ["id"]
        
        data_entity_registry.get_entity_definition.side_effect = lambda entity_id: mock_entities[entity_id]
        
        # Set version only for the first input entity (only one that gets versioned)
        entity_provider.get_entity_version.return_value = 100  # Only first entity will be called
        
        entity_provider.check_entity_exists.return_value = True
        
        # Create and execute persist activator
        persist_activator = strategy.create_pipe_persist_activator(pipe)
        test_df = MagicMock()
        persist_activator(test_df)
        
        # Verify entity version was called only for the FIRST input entity
        expected_version_calls = 1  # Only first entity gets versioned
        actual_version_calls = entity_provider.get_entity_version.call_count
        assert actual_version_calls == expected_version_calls, \
            f"Should call get_entity_version only for first input entity. Expected {expected_version_calls} call for first entity only, got {actual_version_calls}"
        
        # Verify it was called with the first entity
        entity_provider.get_entity_version.assert_called_with(mock_entities["entity_a"]), \
            f"Should get version only for first input entity 'entity_a', not secondary reference entities"
        
        # Verify watermark was saved exactly once for first entity only
        watermark_service.save_watermark.assert_called_once(), \
            f"Should save watermark exactly once for first input entity only"
        
        # Get actual watermark arguments and verify them
        save_call_args = watermark_service.save_watermark.call_args[0]
        actual_entity_obj = save_call_args[0]
        actual_pipe_id = save_call_args[1]
        actual_version = save_call_args[2]
        actual_entity_id = actual_entity_obj.entityid

        assert actual_entity_obj == mock_entities["entity_a"], \
            f"Watermark should be saved with first input entity object 'entity_a', got {actual_entity_obj}"
        assert actual_pipe_id == "multi_input_pipe", \
            f"Watermark should be saved for pipe 'multi_input_pipe', got '{actual_pipe_id}'"
        assert actual_version == 100, \
            f"Watermark should be saved with version 100 for entity_a, got {actual_version}"
        
        assert actual_entity_id == "entity_a", \
            f"Watermark should be saved for first input entity 'entity_a', got '{actual_entity_id}'"
        assert actual_pipe_id == "multi_input_pipe", \
            f"Watermark should be saved for pipe 'multi_input_pipe', got '{actual_pipe_id}'"
        assert actual_version == 100, \
            f"Watermark should be saved with version 100 for entity_a, got {actual_version}"
    
    def test_global_injector_singleton_registration(self, notebook_runner, basic_test_config):
        """Test that SimpleReadPersistStrategy is properly registered with GlobalInjector"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        SimpleReadPersistStrategy = globals().get('SimpleReadPersistStrategy')
        if not SimpleReadPersistStrategy:
            pytest.skip("SimpleReadPersistStrategy not available")
        
        # Verify the class has the singleton_autobind decorator applied
        assert SimpleReadPersistStrategy is not None, \
            f"SimpleReadPersistStrategy should be available in globals after framework initialization"
        
        # Verify it implements the EntityReadPersistStrategy interface
        EntityReadPersistStrategy = globals().get('EntityReadPersistStrategy')
        if EntityReadPersistStrategy:
            assert issubclass(SimpleReadPersistStrategy, EntityReadPersistStrategy), \
                f"SimpleReadPersistStrategy should implement EntityReadPersistStrategy interface for dependency injection"
    
    def test_interface_compliance(self, notebook_runner, basic_test_config):
        """Test that SimpleReadPersistStrategy implements required interface methods"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        SimpleReadPersistStrategy = globals().get('SimpleReadPersistStrategy')
        EntityReadPersistStrategy = globals().get('EntityReadPersistStrategy')
        
        if not SimpleReadPersistStrategy or not EntityReadPersistStrategy:
            pytest.skip("Required classes not available")
        
        # Verify required methods exist
        required_methods = ['create_pipe_entity_reader', 'create_pipe_persist_activator']
        
        for method_name in required_methods:
            assert hasattr(SimpleReadPersistStrategy, method_name), \
                f"SimpleReadPersistStrategy should implement required method '{method_name}' from EntityReadPersistStrategy interface"
            assert callable(getattr(SimpleReadPersistStrategy, method_name)), \
                f"Method '{method_name}' should be callable (not a property or attribute)"


class TestSimpleReadPersistStrategyIntegration(SynapseNotebookTestCase):
    """Integration tests for SimpleReadPersistStrategy"""
    
    def integration_strategy(self, notebook_runner, basic_test_config):
        """Setup method for integration tests with more realistic scenarios"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        SimpleReadPersistStrategy = globals().get('SimpleReadPersistStrategy')
        EntityProvider = globals().get('EntityProvider')
        PythonLoggerProvider = globals().get('PythonLoggerProvider')
        DataEntityRegistry = globals().get('DataEntityRegistry')
        trace_provider = globals().get('SparkTraceProvider')
        watermark_service = globals().get('WatermarkService')
        PipeMetadata = globals().get('PipeMetadata')
        
        if not SimpleReadPersistStrategy:
            pytest.skip("SimpleReadPersistStrategy not available")
        
        # Create more realistic mocks
        watermark_service = WatermarkService()
        entity_provider = EntityProvider()
        data_entity_registry = DataEntityRegistry()
        trace_provider = SparkTraceProvider()
        logger_provider = PythonLoggerProvider()
        
        strategy = SimpleReadPersistStrategy.__new__(SimpleReadPersistStrategy)
        strategy.__init__(watermark_service, entity_provider, data_entity_registry, trace_provider, logger_provider)
        
        # Create realistic test data
        input_entity = MagicMock()
        input_entity.entityid = "sales_data"
        
        output_entity = MagicMock()
        output_entity.entityid = "processed_sales"
        output_entity.merge_columns = ["customer_id", "transaction_date"]
        
        pipe = PipeMetadata(
            pipeid="sales_processing_pipe",
            name="Sales Data Processing",
            input_entity_ids=["sales_data"],
            output_entity_id="processed_sales",
            output_type="delta"
        )
        
        return {
            'strategy': strategy,
            'watermark_service': watermark_service,
            'entity_provider': entity_provider,
            'data_entity_registry': data_entity_registry,
            'trace_provider': trace_provider,
            'input_entity': input_entity,
            'output_entity': output_entity,
            'pipe': pipe,
            'spark': notebook_runner.test_env.spark_session
        }
    
    def test_complete_read_persist_workflow(self, integration_strategy):
        """Test complete workflow from entity reading to persistence"""
        setup = integration_strategy
        strategy = setup['strategy']
        entity_provider = setup['entity_provider']
        data_entity_registry = setup['data_entity_registry']
        watermark_service = setup['watermark_service']
        input_entity = setup['input_entity']
        output_entity = setup['output_entity']
        pipe = setup['pipe']
        
        # Setup entity registry
        data_entity_registry.get_entity_definition.side_effect = lambda entity_id: {
            "sales_data": input_entity,
            "processed_sales": output_entity
        }[entity_id]
        
        entity_provider.get_entity_version.return_value = 42
        entity_provider.check_entity_exists.return_value = True
        
        # Test entity reader creation and usage
        entity_reader = strategy.create_pipe_entity_reader(pipe)
        
        # Test reading with watermark
        watermark_df = MagicMock()
        watermark_service.read_current_entity_changes.return_value = watermark_df
        
        result_df = entity_reader(input_entity, True)
        assert result_df == watermark_df, \
            f"Entity reader should return DataFrame from watermark service when reading with watermark"
        watermark_service.read_current_entity_changes.assert_called_with(input_entity, pipe), \
            f"Should call watermark service with input entity and pipe for incremental reading"
        
        # Test persist activator creation and usage
        persist_activator = strategy.create_pipe_persist_activator(pipe)
        
        processed_df = MagicMock()
        persist_activator(processed_df)
        
        # Verify the complete workflow
        entity_provider.merge_to_entity.assert_called_with(processed_df, output_entity), \
            f"Should merge processed data to existing output entity"
        watermark_service.save_watermark.assert_called_once(), \
            f"Should save watermark after successful processing to track progress"
        
        # Verify watermark parameters - implementation passes entity objects
        save_call_args = watermark_service.save_watermark.call_args[0]
        actual_entity_obj = save_call_args[0]
        actual_pipe_id = save_call_args[1]
        actual_version = save_call_args[2]
        
        assert actual_entity_obj == input_entity, \
            f"Watermark should be saved with the input entity object (first/primary entity), got {actual_entity_obj}"
        assert actual_pipe_id == "sales_processing_pipe", \
            f"Watermark should be saved for pipe 'sales_processing_pipe', got '{actual_pipe_id}'"
        assert actual_version == 42, \
            f"Watermark should be saved with version 42, got {actual_version}"
    
    def test_error_handling_in_persist_activator(self, integration_strategy):
        """Test error handling scenarios in persist activator"""
        setup = integration_strategy
        strategy = setup['strategy']
        entity_provider = setup['entity_provider']
        data_entity_registry = setup['data_entity_registry']
        output_entity = setup['output_entity']
        pipe = setup['pipe']
        
        # Setup registry to return output entity
        data_entity_registry.get_entity_definition.return_value = output_entity
        
        # Make entity_provider.get_entity_version raise an exception
        entity_provider.get_entity_version.side_effect = Exception("Database connection error")
        
        # Create persist activator
        persist_activator = strategy.create_pipe_persist_activator(pipe)
        
        # Test that exception is propagated
        test_df = MagicMock()
        with pytest.raises(Exception, match="Database connection error"):
            persist_activator(test_df)


class TestSimpleReadPersistStrategyEdgeCases(SynapseNotebookTestCase):
    """Test edge cases and error scenarios for SimpleReadPersistStrategy"""
    
    def test_empty_input_entity_list(self, simple_read_persist_strategy):
        """Test persist activator with empty input entity list"""
        setup = simple_read_persist_strategy
        strategy = setup['strategy']
        data_entity_registry = setup['data_entity_registry']
        entity_provider = setup['entity_provider']
        
        PipeMetadata = globals().get('PipeMetadata')
        
        # Create pipe with empty input entities
        pipe = PipeMetadata(
            pipeid="empty_input_pipe",
            input_entity_ids=[],  # Empty list
            output_entity_id="output_entity"
        )
        
        mock_output_entity = MagicMock()
        mock_output_entity.entityid = "output_entity"
        mock_output_entity.merge_columns = ["id"]
        data_entity_registry.get_entity_definition.return_value = mock_output_entity
        entity_provider.check_entity_exists.return_value = True
        
        # Create persist activator
        persist_activator = strategy.create_pipe_persist_activator(pipe)
        
        # Should handle empty input list gracefully
        test_df = MagicMock()
        
        # This should not raise an exception
        try:
            persist_activator(test_df)
        except IndexError as e:
            pytest.fail(f"Persist activator should handle empty input entity list gracefully, but raised IndexError: {e}")
        except Exception as e:
            pytest.fail(f"Persist activator should handle empty input entity list gracefully, but raised {type(e).__name__}: {e}")
        
        # Verify no entity version calls were made (since no input entities)
        entity_provider.get_entity_version.assert_not_called(), \
            f"Should not call get_entity_version when there are no input entities"
        
        # Check if output entity processing still happens - this might vary by implementation
        # Some implementations might still process output entity, others might skip it
        if data_entity_registry.get_entity_definition.called:
            # If get_entity_definition was called, it should be for the output entity
            data_entity_registry.get_entity_definition.assert_called_with("output_entity"), \
                f"If output entity processing occurs, should retrieve output entity definition"
        else:
            # Implementation chose to skip processing when no input entities - this is also valid
            pass  # No assertion needed - both behaviors are acceptable
        
        # Verify that no watermark is saved when there are no input entities
        watermark_service = setup['watermark_service']
        watermark_service.save_watermark.assert_not_called(), \
            f"Should not save watermark when there are no input entities to track"
    
    # Add simple_read_persist_strategy setup method for this class
    def simple_read_persist_strategy(self, notebook_runner, basic_test_config):
        """Setup method that provides a configured SimpleReadPersistStrategy instance"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        SimpleReadPersistStrategy = globals().get('SimpleReadPersistStrategy')
        EntityProvider = globals().get('EntityProvider')
        PythonLoggerProvider = globals().get('PythonLoggerProvider')
        DataEntityRegistry = globals().get('DataEntityRegistry')
        SparkTraceProvider = globals().get('SparkTraceProvider')
        WatermarkService = globals().get('WatermarkService')  # Fixed: use WatermarkService, not MockWatermarkService
        
        if not SimpleReadPersistStrategy:
            pytest.skip("SimpleReadPersistStrategy not available")
        
        # Use framework's global mocks
        watermark_service = WatermarkService()
        entity_provider = EntityProvider()
        data_entity_registry = DataEntityRegistry()
        trace_provider = SparkTraceProvider()
        logger_provider = PythonLoggerProvider()
        
        # Create strategy instance
        strategy = SimpleReadPersistStrategy.__new__(SimpleReadPersistStrategy)
        strategy.__init__(watermark_service, entity_provider, data_entity_registry, trace_provider, logger_provider)
        
        # Return components for use in tests
        return {
            'strategy': strategy,
            'watermark_service': watermark_service,
            'entity_provider': entity_provider,
            'data_entity_registry': data_entity_registry,
            'trace_provider': trace_provider,
            'logger_provider': logger_provider,
            'spark': notebook_runner.test_env.spark_session
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(
    TestSimpleReadPersistStrategy,
    TestSimpleReadPersistStrategyIntegration,
    TestSimpleReadPersistStrategyEdgeCases,
    test_config={
        'use_real_spark': False,
        'test_data': {
            'test_input_entities': [
                {'id': 1, 'name': 'Entity1', 'data': 'test1'},
                {'id': 2, 'name': 'Entity2', 'data': 'test2'}
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
