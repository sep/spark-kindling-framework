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

%run data_pipes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pytest
from unittest.mock import patch, MagicMock, call
from dataclasses import dataclass, fields
from typing import List, Dict, Any, Callable


class TestDataPipes(SynapseNotebookTestCase):
    
    def test_pipe_metadata_dataclass_structure(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        PipeMetadata = globals().get('PipeMetadata')
        if not PipeMetadata:
            pytest.skip("PipeMetadata not available")
        
        # Test that PipeMetadata has all required fields
        metadata_fields = {field.name for field in fields(PipeMetadata)}
        expected_fields = {'pipeid', 'name', 'execute', 'tags', 'input_entity_ids', 'output_entity_id', 'output_type'}
        
        assert metadata_fields == expected_fields
        
        # Test that we can create a PipeMetadata instance
        def dummy_execute(**kwargs):
            return None
        
        metadata = PipeMetadata(
            pipeid="test_pipe",
            name="Test Pipe",
            execute=dummy_execute,
            tags={"env": "test", "owner": "data_team"},
            input_entity_ids=["input.entity1", "input.entity2"],
            output_entity_id="output.entity",
            output_type="delta"
        )
        
        assert metadata.pipeid == "test_pipe"
        assert metadata.name == "Test Pipe"
        assert metadata.execute == dummy_execute
        assert metadata.tags == {"env": "test", "owner": "data_team"}
        assert metadata.input_entity_ids == ["input.entity1", "input.entity2"]
        assert metadata.output_entity_id == "output.entity"
        assert metadata.output_type == "delta"
    
    def test_data_pipes_manager_initialization(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipesManager = globals().get('DataPipesManager')
        if not DataPipesManager:
            pytest.skip("DataPipesManager not available")
        
        # Mock the logger provider
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger
        
        # Test that DataPipesManager can be instantiated
        manager = DataPipesManager.__new__(DataPipesManager)
        manager.__init__(mock_logger_provider)
        
        assert hasattr(manager, 'registry')
        assert isinstance(manager.registry, dict)
        assert len(manager.registry) == 0
        assert hasattr(manager, 'data_pipes_logger')
        
        # Verify logger was requested
        mock_logger_provider.get_logger.assert_called_with("data_pipes_manager")
    
    def test_data_pipes_manager_register_pipe(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipesManager = globals().get('DataPipesManager')
        PipeMetadata = globals().get('PipeMetadata')
        if not DataPipesManager or not PipeMetadata:
            pytest.skip("Required classes not available")
        
        # Create manager instance
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger
        
        manager = DataPipesManager.__new__(DataPipesManager)
        manager.__init__(mock_logger_provider)
        
        def test_execute(**kwargs):
            return kwargs
        
        # Test registering a pipe
        manager.register_pipe(
            "test_pipe",
            name="Test Pipe",
            execute=test_execute,
            tags={"env": "test"},
            input_entity_ids=["entity1"],
            output_entity_id="output_entity",
            output_type="delta"
        )
        
        # Verify pipe was registered
        assert "test_pipe" in manager.registry
        
        pipe = manager.registry["test_pipe"]
        assert isinstance(pipe, PipeMetadata)
        assert pipe.pipeid == "test_pipe"
        assert pipe.name == "Test Pipe"
        assert pipe.execute == test_execute
        assert pipe.tags == {"env": "test"}
        assert pipe.input_entity_ids == ["entity1"]
        assert pipe.output_entity_id == "output_entity"
        assert pipe.output_type == "delta"
    
    def test_data_pipes_manager_get_methods(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipesManager = globals().get('DataPipesManager')
        if not DataPipesManager:
            pytest.skip("DataPipesManager not available")
        
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger
        
        manager = DataPipesManager.__new__(DataPipesManager)
        manager.__init__(mock_logger_provider)
        
        # Test empty registry
        pipe_ids = manager.get_pipe_ids()
        assert len(list(pipe_ids)) == 0
        
        # Test get non-existent pipe
        result = manager.get_pipe_definition("nonexistent")
        assert result is None
        
        # Add some pipes
        def dummy_func(**kwargs):
            return None
            
        manager.register_pipe("pipe1", name="Pipe 1", execute=dummy_func, tags={}, input_entity_ids=[], output_entity_id="out1", output_type="delta")
        manager.register_pipe("pipe2", name="Pipe 2", execute=dummy_func, tags={}, input_entity_ids=[], output_entity_id="out2", output_type="parquet")
        
        # Test getting pipe IDs
        pipe_ids = list(manager.get_pipe_ids())
        assert len(pipe_ids) == 2
        assert "pipe1" in pipe_ids
        assert "pipe2" in pipe_ids
        
        # Test getting pipe definition
        pipe1 = manager.get_pipe_definition("pipe1")
        assert pipe1 is not None
        assert pipe1.pipeid == "pipe1"
        assert pipe1.name == "Pipe 1"
    
    def test_data_pipes_decorator_validation(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipes = globals().get('DataPipes')
        PipeMetadata = globals().get('PipeMetadata')
        if not DataPipes or not PipeMetadata:
            pytest.skip("Required classes not available")
        
        # Mock the registry to avoid injection dependencies
        mock_registry = MagicMock()
        DataPipes.dpregistry = mock_registry
        
        # Test that missing required fields raises an error
        with pytest.raises(ValueError) as exc_info:
            @DataPipes.pipe(
                pipeid="test_pipe",
                name="Test Pipe"
                # Missing: execute, tags, input_entity_ids, output_entity_id, output_type
            )
            def test_function():
                pass
        
        error_message = str(exc_info.value)
        assert "Missing required fields" in error_message
        assert "tags" in error_message
        assert "input_entity_ids" in error_message
        assert "output_entity_id" in error_message
        assert "output_type" in error_message
    
    def test_data_pipes_decorator_successful_registration(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipes = globals().get('DataPipes')
        if not DataPipes:
            pytest.skip("DataPipes not available")
        
        # Mock the registry
        mock_registry = MagicMock()
        DataPipes.dpregistry = mock_registry
        
        # Test successful registration with all required fields
        @DataPipes.pipe(
            pipeid="test_pipe",
            name="Test Pipe",
            tags={"env": "test"},
            input_entity_ids=["input1", "input2"],
            output_entity_id="output",
            output_type="delta"
        )
        def test_pipe_function(**kwargs):
            return kwargs
        
        # Should call register_pipe on the registry
        mock_registry.register_pipe.assert_called_once_with(
            "test_pipe",
            name="Test Pipe",
            execute=test_pipe_function,
            tags={"env": "test"},
            input_entity_ids=["input1", "input2"],
            output_entity_id="output",
            output_type="delta"
        )
        
        # Function should still be callable
        result = test_pipe_function(a=1, b=2)
        assert result == {"a": 1, "b": 2}
    
    def test_data_pipes_executer_initialization(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipesExecuter = globals().get('DataPipesExecuter')
        if not DataPipesExecuter:
            pytest.skip("DataPipesExecuter not available")
        
        # Mock dependencies
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger
        
        mock_entity_registry = MagicMock()
        mock_pipes_registry = MagicMock()
        mock_erps = MagicMock()
        
        # Test initialization
        executer = DataPipesExecuter.__new__(DataPipesExecuter)
        executer.__init__(mock_logger_provider, mock_entity_registry, mock_pipes_registry, mock_erps)
        
        assert executer.erps == mock_erps
        assert executer.dpr == mock_pipes_registry
        assert executer.dpe == mock_entity_registry
        assert hasattr(executer, 'data_pipes_logger')
        
        # Verify logger was requested
        mock_logger_provider.get_logger.assert_called_with("data_pipes_executer")
    
    def test_data_pipes_executer_populate_source_dict(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipesExecuter = globals().get('DataPipesExecuter')
        PipeMetadata = globals().get('PipeMetadata')
        if not DataPipesExecuter or not PipeMetadata:
            pytest.skip("Required classes not available")
        
        # Create executer instance
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger
        
        mock_entity_registry = MagicMock()
        mock_pipes_registry = MagicMock()
        mock_erps = MagicMock()
        
        executer = DataPipesExecuter.__new__(DataPipesExecuter)
        executer.__init__(mock_logger_provider, mock_entity_registry, mock_pipes_registry, mock_erps)
        
        # Create test pipe
        def dummy_execute(**kwargs):
            return None
            
        pipe = PipeMetadata(
            pipeid="test_pipe",
            name="Test Pipe",
            execute=dummy_execute,
            tags={},
            input_entity_ids=["entity.one", "entity.two"],
            output_entity_id="output",
            output_type="delta"
        )
        
        # Mock entity definitions
        entity_def_1 = MagicMock()
        entity_def_2 = MagicMock()
        mock_entity_registry.get_entity_definition.side_effect = [entity_def_1, entity_def_2]
        
        # Mock entity reader
        mock_df_1 = MagicMock()
        mock_df_2 = MagicMock()
        
        def mock_entity_reader(entity_def, is_first):
            if entity_def == entity_def_1:
                return mock_df_1
            elif entity_def == entity_def_2:
                return mock_df_2
            return None
        
        # Test _populate_source_dict
        result = executer._populate_source_dict(mock_entity_reader, pipe)
        
        # Verify result structure
        assert "entity_one" in result
        assert "entity_two" in result
        assert result["entity_one"] == mock_df_1
        assert result["entity_two"] == mock_df_2
        
        # Verify entity registry calls
        assert mock_entity_registry.get_entity_definition.call_count == 2
        mock_entity_registry.get_entity_definition.assert_any_call("entity.one")
        mock_entity_registry.get_entity_definition.assert_any_call("entity.two")
    
    def test_data_pipes_executer_execute_datapipe(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipesExecuter = globals().get('DataPipesExecuter')
        PipeMetadata = globals().get('PipeMetadata')
        if not DataPipesExecuter or not PipeMetadata:
            pytest.skip("Required classes not available")
        
        # Create executer instance
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger
        
        mock_entity_registry = MagicMock()
        mock_pipes_registry = MagicMock()
        mock_erps = MagicMock()
        
        executer = DataPipesExecuter.__new__(DataPipesExecuter)
        executer.__init__(mock_logger_provider, mock_entity_registry, mock_pipes_registry, mock_erps)
        
        # Create test pipe
        mock_processed_df = MagicMock()
        
        def test_execute(**kwargs):
            return mock_processed_df
            
        pipe = PipeMetadata(
            pipeid="test_pipe",
            name="Test Pipe",
            execute=test_execute,
            tags={},
            input_entity_ids=["entity1"],
            output_entity_id="output",
            output_type="delta"
        )
        
        # Mock entity reader and activator
        mock_input_df = MagicMock()
        mock_entity_reader = MagicMock(return_value=mock_input_df)
        mock_activator = MagicMock()
        
        # Mock _populate_source_dict to return test data
        with patch.object(executer, '_populate_source_dict') as mock_populate:
            mock_populate.return_value = {"entity1": mock_input_df}
            
            # Test _execute_datapipe
            executer._execute_datapipe(mock_entity_reader, mock_activator, pipe)
            
            # Verify the pipe function was called
            mock_activator.assert_called_once_with(mock_processed_df)
            
            # Verify logging
            mock_logger.debug.assert_any_call("Prepping data pipe: test_pipe")
            mock_logger.debug.assert_any_call("Running data pipe: test_pipe")
    
    def test_data_pipes_executer_skip_when_no_data(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipesExecuter = globals().get('DataPipesExecuter')
        PipeMetadata = globals().get('PipeMetadata')
        if not DataPipesExecuter or not PipeMetadata:
            pytest.skip("Required classes not available")
        
        # Create executer instance
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger
        
        mock_entity_registry = MagicMock()
        mock_pipes_registry = MagicMock()
        mock_erps = MagicMock()
        
        executer = DataPipesExecuter.__new__(DataPipesExecuter)
        executer.__init__(mock_logger_provider, mock_entity_registry, mock_pipes_registry, mock_erps)
        
        # Create test pipe
        def test_execute(**kwargs):
            return MagicMock()
            
        pipe = PipeMetadata(
            pipeid="test_pipe",
            name="Test Pipe",
            execute=test_execute,
            tags={},
            input_entity_ids=["entity1"],
            output_entity_id="output",
            output_type="delta"
        )
        
        # Mock entity reader and activator
        mock_entity_reader = MagicMock()
        mock_activator = MagicMock()
        
        # Mock _populate_source_dict to return None for first source
        with patch.object(executer, '_populate_source_dict') as mock_populate:
            mock_populate.return_value = {"entity1": None}
            
            # Test _execute_datapipe with no data
            executer._execute_datapipe(mock_entity_reader, mock_activator, pipe)
            
            # Verify activator was NOT called
            mock_activator.assert_not_called()
            
            # Verify skip logging
            mock_logger.debug.assert_any_call("Prepping data pipe: test_pipe")
            mock_logger.debug.assert_any_call("Skipping data pipe: test_pipe")
    
    def test_interfaces_are_abstract(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test that our interfaces are properly abstract
        EntityReadPersistStrategy = globals().get('EntityReadPersistStrategy')
        DataPipesRegistry = globals().get('DataPipesRegistry')
        DataPipesExecution = globals().get('DataPipesExecution')
        
        interfaces_to_test = [
            (EntityReadPersistStrategy, 'create_pipe_entity_reader'),
            (DataPipesRegistry, 'register_pipe'),
            (DataPipesExecution, 'run_datapipes')
        ]
        
        for interface_class, method_name in interfaces_to_test:
            if interface_class:
                # Should not be able to instantiate abstract classes
                with pytest.raises(TypeError):
                    interface_class()
                
                # Should have the expected abstract method
                assert hasattr(interface_class, method_name)
    
    def test_data_pipes_manager_implements_registry_interface(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipesManager = globals().get('DataPipesManager')
        DataPipesRegistry = globals().get('DataPipesRegistry')
        
        if not DataPipesManager or not DataPipesRegistry:
            pytest.skip("Required classes not available")
        
        # Test that DataPipesManager implements DataPipesRegistry interface
        assert issubclass(DataPipesManager, DataPipesRegistry)
        
        # Test that DataPipesManager has all required methods
        required_methods = ['register_pipe', 'get_pipe_ids', 'get_pipe_definition']
        
        for method_name in required_methods:
            assert hasattr(DataPipesManager, method_name)
            assert callable(getattr(DataPipesManager, method_name))
    
    def test_data_pipes_executer_implements_execution_interface(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        DataPipesExecuter = globals().get('DataPipesExecuter')
        DataPipesExecution = globals().get('DataPipesExecution')
        
        if not DataPipesExecuter or not DataPipesExecution:
            pytest.skip("Required classes not available")
        
        # Test that DataPipesExecuter implements DataPipesExecution interface
        assert issubclass(DataPipesExecuter, DataPipesExecution)
        
        # Test that DataPipesExecuter has all required methods
        required_methods = ['run_datapipes']
        
        for method_name in required_methods:
            assert hasattr(DataPipesExecuter, method_name)
            assert callable(getattr(DataPipesExecuter, method_name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(TestDataPipes)
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
