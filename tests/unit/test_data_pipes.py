"""Unit tests for kindling.data_pipes module.

Tests the pipe registration system, metadata handling, and the DataPipesManager.
"""

from dataclasses import fields
from typing import Callable, Dict, List
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from kindling.data_pipes import (
    DataPipes,
    DataPipesExecuter,
    DataPipesExecution,
    DataPipesManager,
    DataPipesRegistry,
    EntityReadPersistStrategy,
    PipeMetadata,
    StageProcessingService,
)
from kindling.injection import GlobalInjector


class TestPipeMetadata:
    """Tests for PipeMetadata dataclass"""

    def test_pipe_metadata_has_required_fields(self):
        """Test that PipeMetadata has all required fields"""
        metadata_fields = {field.name for field in fields(PipeMetadata)}
        expected_fields = {
            "pipeid",
            "name",
            "execute",
            "tags",
            "input_entity_ids",
            "output_entity_id",
            "output_type",
        }

        assert (
            metadata_fields == expected_fields
        ), f"PipeMetadata fields mismatch. Expected: {expected_fields}, Got: {metadata_fields}"

    def test_pipe_metadata_field_types(self):
        """Test that PipeMetadata fields have correct type annotations"""
        field_annotations = PipeMetadata.__annotations__

        assert "pipeid" in field_annotations
        assert "name" in field_annotations
        assert "execute" in field_annotations
        assert "tags" in field_annotations
        assert "input_entity_ids" in field_annotations
        assert "output_entity_id" in field_annotations
        assert "output_type" in field_annotations

        # Check collection types
        assert field_annotations["execute"] == Callable
        assert field_annotations["tags"] == Dict[str, str]
        assert field_annotations["input_entity_ids"] == List[str]

    def test_pipe_metadata_creation(self):
        """Test creating a PipeMetadata instance with all fields"""

        def sample_func(df1, df2):
            return df1

        metadata = PipeMetadata(
            pipeid="test_pipe",
            name="Test Pipe",
            execute=sample_func,
            tags={"env": "test", "version": "1.0"},
            input_entity_ids=["entity1", "entity2"],
            output_entity_id="output_entity",
            output_type="delta",
        )

        assert metadata.pipeid == "test_pipe"
        assert metadata.name == "Test Pipe"
        assert metadata.execute == sample_func
        assert metadata.tags == {"env": "test", "version": "1.0"}
        assert metadata.input_entity_ids == ["entity1", "entity2"]
        assert metadata.output_entity_id == "output_entity"
        assert metadata.output_type == "delta"

    def test_pipe_metadata_with_lambda(self):
        """Test PipeMetadata with lambda function"""

        def lambda_func(x):
            return x

        metadata = PipeMetadata(
            pipeid="lambda_pipe",
            name="Lambda Pipe",
            execute=lambda_func,
            tags={},
            input_entity_ids=["input"],
            output_entity_id="output",
            output_type="parquet",
        )

        assert callable(metadata.execute)
        assert metadata.execute(5) == 5

    def test_pipe_metadata_empty_collections(self):
        """Test PipeMetadata with empty lists and dicts"""

        def empty_func():
            pass

        metadata = PipeMetadata(
            pipeid="minimal_pipe",
            name="Minimal Pipe",
            execute=empty_func,
            tags={},
            input_entity_ids=[],
            output_entity_id="",
            output_type="",
        )

        assert metadata.tags == {}
        assert metadata.input_entity_ids == []


class TestDataPipesManager:
    """Tests for DataPipesManager class"""

    def test_initialization(self):
        """Test DataPipesManager initialization"""
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)

        assert hasattr(manager, "registry")
        assert isinstance(manager.registry, dict)
        assert len(manager.registry) == 0
        assert hasattr(manager, "logger")
        mock_logger_provider.get_logger.assert_called_once_with("data_pipes_manager")
        mock_logger.debug.assert_called()

    def test_register_pipe(self):
        """Test registering a pipe"""
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)

        def test_func(df):
            return df

        manager.register_pipe(
            "test_pipe",
            name="Test Pipe",
            execute=test_func,
            tags={"env": "test"},
            input_entity_ids=["input1"],
            output_entity_id="output1",
            output_type="delta",
        )

        assert "test_pipe" in manager.registry
        pipe = manager.registry["test_pipe"]
        assert isinstance(pipe, PipeMetadata)
        assert pipe.pipeid == "test_pipe"
        assert pipe.name == "Test Pipe"
        assert pipe.execute == test_func
        mock_logger.debug.assert_any_call("Pipe registered: test_pipe")

    def test_register_multiple_pipes(self):
        """Test registering multiple pipes"""
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)

        for i in range(3):
            manager.register_pipe(
                f"pipe{i}",
                name=f"Pipe {i}",
                execute=lambda x: x,
                tags={},
                input_entity_ids=[],
                output_entity_id=f"output{i}",
                output_type="delta",
            )

        assert len(manager.registry) == 3
        assert "pipe0" in manager.registry
        assert "pipe1" in manager.registry
        assert "pipe2" in manager.registry

    def test_register_pipe_overwrites_existing(self):
        """Test that registering a pipe with same ID overwrites the previous one"""
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)

        # Register first version
        manager.register_pipe(
            "test_pipe",
            name="First Version",
            execute=lambda: 1,
            tags={"version": "1"},
            input_entity_ids=["input1"],
            output_entity_id="output1",
            output_type="delta",
        )

        # Register second version
        manager.register_pipe(
            "test_pipe",
            name="Second Version",
            execute=lambda: 2,
            tags={"version": "2"},
            input_entity_ids=["input2"],
            output_entity_id="output2",
            output_type="parquet",
        )

        assert len(manager.registry) == 1
        pipe = manager.registry["test_pipe"]
        assert pipe.name == "Second Version"
        assert pipe.tags == {"version": "2"}

    def test_get_pipe_ids_empty(self):
        """Test getting pipe IDs from empty registry"""
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)

        pipe_ids = manager.get_pipe_ids()
        assert len(list(pipe_ids)) == 0

    def test_get_pipe_ids(self):
        """Test getting pipe IDs"""
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)

        for i in range(3):
            manager.register_pipe(
                f"pipe{i}",
                name=f"Pipe {i}",
                execute=lambda: None,
                tags={},
                input_entity_ids=[],
                output_entity_id="",
                output_type="",
            )

        pipe_ids = list(manager.get_pipe_ids())
        assert len(pipe_ids) == 3
        assert "pipe0" in pipe_ids
        assert "pipe1" in pipe_ids
        assert "pipe2" in pipe_ids

    def test_get_pipe_definition_nonexistent(self):
        """Test getting definition for non-existent pipe"""
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)

        result = manager.get_pipe_definition("nonexistent")
        assert result is None

    def test_get_pipe_definition_existing(self):
        """Test getting definition for existing pipe"""
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)

        def test_func():
            return "test"

        manager.register_pipe(
            "test_pipe",
            name="Test Pipe",
            execute=test_func,
            tags={"env": "test"},
            input_entity_ids=["input"],
            output_entity_id="output",
            output_type="delta",
        )

        pipe = manager.get_pipe_definition("test_pipe")
        assert pipe is not None
        assert isinstance(pipe, PipeMetadata)
        assert pipe.pipeid == "test_pipe"
        assert pipe.name == "Test Pipe"
        assert pipe.execute == test_func

    def test_implements_data_pipes_registry_interface(self):
        """Test that DataPipesManager implements DataPipesRegistry interface"""
        assert issubclass(DataPipesManager, DataPipesRegistry)

        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)
        assert callable(manager.register_pipe)
        assert callable(manager.get_pipe_ids)
        assert callable(manager.get_pipe_definition)


class TestDataPipesDecorator:
    """Tests for DataPipes.pipe decorator"""

    def setup_method(self):
        """Reset DataPipes.dpregistry before each test"""
        DataPipes.dpregistry = None

    def test_decorator_validates_required_fields(self):
        """Test that decorator validates all required fields are present"""
        mock_registry = MagicMock()
        DataPipes.dpregistry = mock_registry

        # Missing all fields except pipeid
        with pytest.raises(ValueError) as exc_info:

            @DataPipes.pipe(pipeid="test_pipe")
            def test_func():
                pass

        error_message = str(exc_info.value)
        assert "Missing required fields" in error_message
        assert "name" in error_message
        assert "tags" in error_message
        assert "input_entity_ids" in error_message
        assert "output_entity_id" in error_message
        assert "output_type" in error_message

    def test_decorator_validates_partial_missing_fields(self):
        """Test that decorator validates when some fields are missing"""
        mock_registry = MagicMock()
        DataPipes.dpregistry = mock_registry

        with pytest.raises(ValueError) as exc_info:

            @DataPipes.pipe(pipeid="test_pipe", name="Test Pipe", tags={})
            def test_func():
                pass

        error_message = str(exc_info.value)
        assert "Missing required fields" in error_message
        assert "input_entity_ids" in error_message
        assert "output_entity_id" in error_message

    def test_decorator_successful_registration(self):
        """Test successful pipe registration with all required fields"""
        mock_registry = MagicMock()
        DataPipes.dpregistry = mock_registry

        @DataPipes.pipe(
            pipeid="test_pipe",
            name="Test Pipe",
            tags={"env": "test"},
            input_entity_ids=["entity1", "entity2"],
            output_entity_id="output_entity",
            output_type="delta",
        )
        def test_func(entity1, entity2):
            return entity1

        # Should call register_pipe on the registry
        mock_registry.register_pipe.assert_called_once()
        call_args = mock_registry.register_pipe.call_args
        assert call_args[0][0] == "test_pipe"
        assert call_args[1]["name"] == "Test Pipe"
        assert call_args[1]["execute"] == test_func

    def test_decorator_returns_original_function(self):
        """Test that decorator returns the original function"""
        mock_registry = MagicMock()
        DataPipes.dpregistry = mock_registry

        @DataPipes.pipe(
            pipeid="test_pipe",
            name="Test Pipe",
            tags={},
            input_entity_ids=[],
            output_entity_id="output",
            output_type="delta",
        )
        def test_func(x):
            return x * 2

        # Function should still be callable
        assert callable(test_func)
        assert test_func(5) == 10

    def test_decorator_adds_execute_parameter(self):
        """Test that decorator adds execute parameter with the decorated function"""
        mock_registry = MagicMock()
        DataPipes.dpregistry = mock_registry

        def my_function():
            return "result"

        decorated = DataPipes.pipe(
            pipeid="test_pipe",
            name="Test Pipe",
            tags={},
            input_entity_ids=[],
            output_entity_id="output",
            output_type="delta",
        )(my_function)

        call_kwargs = mock_registry.register_pipe.call_args[1]
        assert "execute" in call_kwargs
        assert call_kwargs["execute"] == my_function

    def test_decorator_removes_pipeid_from_params(self):
        """Test that decorator removes pipeid before passing to register_pipe"""
        mock_registry = MagicMock()
        DataPipes.dpregistry = mock_registry

        @DataPipes.pipe(
            pipeid="test_pipe",
            name="Test Pipe",
            tags={},
            input_entity_ids=[],
            output_entity_id="output",
            output_type="delta",
        )
        def test_func():
            pass

        call_args = mock_registry.register_pipe.call_args
        assert call_args[0][0] == "test_pipe"
        assert "pipeid" not in call_args[1]

    def test_decorator_lazy_initializes_registry(self):
        """Test that decorator lazily initializes registry from GlobalInjector"""
        DataPipes.dpregistry = None

        mock_registry = MagicMock()
        with patch.object(GlobalInjector, "get", return_value=mock_registry):

            @DataPipes.pipe(
                pipeid="test_pipe",
                name="Test Pipe",
                tags={},
                input_entity_ids=[],
                output_entity_id="output",
                output_type="delta",
            )
            def test_func():
                pass

            assert DataPipes.dpregistry == mock_registry

    def test_decorator_reuses_existing_registry(self):
        """Test that decorator reuses registry if already initialized"""
        mock_registry = MagicMock()
        DataPipes.dpregistry = mock_registry

        with patch.object(GlobalInjector, "get") as mock_get:

            @DataPipes.pipe(
                pipeid="test_pipe",
                name="Test Pipe",
                tags={},
                input_entity_ids=[],
                output_entity_id="output",
                output_type="delta",
            )
            def test_func():
                pass

            mock_get.assert_not_called()
            mock_registry.register_pipe.assert_called_once()


class TestDataPipesExecuter:
    """Tests for DataPipesExecuter class"""

    def setup_method(self):
        """Set up common mocks for each test"""
        self.mock_logger_provider = Mock()
        self.mock_logger = Mock()
        self.mock_logger_provider.get_logger.return_value = self.mock_logger

        self.mock_entity_registry = Mock()
        self.mock_pipes_registry = Mock()
        self.mock_erps = Mock()
        self.mock_trace_provider = Mock()
        self.mock_trace_provider.span.return_value.__enter__ = Mock()
        self.mock_trace_provider.span.return_value.__exit__ = Mock(return_value=False)

    def test_initialization(self):
        """Test DataPipesExecuter initialization"""
        executer = DataPipesExecuter(
            self.mock_logger_provider,
            self.mock_entity_registry,
            self.mock_pipes_registry,
            self.mock_erps,
            self.mock_trace_provider,
        )

        assert executer.erps == self.mock_erps
        assert executer.dpr == self.mock_pipes_registry
        assert executer.dpe == self.mock_entity_registry
        assert executer.tp == self.mock_trace_provider
        assert hasattr(executer, "logger")
        self.mock_logger_provider.get_logger.assert_called_once_with("data_pipes_executer")

    def test_run_datapipes_single_pipe(self):
        """Test running a single data pipe"""
        executer = DataPipesExecuter(
            self.mock_logger_provider,
            self.mock_entity_registry,
            self.mock_pipes_registry,
            self.mock_erps,
            self.mock_trace_provider,
        )

        # Mock pipe metadata
        mock_pipe = Mock(spec=PipeMetadata)
        mock_pipe.pipeid = "test_pipe"
        mock_pipe.name = "Test Pipe"
        mock_pipe.tags = {"env": "test"}
        mock_pipe.input_entity_ids = ["entity1"]
        mock_pipe.execute = Mock(return_value=Mock())

        self.mock_pipes_registry.get_pipe_definition.return_value = mock_pipe

        # Mock entity reader and activator
        mock_reader = Mock(return_value=Mock())
        mock_activator = Mock()
        self.mock_erps.create_pipe_entity_reader.return_value = mock_reader
        self.mock_erps.create_pipe_persist_activator.return_value = mock_activator

        # Mock entity definition
        mock_entity = Mock()
        self.mock_entity_registry.get_entity_definition.return_value = mock_entity

        # Run the pipe
        executer.run_datapipes(["test_pipe"])

        # Verify pipe was retrieved
        self.mock_pipes_registry.get_pipe_definition.assert_called_once_with("test_pipe")

        # Verify trace spans were created
        assert self.mock_trace_provider.span.call_count >= 2

    def test_run_datapipes_multiple_pipes(self):
        """Test running multiple data pipes"""
        executer = DataPipesExecuter(
            self.mock_logger_provider,
            self.mock_entity_registry,
            self.mock_pipes_registry,
            self.mock_erps,
            self.mock_trace_provider,
        )

        # Mock pipe metadata - create properly named pipes
        mock_pipe = Mock(spec=PipeMetadata)
        mock_pipe.pipeid = "pipe0"
        mock_pipe.name = "Test Pipe"
        mock_pipe.tags = {}
        mock_pipe.input_entity_ids = ["entity1"]
        mock_pipe.execute = Mock(return_value=Mock())

        self.mock_pipes_registry.get_pipe_definition.return_value = mock_pipe

        # Mock entity reader and activator
        mock_reader = Mock(return_value=Mock())
        mock_activator = Mock()
        self.mock_erps.create_pipe_entity_reader.return_value = mock_reader
        self.mock_erps.create_pipe_persist_activator.return_value = mock_activator

        # Mock entity definition
        self.mock_entity_registry.get_entity_definition.return_value = Mock()

        # Run the pipes
        executer.run_datapipes(["pipe0", "pipe1", "pipe2"])

        # Verify all pipes were retrieved
        assert self.mock_pipes_registry.get_pipe_definition.call_count == 3

    def test_execute_datapipe_with_single_input(self):
        """Test _execute_datapipe with single input entity"""
        executer = DataPipesExecuter(
            self.mock_logger_provider,
            self.mock_entity_registry,
            self.mock_pipes_registry,
            self.mock_erps,
            self.mock_trace_provider,
        )

        # Mock pipe
        mock_df = Mock()
        mock_pipe = Mock(spec=PipeMetadata)
        mock_pipe.pipeid = "test_pipe"
        mock_pipe.input_entity_ids = ["entity1"]
        mock_pipe.execute = Mock(return_value=mock_df)

        # Mock entity reader and activator
        mock_entity_reader = Mock(return_value=mock_df)
        mock_activator = Mock()

        # Mock entity definition
        mock_entity = Mock()
        self.mock_entity_registry.get_entity_definition.return_value = mock_entity

        # Execute
        result = executer._execute_datapipe(mock_entity_reader, mock_activator, mock_pipe)

        # Verify execute was called
        mock_pipe.execute.assert_called_once()
        mock_activator.assert_called_once_with(mock_df)

    def test_execute_datapipe_with_multiple_inputs(self):
        """Test _execute_datapipe with multiple input entities"""
        executer = DataPipesExecuter(
            self.mock_logger_provider,
            self.mock_entity_registry,
            self.mock_pipes_registry,
            self.mock_erps,
            self.mock_trace_provider,
        )

        # Mock pipe
        mock_df1 = Mock()
        mock_df2 = Mock()
        mock_result = Mock()
        mock_pipe = Mock(spec=PipeMetadata)
        mock_pipe.pipeid = "test_pipe"
        mock_pipe.input_entity_ids = ["entity1", "entity2"]
        mock_pipe.execute = Mock(return_value=mock_result)

        # Mock entity reader
        mock_entity_reader = Mock(side_effect=[mock_df1, mock_df2])
        mock_activator = Mock()

        # Mock entity definitions
        self.mock_entity_registry.get_entity_definition.side_effect = [Mock(), Mock()]

        # Execute
        result = executer._execute_datapipe(mock_entity_reader, mock_activator, mock_pipe)

        # Verify execute was called with both entities
        mock_pipe.execute.assert_called_once()
        call_kwargs = mock_pipe.execute.call_args[1]
        assert "entity1" in call_kwargs
        assert "entity2" in call_kwargs

    def test_execute_datapipe_skips_when_first_source_is_none(self):
        """Test _execute_datapipe skips execution when first source is None"""
        executer = DataPipesExecuter(
            self.mock_logger_provider,
            self.mock_entity_registry,
            self.mock_pipes_registry,
            self.mock_erps,
            self.mock_trace_provider,
        )

        # Mock pipe
        mock_pipe = Mock(spec=PipeMetadata)
        mock_pipe.pipeid = "test_pipe"
        mock_pipe.input_entity_ids = ["entity1"]
        mock_pipe.execute = Mock()

        # Mock entity reader returning None
        mock_entity_reader = Mock(return_value=None)
        mock_activator = Mock()

        # Mock entity definition
        self.mock_entity_registry.get_entity_definition.return_value = Mock()

        # Execute
        executer._execute_datapipe(mock_entity_reader, mock_activator, mock_pipe)

        # Verify execute was NOT called
        mock_pipe.execute.assert_not_called()
        mock_activator.assert_not_called()
        self.mock_logger.debug.assert_any_call("Skipping data pipe: test_pipe")

    def test_populate_source_dict(self):
        """Test _populate_source_dict creates correct dictionary"""
        executer = DataPipesExecuter(
            self.mock_logger_provider,
            self.mock_entity_registry,
            self.mock_pipes_registry,
            self.mock_erps,
            self.mock_trace_provider,
        )

        # Mock pipe
        mock_pipe = Mock(spec=PipeMetadata)
        mock_pipe.input_entity_ids = ["entity.one", "entity.two", "entity.three"]

        # Mock DataFrames
        mock_df1 = Mock()
        mock_df2 = Mock()
        mock_df3 = Mock()

        # Mock entity reader
        mock_entity_reader = Mock(side_effect=[mock_df1, mock_df2, mock_df3])

        # Mock entity definitions
        mock_entities = [Mock(), Mock(), Mock()]
        self.mock_entity_registry.get_entity_definition.side_effect = mock_entities

        # Execute
        result = executer._populate_source_dict(mock_entity_reader, mock_pipe)

        # Verify result
        assert len(result) == 3
        assert "entity_one" in result
        assert "entity_two" in result
        assert "entity_three" in result
        assert result["entity_one"] == mock_df1
        assert result["entity_two"] == mock_df2
        assert result["entity_three"] == mock_df3

        # Verify entity_reader was called with correct parameters
        calls = mock_entity_reader.call_args_list
        assert len(calls) == 3
        assert calls[0][0][1] == True  # First entity, is_first=True
        assert calls[1][0][1] == False  # Second entity, is_first=False
        assert calls[2][0][1] == False  # Third entity, is_first=False

    def test_implements_data_pipes_execution_interface(self):
        """Test that DataPipesExecuter implements DataPipesExecution interface"""
        assert issubclass(DataPipesExecuter, DataPipesExecution)

        executer = DataPipesExecuter(
            self.mock_logger_provider,
            self.mock_entity_registry,
            self.mock_pipes_registry,
            self.mock_erps,
            self.mock_trace_provider,
        )
        assert callable(executer.run_datapipes)


class TestAbstractBaseClasses:
    """Tests for abstract base classes to ensure they define expected interfaces"""

    def test_entity_read_persist_strategy_interface(self):
        """Test EntityReadPersistStrategy has required abstract methods"""
        from abc import ABC

        assert issubclass(EntityReadPersistStrategy, ABC)

        with pytest.raises(TypeError, match=".*abstract.*"):
            EntityReadPersistStrategy()

        # Check required methods exist
        assert hasattr(EntityReadPersistStrategy, "create_pipe_entity_reader")
        assert hasattr(EntityReadPersistStrategy, "create_pipe_persist_activator")

    def test_entity_read_persist_strategy_complete_implementation(self):
        """Test that a complete EntityReadPersistStrategy implementation can be instantiated"""

        class CompleteStrategy(EntityReadPersistStrategy):
            def create_pipe_entity_reader(self, pipe: str):
                return lambda entity, is_first: None

            def create_pipe_persist_activator(self, pipe: PipeMetadata):
                return lambda df: None

        strategy = CompleteStrategy()
        assert strategy is not None
        assert callable(strategy.create_pipe_entity_reader)
        assert callable(strategy.create_pipe_persist_activator)

    def test_data_pipes_registry_interface(self):
        """Test DataPipesRegistry has required abstract methods"""
        from abc import ABC

        assert issubclass(DataPipesRegistry, ABC)

        with pytest.raises(TypeError, match=".*abstract.*"):
            DataPipesRegistry()

        assert hasattr(DataPipesRegistry, "register_pipe")
        assert hasattr(DataPipesRegistry, "get_pipe_ids")
        assert hasattr(DataPipesRegistry, "get_pipe_definition")

    def test_data_pipes_execution_interface(self):
        """Test DataPipesExecution has required abstract methods"""
        from abc import ABC

        assert issubclass(DataPipesExecution, ABC)

        with pytest.raises(TypeError, match=".*abstract.*"):
            DataPipesExecution()

        assert hasattr(DataPipesExecution, "run_datapipes")

    def test_stage_processing_service_interface(self):
        """Test StageProcessingService has required abstract methods"""
        from abc import ABC

        assert issubclass(StageProcessingService, ABC)

        with pytest.raises(TypeError, match=".*abstract.*"):
            StageProcessingService()

        assert hasattr(StageProcessingService, "execute")


class TestPipeMetadataEdgeCases:
    """Test edge cases for PipeMetadata"""

    def test_pipe_metadata_is_dataclass(self):
        """Test that PipeMetadata is properly defined as a dataclass"""
        from dataclasses import is_dataclass

        assert is_dataclass(PipeMetadata)

    def test_pipe_metadata_equality(self):
        """Test that PipeMetadata instances can be compared for equality"""

        def func1():
            pass

        def func2():
            pass

        metadata1 = PipeMetadata(
            pipeid="test",
            name="Test",
            execute=func1,
            tags={"key": "value"},
            input_entity_ids=["input1"],
            output_entity_id="output",
            output_type="delta",
        )

        metadata2 = PipeMetadata(
            pipeid="test",
            name="Test",
            execute=func1,
            tags={"key": "value"},
            input_entity_ids=["input1"],
            output_entity_id="output",
            output_type="delta",
        )

        metadata3 = PipeMetadata(
            pipeid="different",
            name="Different",
            execute=func2,
            tags={},
            input_entity_ids=[],
            output_entity_id="other",
            output_type="parquet",
        )

        assert metadata1 == metadata2
        assert metadata1 != metadata3

    def test_pipe_metadata_repr(self):
        """Test that PipeMetadata has a useful string representation"""
        metadata = PipeMetadata(
            pipeid="test_pipe",
            name="Test Pipe",
            execute=lambda: None,
            tags={"env": "test"},
            input_entity_ids=["input"],
            output_entity_id="output",
            output_type="delta",
        )

        repr_str = repr(metadata)
        assert "PipeMetadata" in repr_str
        assert "test_pipe" in repr_str


class TestDataPipesIntegration:
    """Integration tests for DataPipes"""

    def test_full_pipe_registration_workflow(self):
        """Test complete workflow of registering and retrieving pipes"""
        mock_logger_provider = Mock()
        mock_logger = Mock()
        mock_logger_provider.get_logger.return_value = mock_logger

        manager = DataPipesManager(mock_logger_provider)

        # Define multiple pipes with different configurations
        def transform1(input1):
            return input1

        def transform2(input1, input2):
            return input1

        def transform3(data):
            return data

        pipes_to_register = [
            {
                "pipeid": "bronze_to_silver",
                "name": "Bronze to Silver Transform",
                "execute": transform1,
                "tags": {"layer": "silver", "domain": "sales"},
                "input_entity_ids": ["bronze.sales"],
                "output_entity_id": "silver.sales",
                "output_type": "delta",
            },
            {
                "pipeid": "silver_to_gold",
                "name": "Silver to Gold Aggregation",
                "execute": transform2,
                "tags": {"layer": "gold", "domain": "sales"},
                "input_entity_ids": ["silver.sales", "silver.customers"],
                "output_entity_id": "gold.sales_summary",
                "output_type": "delta",
            },
            {
                "pipeid": "incremental_load",
                "name": "Incremental Data Load",
                "execute": transform3,
                "tags": {"layer": "bronze", "type": "incremental"},
                "input_entity_ids": ["raw.data"],
                "output_entity_id": "bronze.data",
                "output_type": "delta",
            },
        ]

        # Register all pipes
        for pipe_data in pipes_to_register:
            manager.register_pipe(**pipe_data)

        # Verify all were registered
        assert len(list(manager.get_pipe_ids())) == 3

        # Verify each pipe can be retrieved correctly
        for pipe_data in pipes_to_register:
            pipe = manager.get_pipe_definition(pipe_data["pipeid"])
            assert pipe is not None
            assert pipe.pipeid == pipe_data["pipeid"]
            assert pipe.name == pipe_data["name"]
            assert pipe.execute == pipe_data["execute"]
            assert pipe.tags == pipe_data["tags"]
            assert pipe.input_entity_ids == pipe_data["input_entity_ids"]
            assert pipe.output_entity_id == pipe_data["output_entity_id"]
            assert pipe.output_type == pipe_data["output_type"]

    def test_decorator_with_real_function(self):
        """Test decorator with a real function implementation"""
        DataPipes.dpregistry = None

        mock_registry = MagicMock()

        with patch.object(GlobalInjector, "get", return_value=mock_registry):

            @DataPipes.pipe(
                pipeid="real_pipe",
                name="Real Pipe",
                tags={"version": "1.0"},
                input_entity_ids=["input"],
                output_entity_id="output",
                output_type="delta",
            )
            def real_transform(input):
                """Actual transformation logic"""
                return input.select("*").filter("id > 0")

            # Function should still work
            mock_df = Mock()
            mock_df.select.return_value.filter.return_value = "filtered_df"

            result = real_transform(mock_df)
            assert result == "filtered_df"

            # Should be registered
            mock_registry.register_pipe.assert_called_once()
