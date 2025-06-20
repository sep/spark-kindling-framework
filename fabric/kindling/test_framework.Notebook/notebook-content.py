# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Synapse Notebook Testing Framework with pytest Integration
# ========================================================

import pytest
import uuid
import sys
import inspect
from typing import Dict, Any, Optional, List, Union
from unittest.mock import Mock, patch, MagicMock
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
import json
import traceback
from abc import ABC, abstractmethod
from io import StringIO

# Test infrastructure that works with pytest
class NotebookTestEnvironment:
    """
    Test environment that sets up mocks and test doubles for notebook execution.
    This runs BEFORE the notebook is executed via %run.
    """
    
    def __init__(self):
        self.original_globals = {}
        self.test_mocks = {}
        self.spark_session = None
        self.injector_mocks = {}
        
    def setup_test_environment(self, test_config: Optional[Dict[str, Any]] = None):
        """Setup the test environment before notebook execution"""
        config = test_config or {}
        
        # CRITICAL: Setup GlobalInjector FIRST before anything else
        self._setup_injection_mocks()
        
        # Setup base classes and dependencies
        self._setup_base_classes()
        
        # Setup Spark session mock/real instance
        self._setup_spark_session(config.get('use_real_spark', False))
        
        # Setup Synapse-specific mocks
        self._setup_synapse_mocks()
        
        # Setup test data
        self._setup_test_data(config.get('test_data', {}))
        
    def _setup_spark_session(self, use_real_spark: bool = False):
        """Setup Spark session for testing"""
        if use_real_spark:
            from pyspark.sql import SparkSession
            self.spark_session = SparkSession.builder \
                .appName("NotebookTesting") \
                .master("local[2]") \
                .config("spark.sql.shuffle.partitions", "2") \
                .getOrCreate()
        else:
            # Create comprehensive Spark mock
            self.spark_session = self._create_spark_mock()
            
        # Make it globally available
        globals()['spark'] = self.spark_session
        
    def _create_spark_mock(self):
        """Create a comprehensive Spark session mock"""
        spark_mock = MagicMock()
        
        # Mock SparkContext
        sc_mock = MagicMock()
        sc_mock.appName = "TestApp"
        sc_mock._jsc.sc().listenerBus().post = MagicMock()
        spark_mock.sparkContext = sc_mock
        
        # Mock JVM access for Synapse events
        jvm_mock = MagicMock()
        
        # Mock Synapse-specific Java classes
        jvm_mock.com.microsoft.spark.metricevents.ComponentSparkEvent = MagicMock()
        jvm_mock.scala.Option.apply = lambda x: x
        jvm_mock.scala.Option.empty = lambda: None
        jvm_mock.java.util.UUID.fromString = lambda x: x
        jvm_mock.org.slf4j.event.Level.INFO = "INFO"
        jvm_mock.org.apache.log4j.MDC.put = MagicMock()
        jvm_mock.org.apache.log4j.MDC.remove = MagicMock()
        
        spark_mock._jvm = jvm_mock
        
        # Mock DataFrame operations - use MagicMock so tests can set return_value
        spark_mock.createDataFrame = MagicMock()
        
        return spark_mock
        
    def _setup_injection_mocks(self):
        """Setup dependency injection mocks - MUST BE CALLED FIRST"""
        # Store original values if they existinjection
        if 'GlobalInjector' in globals():
            self.original_globals['GlobalInjector'] = globals()['GlobalInjector']
            
        # Simple mock for GlobalInjector - only handles decorators
        class MockGlobalInjector:
            @classmethod
            def singleton_autobind(cls):
                """Mock the decorator - just returns the class unchanged"""
                def decorator(target_class):
                    return target_class
                return decorator
            
            @classmethod
            def get(cls, iface):
                return globals().get( iface.__name__, None )
                
        # Make GlobalInjector available globally
        globals()['GlobalInjector'] = MockGlobalInjector
        
        # Create test mocks that will be injected via constructors
        logger_provider_mock = MagicMock()
        logger_mock = MagicMock()
        logger_provider_mock.get_logger.return_value = logger_mock
        
        event_emitter_mock = MagicMock()
        trace_provider_mock = MagicMock()
        
        # Store for test assertions and constructor injection
        self.injector_mocks = {
            'logger_provider': logger_provider_mock,
            'logger': logger_mock,
            'event_emitter': event_emitter_mock,
            'trace_provider': trace_provider_mock
        }
        
    def _setup_base_classes(self):
        """Setup base classes and dependencies"""
        # Mock BaseServiceProvider before any classes try to inherit from it
        if 'BaseServiceProvider' not in globals():
            class MockBaseServiceProvider:
                pass
            globals()['BaseServiceProvider'] = MockBaseServiceProvider
            
        # Mock PythonLoggerProvider
        if 'PythonLoggerProvider' not in globals():
            class MockPythonLoggerProvider:
                def __init__(self, logger_provider=None):
                    # Match constructor pattern from best mocks
                    if logger_provider:
                        self.logger = logger_provider.get_logger("PythonLoggerProvider")
                    else:
                        self.logger = MagicMock()
                    
                    # Ensure get_logger is a MagicMock that returns a MagicMock
                    self.get_logger = MagicMock(return_value=MagicMock())
                    
            globals()['PythonLoggerProvider'] = MockPythonLoggerProvider
            
        # Mock inject decorator
        if 'inject' not in globals():
            def mock_inject(func):
                return func
            globals()['inject'] = mock_inject

        # REVISED: DataEntityRegistry - simplified to pure MagicMock methods like MockEntityProvider
        if 'DataEntityRegistry' not in globals():
            class DataEntityRegistry:
                def __init__(self, logger_provider=None):
                    # Match constructor pattern from MockWatermarkManager/MockEntityProvider
                    if logger_provider:
                        self.logger = logger_provider.get_logger("DataEntityRegistry")
                    else:
                        self.logger = MagicMock()
                    
                    # Pure MagicMock methods like MockEntityProvider - tests can override these
                    self.register_entity = MagicMock()
                    self.get_entity_ids = MagicMock(return_value=[])
                    self.get_entity_definition = MagicMock(return_value=MagicMock())
                    
            globals()['DataEntityRegistry'] = DataEntityRegistry
        else:
            logger.debug('DataEntityRegistry already in globals')

        # REVISED: MockDataPipesRegistry - simplified to match DataEntityRegistry pattern
        if 'DataPipesRegistry' not in globals():
            class MockDataPipesRegistry:
                def __init__(self, logger_provider=None):
                    # Match the simplified DataEntityRegistry pattern
                    if logger_provider:
                        self.logger = logger_provider.get_logger("DataPipesRegistry")
                    else:
                        self.logger = MagicMock()
                    
                    # Pure MagicMock methods - tests can override these
                    self.register_pipe = MagicMock()
                    self.get_pipe_ids = MagicMock(return_value=[])
                    self.get_pipe_definition = MagicMock(return_value=MagicMock())
                    
            globals()['DataPipesRegistry'] = MockDataPipesRegistry
        else:
            logger.debug('DataPipesRegistry already in globals')        

        # REVISED: EntityReadPersistStrategy - ensure all methods are MagicMock objects
        if 'EntityReadPersistStrategy' not in globals():
            class MockEntityReadPersistStrategy:
                def __init__(self, logger_provider=None, entity_provider=None):
                    # Match constructor pattern from MockWatermarkManager
                    self.entity_provider = entity_provider or MagicMock()
                    
                    if logger_provider:
                        self.logger = logger_provider.get_logger("EntityReadPersistStrategy")
                    else:
                        self.logger = MagicMock()
                    
                    # Ensure methods are MagicMock objects
                    self.create_pipe_entity_reader = MagicMock(return_value=MagicMock())
                    self.create_pipe_persist_activator = MagicMock(return_value=MagicMock())
                    
            globals()['EntityReadPersistStrategy'] = MockEntityReadPersistStrategy
        else:
            logger.debug('EntityReadPersistStrategy already in globals')    

        if 'EntityProvider' not in globals():
            class MockEntityProvider:
                def __init__(self):
                    # Make all methods MagicMocks so tests can assert calls
                    self.read_entity = MagicMock()
                    self.merge_to_entity = MagicMock()
                    self.get_entity_version = MagicMock()
                    self.read_entity_since_version = MagicMock()
                    self.check_entity_exists = MagicMock()  # Add missing method
                    self.write_to_entity = MagicMock()      # Add missing method
                    self.ensure_entity_table = MagicMock()  # ← ADD THIS LINE
                    
                    # Set up default return values that tests can override
                    self.default_df = MagicMock()
                    self.read_entity.return_value = self.default_df
                    self.get_entity_version.return_value = 1
                    self.merge_to_entity.return_value = self.default_df
                    self.read_entity_since_version.return_value = self.default_df
                    self.check_entity_exists.return_value = True
                    self.write_to_entity.return_value = self.default_df

            globals()['EntityProvider'] = MockEntityProvider
        else:
            logger.debug('EntityProvider already in globals')  

        if 'FileIngestionRegistry' not in globals():
            class MockFileIngestionRegistry:
                def __init__(self):
                    self.register_entry = MagicMock()
                    self.get_entry_ids = MagicMock(return_value=[])
                    self.get_entry_definition = MagicMock(return_value=MagicMock())
            
            globals()['FileIngestionRegistry'] = MockFileIngestionRegistry
        else:
            logger.debug('FileIngestionRegistry already in globals')   

        # Mock FileIngestionProcessor interface  
        if 'FileIngestionProcessor' not in globals():
            class MockFileIngestionProcessor:
                def __init__(self):
                    self.process_path = MagicMock()
            
            globals()['FileIngestionProcessor'] = MockFileIngestionProcessor
        else:
            logger.debug('FileIngestionProcessor already in globals') 

        if 'WatermarkEntityFinder' not in globals():
            class MockWatermarkEntityFinder:
                def __init__(self, logger_provider=None):
                    # Match constructor pattern from MockWatermarkManager
                    if logger_provider:
                        self.logger = logger_provider.get_logger("WatermarkEntityFinder")
                    else:
                        self.logger = MagicMock()
                    
                    # REPLACE these lines:
                    # self.get_watermark_entity_for_entity = MagicMock(return_value="watermark_entity_for_entity")
                    # self.get_watermark_entity_for_layer = MagicMock(return_value="watermark_entity_for_layer")
                    
                    # WITH these lines:
                    mock_entity = MagicMock()
                    mock_entity.entityid = "default_watermark_entity"
                    self.get_watermark_entity_for_entity = MagicMock(return_value=mock_entity)
                    self.get_watermark_entity_for_layer = MagicMock(return_value=mock_entity)
            
            globals()['WatermarkEntityFinder'] = MockWatermarkEntityFinder
        else:
            logger.debug('WatermarkEntityFinder already in globals')   

        if 'WatermarkService' not in globals():
            class MockWatermarkManager():
                def __init__(self, entity_provider=None, watermark_entity_finder=None, logger_provider=None):
                    self.ep = entity_provider or MagicMock()
                    self.wef = watermark_entity_finder or MagicMock()
                    
                    if logger_provider:
                        self.logger = logger_provider.get_logger("watermark")
                    else:
                        self.logger = MagicMock()
                
                    # Make all methods MagicMock objects so tests can configure them
                    self.get_watermark = MagicMock(return_value=MagicMock())
                    self.save_watermark = MagicMock(return_value=MagicMock())
                    self.read_current_entity_changes = MagicMock(return_value=MagicMock())
            
            globals()['WatermarkService'] = MockWatermarkManager
            # Add alias for test compatibility
            globals()['MockWatermarkService'] = MockWatermarkManager
        else:
            logger.debug('WatermarkService already in globals')  

        if 'SparkTraceProvider' not in globals():
            class MockSparkTraceProvider:
                def __init__(self, logger_provider=None):
                    # Match constructor pattern from MockWatermarkManager
                    if logger_provider:
                        self.logger = logger_provider.get_logger("SparkTraceProvider")
                    else:
                        self.logger = MagicMock()
                    
                    self.create_span = MagicMock(return_value=self._create_mock_span())
                    self.get_current_trace_id = MagicMock(return_value="test-trace-id")
                    self.set_trace_context = MagicMock()
                    self.create_spark_span = MagicMock(return_value=self._create_mock_span())
                    self.span = MagicMock(return_value=self._create_mock_span())  # Add span method for tests
                
                def _create_mock_span(self):
                    """Create a consistent span mock like MockEntityProvider's _create_mock_df"""
                    span = MagicMock()
                    span.id = "test-span-id"
                    span.trace_id = "test-trace-id"
                    span.__enter__ = MagicMock(return_value=span)
                    span.__exit__ = MagicMock(return_value=None)
                    return span
            
            globals()['SparkTraceProvider'] = MockSparkTraceProvider
        else:
            logger.debug('SparkTraceProvider already in globals') 

        if 'DataPipesExecution' not in globals():
            class MockDataPipesExecution:
                def __init__(self, logger_provider=None, entity_registry=None, pipes_registry=None, read_persist_strategy=None):
                    self.logger_provider = logger_provider or MagicMock()
                    self.entity_registry = entity_registry or MagicMock()
                    self.pipes_registry = pipes_registry or MagicMock()
                    read_persist_strategy = read_persist_strategy or MagicMock()
                    
                    # REPLACE: def run_datapipes(self, pipes): return MagicMock()
                    # WITH:
                    self.run_datapipes = MagicMock()  # ← REPLACE with this line

            globals()['DataPipesExecution'] = MockDataPipesExecution
        else:
            logger.debug('DataPipesExecution already in globals')   

        if 'SimpleReadPersistStrategy' not in globals():
            class MockSimpleReadPersistStrategy:
                def __init__(self, watermark_service=None, entity_provider=None, logger_provider=None):
                    self.watermark_service = watermark_service or MagicMock()
                    self.entity_provider = entity_provider or MagicMock()
                    if logger_provider:
                        self.logger = logger_provider.get_logger("SimpleReadPersistStrategy")
                    else:
                        self.logger = MagicMock()
                
                def create_pipe_entity_reader(self, pipe):
                    return MagicMock()
                
                def create_pipe_persist_activator(self, pipe):
                    return MagicMock()
            
            globals()['SimpleReadPersistStrategy'] = MockSimpleReadPersistStrategy
        else:
            logger.debug('SimpleReadPersistStrategy already in globals')   


        if 'StageProcessingService' not in globals():
            class StageProcessingService:
                """Interface for stage processing services"""
                def execute(self, stage: str, stage_description: str, stage_details: dict, layer: str):
                    """Execute stage processing"""
                    pass
            
            globals()['StageProcessingService'] = StageProcessingService
        else:
            logger.debug('StageProcessingService already in globals')   

        # REVISED: PipeMetadata - now matches constructor patterns with optional parameters
        if 'PipeMetadata' not in globals():
            class MockPipeMetadata:
                def __init__(self, pipeid=None, name=None, input_entity_ids=None, output_entity_id=None, output_type=None, logger_provider=None):
                    # Match constructor pattern by accepting logger_provider even if not used
                    if logger_provider:
                        self.logger = logger_provider.get_logger("PipeMetadata")
                    else:
                        self.logger = MagicMock()
                    
                    self.pipeid = pipeid
                    self.name = name
                    self.input_entity_ids = input_entity_ids or []
                    self.output_entity_id = output_entity_id
                    self.output_type = output_type
            
            globals()['PipeMetadata'] = MockPipeMetadata
        else:
            logger.debug('PipeMetadata already in globals')   

    def _setup_synapse_mocks(self):
        """Setup Synapse-specific mocks"""
        # Mock notebook_import function
        if 'notebook_import' not in globals():
            def mock_notebook_import(module_name):
                pass
            globals()['notebook_import'] = mock_notebook_import
        
        # Mock get_or_create_spark_session
        if 'get_or_create_spark_session' not in globals():
            def mock_get_or_create_spark_session():
                return self.spark_session
            globals()['get_or_create_spark_session'] = mock_get_or_create_spark_session
        
    def _setup_test_data(self, test_data: Dict[str, Any]):
        """Setup test data in global namespace"""
        for key, value in test_data.items():
            globals()[key] = value
            
    def cleanup(self):
        """Cleanup test environment"""
        # Restore original globals if they were overridden
        for key, value in self.original_globals.items():
            globals()[key] = value
            
        # Clear any stored instances
        if 'GlobalInjector' in globals() and hasattr(globals()['GlobalInjector'], '_instances'):
            globals()['GlobalInjector']._instances.clear()


# Pre-execution setup utility - MAIN ENTRY POINT
def setup_global_test_environment(test_config: Optional[Dict[str, Any]] = None):
    """
    MAIN ENTRY POINT: Setup test environment in global scope.
    Call this at the very beginning of your test notebook/file.
    
    Args:
        test_config: Optional configuration dict
            - use_real_spark: bool (default False)
            - test_data: dict of test data to inject
    
    Returns:
        NotebookTestEnvironment instance for further configuration
    """
    env = NotebookTestEnvironment()
    env.setup_test_environment(test_config)
    return env

# Alternative: Quick setup function
def quick_mock_setup():
    """
    Quick setup with sensible defaults. Use this if you just want basic mocking.
    """
    return setup_global_test_environment({'use_real_spark': False})


@dataclass
class TestSpan:
    """Test double for SparkSpan"""
    id: str
    component: str
    operation: str
    attributes: Dict[str, str]
    traceId: uuid.UUID
    reraise: bool
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None


class NotebookTestRunner:
    """
    Test runner that coordinates the %run strategy for notebook testing.
    """
    
    def __init__(self):
        self.test_env = NotebookTestEnvironment()
        self.execution_results = {}
        
    def prepare_test_environment(self, test_config: Optional[Dict[str, Any]] = None):
        """
        Prepare the test environment. Call this BEFORE %run notebook.
        
        Args:
            test_config: Configuration for test environment
                - use_real_spark: bool - Use real Spark session vs mock
                - test_data: dict - Test data to inject
                - mock_config: dict - Mock configuration
        """
        self.test_env.setup_test_environment(test_config)
        
        # Setup test result capture
        self._setup_result_capture()
        
    def _setup_result_capture(self):
        """Setup mechanisms to capture execution results"""
        # Patch print to capture output
        self.captured_output = []
        
        original_print = print
        def capture_print(*args, **kwargs):
            self.captured_output.append(' '.join(str(arg) for arg in args))
            return original_print(*args, **kwargs)
        
        globals()['print'] = capture_print
        
    def get_mock(self, service_name: str):
        """Get a specific mock for testing"""
        return self.test_env.injector_mocks.get(service_name)
        
    def get_captured_output(self) -> List[str]:
        """Get captured print output"""
        return self.captured_output.copy()
        
    def assert_event_emitted(self, component: str, operation: str, count: int = 1):
        """Assert that a custom event was emitted"""
        event_emitter = self.get_mock('event_emitter')
        calls = event_emitter.emit_custom_event.call_args_list
        
        matching_calls = [
            call for call in calls 
            if call[0][0] == component and call[0][1].startswith(operation)
        ]
        
        assert len(matching_calls) >= count, \
            f"Expected at least {count} calls to emit_custom_event with component='{component}' and operation='{operation}', got {len(matching_calls)}"
            
    def assert_mdc_context_used(self, expected_keys: List[str]):
        """Assert that MDC context was used with expected keys"""
        # Check if setLocalProperty was called with MDC keys
        spark_calls = self.test_env.spark_session.sparkContext.setLocalProperty.call_args_list
        mdc_calls = [call for call in spark_calls if call[0][0].startswith('mdc.')]
        
        for key in expected_keys:
            mdc_key = f"mdc.{key}"
            assert any(call[0][0] == mdc_key for call in mdc_calls), \
                f"Expected MDC key '{key}' to be set"
                
    def cleanup(self):
        """Cleanup test environment"""
        self.test_env.cleanup()

def run_notebook_tests(*test_classes, test_config=None):
    """
    Run tests with FORCED config that overrides any existing environment
    """
    if test_config is None:
        test_config = {'use_real_spark': False}
    
    print(f"FORCING test environment setup with: {test_config}")
    
    # Force create new environment that overrides anything existing
    collector = MemoryTestCollector(test_classes, test_config)
    results = collector.run_tests()
    
    if results["success"]:
        print("✓ All tests PASSED")
    else:
        print("✗ Some tests FAILED")
    
    return results

def run_tests_in_folder(folder_name, test_config=None):
    """Run pytest test classes from notebooks in the specified folder"""
    if test_config is None:
        test_config = {'use_real_spark': False}
    
    # Get notebooks from folder
    try:
        notebooks = get_all_notebooks_for_folder(folder_name)
    except Exception as e:
        logger.error("Accessing folder '{folder_name}': {e}")
        return {"success": False, "error": str(e)}
    
    if not notebooks:
        logger.error(f"No notebooks found in folder '{folder_name}'")
        return {"success": True, "passed": 0, "failed": 0}
    
    test_classes = []

    # Extract pytest test classes from each notebook
    for notebook in notebooks:
        try:
            pytest_cell_code = extract_pytest_cell(notebook.name)
            if pytest_cell_code:
                classes = execute_test_cell_with_imports(pytest_cell_code, notebook.name)
                test_classes.extend(classes)
                logger.info(f"Found {len(classes)} test classes in {notebook.name}")
        except Exception as e:
            logger.error(f"Failed to process {notebook.name}: {e}")
            continue
    
    if not test_classes:
        logger.info(f"No pytest test classes found in folder '{folder_name}'")
        return {"success": True, "passed": 0, "failed": 0}
    
    # Run tests using existing framework
    logger.info(f"Running tests from {len(test_classes)} test classes...")
    return run_notebook_tests(*test_classes, test_config=test_config)


def extract_pytest_cell(notebook_name):
    """Extract the cell that starts with 'import pytest' from a notebook"""
    client = get_synapse_client()
    notebook = client.notebook.get_notebook(notebook_name)
    
    code = ""

    for cell in notebook.properties.cells:
        if cell.cell_type == "code":
            if isinstance(cell.source, list):
                cell_code = "".join(str(line) for line in cell.source)
            else:
                cell_code = str(cell.source)
            
            # Check if cell starts with 'import pytest'
            if cell_code.strip().startswith('import pytest'):
                code = code + "\n" + cell_code

            if cell_code.strip().startswith('%run') and not 'environment_bootstrap' in cell_code and not 'test_framework' in cell_code:
                logger.debug(f"Adding %run to code -- {cell_code}")
                code = code + "\n" + cell_code

    return None if code == "" else code


def execute_test_cell_with_imports(code, notebook_name):
    """Execute pytest cell code after handling %run imports"""
    import re
    
    # Find %run commands and convert to imports
    run_pattern = r'%run\s+([^"\'\s]+)'
    run_matches = re.findall(run_pattern, code)
    
    logger.debug(f"run_matches = {run_matches}")

    # Setup module globals
    module_globals = globals().copy()
    
    import_code = ""
    # Import notebooks referenced by %run commands
    for notebook_to_run in run_matches:
        try:
            logger.debug(f"Importing notebook {notebook_to_run} for test {notebook_name}")
            nb_code, _ = load_notebook_code(notebook_to_run)
            import_code = import_code + "\n" + nb_code      
        except Exception as e:
            logger.error(f"Failed to import {notebook_to_run}: {e}")

    # Remove %run lines from the code before executing
    cleaned_code = import_code + "\n" + re.sub(run_pattern, '', code)
    
    # Execute the cleaned pytest cell code
    test_classes = []
    try:
        exec(compile(cleaned_code, notebook_name, 'exec'), module_globals)
        
        # Find test classes
        for name, obj in module_globals.items():
            if (inspect.isclass(obj) and 
                (name.startswith('Test') or name.endswith('Test') or 
                 any(method.startswith('test_') for method in dir(obj)))):
                test_classes.append(obj)
                
    except Exception as e:
        logger.error(f"Error executing pytest cell from {notebook_name}: {e}")
    
    return test_classes


# Usage:
# results = run_tests_in_folder("test_folder")
# results = run_tests_in_folder("test_folder", {'use_real_spark': True})


# Usage:
# results = run_tests_in_folder("test_folder")
# results = run_tests_in_folder("test_folder", {'use_real_spark': True})


# Quick usage examples:
#
# # Basic usage:
# results = run_tests_in_folder("my_test_folder")
#
# # With custom config:
# config = {'use_real_spark': True, 'test_data': {'key': 'value'}}
# results = run_tests_in_folder("integration_tests", config)
#
# # With automatic setup:
# results = run_tests_in_folder_with_setup("unit_tests")


# Test utilities and fixtures
class SynapseNotebookTestCase:
    """Base class for Synapse notebook test cases"""
    
    def notebook_runner(self):
        """Setup method providing notebook test runner - cleanup handled by test runner"""
        runner = NotebookTestRunner()
        return runner
        
    def basic_test_config(self):
        """Basic test configuration"""
        return {
            'use_real_spark': False,
            'test_data': {
                'test_input': 'test_value',
                'test_df_data': [
                    {'id': 1, 'name': 'Alice', 'value': 100},
                    {'id': 2, 'name': 'Bob', 'value': 200}
                ]
            }
        }
        
    def real_spark_config(self):
        """Configuration using real Spark session"""
        return {
            'use_real_spark': True,
            'test_data': {}
        }


# Decorators and utilities
def synapse_notebook_test(test_config: Optional[Dict[str, Any]] = None):
    """
    Decorator for Synapse notebook tests using %run strategy.
    
    Usage:
        @synapse_notebook_test({'use_real_spark': False})
        def test_my_notebook(notebook_runner):
            # Notebook will be %run with test environment setup
            notebook_runner.assert_event_emitted('DataProcessor', 'PROCESS')
    """
    def decorator(test_func):
        def wrapper(*args, **kwargs):
            runner = NotebookTestRunner()
            runner.prepare_test_environment(test_config)
            try:
                return test_func(runner, *args, **kwargs)
            finally:
                runner.cleanup()
        return wrapper
    return decorator


# Test execution helpers
def create_test_dataframe(data: List[Dict], spark_session=None):
    """Create a test DataFrame"""
    spark = spark_session or globals().get('spark')
    if hasattr(spark, 'createDataFrame'):
        return spark.createDataFrame(data)
    else:
        # Return mock DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = len(data)
        mock_df.collect.return_value = data
        return mock_df


# pytest Integration
class TestCollectorPlugin:
    """Custom pytest plugin to collect test classes from memory"""
    
    def __init__(self, test_classes, test_config=None):
        self.test_classes = test_classes
        self.test_config = test_config or {}
        
    def pytest_configure(self, config):
        """Configure pytest with our test environment"""
        # Setup global test environment
        self.env = NotebookTestEnvironment()
        self.env.setup_test_environment(self.test_config)
        
    def pytest_unconfigure(self, config):
        """Cleanup after pytest finishes"""
        if hasattr(self, 'env'):
            self.env.cleanup()
    
    def pytest_collection_modifyitems(self, config, items):
        """Modify collected items to include our in-memory test classes"""
        # Clear default collected items
        original_items = items[:]
        items.clear()
        
        # Add our test classes
        for test_class in self.test_classes:
            # Create test instance
            instance = test_class()
            
            # Find test methods
            test_methods = [method for method in dir(instance) if method.startswith('test_')]
            
            for method_name in test_methods:
                # Create a test item that pytest can run
                test_id = f"{test_class.__name__}::{method_name}"
                
                # Create a callable that pytest can execute
                def make_test_func(cls, method):
                    def test_func():
                        # Create fresh instance for each test
                        test_instance = cls()
                        test_method = getattr(test_instance, method)
                        
                        # Use pytest's fixture injection
                        return pytest.main(['-s'], plugins=[])
                    return test_func
                
                # Add to pytest's item collection
                items.append(pytest.Item.from_parent(
                    parent=None,
                    name=test_id,
                ))

class MemoryTestCollector:
    """Test collector that FORCES environment setup to override notebook interference"""
    
    def __init__(self, test_classes, test_config=None):
        self.test_classes = test_classes
        self.test_config = test_config or {}
        self.env = None
        
    def run_tests(self):
        """Run tests with FORCED environment setup"""
        # FORCE setup environment - this will override any existing spark
        print(f"FORCING environment setup with config: {self.test_config}")
        self.env = NotebookTestEnvironment()
        self.env.setup_test_environment(self.test_config)
        
        # Verify the spark session type
        spark_type = str(type(globals().get('spark')))
        use_real = self.test_config.get('use_real_spark', False)
        print(f"After FORCED setup: spark type = {spark_type}, use_real_spark = {use_real}")
        
        passed = 0
        failed = 0
        failures = []
        all_tests = []
        
        try:
            # Collect all tests
            for test_class in self.test_classes:
                test_methods = [name for name in dir(test_class) 
                              if name.startswith('test_') and callable(getattr(test_class, name))]
                
                for method_name in test_methods:
                    all_tests.append((test_class, method_name))
            
            print(f"Running {len(all_tests)} tests...")
            print("=" * 80)
            
            # Run each test
            for test_class, method_name in all_tests:
                test_name = f"{test_class.__name__}::{method_name}"
                
                try:
                    # Create fresh test instance
                    test_instance = test_class()
                    test_method = getattr(test_instance, method_name)
                    
                    # Create runner that uses the shared environment (NO new environment setup)
                    runner = NotebookTestRunner()
                    runner.test_env = self.env  # Share the FORCED environment
                    runner._setup_result_capture()
                    
                    # Before running test, verify spark is still correct
                    current_spark_type = str(type(globals().get('spark')))
                    if current_spark_type != spark_type:
                        print(f"🚨 WARNING: Spark type changed before {test_name}: {current_spark_type}")
                    
                    try:
                        # Inspect method signature for fixture injection
                        sig = inspect.signature(test_method)
                        kwargs = {}
                        
                        for param_name in sig.parameters:
                            if param_name == 'self':
                                continue
                            elif param_name == 'notebook_runner':
                                kwargs[param_name] = runner
                            elif param_name == 'basic_test_config':
                                kwargs[param_name] = self.test_config
                            elif hasattr(test_instance, param_name):
                                # This is a setup method - call it to get the setup value
                                setup_method = getattr(test_instance, param_name)
                                
                                # Get setup method dependencies
                                setup_sig = inspect.signature(setup_method)
                                setup_kwargs = {}
                                
                                for setup_param in setup_sig.parameters:
                                    if setup_param == 'self':
                                        continue
                                    elif setup_param == 'notebook_runner':
                                        setup_kwargs[setup_param] = runner
                                    elif setup_param == 'basic_test_config':
                                        setup_kwargs[setup_param] = self.test_config
                                
                                # Call the setup method to get the value
                                kwargs[param_name] = setup_method(**setup_kwargs)
                        
                        # Run the test
                        test_method(**kwargs)
                        print(f"✓ {test_name} PASSED")
                        passed += 1
                        
                    except Exception as e:
                        raise
                        
                except Exception as e:
                    print(f"✗ {test_name} FAILED: {e}")
                    failures.append((test_name, str(e)))
                    failed += 1
            
            print("=" * 80)
            print(f"Results: {passed} passed, {failed} failed")
            
            if failures:
                print("\nFailed tests:")
                for test_name, error in failures:
                    print(f"  - {test_name}: {error}")
            
            return {
                "exit_code": 1 if failed > 0 else 0,
                "success": failed == 0,
                "passed": passed,
                "failed": failed,
                "failures": failures
            }
            
        finally:
            # Cleanup once at the end
            if self.env:
                self.env.cleanup()

def run_tests_in_folder(folder_name, test_config=None):
    """Run pytest test classes from notebooks in the specified folder"""
    if test_config is None:
        test_config = {'use_real_spark': True}
    
    # Get notebooks from folder
    try:
        notebooks = get_all_notebooks_for_folder(folder_name)
    except Exception as e:
        logger.error("Accessing folder '{folder_name}': {e}")
        return {"success": False, "error": str(e)}
    
    if not notebooks:
        logger.error(f"No notebooks found in folder '{folder_name}'")
        return {"success": True, "passed": 0, "failed": 0}
    
    test_classes = []

    # Extract pytest test classes from each notebook FIRST
    for notebook in notebooks:
        try:
            pytest_cell_code = extract_pytest_cell(notebook.name)
            if pytest_cell_code:
                classes = execute_test_cell_with_imports(pytest_cell_code, notebook.name)
                test_classes.extend(classes)
                logger.info(f"Found {len(classes)} test classes in {notebook.name}")
        except Exception as e:
            logger.error(f"Failed to process {notebook.name}: {e}")
            continue
    
    if not test_classes:
        logger.info(f"No pytest test classes found in folder '{folder_name}'")
        return {"success": True, "passed": 0, "failed": 0}
    
    # CRITICAL FIX: Force environment setup AFTER extraction
    # This ensures our config overrides any notebook spark setup
    logger.debug(f"Forcing environment setup with: {test_config}")
    
    # Use the forced collector that sets up environment after extraction
    collector = MemoryTestCollector(test_classes, test_config)
    results = collector.run_tests()
    
    if results["success"]:
        print("✓ All tests PASSED")
    else:
        print("✗ Some tests FAILED")
    
    return results


def run_single_test_class(test_class, test_config: Optional[Dict[str, Any]] = None):
    """
    Run a single test class using pytest
    
    Args:
        test_class: Test class to run
        test_config: Optional test configuration
        
    Returns:
        Dict with test results
    """
    return run_notebook_tests(test_class, test_config=test_config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
