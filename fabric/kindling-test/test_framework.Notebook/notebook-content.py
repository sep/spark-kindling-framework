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
        # Store original values if they exist
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

        if 'FileIngestionRegistry' not in globals():
            class MockFileIngestionRegistry:
                def __init__(self):
                    self.register_entry = MagicMock()
                    self.get_entry_ids = MagicMock(return_value=[])
                    self.get_entry_definition = MagicMock(return_value=MagicMock())
            
            globals()['FileIngestionRegistry'] = MockFileIngestionRegistry
        
        # Mock FileIngestionProcessor interface  
        if 'FileIngestionProcessor' not in globals():
            class MockFileIngestionProcessor:
                def __init__(self):
                    self.process_path = MagicMock()
            
            globals()['FileIngestionProcessor'] = MockFileIngestionProcessor

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

        if 'StageProcessingService' not in globals():
            class StageProcessingService:
                """Interface for stage processing services"""
                def execute(self, stage: str, stage_description: str, stage_details: dict, layer: str):
                    """Execute stage processing"""
                    pass
            
            globals()['StageProcessingService'] = StageProcessingService

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
    """Simple test runner that works with pytest-style fixtures"""
    
    def __init__(self, test_classes, test_config=None):
        self.test_classes = test_classes
        self.test_config = test_config or {}
        self.env = None
        
    def run_tests(self):
        """Run tests with manual fixture injection"""
        # Setup environment once
        self.env = NotebookTestEnvironment()
        self.env.setup_test_environment(self.test_config)
        
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
                    
                    # Create notebook runner for this test
                    runner = NotebookTestRunner()
                    runner.prepare_test_environment(self.test_config)
                    
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
                        
                    finally:
                        runner.cleanup()
                        
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
            # Cleanup
            if self.env:
                self.env.cleanup()


def run_notebook_tests(*test_classes, test_config: Optional[Dict[str, Any]] = None):
    """
    Run tests with pytest-style fixtures in notebook environment
    
    Args:
        *test_classes: Variable number of test classes to run
        test_config: Optional test configuration
        
    Returns:
        Dict with test results
    """
    if test_config is None:
        test_config = {'use_real_spark': False}
    
    print(f"Running tests from {len(test_classes)} test classes...")
    
    # Use the memory test collector
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


# Specific tests for SparkTrace functionality
class TestSparkTraceComponents(SynapseNotebookTestCase):
    """Tests specifically for the SparkTrace, SynapseSparkTrace, and related components"""
    
    def test_custom_event_emitter_interface(self, notebook_runner, basic_test_config):
        """Test that CustomEventEmitter interface works correctly"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # After %run of your notebook, test the emitter
        event_emitter = notebook_runner.get_mock('event_emitter')
        
        # Test emit_custom_event call
        test_trace_id = uuid.uuid4()
        event_emitter.emit_custom_event(
            component="TestComponent",
            operation="TestOperation", 
            details={"key": "value"},
            eventId="test-123",
            traceId=test_trace_id
        )
        
        # Verify the call was made correctly
        event_emitter.emit_custom_event.assert_called_with(
            "TestComponent", "TestOperation", {"key": "value"}, "test-123", test_trace_id
        )
    
    def test_synapse_event_emitter_initialization(self, notebook_runner, basic_test_config):
        """Test SynapseEventEmitter initialization and logger injection"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Simulate creating SynapseEventEmitter instance
        # After %run, verify logger was requested from injector
        logger_provider = notebook_runner.get_mock('logger_provider')
        logger_provider.get_logger.assert_called()
        
    def test_mdc_context_manager(self, notebook_runner, basic_test_config):
        """Test the mdc_context context manager"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test MDC context usage (this would be called in your notebook)
        test_context = {
            'trace_id': 'test-trace-123',
            'span_id': 'span-456', 
            'component': 'TestComponent',
            'operation': 'TestOp'
        }
        
        # Simulate mdc_context usage
        spark = notebook_runner.test_env.spark_session
        mdc_mock = spark._jvm.org.apache.log4j.MDC
        
        # Verify MDC operations would be called
        for key, value in test_context.items():
            spark.sparkContext.setLocalProperty(f"mdc.{key}", value)
            mdc_mock.put(key, value)
        
        # Assert MDC context was set
        notebook_runner.assert_mdc_context_used(['trace_id', 'span_id', 'component', 'operation'])
        
    def test_spark_span_dataclass(self, notebook_runner, basic_test_config):
        """Test SparkSpan dataclass creation and attributes"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test SparkSpan creation
        test_trace_id = uuid.uuid4()
        span = TestSpan(
            id="test-span-1",
            component="TestComponent", 
            operation="TestOperation",
            attributes={"key": "value"},
            traceId=test_trace_id,
            reraise=True
        )
        
        assert span.id == "test-span-1"
        assert span.component == "TestComponent"
        assert span.operation == "TestOperation" 
        assert span.attributes == {"key": "value"}
        assert span.traceId == test_trace_id
        assert span.reraise is True
        assert span.start_time is not None
        assert span.end_time is None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
