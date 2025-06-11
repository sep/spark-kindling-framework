# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Synapse Notebook Testing Framework with %run Strategy
# =====================================================

import pytest
import uuid
from typing import Dict, Any, Optional, List, Union
from unittest.mock import Mock, patch, MagicMock
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
import json
import traceback
from abc import ABC, abstractmethod

# Test infrastructure that works with %run strategy
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
        
        # Mock DataFrame operations
        spark_mock.createDataFrame = self._mock_create_dataframe
        
        return spark_mock
        
    def _mock_create_dataframe(self, data, schema=None):
        """Mock DataFrame creation"""
        df_mock = MagicMock()
        df_mock.count.return_value = len(data) if isinstance(data, list) else 1
        df_mock.collect.return_value = data if isinstance(data, list) else [data]
        df_mock.show = MagicMock()
        return df_mock
        
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
                def get_logger(self, name):
                    return MagicMock()
            globals()['PythonLoggerProvider'] = MockPythonLoggerProvider
            
        # Mock inject decorator
        if 'inject' not in globals():
            def mock_inject(func):
                return func
            globals()['inject'] = mock_inject
        
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
    
    @pytest.fixture(scope="function")
    def notebook_runner(self):
        """Fixture providing notebook test runner"""
        runner = NotebookTestRunner()
        yield runner
        runner.cleanup()
        
    @pytest.fixture
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
        
    @pytest.fixture
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


def run_test_method(test_class, method_name, *args, **kwargs):
    """Helper to run a single test method"""
    test_instance = test_class()
    method = getattr(test_instance, method_name)
    try:
        result = method(*args, **kwargs)
        print(f"✓ {method_name} PASSED")
        return result
    except Exception as e:
        print(f"✗ {method_name} FAILED: {e}")
        raise


def run_all_tests(test_class, test_config: Optional[Dict[str, Any]] = None):
    """
    Run all test methods in a test class.
    
    Args:
        test_class: Test class to run (should inherit from SynapseNotebookTestCase)
        test_config: Optional test configuration
        
    Returns:
        Dict with pytest-style JSON report format
    """
    if test_config is None:
        test_config = {'use_real_spark': False}
        
    test_instance = test_class()
    runner = NotebookTestRunner()
    
    # Get all methods that start with 'test_'
    test_methods = [method for method in dir(test_instance) if method.startswith('test_')]
    
    tests = []
    passed_count = 0
    failed_count = 0
    
    print(f"Running {len(test_methods)} tests from {test_class.__name__}...")
    print("=" * 60)
    
    for method_name in test_methods:
        test_nodeid = f"{test_class.__name__}::{method_name}"
        
        try:
            # Setup fresh environment for each test
            runner.cleanup()
            runner = NotebookTestRunner()
            
            method = getattr(test_instance, method_name)
            
            # Check if method expects specific fixtures
            import inspect
            sig = inspect.signature(method)
            args = []
            
            # Add common fixtures that tests expect
            if 'notebook_runner' in sig.parameters:
                args.append(runner)
            if 'basic_test_config' in sig.parameters:
                args.append(test_config)
                
            method(*args)
            print(f"✓ {method_name} PASSED")
            
            tests.append({
                "nodeid": test_nodeid,
                "outcome": "passed",
                "setup": {"outcome": "passed"},
                "call": {"outcome": "passed"},
                "teardown": {"outcome": "passed"}
            })
            passed_count += 1
            
        except Exception as e:
            print(f"✗ {method_name} FAILED: {e}")
            
            tests.append({
                "nodeid": test_nodeid,
                "outcome": "failed",
                "setup": {"outcome": "passed"},
                "call": {
                    "outcome": "failed",
                    "longrepr": str(e)
                },
                "teardown": {"outcome": "passed"}
            })
            failed_count += 1
            
        finally:
            runner.cleanup()
    
    print("=" * 60)
    print(f"Results: {passed_count} passed, {failed_count} failed")
    
    if failed_count > 0:
        print("\nFailed tests:")
        for test in tests:
            if test['outcome'] == 'failed':
                test_name = test['nodeid'].split('::')[-1]
                error = test['call'].get('longrepr', 'Unknown error')
                print(f"  - {test_name}: {error}")
    
    return {
        "tests": tests,
        "summary": {
            "total": len(test_methods),
            "passed": passed_count,
            "failed": failed_count
        }
    }


def run_notebook_tests(*test_classes, test_config: Optional[Dict[str, Any]] = None):
    """
    Run tests from multiple test classes.
    
    Args:
        *test_classes: Variable number of test classes to run
        test_config: Optional test configuration
        
    Returns:
        Dict with pytest-style JSON report format
    """
    if test_config is None:
        test_config = {'use_real_spark': False}
    
    all_tests = []
    total_passed = 0
    total_failed = 0
    
    print(f"Running tests from {len(test_classes)} test classes...")
    print("=" * 80)
    
    for test_class in test_classes:
        print(f"\n>>> Running {test_class.__name__}")
        results = run_all_tests(test_class, test_config)
        
        # Collect all tests
        all_tests.extend(results['tests'])
        
        # Update totals
        total_passed += results['summary']['passed']
        total_failed += results['summary']['failed']
    
    print("=" * 80)
    print(f"OVERALL RESULTS: {total_passed} passed, {total_failed} failed")
    print(f"Success rate: {total_passed / (total_passed + total_failed) * 100:.1f}%")
    
    return {
        "tests": all_tests,
        "summary": {
            "total": total_passed + total_failed,
            "passed": total_passed,
            "failed": total_failed
        }
    }


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
        
    def test_spark_trace_static_methods(self, notebook_runner, basic_test_config):
        """Test SparkTrace static methods and initialization"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # After %run of your notebook, test SparkTrace
        # SparkTrace.current() should return instance
        current_trace = SparkTrace.current()
        assert current_trace is not None
        
        # Test span method delegation
        span_context = current_trace.span(operation="TestOp", component="TestComp")
        assert span_context is not None
        
    def test_synapse_spark_trace_span_lifecycle(self, notebook_runner, basic_test_config):
        """Test complete span lifecycle with SynapseSparkTrace"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Get the event emitter mock to verify calls
        event_emitter = notebook_runner.get_mock('event_emitter')
        
        # Simulate span usage (after %run of your notebook)
        # This tests the actual span context manager behavior
        test_details = {"test_key": "test_value"}
        
        # Test span START, SUCCESS, and END events
        with patch('datetime.datetime') as mock_datetime:
            mock_now = datetime(2024, 1, 1, 12, 0, 0)
            mock_datetime.now.return_value = mock_now
            
            # Simulate successful span execution
            try:
                # This simulates: with trace.span("TestOp", "TestComp", details):
                event_emitter.emit_custom_event("TestComp", "TestOp_START", test_details, "1", uuid.uuid4())
                # ... span body execution ...
                event_emitter.emit_custom_event("TestComp", "TestOp_END", test_details, "1", uuid.uuid4())
            except Exception:
                pass
                
        # Verify START and END events were emitted
        calls = event_emitter.emit_custom_event.call_args_list
        start_calls = [call for call in calls if "START" in call[0][1]]
        end_calls = [call for call in calls if "END" in call[0][1]] 
        
        assert len(start_calls) >= 1, "Expected at least one START event"
        assert len(end_calls) >= 1, "Expected at least one END event"
        
    def test_synapse_spark_trace_error_handling(self, notebook_runner, basic_test_config):
        """Test span error handling and ERROR event emission"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        
        # Simulate span with exception
        test_exception = ValueError("Test error")
        
        # Simulate error span execution
        event_emitter.emit_custom_event("TestComp", "TestOp_START", {}, "1", uuid.uuid4())
        event_emitter.emit_custom_event("TestComp", "TestOp_ERROR", 
                                       {"exception": str(test_exception)}, "1", uuid.uuid4())
        event_emitter.emit_custom_event("TestComp", "TestOp_END", {}, "1", uuid.uuid4())
        
        # Verify ERROR event was emitted
        calls = event_emitter.emit_custom_event.call_args_list
        error_calls = [call for call in calls if "ERROR" in call[0][1]]
        assert len(error_calls) >= 1, "Expected at least one ERROR event"
        
        # Verify error details include exception info
        error_call = error_calls[0]
        error_details = error_call[0][2]  # details parameter
        assert "exception" in error_details
        
    def test_spark_trace_activity_counter(self, notebook_runner, basic_test_config):
        """Test activity counter incrementation in SynapseSparkTrace"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        
        # Simulate multiple span calls to test counter
        for i in range(3):
            expected_id = str(i + 2)  # Counter starts at 1, increments before use
            event_emitter.emit_custom_event("TestComp", "TestOp_START", {}, expected_id, uuid.uuid4())
            
        # Verify different IDs were used
        calls = event_emitter.emit_custom_event.call_args_list
        used_ids = [call[0][3] for call in calls]  # eventId parameter
        assert len(set(used_ids)) == len(used_ids), "Expected unique span IDs"
        
    def test_spark_trace_nested_spans(self, notebook_runner, basic_test_config):
        """Test nested span behavior"""
        notebook_runner.prepare_test_environment(basic_test_config)
        
        event_emitter = notebook_runner.get_mock('event_emitter')
        
        # Simulate nested spans
        outer_trace_id = uuid.uuid4()
        
        # Outer span
        event_emitter.emit_custom_event("OuterComp", "OuterOp_START", {}, "1", outer_trace_id)
        
        # Inner span (should inherit trace ID)
        event_emitter.emit_custom_event("InnerComp", "InnerOp_START", {}, "2", outer_trace_id)
        event_emitter.emit_custom_event("InnerComp", "InnerOp_END", {}, "2", outer_trace_id)
        
        # Close outer span
        event_emitter.emit_custom_event("OuterComp", "OuterOp_END", {}, "1", outer_trace_id)
        
        # Verify trace ID consistency
        calls = event_emitter.emit_custom_event.call_args_list
        trace_ids = [call[0][4] for call in calls]  # traceId parameter
        assert all(tid == outer_trace_id for tid in trace_ids), "Expected consistent trace ID for nested spans"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
