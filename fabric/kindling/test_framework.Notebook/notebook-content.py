# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

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

# ==============================================================================
# Service Wrapper for Dependency Injection
# ==============================================================================

class CombinedFramework:
    """Combines environment service, notebook loader, and config"""
    def __init__(self, environment_service, notebook_loader, config):
        self.environment_service = environment_service
        self.notebook_loader = notebook_loader
        self.config = config
        
    def __getattr__(self, name):
        # Try environment_service first
        if hasattr(self.environment_service, name):
            return getattr(self.environment_service, name)
        # Then notebook_loader
        elif hasattr(self.notebook_loader, name):
            return getattr(self.notebook_loader, name)
        else:
            raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")


class InjectorServiceWrapper:
    """Wrapper that uses DI to get services when available, or falls back to provided instances"""
    
    @staticmethod
    def get_combined_framework(
        environment_service=None,
        notebook_loader=None, 
        config=None
    ) -> CombinedFramework:
        """
        Get CombinedFramework either through DI or provided instances.
        
        Args:
            environment_service: Optional pre-created environment service
            notebook_loader: Optional pre-created notebook loader
            config: Optional pre-created config service
            
        Returns:
            CombinedFramework instance
        """
        # Try to use DI if available
        if InjectorServiceWrapper._can_use_di():
            return InjectorServiceWrapper._get_framework_via_di()
        
        # Fall back to provided instances or minimal framework
        if environment_service and notebook_loader and config:
            return CombinedFramework(environment_service, notebook_loader, config)
        
        # Try global framework as last resort
        if 'framework' in globals():
            fw = globals()['framework']
            return CombinedFramework(
                fw.environment_service,
                fw.notebook_loader,
                fw.config
            )
        
        raise RuntimeError(
            "Cannot create CombinedFramework: DI not available and no services provided. "
            "Either provide environment_service, notebook_loader, and config, or ensure "
            "Kindling framework is available."
        )
    
    @staticmethod
    def _can_use_di() -> bool:
        """Check if DI infrastructure is available"""
        try:
            from kindling.injection import get_kindling_service
            return True
        except ImportError:
            return False
    
    @staticmethod
    def _get_framework_via_di() -> CombinedFramework:
        """Get framework using dependency injection"""
        from kindling.injection import get_kindling_service
        from kindling.platform_provider import PlatformServiceProvider
        from kindling.notebook_framework import NotebookManager
        from kindling.spark_config import ConfigService
        
        return CombinedFramework(
            get_kindling_service(NotebookManager),
            get_kindling_service(PlatformServiceProvider).get_service(),
            get_kindling_service(ConfigService)
        )


# ==============================================================================
# Tester Class - Extracted from module-level functions
# ==============================================================================

class NotebookTester:
    """
    Handles test discovery, execution, and framework integration.
    Replaces module-level test execution functions with a cleaner interface.
    """
    
    def __init__(self, combined_framework: CombinedFramework):
        """
        Initialize tester with framework services.
        
        Args:
            combined_framework: CombinedFramework providing access to services
        """
        self.framework = combined_framework
        self.logger = self._get_logger()
    
    def _get_logger(self):
        """Get logger instance"""
        # Try to use framework logger if available
        if hasattr(self.framework, 'logger'):
            return self.framework.logger
        # Fall back to print-based logger
        return type('Logger', (), {
            'info': print,
            'error': print,
            'debug': print,
            'warning': print
        })()
    
    def run_notebook_suites(self, test_suites: Dict, test_suite_configs: Optional[Dict] = None) -> Dict:
        """
        Run multiple test suites with optional per-suite configuration.
        
        Args:
            test_suites: Dict mapping suite names to test class lists
            test_suite_configs: Optional dict mapping suite names to configs
            
        Returns:
            Dict mapping suite names to their results
        """
        results = {}
        for key in test_suites.keys():
            print(f"Running test suite: {key}")
            current_config = test_suite_configs.get(key, None) if test_suite_configs else None
            env = setup_global_test_environment(current_config)
            results[key] = self.run_notebook_tests(
                *test_suites[key], 
                test_config=current_config, 
                env=env
            )
        return results
    
    def run_notebook_tests(self, *test_classes, test_config=None, env=None) -> Dict:
        """
        Run tests with forced config that overrides any existing environment.
        
        Args:
            test_classes: Test classes to run
            test_config: Optional test configuration
            env: Optional pre-created test environment
            
        Returns:
            Dict with test results
        """
        if test_config is None:
            test_config = {'use_real_spark': False}
        
        collector = MemoryTestCollector(test_classes, test_config=test_config, env=env)
        results = collector.run_tests()
        
        if results["success"]:
            print("✓ All tests PASSED")
        else:
            print("✗ Some tests FAILED")
        
        return results
    
    def run_tests_in_folder(self, folder_name: str, test_config: Optional[Dict] = None) -> Dict:
        """
        Run pytest test classes from notebooks in the specified folder.
        
        Args:
            folder_name: Name of folder containing test notebooks
            test_config: Optional test configuration
            
        Returns:
            Dict with aggregated test results
        """
        if test_config is None:
            test_config = {'use_real_spark': False}
        
        notebooks = self.framework.get_notebooks_for_folder(folder_name)

        if not notebooks:
            self.logger.error(f"No notebooks found in folder '{folder_name}'")
            return {"success": True, "passed": 0, "failed": 0}
        
        test_suites = {}
        test_suite_configs = {}

        # Extract pytest test classes from each notebook
        for notebook in notebooks:
            try:
                notebook_with_code = self.framework.get_notebook(notebook.name)

                pytest_cell_config = self._extract_config_cell(notebook_with_code)
                if pytest_cell_config:
                    temp_globals = {}
                    exec(compile(pytest_cell_config, notebook.name, 'exec'), temp_globals)
                    test_suite_configs[notebook.name] = temp_globals.get('test_config', None)

                pytest_cell_code = self._extract_pytest_cell(notebook_with_code)
                if pytest_cell_code:
                    test_suites[notebook.name] = self._execute_test_cell_with_imports(
                        pytest_cell_code, 
                        notebook.name
                    )
                    self.logger.info(f"Found {len(test_suites[notebook.name])} tests in suite {notebook.name}")
            except Exception as e:
                self.logger.error(f"Failed to process {notebook.name}: {e}")
                continue
        
        if not test_suites:
            self.logger.info(f"No pytest test classes found in folder '{folder_name}'")
            return {"success": True, "passed": 0, "failed": 0}
        
        # Run tests using existing framework
        self.logger.info(f"Running tests from {len(test_suites)} test suites...")
        return self.run_notebook_suites(test_suites, test_suite_configs=test_suite_configs)
    
    def _extract_config_cell(self, notebook) -> Optional[str]:
        """Extract the cell that starts with 'test_config' from a notebook"""
        for cell in notebook.properties.cells:
            if cell.cell_type == "code":
                if isinstance(cell.source, list):
                    cell_code = "".join(str(line) for line in cell.source)
                else:
                    cell_code = str(cell.source)
                
                if cell_code.strip().startswith('test_config'):
                    return cell_code
        return None
    
    def _extract_pytest_cell(self, notebook) -> Optional[str]:
        """Extract cells containing pytest code from a notebook"""
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

                if (cell_code.strip().startswith('%run') and 
                    'environment_bootstrap' not in cell_code and 
                    'test_framework' not in cell_code):
                    code = code + "\n" + cell_code

        return None if code == "" else code
    
    def _execute_test_cell_with_imports(self, code: str, notebook_name: str) -> List:
        """Execute pytest cell code after handling %run imports"""
        import re
        
        run_pattern = r'%run\s+([^"\'\s]+)'
        run_matches = re.findall(run_pattern, code)
        
        self.logger.debug(f"run_matches = {run_matches}")

        # Setup module globals
        module_globals = globals().copy()
        
        import_code = ""
        # Import notebooks referenced by %run commands
        for notebook_to_run in run_matches:
            try:
                self.logger.debug(f"Importing notebook {notebook_to_run} for test {notebook_name}")
                nb_code, _ = self.framework.load_notebook_code(notebook_to_run, True)
                import_code = import_code + "\n" + nb_code      
            except Exception as e:
                self.logger.error(f"Failed to import {notebook_to_run}: {e}")

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
            self.logger.error(f"Error executing pytest cell from {notebook_name}: {e}")
            self.logger.error(f"Code: {cleaned_code}")
        
        return test_classes
    
    def run_single_test_class(self, test_class, test_config: Optional[Dict[str, Any]] = None) -> Dict:
        """
        Run a single test class.
        
        Args:
            test_class: Test class to run
            test_config: Optional test configuration
            
        Returns:
            Dict with test results
        """
        return self.run_notebook_tests(test_class, test_config=test_config)


# ==============================================================================
# Legacy compatibility functions - delegate to NotebookTester
# ==============================================================================

def get_fw_client() -> CombinedFramework:
    """Get framework client - legacy compatibility function"""
    return InjectorServiceWrapper.get_combined_framework()


def run_notebook_suites(test_suites, test_suite_configs=None):
    """Legacy compatibility wrapper"""
    tester = NotebookTester(get_fw_client())
    return tester.run_notebook_suites(test_suites, test_suite_configs)


def run_notebook_tests(*test_classes, test_config=None, env=None):
    """Legacy compatibility wrapper"""
    tester = NotebookTester(get_fw_client())
    return tester.run_notebook_tests(*test_classes, test_config=test_config, env=env)


def run_tests_in_folder(folder_name, test_config=None):
    """Legacy compatibility wrapper"""
    tester = NotebookTester(get_fw_client())
    return tester.run_tests_in_folder(folder_name, test_config)


def run_single_test_class(test_class, test_config: Optional[Dict[str, Any]] = None):
    """Legacy compatibility wrapper"""
    tester = NotebookTester(get_fw_client())
    return tester.run_single_test_class(test_class, test_config)


# ==============================================================================
# Test Infrastructure - NotebookTestEnvironment
# ==============================================================================

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
        
        self._delete_global_mocks()

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
            
            @classmethod
            def get(cls, iface):
                return globals().get(iface.__name__, None)
                
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
            
        # Mock PythonLoggerProvider
        if 'PythonLoggerProvider' not in globals():
            class MockPythonLoggerProvider:
                def __init__(self, logger_provider=None):
                    if logger_provider:
                        self.logger = logger_provider.get_logger("PythonLoggerProvider")
                    else:
                        self.logger = MagicMock()
                    
                    self.get_logger = MagicMock(return_value=MagicMock())
                    
            globals()['PythonLoggerProvider'] = MockPythonLoggerProvider
            
        # Mock inject decorator
        if 'inject' not in globals():
            def mock_inject(func):
                return func
            globals()['inject'] = mock_inject

        if 'DataEntityRegistry' not in globals():
            class DataEntityRegistry:
                def __init__(self, logger_provider=None):
                    if logger_provider:
                        self.logger = logger_provider.get_logger("DataEntityRegistry")
                    else:
                        self.logger = MagicMock()
                    
                    self.register_entity = MagicMock()
                    self.get_entity_ids = MagicMock(return_value=[])
                    self.get_entity_definition = MagicMock(return_value=MagicMock())
                    
            globals()['DataEntityRegistry'] = DataEntityRegistry

        if 'PlatformServiceProvider' not in globals():
            class MockPlatformServiceProvider:
                def __init__(self):
                    pes = MagicMock()
                    self.get_service = MagicMock(return_value=pes)
            globals()['PlatformServiceProvider'] = MockPlatformServiceProvider

        if 'DataPipesRegistry' not in globals():
            class MockDataPipesRegistry:
                def __init__(self, logger_provider=None):
                    if logger_provider:
                        self.logger = logger_provider.get_logger("DataPipesRegistry")
                    else:
                        self.logger = MagicMock()
                    
                    self.register_pipe = MagicMock()
                    self.get_pipe_ids = MagicMock(return_value=[])
                    self.get_pipe_definition = MagicMock(return_value=MagicMock())
                    
            globals()['DataPipesRegistry'] = MockDataPipesRegistry

        if 'EntityReadPersistStrategy' not in globals():
            class MockEntityReadPersistStrategy:
                def __init__(self, logger_provider=None, entity_provider=None):
                    self.entity_provider = entity_provider or MagicMock()
                    
                    if logger_provider:
                        self.logger = logger_provider.get_logger("EntityReadPersistStrategy")
                    else:
                        self.logger = MagicMock()
                    
                    self.create_pipe_entity_reader = MagicMock(return_value=MagicMock())
                    self.create_pipe_persist_activator = MagicMock(return_value=MagicMock())
                    
            globals()['EntityReadPersistStrategy'] = MockEntityReadPersistStrategy

        if 'EntityProvider' not in globals():
            class MockEntityProvider:
                def __init__(self):
                    self.read_entity = MagicMock()
                    self.merge_to_entity = MagicMock()
                    self.get_entity_version = MagicMock()
                    self.read_entity_since_version = MagicMock()
                    self.check_entity_exists = MagicMock()
                    self.write_to_entity = MagicMock()
                    self.ensure_entity_table = MagicMock()
                    
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

        if 'FileIngestionProcessor' not in globals():
            class MockFileIngestionProcessor:
                def __init__(self):
                    self.process_path = MagicMock()
            
            globals()['FileIngestionProcessor'] = MockFileIngestionProcessor

        if 'WatermarkEntityFinder' not in globals():
            class MockWatermarkEntityFinder:
                def __init__(self, logger_provider=None):
                    if logger_provider:
                        self.logger = logger_provider.get_logger("WatermarkEntityFinder")
                    else:
                        self.logger = MagicMock()
                    
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
                
                    self.get_watermark = MagicMock(return_value=MagicMock())
                    self.save_watermark = MagicMock(return_value=MagicMock())
                    self.read_current_entity_changes = MagicMock(return_value=MagicMock())
            
            globals()['WatermarkService'] = MockWatermarkManager
            globals()['MockWatermarkService'] = MockWatermarkManager

        if 'SparkTraceProvider' not in globals():
            class MockSparkTraceProvider:
                def __init__(self, logger_provider=None):
                    if logger_provider:
                        self.logger = logger_provider.get_logger("SparkTraceProvider")
                    else:
                        self.logger = MagicMock()
                    
                    self.create_span = MagicMock(return_value=self._create_mock_span())
                    self.get_current_trace_id = MagicMock(return_value="test-trace-id")
                    self.set_trace_context = MagicMock()
                    self.create_spark_span = MagicMock(return_value=self._create_mock_span())
                    self.span = MagicMock(return_value=self._create_mock_span())
                
                def _create_mock_span(self):
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
                    
                    self.run_datapipes = MagicMock()

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
            class MockStageProcessingService:
                """Interface for stage processing services"""
                def execute(self, stage: str, stage_description: str, stage_details: dict, layer: str):
                    """Execute stage processing"""
                    pass
            
            globals()['StageProcessingService'] = MockStageProcessingService

        if 'PipeMetadata' not in globals():
            class MockPipeMetadata:
                def __init__(self, pipeid=None, name=None, input_entity_ids=None, output_entity_id=None, output_type=None, logger_provider=None):
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

        if 'ConfigService' not in globals():
            class MockConfigService:
                def __init__(self, config_data=None, logger_provider=None):
                    self.config_data = config_data or {}
                    if logger_provider:
                        self.logger = logger_provider.get_logger("ConfigService")
                    else:
                        self.logger = MagicMock()
                    
                    # Example mocked methods (customize with actual method names if needed)
                    self.get = MagicMock(side_effect=lambda key, default=None: self.config_data.get(key, default))
                    self.set = MagicMock(side_effect=lambda key, value: self.config_data.__setitem__(key, value))
                    self.to_dict = MagicMock(return_value=self.config_data.copy())
                    # Add other methods or behaviors as needed for your use-case

            globals()['ConfigService'] = MockConfigService
            globals()['MockConfigService'] = MockConfigService

    mocked_globals = [
        'BaseServiceProvider',
        'PythonLoggerProvider', 
        'inject',
        'DataEntityRegistry',
        'DataPipesRegistry',
        'EntityReadPersistStrategy',
        'EntityProvider',
        'FileIngestionRegistry',
        'FileIngestionProcessor',
        'WatermarkEntityFinder',
        'WatermarkService',
        'SparkTraceProvider',
        'DataPipesExecution',
        'SimpleReadPersistStrategy',
        'StageProcessingService',
        'PipeMetadata',
        'ConfigService'
    ]

    def _delete_global_mocks(self):
        """Delete all mocked globals from the global namespace"""
        for global_name in self.mocked_globals:
            if global_name in globals():
                del globals()[global_name]

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


# ==============================================================================
# Pre-execution setup utilities
# ==============================================================================

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


def quick_mock_setup():
    """Quick setup with sensible defaults. Use this if you just want basic mocking."""
    return setup_global_test_environment({'use_real_spark': False})


# ==============================================================================
# Test Span and Test Runner
# ==============================================================================

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
    
    def __init__(self, env=None):
        self.test_env = env or NotebookTestEnvironment()
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
        self._setup_result_capture()
        
    def _setup_result_capture(self):
        """Setup mechanisms to capture execution results"""
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
        spark_calls = self.test_env.spark_session.sparkContext.setLocalProperty.call_args_list
        mdc_calls = [call for call in spark_calls if call[0][0].startswith('mdc.')]
        
        for key in expected_keys:
            mdc_key = f"mdc.{key}"
            assert any(call[0][0] == mdc_key for call in mdc_calls), \
                f"Expected MDC key '{key}' to be set"
                
    def cleanup(self):
        """Cleanup test environment"""
        self.test_env.cleanup()


# ==============================================================================
# Test Base Class and Utilities
# ==============================================================================

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


def synapse_notebook_test(test_config: Optional[Dict[str, Any]] = None):
    """
    Decorator for Synapse notebook tests using %run strategy.
    
    Usage:
        @synapse_notebook_test({'use_real_spark': False})
        def test_my_notebook(notebook_runner):
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


def create_test_dataframe(data: List[Dict], spark_session=None):
    """Create a test DataFrame"""
    spark = spark_session or globals().get('spark')
    if hasattr(spark, 'createDataFrame'):
        return spark.createDataFrame(data)
    else:
        mock_df = MagicMock()
        mock_df.count.return_value = len(data)
        mock_df.collect.return_value = data
        return mock_df


# ==============================================================================
# pytest Integration
# ==============================================================================

class TestCollectorPlugin:
    """Custom pytest plugin to collect test classes from memory"""
    
    def __init__(self, test_classes, test_config=None):
        self.test_classes = test_classes
        self.test_config = test_config or {}
        
    def pytest_configure(self, config):
        """Configure pytest with our test environment"""
        self.env = NotebookTestEnvironment()
        self.env.setup_test_environment(self.test_config)
        
    def pytest_unconfigure(self, config):
        """Cleanup after pytest finishes"""
        if hasattr(self, 'env'):
            self.env.cleanup()
    
    def pytest_collection_modifyitems(self, config, items):
        """Modify collected items to include our in-memory test classes"""
        original_items = items[:]
        items.clear()
        
        for test_class in self.test_classes:
            instance = test_class()
            test_methods = [method for method in dir(instance) if method.startswith('test_')]
            
            for method_name in test_methods:
                test_id = f"{test_class.__name__}::{method_name}"
                
                def make_test_func(cls, method):
                    def test_func():
                        test_instance = cls()
                        test_method = getattr(test_instance, method)
                        return pytest.main(['-s'], plugins=[])
                    return test_func
                
                items.append(pytest.Item.from_parent(
                    parent=None,
                    name=test_id,
                ))


class MemoryTestCollector:
    """Test collector that FORCES environment setup to override notebook interference"""
    
    def __init__(self, test_classes, test_config=None, env=None):
        self.test_classes = test_classes
        self.test_config = test_config or {}
        self.env = env

    def run_tests(self, env=None):
        """Run tests with FORCED environment setup"""
        use_real = self.test_config.get('use_real_spark', False)
      
        self.env = env or self.env or NotebookTestEnvironment()
        
        runner = NotebookTestRunner()
        runner.test_env = self.env
        runner._setup_result_capture()

        passed = 0
        failed = 0
        failures = []
        all_tests = []
        
        spark_type = str(type(globals().get('spark')))

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
                    test_instance = test_class()
                    test_method = getattr(test_instance, method_name)
                    
                    current_spark_type = str(type(globals().get('spark')))

                    if current_spark_type != spark_type:
                        print(f"🚨 WARNING: Spark type changed before {test_name}: {current_spark_type}")
                    
                    try:
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
                                setup_method = getattr(test_instance, param_name)
                                
                                setup_sig = inspect.signature(setup_method)
                                setup_kwargs = {}
                                
                                for setup_param in setup_sig.parameters:
                                    if setup_param == 'self':
                                        continue
                                    elif setup_param == 'notebook_runner':
                                        setup_kwargs[setup_param] = runner
                                    elif setup_param == 'basic_test_config':
                                        setup_kwargs[setup_param] = self.test_config
                                
                                kwargs[param_name] = setup_method(**setup_kwargs)
                        
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
            if self.env:
                self.env.cleanup()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
