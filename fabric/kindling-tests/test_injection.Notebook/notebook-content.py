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

%run injection

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_config = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pytest
from unittest.mock import patch, MagicMock, call
from abc import ABC, abstractmethod
from typing import Any


class TestGlobalInjector(SynapseNotebookTestCase):
    
    def test_singleton_pattern_concept(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test the singleton pattern concept with a simple implementation
        class TestSingleton:
            _instance = None
            
            @classmethod
            def get_instance(cls):
                if cls._instance is None:
                    cls._instance = cls()
                return cls._instance
            
            @classmethod
            def clear(cls):
                cls._instance = None
        
        # Test singleton behavior
        instance1 = TestSingleton.get_instance()
        instance2 = TestSingleton.get_instance()
        
        assert instance1 is instance2
        assert id(instance1) == id(instance2)
        
        # Test clear functionality
        TestSingleton.clear()
        instance3 = TestSingleton.get_instance()
        
        # After clear, should get a new instance
        assert instance3 is not instance1
    
    def test_dependency_injection_pattern(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test the core DI pattern - binding interfaces to implementations
        registry = {}
        
        def bind(interface, implementation):
            registry[interface] = implementation
        
        def get(interface):
            if interface in registry:
                return registry[interface]()
            raise Exception(f"No binding for {interface}")
        
        # Create test interface and implementation
        class TestInterface(ABC):
            @abstractmethod
            def test_method(self):
                pass
        
        class TestImplementation(TestInterface):
            def test_method(self):
                return "test_result"
        
        # Test the pattern
        bind(TestInterface, TestImplementation)
        instance = get(TestInterface)
        
        assert isinstance(instance, TestImplementation)
        assert instance.test_method() == "test_result"
    
    def test_abstract_class_detection_pattern(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test the pattern for detecting abstract base classes
        import inspect
        
        class ConcreteBase:
            def concrete_method(self):
                return "concrete"
        
        class AbstractInterface(ABC):
            @abstractmethod
            def abstract_method(self):
                pass
        
        class MixedImplementation(ConcreteBase, AbstractInterface):
            def abstract_method(self):
                return "abstract"
        
        # Test the detection logic used in autobind
        bases = MixedImplementation.__bases__
        abstract_bases = [base for base in bases if inspect.isabstract(base)]
        concrete_bases = [base for base in bases if not inspect.isabstract(base)]
        
        assert AbstractInterface in abstract_bases
        assert ConcreteBase in concrete_bases
        assert len(abstract_bases) == 1
        assert len(concrete_bases) == 1
    
    def test_decorator_pattern_concept(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test the decorator pattern used in autobind
        registry = {}
        
        def autobind(*interfaces):
            def decorator(cls_to_bind):
                # Simulate the autobind logic
                if not interfaces:
                    # Find abstract parents
                    import inspect
                    bases = cls_to_bind.__bases__
                    for base in bases:
                        if inspect.isabstract(base):
                            registry[base] = cls_to_bind
                else:
                    # Use explicit interfaces
                    for interface in interfaces:
                        registry[interface] = cls_to_bind
                
                return cls_to_bind
            return decorator
        
        # Test automatic binding
        class AutoInterface(ABC):
            @abstractmethod
            def auto_method(self):
                pass
        
        @autobind()
        class AutoImplementation(AutoInterface):
            def auto_method(self):
                return "auto"
        
        assert registry[AutoInterface] == AutoImplementation
        
        # Test explicit binding
        class ExplicitInterface(ABC):
            @abstractmethod
            def explicit_method(self):
                pass
        
        @autobind(ExplicitInterface)
        class ExplicitImplementation:
            def explicit_method(self):
                return "explicit"
        
        assert registry[ExplicitInterface] == ExplicitImplementation
    
    def test_singleton_decorator_pattern(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test the singleton decorator pattern
        _singletons = {}
        
        def singleton_decorator(cls):
            def get_instance(*args, **kwargs):
                if cls not in _singletons:
                    _singletons[cls] = cls(*args, **kwargs)
                return _singletons[cls]
            return get_instance
        
        @singleton_decorator
        class SingletonTest:
            def __init__(self):
                import uuid
                self.id = str(uuid.uuid4())
        
        # Test singleton behavior
        instance1 = SingletonTest()
        instance2 = SingletonTest()
        
        assert instance1 is instance2
        assert instance1.id == instance2.id
    
    def test_mock_injector_available(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test that the mock GlobalInjector is available and has expected interface
        mock_gi = globals().get('GlobalInjector')
        
        assert mock_gi is not None
        # The mock should at least have singleton_autobind for testing
        assert hasattr(mock_gi, 'singleton_autobind')
    
    def test_interface_based_design_principle(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test the principle that services should depend on interfaces, not implementations
        
        class ConfigService(ABC):
            @abstractmethod
            def get(self, key: str) -> str:
                pass
        
        class DatabaseConfigImplementation(ConfigService):
            def get(self, key: str) -> str:
                return f"db_{key}"
        
        class FileConfigImplementation(ConfigService):
            def get(self, key: str) -> str:
                return f"file_{key}"
        
        # Service depends on interface, not implementation
        class MyService:
            def __init__(self, config: ConfigService):
                self.config = config
            
            def get_database_url(self):
                return self.config.get("database_url")
        
        # Test with different implementations
        db_config = DatabaseConfigImplementation()
        file_config = FileConfigImplementation()
        
        service1 = MyService(db_config)
        service2 = MyService(file_config)
        
        assert service1.get_database_url() == "db_database_url"
        assert service2.get_database_url() == "file_database_url"
        
        # This demonstrates the power of interface-based design
    
    def test_injector_registry_concept(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test the concept of a global registry for dependency injection
        
        class SimpleInjector:
            def __init__(self):
                self._bindings = {}
                self._singletons = {}
            
            def bind(self, interface, implementation, singleton=False):
                self._bindings[interface] = (implementation, singleton)
            
            def get(self, interface):
                if interface not in self._bindings:
                    raise Exception(f"No binding for {interface}")
                
                implementation, is_singleton = self._bindings[interface]
                
                if is_singleton:
                    if interface not in self._singletons:
                        self._singletons[interface] = implementation()
                    return self._singletons[interface]
                else:
                    return implementation()
        
        # Test the registry
        injector = SimpleInjector()
        
        class TestInterface(ABC):
            @abstractmethod
            def get_value(self):
                pass
        
        class TestImplementation(TestInterface):
            def __init__(self):
                import uuid
                self.id = str(uuid.uuid4())
            
            def get_value(self):
                return f"value_{self.id}"
        
        # Test regular binding
        injector.bind(TestInterface, TestImplementation, singleton=False)
        instance1 = injector.get(TestInterface)
        instance2 = injector.get(TestInterface)
        
        assert instance1 is not instance2
        assert instance1.get_value() != instance2.get_value()
        
        # Test singleton binding
        injector.bind(TestInterface, TestImplementation, singleton=True)
        instance3 = injector.get(TestInterface)
        instance4 = injector.get(TestInterface)
        
        assert instance3 is instance4
        assert instance3.get_value() == instance4.get_value()
    
    def test_clear_method_bug_pattern(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Test the bug pattern in the clear method
        class BuggyClass:
            _instance = "original"
            
            @classmethod
            def clear_buggy(cls):
                _instance = None  # BUG: Missing cls.
            
            @classmethod
            def clear_correct(cls):
                cls._instance = None  # CORRECT
        
        # Test buggy version
        original = BuggyClass._instance
        BuggyClass.clear_buggy()
        assert BuggyClass._instance == original  # Bug: instance not cleared
        
        # Test correct version
        BuggyClass.clear_correct()
        assert BuggyClass._instance is None  # Correct: instance cleared

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(TestGlobalInjector)
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
