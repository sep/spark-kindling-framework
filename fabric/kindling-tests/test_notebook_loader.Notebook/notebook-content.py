# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

%run notebook_framework

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pytest
from unittest.mock import patch, MagicMock, call
from typing import Dict, Any


class TestNotebookLoader(SynapseNotebookTestCase):
    
    def test_loader_can_be_instantiated(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        # Get the class from globals
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        # Create mock dependencies
        mock_platform_provider = MagicMock()
        mock_logger_provider = MagicMock()
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger
        
        mock_env_service = MagicMock()
        mock_platform_provider.get_service.return_value = mock_env_service
        
        # Create instance
        loader = NotebookLoader(mock_platform_provider, mock_logger_provider)
        
        assert loader.es == mock_env_service
        assert loader.logger == mock_logger
        assert loader._loaded_modules == {}
        assert loader._notebook_cache is None
        assert loader._folder_cache is None
    
    def test_get_all_notebooks_caches_results(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        mock_env_service = MagicMock()
        mock_notebooks = [MagicMock(name=f"notebook_{i}") for i in range(3)]
        mock_env_service.get_notebooks.return_value = mock_notebooks
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.es = mock_env_service
        loader.logger = MagicMock()
        loader._notebook_cache = None
        
        # First call should fetch from service
        result1 = loader.get_all_notebooks()
        assert result1 == mock_notebooks
        mock_env_service.get_notebooks.assert_called_once()
        
        # Second call should use cache
        result2 = loader.get_all_notebooks()
        assert result2 == mock_notebooks
        mock_env_service.get_notebooks.assert_called_once()  # Still only called once
    
    def test_get_all_notebooks_force_refresh(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        mock_env_service = MagicMock()
        mock_notebooks = [MagicMock(name=f"notebook_{i}") for i in range(3)]
        mock_env_service.get_notebooks.return_value = mock_notebooks
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.es = mock_env_service
        loader.logger = MagicMock()
        loader._notebook_cache = None
        
        # First call
        loader.get_all_notebooks()
        
        # Force refresh should call service again
        loader.get_all_notebooks(force_refresh=True)
        assert mock_env_service.get_notebooks.call_count == 2
    
    def test_get_all_packages_filters_ignored_folders(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.logger = MagicMock()
        
        # Mock the helper methods
        loader.get_all_folders = MagicMock(return_value={'pkg1', 'pkg2', 'tests', 'ignored'})
        loader._folder_has_notebooks = MagicMock(return_value=True)
        loader.get_notebooks_for_folder = MagicMock(return_value=[
            MagicMock(name='pkg1_init'),
            MagicMock(name='module1')
        ])
        loader.get_package_dependencies = MagicMock(return_value=[])
        loader._resolve_dependency_order = MagicMock(side_effect=lambda x: list(x.keys()))
        
        result = loader.get_all_packages(ignored_folders=['tests', 'ignored'])
        
        # Should only check non-ignored folders
        assert 'tests' not in result
        assert 'ignored' not in result
    
    def test_import_notebooks_detects_existing_package(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.logger = MagicMock()
        
        # Mock _prepare_package_reload
        loader._prepare_package_reload = MagicMock()
        
        # Mock load_notebook_code and other dependencies
        loader.load_notebook_code = MagicMock(return_value=("# code", []))
        loader.get_notebooks_for_folder = MagicMock(return_value=[])
        loader._resolve_dependency_order = MagicMock(return_value=['test_nb'])
        loader.import_notebook_as_module = MagicMock()
        loader._install_package_dependencies = MagicMock()
        
        # Simulate package already in sys.modules
        import sys
        sys.modules['test_package'] = MagicMock()
        
        try:
            loader.import_notebooks_into_module('test_package', ['test_nb'])
            
            # Should have called prepare_package_reload
            loader._prepare_package_reload.assert_called_once_with('test_package')
            loader.logger.info.assert_any_call("Package test_package already loaded - preparing reload")
        finally:
            # Cleanup
            if 'test_package' in sys.modules:
                del sys.modules['test_package']
    
    def test_is_class_from_package_filters_correctly(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        
        # Create test classes
        class PackageClass:
            pass
        PackageClass.__module__ = 'my_package.module'
        
        class FrameworkClass:
            pass
        FrameworkClass.__module__ = 'kindling.injection'
        
        class TopLevelClass:
            pass
        TopLevelClass.__module__ = 'my_package'
        
        # Test filtering
        assert loader._is_class_from_package(PackageClass, 'my_package') is True
        assert loader._is_class_from_package(TopLevelClass, 'my_package') is True
        assert loader._is_class_from_package(FrameworkClass, 'my_package') is False
    
    def test_unbind_class_and_interfaces_clears_injector(self, notebook_runner, basic_test_config):
            notebook_runner.prepare_test_environment(basic_test_config)
            
            NotebookLoader = globals().get('NotebookLoader')
            if not NotebookLoader:
                pytest.skip("NotebookLoader not available")
            
            loader = NotebookLoader.__new__(NotebookLoader)
            loader.logger = MagicMock()
            
            # Create mock injector
            mock_injector = MagicMock()
            mock_binder = MagicMock()
            mock_binder._bindings = {}
            mock_injector.binder = mock_binder
            
            mock_scope = MagicMock()
            mock_scope._cache = {}
            mock_injector._stack = [mock_scope]
            
            # Create test class with interface
            from abc import ABC, abstractmethod
            
            class TestInterface(ABC):
                @abstractmethod
                def test_method(self):
                    pass
            
            class TestClass(TestInterface):
                def test_method(self):
                    pass
            
            # Add to mock bindings before unbinding
            mock_binder._bindings[TestClass] = 'binding'
            mock_binder._bindings[TestInterface] = 'interface_binding'
            mock_scope._cache[TestClass] = 'instance'
            mock_scope._cache[TestInterface] = 'interface_instance'
            
            # Mock GlobalInjector.get_injector
            original_global_injector = globals().get('GlobalInjector')
            
            try:
                # Create a mock GlobalInjector
                class MockGlobalInjector:
                    @classmethod
                    def get_injector(cls):
                        return mock_injector
                
                globals()['GlobalInjector'] = MockGlobalInjector
                
                loader._unbind_class_and_interfaces(TestClass)
                
                # Should have cleared both class and interface bindings
                assert TestClass not in mock_binder._bindings, f"TestClass should be unbound, bindings: {mock_binder._bindings.keys()}"
                assert TestInterface not in mock_binder._bindings, f"TestInterface should be unbound, bindings: {mock_binder._bindings.keys()}"
                assert TestClass not in mock_scope._cache, f"TestClass should be cleared from cache, cache: {mock_scope._cache.keys()}"
                assert TestInterface not in mock_scope._cache, f"TestInterface should be cleared from cache, cache: {mock_scope._cache.keys()}"
            finally:
                # Restore original GlobalInjector
                if original_global_injector:
                    globals()['GlobalInjector'] = original_global_injector     
    
    def test_load_notebook_code_extracts_cells(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.logger = MagicMock()
        
        # Create mock notebook with cells
        mock_cell1 = MagicMock()
        mock_cell1.cell_type = 'code'
        mock_cell1.source = ['print("hello")', '\n', 'x = 1']
        
        mock_cell2 = MagicMock()
        mock_cell2.cell_type = 'code'
        mock_cell2.source = 'print("world")'
        
        mock_notebook = MagicMock()
        mock_notebook.properties.cells = [mock_cell1, mock_cell2]
        
        mock_env_service = MagicMock()
        mock_env_service.get_notebook.return_value = mock_notebook
        loader.es = mock_env_service
        
        loader._process_code_blocks = MagicMock(return_value=(['code1', 'code2'], []))
        
        code, imports = loader.load_notebook_code('test_notebook')
        
        assert '# ---- Next Cell ----' in code
        loader._process_code_blocks.assert_called_once()

    def test_process_code_blocks_replaces_notebook_imports(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.logger = MagicMock()
        
        # Format that matches the regex: notebook_import('package.module')
        # The regex is: r"notebook_import\(['\"]([\w_]*)\.([\w_]+)['\"]\)"
        code_blocks = [
            "notebook_import('mypackage.mymodule')",
            'x = 1',
            "notebook_import('other.module')"
        ]
        
        processed, imports = loader._process_code_blocks(code_blocks, suppress_nbimports=False)
        
        # Should have found imports (second capture group from regex)
        assert 'mymodule' in imports, f"Expected 'mymodule' in imports, got {imports}"
        assert 'module' in imports, f"Expected 'module' in imports, got {imports}"
        
        # Should have replaced notebook_import calls
        assert 'from mypackage.mymodule import *' in processed[0], \
            f"Expected 'from mypackage.mymodule import *' in {processed[0]}"
        assert 'x = 1' in processed[1], f"Expected 'x = 1' unchanged in {processed[1]}"
        assert 'from other.module import *' in processed[2], \
            f"Expected 'from other.module import *' in {processed[2]}"

    def test_resolve_dependency_order_handles_dependencies(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.logger = MagicMock()
        
        # Create dependency graph: A depends on B, B depends on C
        dependencies = {
            'module_a': ['module_b'],
            'module_b': ['module_c'],
            'module_c': []
        }
        
        result = loader._resolve_dependency_order(dependencies)
        
        # C should come before B, B before A
        assert result.index('module_c') < result.index('module_b')
        assert result.index('module_b') < result.index('module_a')
    
    def test_install_package_dependencies_calls_pip(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.logger = MagicMock()
        
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            
            result = loader._install_package_dependencies('test_pkg', ['pandas', 'numpy'])
            
            assert result is True
            mock_run.assert_called_once()
            call_args = mock_run.call_args[0][0]
            assert 'pandas' in call_args
            assert 'numpy' in call_args
    
    def test_notebook_import_skips_in_interactive_mode(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.logger = MagicMock()
        
        with patch('__main__.is_interactive_session', return_value=True):
            result = loader.notebook_import('.mypackage.mymodule')
            
            assert result is None
    
    def test_get_package_structure_organizes_notebooks(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        
        # Create mock notebooks
        mock_nb1 = MagicMock(name='pkg/module1')
        mock_nb2 = MagicMock(name='pkg/subfolder/module2')
        mock_nb3 = MagicMock(name='pkg/module3')
        
        loader.get_notebooks_for_package = MagicMock(return_value=[mock_nb1, mock_nb2, mock_nb3])
        loader._get_relative_path = MagicMock(side_effect=['module1', 'subfolder/module2', 'module3'])
        
        result = loader.get_package_structure('pkg')
        
        assert result['name'] == 'pkg'
        assert len(result['notebooks']) == 2  # module1 and module3
        assert 'subfolder' in result['subfolders']
    
    def test_refresh_cache_clears_caches(self, notebook_runner, basic_test_config):
        notebook_runner.prepare_test_environment(basic_test_config)
        
        NotebookLoader = globals().get('NotebookLoader')
        if not NotebookLoader:
            pytest.skip("NotebookLoader not available")
        
        loader = NotebookLoader.__new__(NotebookLoader)
        loader.logger = MagicMock()
        loader._notebook_cache = ['cached']
        loader._folder_cache = {'cached'}
        
        loader.refresh_cache()
        
        assert loader._notebook_cache is None
        assert loader._folder_cache is None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = run_notebook_tests(TestNotebookLoader)
print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
