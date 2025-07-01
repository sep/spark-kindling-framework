# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from typing import Dict, List, Optional, Union, Any, Set
from dataclasses import dataclass, field
import enum
import types
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass

from typing import Dict, List, Optional, Any, Union
import sys
import types
import re
import json
from pathlib import Path

level_hierarchy = { 
    "ALL": 0,
    "DEBUG": 1,
    "INFO": 2,
    "WARN": 3,
    "WARNING": 3,  # Alias for WARN
    "ERROR": 4,
    "FATAL": 5,
    "OFF": 6
}

currentLevel = "INFO"

globals()["kindling_environment_factories"] = {}

def get_or_create_spark_session():
    import sys
  
    # Check if spark variable exists in globals
    if 'spark' not in globals():
        # Not in managed environment, need to create our own session
        try:
            from pyspark.sql import SparkSession
            
            # Create and configure a new SparkSession
            print("Creating new SparkSession...")
            spark_session = SparkSession.builder \
                .appName("KindlingSession") \
                .config("spark.driver.memory", "2g") \
                .getOrCreate()
            
            # Add to globals so code can access it as 'spark'
            globals()['spark'] = spark_session
            #logger.debug("SparkSession created and assigned to global 'spark' variable")
            
        except ImportError:
            #logger.error("PySpark not installed. Please install with: pip install pyspark")
            sys.exit(1)
    else:
        pass

    return globals()['spark']

def get_spark_log_level():
    return get_or_create_spark_session().sparkContext._jvm.org.apache.log4j.LogManager.getRootLogger().getLevel()


def create_console_logger(config):
    """Create a simple console logger for bootstrap process that respects Spark log level"""
    from pyspark.sql import SparkSession

    currentLevel = config.get("log_level", None) or get_spark_log_level()
    
    def should_log(level):
        level_log_rank = level_hierarchy.get(level.upper(), 2)
        current_log_rank = level_hierarchy.get(currentLevel, 2)
        return level_log_rank >= current_log_rank
    
    #print(f"Current log level = {currentLevel}")
 
    return type('ConsoleLogger', (), {
        'debug': lambda self, *args, **kwargs: print("DEBUG:", *args) if should_log("DEBUG") else None,
        'info': lambda self, *args, **kwargs: print("INFO:", *args) if should_log("INFO") else None,
        'error': lambda self, *args, **kwargs: print("ERROR:", *args) if should_log("ERROR") else None,
        'warning': lambda self, *args, **kwargs: print("WARNING:", *args) if should_log("WARNING") else None,
    })()

class EnvironmentService:
    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def get_token(self, audience: str) -> str:
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass
    
    @abstractmethod
    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        pass
    
    @abstractmethod
    def read(self, path: str, encoding: str = 'utf-8') -> Union[str, bytes]:
        pass
    
    @abstractmethod
    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        pass
    
    @abstractmethod
    def list(self, path: str) -> List[str]:
        pass

    @abstractmethod
    def get_notebooks(self) -> List:
        pass
    
    @abstractmethod
    def get_notebook(self, name: str):
        pass
    
    @abstractmethod
    def create_notebook(self, name: str, notebook):
        pass
    
    @abstractmethod
    def update_notebook(self, name: str, notebook) :
        pass
    
    @abstractmethod
    def delete_notebook(self, name: str) -> None:
        pass

    @abstractmethod
    def get_spark_session(self):
        pass
    
    @abstractmethod
    def get_config(self, key: str, default: Any = None) -> Any:
        pass
    
    @abstractmethod
    def set_config(self, key: str, value: Any) -> None:
        pass
    
    @abstractmethod
    def get_log_level(self) -> str:
        pass

    @abstractmethod
    def is_interactive_session(self) -> bool:
        pass
    
    @abstractmethod
    def get_workspace_info(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def get_cluster_info(self) -> Dict[str, Any]:
        pass

def is_interactive_session(force_interactive: Optional[bool] = None) -> bool:
    if force_interactive is not None:
        return force_interactive
    
    try:
        if 'get_ipython' not in globals():
            try:
                from IPython import get_ipython
            except ImportError:
                return False
        else:
            get_ipython = globals()['get_ipython']
            
        ipython = get_ipython()
        if ipython is None:
            return False
            
        try:
            connection_file = ipython.config.get('IPKernelApp', {}).get('connection_file')
            if connection_file:
                return True
        except Exception:
            pass
            
        return False
    except Exception:
        return False


def safe_get_global(var_name: str, default: Any = None) -> Any:
    try:
        import __main__
        return getattr(__main__, var_name, default)
    except Exception:
        return default

class BootstrapPhase(enum.Enum):
    INITIAL = "initial"
    DISCOVERY = "discovery"
    BACKEND_INIT = "backend_init"
    PACKAGE_DISCOVERY = "package_discovery"
    PACKAGE_LOADING = "package_loading"
    FINALIZATION = "finalization"
    COMPLETE = "complete"
    ERROR = "error"

@dataclass
class FrameworkState:
    current_phase: BootstrapPhase = BootstrapPhase.INITIAL
    platform_name: str = "unknown"
    backend_name: str = "unknown"
    is_interactive: bool = False
    workspace_info: Dict[str, Any] = field(default_factory=dict)
    cluster_info: Dict[str, Any] = field(default_factory=dict)
    
    discovered_packages: List[str] = field(default_factory=list)
    loaded_packages: Dict[str, types.ModuleType] = field(default_factory=dict)
    failed_packages: List[str] = field(default_factory=list)
    
    discovered_folders: Set[str] = field(default_factory=set)
    ignored_folders: List[str] = field(default_factory=list)
    
    environment_valid: bool = False
    backend_initialized: bool = False
    packages_loaded: bool = False
    
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def add_error(self, error: str):
        self.errors.append(error)
        self.current_phase = BootstrapPhase.ERROR
    
    def add_warning(self, warning: str):
        self.warnings.append(warning)
    
    def advance_phase(self, new_phase: BootstrapPhase):
        self.current_phase = new_phase
    
    def is_complete(self) -> bool:
        return self.current_phase == BootstrapPhase.COMPLETE
    
    def has_errors(self) -> bool:
        return len(self.errors) > 0 or self.current_phase == BootstrapPhase.ERROR
    
    def get_summary(self) -> Dict[str, Any]:
        return {
            'phase': self.current_phase.value,
            'platform': self.platform_name,
            'backend': self.backend_name,
            'interactive': self.is_interactive,
            'packages_loaded': len(self.loaded_packages),
            'packages_failed': len(self.failed_packages),
            'errors': len(self.errors),
            'warnings': len(self.warnings),
            'complete': self.is_complete()
        }

class NotebookMetadata:
    def __init__(self, **kwargs):
        self.kernelspec = kwargs.get('kernelspec')
        self.language_info = kwargs.get('language_info')
        self.additional_properties = kwargs.get('additional_properties', {})


class NotebookCellMetadata:
    def __init__(self, **kwargs):
        self.additional_properties = kwargs.get('additional_properties', {})


class NotebookCell:
    def __init__(self, **kwargs):
        self.cell_type = kwargs.get('cell_type', 'code')
        self.source = kwargs.get('source', [])
        self.metadata = NotebookCellMetadata(**kwargs.get('metadata', {}))
        self.outputs = kwargs.get('outputs', [])
        self.execution_count = kwargs.get('execution_count')
        self.additional_properties = kwargs.get('additional_properties', {})

    def get_source_as_string(self) -> str:
        if isinstance(self.source, list):
            return ''.join(str(line) for line in self.source)
        return str(self.source)


class BigDataPoolReference:
    def __init__(self, **kwargs):
        self.type = kwargs.get('type', 'BigDataPoolReference')
        self.reference_name = kwargs.get('reference_name')


class NotebookSessionProperties:
    def __init__(self, **kwargs):
        self.driver_memory = kwargs.get('driver_memory')
        self.driver_cores = kwargs.get('driver_cores')
        self.executor_memory = kwargs.get('executor_memory')
        self.executor_cores = kwargs.get('executor_cores')
        self.num_executors = kwargs.get('num_executors')


class NotebookFolder:
    def __init__(self, **kwargs):
        self.name = kwargs.get('name', '')
        self.path = kwargs.get('path', '')


class Notebook:
    def __init__(self, **kwargs):
        self.description = kwargs.get('description')
        self.big_data_pool = kwargs.get('big_data_pool')
        if self.big_data_pool and isinstance(self.big_data_pool, dict):
            self.big_data_pool = BigDataPoolReference(**self.big_data_pool)
        
        self.session_properties = kwargs.get('session_properties')
        if self.session_properties and isinstance(self.session_properties, dict):
            self.session_properties = NotebookSessionProperties(**self.session_properties)
        
        self.metadata = NotebookMetadata(**kwargs.get('metadata', {}))
        self.nbformat = kwargs.get('nbformat', 4)
        self.nbformat_minor = kwargs.get('nbformat_minor', 2)
        
        folder_data = kwargs.get('folder', {})
        if isinstance(folder_data, dict):
            self.folder = NotebookFolder(**folder_data)
        else:
            self.folder = NotebookFolder(name='', path='')
        
        cells_data = kwargs.get('cells', [])
        self.cells = []
        for cell_data in cells_data:
            if isinstance(cell_data, dict):
                self.cells.append(NotebookCell(**cell_data))
            else:
                self.cells.append(cell_data)
        
        self.additional_properties = kwargs.get('additional_properties', {})

    def get_code_cells(self) -> List[NotebookCell]:
        return [cell for cell in self.cells if cell.cell_type == 'code']


class NotebookResource:
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.name = kwargs.get('name')
        self.type = kwargs.get('type', 'Microsoft.Synapse/workspaces/notebooks')
        self.etag = kwargs.get('etag')
        
        properties_data = kwargs.get('properties', {})
        if isinstance(properties_data, dict):
            self.properties = Notebook(**properties_data)
        else:
            self.properties = properties_data
        
        self.additional_properties = kwargs.get('additional_properties', {})


class ArtifactRenameRequest:
    def __init__(self, new_name: str):
        self.new_name = new_name

import re
import sys
import types
import os
import subprocess
import uuid
import shutil
from typing import List, Tuple, Dict, Any, Optional
from importlib.machinery import ModuleSpec

class NotebookLoader:
    def __init__(self, backend: EnvironmentService, config, logger = None ):
        self.backend = backend
        self.es = backend
        self.logger = logger or create_console_logger(config)
        self._loaded_modules: Dict[str, types.ModuleType] = {}
        self._notebook_cache = None
        self._folder_cache = None

    def get_all_notebooks(self, force_refresh: bool = False) -> List[NotebookResource]:
        """Get all notebooks from the backend"""
        if self._notebook_cache is None or force_refresh:
            try:
                self._notebook_cache = self.es.get_notebooks()
                self.logger.debug(f"Discovered {len(self._notebook_cache)} notebooks")
            except Exception as e:
                self.logger.error(f"Failed to discover notebooks: {str(e)}")
                raise e
        
        return self._notebook_cache
    
    def get_all_folders(self, force_refresh: bool = False) -> Set[str]:
        """Get all unique folder paths containing notebooks"""
        if self._folder_cache is None or force_refresh:
            notebooks = self.get_all_notebooks(force_refresh)
            folders = set()
            
            for notebook in notebooks:
                if notebook.name:
                    # Extract folder path from notebook name
                    folder_path = self._extract_folder_path(notebook)
                    if folder_path:
                        folders.add(folder_path)
                        # Also add parent folders
                        folders.update(self._get_parent_folders(folder_path))
            
            self._folder_cache = folders
            self.logger.debug(f"Discovered {len(folders)} folders")
        
        return self._folder_cache
    
    def get_all_packages(self, ignored_folders: Optional[List[str]] = None) -> List[str]:
        self.logger.debug(f"Getting all packages ...")

        if ignored_folders is None:
            ignored_folders = []
        
        folders = self.get_all_folders()
        packages = set()
        
        self.logger.debug(f"Searching folders {folders} for packages ...")        
        for folder in folders:
            # Get top-level folder name
            self.logger.debug(f"Searching folders {folder} for packages ...")   
            if folder not in ignored_folders:
                # Check if this folder actually contains notebooks
                if self._folder_has_notebooks(folder):
                    nbs_in_folder = self.get_notebooks_for_folder(folder)
                    nbs = [nb.name for nb in nbs_in_folder]
                    self.logger.debug(f"nbs in {folder} = {nbs}")

                    init_nb_name = f"{folder.split('/')[-1]}_init"

                    if init_nb_name in nbs:
                        packages.add(folder)
                    else:
                        self.logger.debug(f"{init_nb_name} not in folder {folder}")
                else:
                    self.logger.debug(f"Folder ({folder}) has notebooks returned false")
            else:
                self.logger.debug(f"{folder} in ignored folders ...")  

        package_list = sorted(list(packages))
        self.logger.debug(f"Discovered {len(package_list)} packages: {package_list}")
        return package_list
    
    def get_notebooks_for_folder(self, folder_path: str) -> List[NotebookResource]:
        """Get all notebooks in a specific folder (including subfolders)"""
        notebooks = self.get_all_notebooks()
        folder_notebooks = []
        
        for notebook in notebooks:
            if notebook.name:
                notebook_folder = self._extract_folder_path(notebook)
                # Check if notebook is in this folder or its subfolders
                if notebook_folder and (notebook_folder == folder_path or notebook_folder.startswith(folder_path + "/")):
                    folder_notebooks.append(notebook)
        
        self.logger.debug(f"Found {len(folder_notebooks)} notebooks in folder '{folder_path}'")
        return folder_notebooks
    
    def get_notebooks_for_package(self, package_name: str, include_subfolders: bool = True) -> List[NotebookResource]:
        """Get all notebooks for a specific package"""
        return self.get_notebooks_for_folder(package_name) if include_subfolders else self._get_direct_notebooks_for_folder(package_name)
    
    def get_package_structure(self, package_name: str) -> Dict[str, Any]:
        """Get the complete structure of a package"""
        notebooks = self.get_notebooks_for_package(package_name)
        structure = {
            'name': package_name,
            'notebooks': [],
            'subfolders': {}
        }
        
        for notebook in notebooks:
            if notebook.name:
                relative_path = self._get_relative_path(notebook.name, package_name)
                if '/' in relative_path:
                    # Notebook is in a subfolder
                    subfolder = relative_path.split('/')[0]
                    if subfolder not in structure['subfolders']:
                        structure['subfolders'][subfolder] = []
                    structure['subfolders'][subfolder].append(notebook)
                else:
                    # Notebook is directly in package folder
                    structure['notebooks'].append(notebook)
        
        return structure
    
    def find_notebook_by_name(self, notebook_name: str, package_name: Optional[str] = None) -> Optional[NotebookResource]:
        """Find a specific notebook by name, optionally within a package"""
        notebooks = self.get_notebooks_for_package(package_name) if package_name else self.get_all_notebooks()
        
        # Try exact match first
        for notebook in notebooks:
            if notebook.name == notebook_name:
                return notebook
        
        # Try name without extension
        base_name = notebook_name.replace('.ipynb', '').replace('.py', '')
        for notebook in notebooks:
            if notebook.name:
                notebook_base = notebook.name.replace('.ipynb', '').replace('.py', '')
                if notebook_base == base_name or notebook_base.endswith('/' + base_name):
                    return notebook
        
        return None
    
    def get_package_dependencies(self, package_name: str) -> List[str]:
        """Analyze package dependencies (basic implementation)"""
        notebooks = self.get_notebooks_for_package(package_name)
        dependencies = set()
        
        for notebook in notebooks:
            try:
                # This would need actual notebook content analysis
                # For now, return empty dependencies
                pass
            except Exception as e:
                self.logger.warning(f"Failed to analyze dependencies for {notebook.name}: {str(e)}")
        
        return sorted(list(dependencies))
    
    def _extract_folder_path(self, notebook) -> Optional[str]:
        return notebook.properties.folder.path
    
    def _get_parent_folders(self, folder_path: str) -> List[str]:
        """Get all parent folder paths"""
        parents = []
        parts = folder_path.split('/')
        for i in range(1, len(parts)):
            parent = '/'.join(parts[:i])
            if parent:
                parents.append(parent)
        return parents
    
    def _get_top_level_folder(self, folder_path: str) -> Optional[str]:
        """Get the top-level folder name"""
        if '/' in folder_path:
            return folder_path.split('/')[0]
        return folder_path if folder_path else None
    
    def _folder_has_notebooks(self, folder_path: str) -> bool:
        """Check if a folder contains any notebooks"""
        notebooks = self.get_all_notebooks()
        for notebook in notebooks:
            if notebook.name:
                notebook_folder = self._extract_folder_path(notebook)
                if notebook_folder and (notebook_folder == folder_path or notebook_folder.startswith(folder_path + "/")):
                    return True
        return False
    
    def _get_direct_notebooks_for_folder(self, folder_path: str) -> List[NotebookResource]:
        """Get notebooks directly in a folder (not in subfolders)"""
        notebooks = self.get_all_notebooks()
        direct_notebooks = []
        
        for notebook in notebooks:
            if notebook.name:
                notebook_folder = self._extract_folder_path(notebook)
                if notebook_folder == folder_path:
                    direct_notebooks.append(notebook)
        
        return direct_notebooks
    
    def _get_relative_path(self, notebook_name: str, package_name: str) -> str:
        """Get relative path of notebook within package"""
        if notebook_name.startswith(package_name + '/'):
            return notebook_name[len(package_name) + 1:]
        return notebook_name
    
    def refresh_cache(self):
        """Force refresh of all caches"""
        self._notebook_cache = None
        self._folder_cache = None
        self.logger.info("Discovery cache refreshed")

    def _execute_notebook_code( self, notebook_name ):
        code, _ = self.load_notebook_code(notebook_name)
        self.import_notebook_as_module(notebook_name, code)

    def load_notebook_code(self, notebook_name: str, suppress_nbimports = False) -> Tuple[str, List[str]]:
        try:
            self.logger.debug(f"Loading notebook code for: {notebook_name}")
            notebook = self.es.get_notebook(notebook_name)
            
            code_blocks = []
            for cell in notebook.properties.cells:
                if cell.cell_type == "code":
                    cell_code = self._extract_cell_code(cell)
                    if cell_code.strip():
                        code_blocks.append(cell_code)
            
            if len(code_blocks) > 0:
                self.logger.debug(f"Found code for: {notebook_name}")

            processed_blocks, imported_modules = self._process_code_blocks(code_blocks, suppress_nbimports)
            final_code = "\n\n# ---- Next Cell ----\n\n".join(processed_blocks)
            
            return final_code, imported_modules
        except Exception as e:
            self.logger.error(f"Failed to load notebook {notebook_name}: {str(e)}")
    
    def _extract_cell_code(self, cell) -> str:
        if isinstance(cell.source, list):
            cell_code = ""
            for i, line in enumerate(cell.source):
                line_str = str(line)
                if i < len(cell.source) - 1 and line_str.rstrip().endswith('\\'):
                    cell_code += line_str.rstrip()[:-1]
                else:
                    cell_code += line_str
                    if i < len(cell.source) - 1:
                        cell_code += "\n"
            return cell_code
        else:
            return str(cell.source)
    
    def _process_code_blocks(self, code_blocks: List[str], suppress_nbimports = False) -> Tuple[List[str], List[str]]:
        processed_blocks = []
        all_imported_modules = []
        
        notebook_import_pattern = r"notebook_import\(['\"]([\w_]*)\.([\w_]+)['\"]\)"
        
        for block in code_blocks:
            matches = re.findall(notebook_import_pattern, block)
            
            for package_name, module_name in matches:
                all_imported_modules.append(module_name)
            
            modified_block = re.sub(
                notebook_import_pattern, 
                self._create_import_replacement(),
                block
            ) if not suppress_nbimports else block
            
            processed_blocks.append(modified_block)
        
        return processed_blocks, all_imported_modules
    
    def _create_import_replacement(self):
        def replacement(match):
            package_name = match.group(1)
            module_name = match.group(2)
            importrepl = f"from {package_name}.{module_name} import *"
            self.logger.debug(f"import replacement = {importrepl}")
            return importrepl
        
        return replacement
    
    def import_notebook_as_module(self, notebook_name: str, code: str, 
                                pkg_name: Optional[str] = None, 
                                module_name: Optional[str] = None,
                                include_globals: Optional[Dict[str, Any]] = None) -> types.ModuleType:
        if include_globals is None:
            import __main__
            include_globals = __main__.__dict__
        
        effective_module_name = module_name or f"{pkg_name}.{notebook_name}" if pkg_name else notebook_name
        
        if effective_module_name in sys.modules:
            return sys.modules[effective_module_name]
        
        module = types.ModuleType(effective_module_name)
        module.__spec__ = ModuleSpec(
            name=effective_module_name,
            loader=None,
            origin='dynamically created from notebook'
        )
        
        sys.modules[effective_module_name] = module
        
        if pkg_name:
            package = self._setup_dynamic_package(pkg_name)
            module.__package__ = pkg_name
            setattr(package, notebook_name, module)
        
        module_dict = module.__dict__
        
        if include_globals:
            for key, value in include_globals.items():
                if key not in module_dict:
                    module_dict[key] = value
        
        try:
            compiled_code = compile(code, effective_module_name, 'exec')
            exec(compiled_code, module_dict)
            
            self._loaded_modules[effective_module_name] = module
            return module
        except Exception as e:
            if effective_module_name in sys.modules:
                del sys.modules[effective_module_name]
            self.logger.error(f"Failed to create module {effective_module_name}: {str(e)}")
    
    def _setup_dynamic_package(self, package_name: str) -> types.ModuleType:
        if package_name not in sys.modules:
            package_path = f'./{package_name}'
            package = types.ModuleType(package_name)
            package.__path__ = []
            package.__package__ = package_name
            package.__file__ = os.path.join(package_path, '__init__.py')
            
            package.__spec__ = ModuleSpec(
                name=package_name,
                loader=None,
                origin=package.__file__,
                is_package=True
            )
            
            sys.modules[package_name] = package
        else:
            package = sys.modules[package_name]
        
        return package
    
    def import_notebooks_into_module(self, package_name: str, notebook_names: List[str]) -> Dict[str, types.ModuleType]:
        notebook_code = {}
        notebook_dependencies = {}
        
        for nb_name in notebook_names:
            try:
                code, dependencies = self.load_notebook_code(nb_name)
                notebook_code[nb_name] = code
                notebook_dependencies[nb_name] = dependencies
            except Exception as e:
                self.logger.error(f"Exception loading notebook code for {nb_name}: {e}")
                continue
        
        load_order = self._resolve_notebook_dependencies(notebook_dependencies)
        
        self.logger.debug(f"Load order = {load_order}")

        imported_modules = {}
        for nb_name in load_order:
            if nb_name in notebook_code:
                try:
                    module = self.import_notebook_as_module(nb_name, notebook_code[nb_name], package_name)
                    imported_modules[nb_name] = module
                except Exception as e:
                    self.logger.error(f"Exception importing notebook as module for {nb_name}: {e}")
                    pass
        
        return imported_modules
    
    def _resolve_notebook_dependencies(self, dependencies: Dict[str, List[str]]) -> List[str]:      
        try:
            from graphlib import TopologicalSorter
            return list(TopologicalSorter(dependencies).static_order())   
        except Exception as e:
            self.logger.error(f"Dependency resolution error: {e}")
            return list(dependencies.keys())
    
    def notebook_import(self, notebook_ref: str) -> Optional[types.ModuleType]:
        
        is_interactive = is_interactive_session()
        
        if is_interactive:
            return None
        
        if '.' in notebook_ref:
            parts = notebook_ref.split('.')
            package_name = parts[0]
            notebook_name = parts[1]
        else:
            package_name = None
            notebook_name = notebook_ref
        
        if notebook_ref in sys.modules:
            module = sys.modules[notebook_ref]
            
            import __main__
            for name in dir(module):
                if not name.startswith('_'):
                    setattr(__main__, name, getattr(module, name))
            
            return module
        else:
            return None
    
    def _generate_python_dependency_block(self, package_definition):
        output = ""
        for dep in package_definition.dependencies:
            output += f'        "{dep}",\n'
        return output

    def publish_notebook_folder_as_package(self, folder_name, package_name, location, version = "0.1.0"):
        import os
        import uuid
        import shutil
        from setuptools import setup, find_packages

        # Create a temporary folder with a unique name
        temp_folder_path = f"/tmp/dist_{uuid.uuid4().hex}"
        os.makedirs(temp_folder_path, exist_ok=True)
        
        # Get all notebooks from the folder
        nbs = self.get_notebooks_for_folder(folder_name)

        # Filter modules and create package structure
        filtered_modules = [nb.name for nb in nbs if '_init' not in nb.name]
        import_lines = [f"from . import {nb_name}" for nb_name in filtered_modules]
        init_content = "\n".join(import_lines)
        if init_content:
            init_content += "\n"

        self._execute_notebook_code( f"{folder_name}_init" )
        regentry = NotebookPackages.get_package_definition(folder_name)
        dependency_block = self._generate_python_dependency_block(regentry)

        # Create pyproject.toml file in the temp_folder_path
        pyproject_content = f"""[build-system]
    requires = ["setuptools>=42", "wheel"]
    build-backend = "setuptools.build_meta"

    [project]
    name = "{package_name}"
    version = "{version}"
    description = "Package created from notebooks in {folder_name}"
    readme = "README.md"
    authors = [{{"name" = "Author"}}]
    license = {{"text" = "MIT"}}
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
    dependencies = [
{dependency_block}    ]
    requires-python = ">=3.7"
    """

        package_dir = os.path.join(temp_folder_path, package_name)
        os.makedirs(package_dir, exist_ok=True)
        
        # Write __init__.py file
        with open(f"{package_dir}/__init__.py", "w") as f:
            f.write(init_content)

        # Write each notebook as a Python module
        for nb_name in filtered_modules:
            code, _ = self.load_notebook_code(nb_name)
            with open(f"{package_dir}/{nb_name}.py", "w") as f:
                f.write(code)

        # Create a minimal README.md
        readme_content = f"""# {package_name}

    A package created from notebooks in {folder_name}.
    """
        
        # Write pyproject.toml to the root directory
        with open(os.path.join(temp_folder_path, "pyproject.toml"), "w") as f:
            f.write(pyproject_content)
            
        # Write README.md to the root directory
        with open(os.path.join(temp_folder_path, "README.md"), "w") as f:
            f.write(readme_content)
        
        # Build the wheel package
        current_dir = os.getcwd()
        try:
            # Change to the temporary directory
            os.chdir(temp_folder_path)
            
            # Build the wheel
            subprocess.check_call(["pip", "wheel", ".", "--no-deps", "-w", "dist/"])
            
            # Find the built wheel file
            wheel_files = os.listdir("dist/")
            wheel_file = wheel_files[0]
            wheel_path = os.path.join(temp_folder_path, "dist", wheel_file)
            
            # For local file system destination
            if location.startswith("/"):
                os.makedirs(os.path.dirname(location), exist_ok=True)
                shutil.copy2(wheel_path, location)
            else:
                local_path = f"file:///{wheel_path}"
                self.es.copy(local_path, f"{location}/{wheel_file}", True)
            
            print(f"Package {package_name} successfully built and saved to {location}")
            
        finally:
            os.chdir(current_dir)
            # Clean up temporary files
            shutil.rmtree(temp_folder_path)

import sys
import types
from dataclasses import dataclass, fields
from typing import Dict, List, Set, Optional, Any

@dataclass
class NotebookPackage:
    name: str
    dependencies: List[str]
    tags: Dict[str, str]
    description: Optional[str] = None
    version: Optional[str] = "0.1.0"
    author: Optional[str] = None


class NotebookPackages:
    registry: Dict[str, NotebookPackage] = {}
    
    @classmethod
    def register(cls, **decorator_params):
        required_fields = {field.name for field in fields(NotebookPackage)}
        provided_fields = set(decorator_params.keys())
        missing_fields = {'name', 'dependencies', 'tags'} - provided_fields
        
        if missing_fields:
            raise ValueError(f"Missing required fields in package decorator: {missing_fields}")
        
        package_name = decorator_params['name']
        package_params = {k: v for k, v in decorator_params.items() if k != 'name'}
        cls.register_package(package_name, **package_params)
        return None
    
    @classmethod
    def register_package(cls, name: str, **package_params):
        package_params.setdefault('description', f"Notebook package: {name}")
        package_params.setdefault('version', "0.1.0")
        package_params.setdefault('author', "Unknown")
        
        package = NotebookPackage(name=name, **package_params)
        cls.registry[name] = package
        cls._validate_dependencies(name, package.dependencies)
    
    @classmethod
    def _validate_dependencies(cls, package_name: str, dependencies: List[str]):
        if package_name in dependencies:
            raise DependencyError(f"Package {package_name} cannot depend on itself")
        
        for dep in dependencies:
            if dep in cls.registry:
                dep_package = cls.registry[dep]
                if package_name in dep_package.dependencies:
                    raise DependencyError(f"Circular dependency detected: {package_name} <-> {dep}")
    
    @classmethod
    def get_package_names(cls) -> List[str]:
        return list(cls.registry.keys())
    
    @classmethod
    def get_package_definition(cls, name: str) -> Optional[NotebookPackage]:
        return cls.registry.get(name)
    
    @classmethod
    def get_packages_by_tag(cls, tag_key: str, tag_value: str = None) -> List[NotebookPackage]:
        matching_packages = []
        
        for package in cls.registry.values():
            if tag_key in package.tags:
                if tag_value is None or package.tags[tag_key] == tag_value:
                    matching_packages.append(package)
        
        return matching_packages
    
    @classmethod
    def get_dependency_graph(cls) -> Dict[str, List[str]]:
        return {name: package.dependencies for name, package in cls.registry.items()}
    
    @classmethod
    def get_dependents(cls, package_name: str) -> List[str]:
        dependents = []
        
        for name, package in cls.registry.items():
            if package_name in package.dependencies:
                dependents.append(name)
        
        return dependents
    
    @classmethod
    def get_all_dependencies(cls, package_name: str, visited: Set[str] = None) -> Set[str]:
        if visited is None:
            visited = set()
        
        if package_name in visited:
            raise DependencyError(f"Circular dependency detected involving {package_name}")
        
        if package_name not in cls.registry:
            return set()
        
        visited.add(package_name)
        all_deps = set()
        
        package = cls.registry[package_name]
        for dep in package.dependencies:
            all_deps.add(dep)
            all_deps.update(cls.get_all_dependencies(dep, visited.copy()))
        
        return all_deps
    
    @classmethod
    def clear_registry(cls):
        cls.registry.clear()


def notebook_import(notebook_ref: str) -> Optional[types.ModuleType]:
    is_interactive = is_interactive_session()
    
    #print(f"is_interactive = {is_interactive}")

    if is_interactive:
        return None
    
    if '.' in notebook_ref:
        parts = notebook_ref.split('.')
        notebook_name = parts[1]
        package_name = parts[0]
        module_name = notebook_ref
    else:
        notebook_name = notebook_ref
        package_name = None
        module_name = notebook_ref
    
    if module_name in sys.modules:
        module = sys.modules[module_name]
        
        import __main__
        imported_count = 0
        
        for name in dir(module):
            if not name.startswith('_'):
                setattr(__main__, name, getattr(module, name))
                imported_count += 1
        
        return module
    else:
        return None

from typing import Dict, List, Set, Optional, Tuple

def install_bootstrap_dependencies(bootstrap_config):
    """Install packages needed for bootstrap process"""
    import sys
    import subprocess
    import importlib.util

    def is_package_installed(package_name):
        try:
            spec = importlib.util.find_spec(package_name)
            return spec is not None
        except ValueError as e:
            if ".__spec__ is None" in str(e):
                return False
            return False

    def load_if_needed(package_name):
        if not is_package_installed(package_name):
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
                logger.debug(f"Installed package: {package_name}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to install {package_name}: {e}")
                # Don't raise - let it continue
        else:
            logger.debug(f"Package already installed: {package_name}")

    for package in bootstrap_config.get('required_packages', []):
        load_if_needed(package)

from typing import Optional, Dict, Any
import types
class BootstrapStateMachine:
    def __init__(self, config, es, logger = None):
        self.es = es
        self.logger = logger or create_console_logger(config)
        self.nl = NotebookLoader(es,config,self.logger)
        self.framework = types.SimpleNamespace()
        self.framework.state = FrameworkState()
        self.config = types.SimpleNamespace(**config)

    def bootstrap(self):
        try:
            self._execute_discovery_phase()
            self._execute_backend_phase()
            self._execute_package_discovery_phase()
            self._execute_package_loading_phase()
            self._execute_finalization_phase()
            
            self.framework.state.advance_phase(BootstrapPhase.COMPLETE)
            self.logger.info("Framework bootstrap completed successfully")
            
        except Exception as e:
            self.framework.state.add_error(f"Bootstrap failed: {str(e)}")
            self.logger.error(f"Bootstrap failed: {str(e)}")
            raise Exception(f"Bootstrap failed: {str(e)}") from e
        
        return self.es
    


    def _execute_discovery_phase(self):
        self.logger.info("Starting discovery phase")
        self.framework.state.advance_phase(BootstrapPhase.DISCOVERY)
        
        try:
            platform_name = self._detect_platform()
            self.framework.state.platform_name = platform_name
            self.framework.state.backend_name = self.config.platform_environment or platform_name
            
            is_interactive = self._detect_interactive_mode()
            self.framework.state.is_interactive = is_interactive
            
            self.logger.info(f"Platform detected: {platform_name}")
            self.logger.info(f"Interactive mode: {is_interactive}")
            
        except Exception as e:
            raise Exception(f"Discovery phase failed: {str(e)}") from e
    
    def _execute_backend_phase(self):
        self.logger.info("Starting backend initialization phase")
        self.framework.state.advance_phase(BootstrapPhase.BACKEND_INIT)
        
        try:
            backend_config = types.SimpleNamespace(
                workspace_id=self.config.workspace_id,
                endpoint=self.config.workspace_endpoint,
                spark_configs=self.config.spark_configs
            )
            
            self.framework.backend = self.es
        
            self.framework.backend.initialize()
            self.framework.state.backend_initialized = True
            
            workspace_info = self.framework.backend.get_workspace_info()
            cluster_info = self.framework.backend.get_cluster_info()
            
            self.framework.state.workspace_info = workspace_info
            self.framework.state.cluster_info = cluster_info
            self.framework.state.environment_valid = True
            
            self.logger.info(f"Backend initialized: {self.framework.state.backend_name}")
            
        except Exception as e:
            raise Exception(f"Backend initialization failed: {str(e)}") from e
    
    def _execute_package_discovery_phase(self):
        self.logger.info("Starting package discovery phase")
        self.framework.state.advance_phase(BootstrapPhase.PACKAGE_DISCOVERY)
        
        try:
            self.framework.discovery = self.nl
            self.framework.loader = self.nl
            
            if self.config.load_local_packages:
                discovered_folders = self.framework.discovery.get_all_folders()
                self.framework.state.discovered_folders = discovered_folders
                
                discovered_packages = self.framework.discovery.get_all_packages(
                    ignored_folders=self.config.ignored_folders
                )
                self.framework.state.discovered_packages = discovered_packages
                self.framework.state.ignored_folders = self.config.ignored_folders
                
                self.logger.info(f"Discovered {len(discovered_packages)} packages")
                self.logger.debug(f"Packages: {discovered_packages}")
            
        except Exception as e:
            raise Exception(f"Package discovery failed: {str(e)}") from e
    
    def _execute_package_loading_phase(self):
        self.logger.info("Starting package loading phase")
        self.framework.state.advance_phase(BootstrapPhase.PACKAGE_LOADING)

        try:
            if self.config.load_local_packages and self.framework.state.discovered_packages:
                for package_name in self.framework.state.discovered_packages:
                    try:
                        notebooks = self.framework.discovery.get_notebooks_for_folder(package_name)
                        notebook_names = [nb.name for nb in notebooks if '_init' not in nb.name]
                        
                        if notebook_names:
                            loaded_modules = self.framework.loader.import_notebooks_into_module(
                                package_name, notebook_names
                            )
                            self.framework.state.loaded_packages.update(loaded_modules)
                            self.logger.info(f"Loaded package: {package_name} ({len(loaded_modules)} modules)")
                        
                    except Exception as e:
                        self.framework.state.failed_packages.append(package_name)
                        self.framework.state.add_warning(f"Failed to load package {package_name}: {str(e)}")
                        self.logger.warning(f"Failed to load package {package_name}: {str(e)}")
            
            if self.config.required_packages:
                for package_name in self.config.required_packages:
                    if package_name not in self.framework.state.loaded_packages:
                        self.framework.state.add_warning(f"Required package not loaded: {package_name}")
            
            self.framework.state.packages_loaded = True
            self.logger.info(f"Package loading completed: {len(self.framework.state.loaded_packages)} modules loaded")
            
        except Exception as e:
            raise Exception(f"Package loading failed: {str(e)}") from e
    
    def _execute_finalization_phase(self):
        self.logger.info("Starting finalization phase")
        self.framework.state.advance_phase(BootstrapPhase.FINALIZATION)
        
        try:
            import __main__
            
            if hasattr(__main__, 'framework'):
                self.logger.warning("Framework already exists in global scope, overwriting")
            
            __main__.framework = self.framework
            
            __main__.notebook_import = self.framework.loader.notebook_import
            
            self.logger.info("Framework finalization completed")
            
        except Exception as e:
            raise Exception(f"Finalization failed: {str(e)}") from e
    
    def _detect_platform(self) -> str:
        try:
            import __main__
            
            if hasattr(__main__, 'dbutils') and getattr(__main__, 'dbutils') is not None:
                return 'databricks'
            
            if hasattr(__main__, 'mssparkutils') and getattr(__main__, 'mssparkutils') is not None:
                spark = getattr(__main__, 'spark', None)
                if spark and 'synapse' in spark.sparkContext.appName.lower():
                    return 'synapse'
                return 'fabric'
            
            return 'unknown'
        except Exception:
            return 'unknown'
    
    def _detect_interactive_mode(self) -> bool:
        if self.config.is_interactive is not None:
            return self.config.is_interactive
        
        try:
            if 'get_ipython' not in globals():
                try:
                    from IPython import get_ipython
                except ImportError:
                    return False
            else:
                get_ipython = globals()['get_ipython']
                
            ipython = get_ipython()
            if ipython is None:
                return False
                
            connection_file = ipython.config.get('IPKernelApp', {}).get('connection_file')
            return connection_file is not None
        except Exception:
            return False

def bootstrap_framework(config, logger):
    import types
    objconfig = types.SimpleNamespace(**config)
    envfact = globals()["kindling_environment_factories"].get(objconfig.platform_environment, None)
    
    env = None
    if envfact:
        env = envfact(config, logger)

    globals()["platform_environment_service"] = env

    state_machine = BootstrapStateMachine(config, env)

    bootstrap = state_machine.bootstrap()
    
    globals()["is_interactive_session"] = lambda: objconfig.is_interactive if (objconfig.is_interactive is not None) else state_machine.es.is_interactive_session() 
    
    if is_interactive_session():
        globals()["get_all_notebooks_for_folder"] = state_machine.nl.get_notebooks_for_folder
        globals()["logger"] = create_console_logger(config)
        client = types.SimpleNamespace()
        client.notebook = state_machine.es
        globals()["get_synapse_client"] = lambda: client
        globals()["load_notebook_code"] = state_machine.nl.load_notebook_code

    return bootstrap

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
