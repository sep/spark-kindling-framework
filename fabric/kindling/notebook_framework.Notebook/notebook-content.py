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

def is_interactive_session(force_interactive: Optional[bool] = None) -> bool:
    """Check if running in interactive session"""
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

def notebook_import(notebook_ref: str) -> Optional[types.ModuleType]:
    is_interactive = is_interactive_session()
    
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

notebook_import('.injection')
notebook_import('.spark_config')
notebook_import('.platform_provider')
notebook_import('.spark_log_provider')

level_hierarchy = { 
    "ALL": 0,
    "DEBUG": 1,
    "INFO": 2,
    "WARN": 3,
    "WARNING": 3,
    "ERROR": 4,
    "FATAL": 5,
    "OFF": 6
}

currentLevel = "INFO"

def get_or_create_spark_session():
    import sys
  
    if 'spark' not in globals():
        try:
            from pyspark.sql import SparkSession
            
            print("Creating new SparkSession...")
            spark_session = SparkSession.builder \
                .appName("KindlingSession") \
                .config("spark.driver.memory", "2g") \
                .getOrCreate()
            
            globals()['spark'] = spark_session
            
        except ImportError:
            sys.exit(1)
    else:
        pass

    return globals()['spark']

def get_spark_log_level():
    return get_or_create_spark_session().sparkContext._jvm.org.apache.log4j.LogManager.getRootLogger().getLevel()


def create_console_logger(config):
    from pyspark.sql import SparkSession

    currentLevel = config.get("log_level", None) or get_spark_log_level()
    
    def should_log(level):
        level_log_rank = level_hierarchy.get(level.upper(), 2)
        current_log_rank = level_hierarchy.get(currentLevel, 2)
        return level_log_rank >= current_log_rank
 
    return type('ConsoleLogger', (), {
        'debug': lambda self, *args, **kwargs: print("DEBUG:", *args) if should_log("DEBUG") else None,
        'info': lambda self, *args, **kwargs: print("INFO:", *args) if should_log("INFO") else None,
        'error': lambda self, *args, **kwargs: print("ERROR:", *args) if should_log("ERROR") else None,
        'warning': lambda self, *args, **kwargs: print("WARNING:", *args) if should_log("WARNING") else None,
    })()

class EnvironmentService:
    @abstractmethod
    def get_platform_name(self):
        pass

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
    def move(self, path: str, dest: str) -> None:
        pass    

    @abstractmethod
    def delete(self, path: str, recurse = False) -> None:
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

from typing import Dict, List, Optional, Any, Set, Tuple
from abc import ABC, abstractmethod
import types

class NotebookManager(ABC):
    """Abstract base class defining the interface for notebook management operations."""
    
    @abstractmethod
    def get_all_notebooks(self, force_refresh: bool = False) -> List:
        """Get all available notebooks, optionally forcing a cache refresh."""
        pass
    
    @abstractmethod
    def get_all_folders(self, force_refresh: bool = False) -> Set[str]:
        """Get all available folders, optionally forcing a cache refresh."""
        pass
    
    @abstractmethod
    def get_all_packages(self, ignored_folders: Optional[List[str]] = None) -> List[str]:
        """Get all packages, optionally ignoring specified folders."""
        pass
    
    @abstractmethod
    def get_notebooks_for_folder(self, folder_path: str) -> List:
        """Get all notebooks within a specific folder path."""
        pass
    
    @abstractmethod
    def get_notebooks_for_package(self, package_name: str, include_subfolders: bool = True) -> List:
        """Get all notebooks for a package, optionally including subfolders."""
        pass
    
    @abstractmethod
    def get_package_structure(self, package_name: str) -> Dict[str, Any]:
        """Get the structure of a package including notebooks and subfolders."""
        pass
    
    @abstractmethod
    def find_notebook_by_name(self, notebook_name: str, package_name: Optional[str] = None) -> Optional:
        """Find a notebook by name, optionally within a specific package."""
        pass
    
    @abstractmethod
    def get_package_dependencies(self, package_name: str) -> List[str]:
        """Get the dependencies for a specific package."""
        pass
    
    @abstractmethod
    def refresh_cache(self) -> None:
        """Refresh the internal cache of notebooks and folders."""
        pass
    
    @abstractmethod
    def load_notebook_code(self, notebook_name: str, suppress_nbimports: bool = False) -> Tuple[str, List[str]]:
        """Load the code from a notebook, returning the code and imported modules."""
        pass
    
    @abstractmethod
    def import_notebook_as_module(self, notebook_name: str, code: str, 
                                pkg_name: Optional[str] = None, 
                                module_name: Optional[str] = None,
                                include_globals: Optional[Dict[str, Any]] = None) -> types.ModuleType:
        """Import a notebook as a Python module."""
        pass
    
    @abstractmethod
    def import_notebooks_into_module(self, package_name: str, notebook_names: List[str]) -> Dict[str, types.ModuleType]:
        """Import multiple notebooks into modules within a package."""
        pass
    
    @abstractmethod
    def notebook_import(self, notebook_ref: str) -> Optional[types.ModuleType]:
        """Import a notebook by reference string."""
        pass
    
    @abstractmethod
    def publish_all_notebook_folders_as_packages(self, location: str, version: str = "0.1.0") -> None:
        """Publish all notebook folders as Python packages to a location."""
        pass
    
    @abstractmethod
    def publish_notebook_folder_as_package(self, folder_name: str, package_name: str, 
                                         location: str, version: str, 
                                         extra_dependencies: Optional[List[str]] = None) -> None:
        """Publish a specific notebook folder as a Python package."""
        pass

try:
    from kindling.injection import *
except (ImportError, ModuleNotFoundError):
    # injection module is not available, create a no-op version
    class _NoOpInjector:
        @staticmethod
        def singleton_autobind():
            """No-op decorator that returns the original function/class unchanged."""
            def decorator(func_or_class):
                return func_or_class
            return decorator
    
    GlobalInjector = _NoOpInjector()

    def inject(func):
        return func

try:
    from kindling.platform_provider import PlatformEnvironmentProvider
except (ImportError, ModuleNotFoundError):
    # kindling.platform_provider module is not available, define interface locally
    class PlatformEnvironmentProvider:
        def get_service(self):
            pass

try:
    from kindling.spark_log_provider import PythonLoggerProvider
except (ImportError, ModuleNotFoundError):
    # kindling.platform_provider module is not available, define interface locally
    class PythonLoggerProvider:
        def get_logger(self, name):
            pass

def create_notebook_loader(es, config):
    """Create a dynamic object that implements PlatformEnvironmentProvider interface."""
    
    class DynamicPlatformProvider:
        def get_service(self):
            return es

    class DynamicLoggerProvider:
        def get_logger(self, name):
            return create_console_logger(config)
    
    return NotebookLoader( DynamicPlatformProvider(), DynamicLoggerProvider() )

@GlobalInjector.singleton_autobind()
class NotebookLoader(NotebookManager):
    @inject
    def __init__(self, psp: PlatformEnvironmentProvider, plp: PythonLoggerProvider):
        self.es = psp.get_service()
        self.logger = plp.get_logger("NotebookLoader")
        self._loaded_modules: Dict[str, types.ModuleType] = {}
        self._notebook_cache = None
        self._folder_cache = None

    def get_all_notebooks(self, force_refresh: bool = False) -> List[NotebookResource]:
        if self._notebook_cache is None or force_refresh:
            try:
                self._notebook_cache = self.es.get_notebooks()
                self.logger.debug(f"Discovered {len(self._notebook_cache)} notebooks")
            except Exception as e:
                self.logger.error(f"Failed to discover notebooks: {str(e)}")
                raise e
        
        return self._notebook_cache
    
    def get_all_folders(self, force_refresh: bool = False) -> Set[str]:
        if self._folder_cache is None or force_refresh:
            notebooks = self.get_all_notebooks(force_refresh)
            folders = set()
            
            for notebook in notebooks:
                if notebook.name:
                    folder_path = self._extract_folder_path(notebook)
                    if folder_path:
                        folders.add(folder_path)
                        folders.update(self._get_parent_folders(folder_path))
            
            self._folder_cache = folders
            self.logger.debug(f"Discovered {len(folders)} folders")
        
        return self._folder_cache
    
    def get_all_packages(self, ignored_folders: Optional[List[str]] = None) -> List[str]:
        self.logger.debug(f"Getting all packages ...")

        if ignored_folders is None:
            ignored_folders = []
        
        folders = self.get_all_folders()
        packages = {}
        
        self.logger.debug(f"Searching folders {folders} for packages ...")        
        for folder in folders:
            self.logger.debug(f"Searching folders {folder} for packages ...")   
            if folder not in ignored_folders:
                if self._folder_has_notebooks(folder):
                    nbs_in_folder = self.get_notebooks_for_folder(folder)
                    nbs = [nb.name for nb in nbs_in_folder]
                    self.logger.debug(f"nbs in {folder} = {nbs}")

                    init_nb_name = f"{folder.split('/')[-1]}_init"

                    if init_nb_name in nbs:
                        self.logger.debug(f"{init_nb_name} in folder {folder}")
                        packages[folder] = self.get_package_dependencies(folder)
                    else:
                        self.logger.debug(f"{init_nb_name} not in folder {folder}")
                else:
                    self.logger.debug(f"Folder ({folder}) has notebooks returned false")
            else:
                self.logger.debug(f"{folder} in ignored folders ...")  

        package_list = self._resolve_dependency_order(packages)

        filtered_package_list = [pkg for pkg in package_list if pkg in packages.keys()]

        self.logger.debug(f"Discovered {len(filtered_package_list)} packages: {filtered_package_list}")
        return filtered_package_list
    
    def get_notebooks_for_folder(self, folder_path: str) -> List[NotebookResource]:
        notebooks = self.get_all_notebooks()
        folder_notebooks = []
        
        for notebook in notebooks:
            if notebook.name:
                notebook_folder = self._extract_folder_path(notebook)
                if notebook_folder and (notebook_folder == folder_path or notebook_folder.startswith(folder_path + "/")):
                    folder_notebooks.append(notebook)
        
        self.logger.debug(f"Found {len(folder_notebooks)} notebooks in folder '{folder_path}'")
        return folder_notebooks
    
    def get_notebooks_for_package(self, package_name: str, include_subfolders: bool = True) -> List[NotebookResource]:
        return self.get_notebooks_for_folder(package_name) if include_subfolders else self._get_direct_notebooks_for_folder(package_name)
    
    def get_package_structure(self, package_name: str) -> Dict[str, Any]:
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
                    subfolder = relative_path.split('/')[0]
                    if subfolder not in structure['subfolders']:
                        structure['subfolders'][subfolder] = []
                    structure['subfolders'][subfolder].append(notebook)
                else:
                    structure['notebooks'].append(notebook)
        
        return structure
    
    def find_notebook_by_name(self, notebook_name: str, package_name: Optional[str] = None) -> Optional[NotebookResource]:
        notebooks = self.get_notebooks_for_package(package_name) if package_name else self.get_all_notebooks()
        
        for notebook in notebooks:
            if notebook.name == notebook_name:
                return notebook
        
        base_name = notebook_name.replace('.ipynb', '').replace('.py', '')
        for notebook in notebooks:
            if notebook.name:
                notebook_base = notebook.name.replace('.ipynb', '').replace('.py', '')
                if notebook_base == base_name or notebook_base.endswith('/' + base_name):
                    return notebook
        
        return None
    
    def get_package_dependencies(self, package_name: str) -> List[str]:
        self.logger.debug(f"Getting package dependencies for {package_name}")

        code, _ = self.load_notebook_code(f"{package_name}_init")
        if not code:
            self.logger.error(f"No code loaded from notebook {package_name}")
            return []

        import __main__
        
        # Create execution context with NotebookPackages available
        exec_context = __main__.__dict__.copy()
        exec_context['NotebookPackages'] = NotebookPackages
        
        exec(compile(code, f"{package_name}_init", 'exec'), exec_context)

        return NotebookPackages.get_package_definition(package_name).dependencies
    
    def _extract_folder_path(self, notebook) -> Optional[str]:
        return notebook.properties.folder.path
    
    def _get_parent_folders(self, folder_path: str) -> List[str]:
        parents = []
        parts = folder_path.split('/')
        for i in range(1, len(parts)):
            parent = '/'.join(parts[:i])
            if parent:
                parents.append(parent)
        return parents
    
    def _get_top_level_folder(self, folder_path: str) -> Optional[str]:
        if '/' in folder_path:
            return folder_path.split('/')[0]
        return folder_path if folder_path else None
    
    def _folder_has_notebooks(self, folder_path: str) -> bool:
        notebooks = self.get_all_notebooks()
        for notebook in notebooks:
            if notebook.name:
                notebook_folder = self._extract_folder_path(notebook)
                if notebook_folder and (notebook_folder == folder_path or notebook_folder.startswith(folder_path + "/")):
                    return True
        return False
    
    def _get_direct_notebooks_for_folder(self, folder_path: str) -> List[NotebookResource]:
        notebooks = self.get_all_notebooks()
        direct_notebooks = []
        
        for notebook in notebooks:
            if notebook.name:
                notebook_folder = self._extract_folder_path(notebook)
                if notebook_folder == folder_path:
                    direct_notebooks.append(notebook)
        
        return direct_notebooks
    
    def _get_relative_path(self, notebook_name: str, package_name: str) -> str:
        if notebook_name.startswith(package_name + '/'):
            return notebook_name[len(package_name) + 1:]
        return notebook_name
    
    def refresh_cache(self):
        self._notebook_cache = None
        self._folder_cache = None
        self.logger.info("Discovery cache refreshed")

    def _execute_notebook_code(self, notebook_name):
        self.logger.debug(f"Attempting to execute notebook: {notebook_name}")
        try:
            code, _ = self.load_notebook_code(notebook_name)
            if not code:
                self.logger.error(f"No code loaded from notebook {notebook_name}")
                return
            
            self.logger.debug(f"Loaded code length: {len(code)} characters")
            self.logger.debug(f"Code preview: {code[:200]}...")

            self.import_notebook_as_module(notebook_name, code)
            self.logger.debug(f"Successfully executed notebook {notebook_name}")
        except Exception as e:
            self.logger.error(f"Failed to execute notebook {notebook_name}: {str(e)}")
            raise

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
            del sys.modules[effective_module_name]
        
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
        # Check if package already exists and prepare for reload if so
        if package_name in sys.modules:
            self.logger.info(f"Package {package_name} already loaded - preparing reload")
            self._prepare_package_reload(package_name)
        
        # First, check if this package has dependencies that need to be installed
        try:
            init_notebook_name = f"{package_name}_init"
            if init_notebook_name not in notebook_names:
                # Try to find and load the init notebook to get dependencies
                init_notebooks = [nb.name for nb in self.get_notebooks_for_folder(package_name) if nb.name == init_notebook_name]
                if init_notebooks:
                    notebook_names = [init_notebook_name] + notebook_names
        except Exception as e:
            self.logger.debug(f"Could not find init notebook for {package_name}: {e}")
        
        notebook_code = {}
        notebook_dependencies = {}
        package_pip_dependencies = []        
        
        for nb_name in notebook_names:
            try:
                code, dependencies = self.load_notebook_code(nb_name)
                notebook_code[nb_name] = code
                notebook_dependencies[nb_name] = dependencies
                
                # If this is the init notebook, extract pip dependencies
                if nb_name == f"{package_name}_init":
                    try:
                        # Execute the init code to register the package
                        import __main__
                        exec_context = __main__.__dict__.copy()
                        exec_context['NotebookPackages'] = NotebookPackages
                        exec(compile(code, nb_name, 'exec'), exec_context)
                        
                        # Get the package definition
                        package_def = NotebookPackages.get_package_definition(package_name)
                        if package_def:
                            package_pip_dependencies = package_def.dependencies
                            self.logger.info(f"Found pip dependencies for {package_name}: {package_pip_dependencies}")
                    except Exception as e:
                        self.logger.warning(f"Failed to extract dependencies from {nb_name}: {e}")
                        
            except Exception as e:
                self.logger.error(f"Exception loading notebook code for {nb_name}: {e}")
                continue
        
        # Install pip dependencies if found
        if package_pip_dependencies:
            self._install_package_dependencies(package_name, package_pip_dependencies)
        
        load_order = self._resolve_dependency_order(notebook_dependencies)
        
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

    def _prepare_package_reload(self, package_name: str) -> None:
        self.logger.debug(f"Clearing bindings for package: {package_name}")
        
        modules_to_remove = []
        if package_name in sys.modules:
            modules_to_remove.append(package_name)
        
        for module_name in list(sys.modules.keys()):
            if module_name.startswith(f"{package_name}."):
                modules_to_remove.append(module_name)

        classes_to_unbind = set()
        for module_name in modules_to_remove:
            module = sys.modules.get(module_name)
            if module:
                for name in dir(module):
                    obj = getattr(module, name, None)
                    if obj is not None and isinstance(obj, type):
                        if self._is_class_from_package(obj, package_name):
                            classes_to_unbind.add(obj)
        
        injector = GlobalInjector.get_injector()
        for cls in classes_to_unbind:
            self._unbind_class_and_interfaces(cls)

        for module_name in modules_to_remove:
            del sys.modules[module_name]
            self.logger.debug(f"Removed module from sys.modules: {module_name}")

    def _unbind_classes_from_module(self, module, package_name: str):
        """Unbind only classes that are defined in this module, not imported ones"""
        for name in dir(module):
            obj = getattr(module, name, None)
            if obj is not None and isinstance(obj, type):
                if self._is_class_from_package(obj, package_name):
                    self._unbind_class_and_interfaces(obj)

    def _is_class_from_package(self, cls, package_name: str) -> bool:
        """Check if a class is actually defined in the given package"""
        try:
            class_module = cls.__module__
            return (class_module == package_name or 
                    class_module.startswith(f"{package_name}."))
        except AttributeError:
            return False

    def _unbind_class_and_interfaces(self, cls):
        """Unbind a class and all its abstract base classes from the injector"""
        import inspect
        
        injector = GlobalInjector.get_injector()
        
        for base in cls.__bases__:
            if inspect.isabstract(base):
                self._clear_binding(injector, base)
        
        self._clear_binding(injector, cls)

    def _clear_binding(self, injector, interface):
        """Clear both binding and singleton cache for an interface"""
        if hasattr(injector.binder, '_bindings') and interface in injector.binder._bindings:
            del injector.binder._bindings[interface]
            self.logger.debug(f"Unbound: {interface.__name__}")
        
        if hasattr(injector, '_stack') and len(injector._stack) > 0:
            scope = injector._stack[0]
            if hasattr(scope, '_cache') and interface in scope._cache:
                del scope._cache[interface]
                self.logger.debug(f"Cleared singleton cache: {interface.__name__}")

    def _install_package_dependencies(self, package_name: str, dependencies: List[str]) -> bool:
        """Install pip dependencies for a notebook package"""
        if not dependencies:
            return True
            
        try:
            import subprocess
            import sys
            
            self.logger.info(f"Installing {len(dependencies)} pip dependencies for {package_name}")
            
            pip_args = [sys.executable, "-m", "pip", "install"] + dependencies + [
                "--disable-pip-version-check",
                "--no-warn-conflicts"
            ]
            
            result = subprocess.run(pip_args, capture_output=True, text=True)
            
            if result.returncode != 0:
                self.logger.error(f"Failed to install dependencies for {package_name}: {result.stderr}")
                return False
            
            self.logger.info(f"Successfully installed dependencies for {package_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Exception installing dependencies for {package_name}: {str(e)}")
            return False
    
    def _resolve_dependency_order(self, dependencies: Dict[str, List[str]]) -> List[str]:      
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
    
    def _generate_python_dependency_block(self, dependencies):
        output = ""
        for dep in dependencies:
            output += f'        "{dep}",\n'
        return output

    def publish_all_notebook_folders_as_packages(self, location, version = "0.1.0"):
        pkgs = self.get_all_packages()
        for pkg in pkgs:
            publish_notebook_folder_as_package(pkg, pkg, location)

    def publish_notebook_folder_as_package(self, folder_name, package_name, location, version, extra_dependencies = None):
        import os
        import uuid
        import shutil
        from setuptools import setup, find_packages

        temp_folder_path = f"/tmp/dist_{uuid.uuid4().hex}"
        os.makedirs(temp_folder_path, exist_ok=True)
        
        nbs = self.get_notebooks_for_folder(folder_name)

        filtered_modules = [nb.name for nb in nbs if '_init' not in nb.name]
        import_lines = [f"from . import {nb_name}" for nb_name in filtered_modules]
        init_content = "\n".join(import_lines)
        if init_content:
            init_content += "\n"

        self._execute_notebook_code( f"{folder_name}_init" )
        deps = NotebookPackages.get_package_definition(folder_name).dependencies
        if extra_dependencies:
            deps.extend(extra_dependencies)
        dependency_block = self._generate_python_dependency_block(deps)

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
        
        with open(f"{package_dir}/__init__.py", "w") as f:
            f.write(init_content)

        for nb_name in filtered_modules:
            code, _ = self.load_notebook_code(nb_name)
            with open(f"{package_dir}/{nb_name}.py", "w") as f:
                f.write(code)

        readme_content = f"""# {package_name}

    A package created from notebooks in {folder_name}.
    """
        
        with open(os.path.join(temp_folder_path, "pyproject.toml"), "w") as f:
            f.write(pyproject_content)
            
        with open(os.path.join(temp_folder_path, "README.md"), "w") as f:
            f.write(readme_content)
        
        current_dir = os.getcwd()
        try:
            os.chdir(temp_folder_path)
            
            subprocess.check_call(["pip", "wheel", ".", "--no-deps", "-w", "dist/"])
            
            wheel_files = os.listdir("dist/")
            wheel_file = wheel_files[0]
            wheel_path = os.path.join(temp_folder_path, "dist", wheel_file)
            
            if location.startswith("/"):
                os.makedirs(os.path.dirname(location), exist_ok=True)
                shutil.copy2(wheel_path, location)
            else:
                local_path = f"file:///{wheel_path}"
                self.es.copy(local_path, f"{location}/{wheel_file}", True)
            
            print(f"Package {package_name} successfully built and saved to {location}")
            
        finally:
            os.chdir(current_dir)
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

class DependencyError(Exception):
    pass

import __main__
if not hasattr(__main__, 'kindling_environment_factories'):
    setattr(__main__, 'kindling_environment_factories', {})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
