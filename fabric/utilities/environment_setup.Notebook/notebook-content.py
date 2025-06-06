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

%run notebook_package

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run fabric_notebook_sdk

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# BOOTSTRAP CONFIGURATION
# =============================================================================

BOOTSTRAP_CONFIG = {
    'workspace_endpoint': "https://sep-syws-dataanalytics-dev.dev.azuresynapse.net",
    'package_storage_path': "Files/artifacts/packages/latest",
    'required_packages': ["azure.identity", "injector", "dynaconf", "pytest"],
    'ignored_folders': ['utilities'],
    'spark_configs': {
        'spark.databricks.delta.schema.autoMerge.enabled': 'true'
    }
}

# =============================================================================
# BOOTSTRAP STATE MANAGEMENT
# =============================================================================

class BootstrapState:
    """Manages state during bootstrap process"""
    def __init__(self):
        self.loaded_packages = set()
        self.initialized_clients = {}
        self.discovered_notebooks = []
        self.execution_mode = None
        self.framework_initialized = False
        
    def mark_package_loaded(self, package_name):
        self.loaded_packages.add(package_name)
        
    def is_package_loaded(self, package_name):
        return package_name in self.loaded_packages

# Global state instance
bootstrap_state = BootstrapState()

# =============================================================================
# BOOTSTRAP UTILITIES
# =============================================================================

class BootstrapException(Exception):
    """Custom exception for bootstrap failures"""
    pass

def create_console_logger():
    """Create a simple console logger for bootstrap process"""
    return type('ConsoleLogger', (), {
        'debug': lambda self, *args, **kwargs: print("DEBUG:", *args),
        'info': lambda self, *args, **kwargs: print("INFO:", *args),
        'error': lambda self, *args, **kwargs: print("ERROR:", *args),
        'warning': lambda self, *args, **kwargs: print("WARNING:", *args),
    })()

def safe_bootstrap_operation(operation_name, operation_func, *args, **kwargs):
    """Wrapper for bootstrap operations with consistent error handling"""
    try:
        logger.info(f"Starting {operation_name}...")
        result = operation_func(*args, **kwargs)
        logger.info(f"Completed {operation_name}")
        return result
    except Exception as e:
        logger.error(f"Failed {operation_name}: {str(e)}")
        raise BootstrapException(f"Bootstrap failed at {operation_name}: {str(e)}") from e

# Initialize logger
logger = create_console_logger()

# =============================================================================
# PHASE 1: ENVIRONMENT CLEANUP AND SETUP
# =============================================================================

def cleanup_previous_bootstrap():
    """Clean up any previous bootstrap attempts"""
    if 'frameworkPackageGuard' in globals():
        del globals()['frameworkPackageGuard']
        logger.debug("Removed previous frameworkPackageGuard")
    
    if 'frameworkLocalRunGuard' in globals():
        del globals()['frameworkLocalRunGuard']
        logger.debug("Removed previous frameworkLocalRunGuard")

def setup_spark_environment():
    """Configure Spark settings for the session"""
    for config_key, config_value in BOOTSTRAP_CONFIG['spark_configs'].items():
        spark.conf.set(config_key, config_value)
        logger.debug(f"Set Spark config: {config_key} = {config_value}")

def install_bootstrap_dependencies():
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
            subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
            logger.debug(f"Installed package: {package_name}")
        else:
            logger.debug(f"Package already installed: {package_name}")

    for package in BOOTSTRAP_CONFIG['required_packages']:
        load_if_needed(package)

def is_interactive_session():
    """Determine if running in interactive vs job mode"""
    import IPython
    
    if 'is_interactive' in globals():
        logger.debug(f"is_interactive in global: {globals()['is_interactive']}")
        return globals()['is_interactive']

    try:
        ipython = get_ipython()
        
        if ipython is None:
            return False
            
        try:
            connection_file = ipython.config.get('IPKernelApp', {}).get('connection_file')
            if connection_file:
                return True
        except:
            pass
            
        return False
    except:
        return False

def setup_synapse_credentials():
    return DefaultAzureCredential()


# =============================================================================
# PHASE 3: SYNAPSE CLIENT INITIALIZATION
# =============================================================================

def initialize_synapse_client():
    """Set up authenticated Synapse client"""

    credential = setup_synapse_credentials()
    endpoint = BOOTSTRAP_CONFIG['workspace_endpoint']
    client = ArtifactsClient(endpoint=endpoint, credential=credential)
    
    bootstrap_state.initialized_clients['synapse'] = client
    logger.debug(f"Synapse client initialized for endpoint: {endpoint}")
    return client

# =============================================================================
# PHASE 4: NOTEBOOK DISCOVERY AND MANAGEMENT
# =============================================================================

def get_synapse_client():
    """Get the initialized Synapse client"""
    if 'synapse' not in bootstrap_state.initialized_clients:
        raise BootstrapException("Synapse client not initialized")
    return bootstrap_state.initialized_clients['synapse']

def get_all_notebooks():
    """Get all notebooks from the workspace"""
    client = get_synapse_client()
    return list(client.notebook.get_notebooks_by_workspace())

def get_all_notebooks_for_folder(folder_name):
    """Get all notebooks in a specific folder"""
    notebooks = get_all_notebooks()
    filtered_notebooks = []
    
    for notebook in notebooks:
        if (hasattr(notebook, 'properties') and 
            hasattr(notebook.properties, 'folder') and 
            hasattr(notebook.properties.folder, 'name')):
            
            if notebook.properties.folder.name == folder_name:
                filtered_notebooks.append(notebook)
    return filtered_notebooks

def get_all_packages():
    """Get all available packages by finding _init notebooks"""
    ignored_folders = BOOTSTRAP_CONFIG['ignored_folders']
    all_notebooks = get_all_notebooks()
    
    packages = []
    for notebook in all_notebooks:
        if (hasattr(notebook, 'properties') and 
            hasattr(notebook.properties, 'folder') and 
            hasattr(notebook.properties.folder, 'name') and
            '_init' in notebook.name):
            
            folder_name = notebook.properties.folder.name
            if folder_name not in ignored_folders:
                packages.append(folder_name)
    
    return list(set(packages))  # Remove duplicates

def load_notebook_code(notebook_name):
    """Load and parse notebook code"""
    import re
    
    client = get_synapse_client()
    notebook = client.notebook.get_notebook(notebook_name)

    code_blocks = []
    for cell in notebook.properties.cells:
        if cell.cell_type == "code":
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
                code_blocks.append(cell_code)
            else:
                code_blocks.append(str(cell.source))

    def import_replacement(match):
        module_name = match.group(2)
        package_name = match.group(1)
        return f"from {package_name}.{module_name} import *"

    processed_blocks = []
    replacements = []

    for block in code_blocks:
        pattern = r"notebook_import\(['\"]([\w_]*)\.([\w_]+)['\"]\)"    
        matches = re.findall(pattern, block)
        if matches:
            selected_matches = (item[1] for item in matches)
            replacements.extend(selected_matches)

        modified_block = re.sub(pattern, import_replacement, block)
        processed_blocks.append(modified_block)
        
    return "\n\n# ---- Next Cell ----\n\n".join(processed_blocks), replacements

# =============================================================================
# PHASE 5: DEPENDENCY RESOLUTION
# =============================================================================

def topological_sort(graph):
    """Sort dependencies topologically to determine load order"""
    visited = set()
    temp_visited = set()
    result = []
    
    def visit(node):
        if node in temp_visited:
            raise ValueError(f"Circular dependency detected involving {node}")
        if node in visited:
            return
        
        temp_visited.add(node)
        
        for dep in graph.get(node, []):
            for nb in graph:
                if dep in nb and nb != node:
                    visit(nb)
                    break
        
        temp_visited.remove(node)
        visited.add(node)
        result.append(node)
    
    for node in graph:
        if node not in visited:
            visit(node)
    
    return result

def generate_notebook_graph(notebook_names):
    """Generate dependency graph for notebooks"""
    dependencies = {}
    
    for nb_name in notebook_names:
        nb_code, imported_modules = load_notebook_code(nb_name)
        dependencies[nb_name] = imported_modules
    
    try:
        ordered_notebooks = topological_sort(dependencies)
        logger.debug(f"Dependency resolution order: {ordered_notebooks}")
    except ValueError as e:
        logger.error(f"Dependency resolution error: {e}")
        ordered_notebooks = notebook_names
        logger.debug(f"Using fallback order: {ordered_notebooks}")

    return ordered_notebooks

# =============================================================================
# PHASE 6: MODULE IMPORT SYSTEM
# =============================================================================

def setup_dynamic_package(package_name):
    """Create a dynamic package in sys.modules"""
    import sys
    import types
    
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = []  # Required for packages
        package.__package__ = package_name
        sys.modules[package_name] = package
    else:
        package = sys.modules[package_name]
    
    return package

def import_notebook_as_module(notebook_name, code, pkg_name=None, module_name=None, include_globals=None):
    """Import a notebook as a Python module"""
    import sys
    import types
    
    if include_globals is None:
        include_globals = globals()
    
    module_name = module_name or f"{pkg_name}.{notebook_name}" if pkg_name else notebook_name

    if module_name in sys.modules:
        return sys.modules[module_name]

    module = types.ModuleType(module_name)
    sys.modules[module_name] = module 
    
    if pkg_name:
        package = setup_dynamic_package(pkg_name)
        module.__package__ = pkg_name   
        setattr(package, notebook_name, module)

    module_dict = module.__dict__
    
    if include_globals:
        for key, value in include_globals.items():
            if key not in module_dict:
                module_dict[key] = value
    
    compiled_code = compile(code, module_name, 'exec')
    try:
        exec(compiled_code, module_dict)
    except Exception as e:
        logger.error(f"Error executing code for module {module_name}: {e}")
        raise
    
    return module

def notebook_import(notebook_ref):
    """Import a notebook module into the global namespace"""
    is_interactive = is_interactive_session()
    
    if is_interactive:
        logger.debug("Using no-op to avoid importing notebook for interactive...")
        return  # Do nothing, this means we are using %run
    
    logger.debug(f"Importing notebook module {notebook_ref}...")

    nbrs = notebook_ref.split('.')
    notebook_name = nbrs[1] if '.' in notebook_ref else notebook_ref
    package_name = nbrs[0]
    
    import sys
    if notebook_ref in sys.modules:
        module = sys.modules[notebook_ref]
        
        for name in dir(module):
            if not name.startswith('_'):  # Skip private attributes
                globals()[name] = getattr(module, name)
        
        return module
    else:
        logger.warning(f"Module {notebook_name} not found in sys.modules. It may not have been registered.")
        return None

# =============================================================================
# PHASE 7: PACKAGE INSTALLATION AND LOADING
# =============================================================================

def install_package_from_lake(package_name):
    """Install package from data lake storage"""
    import uuid
    import IPython
    from notebookutils import mssparkutils
    
    package_file = f"{package_name}-0.1.0-py3-none-any.whl"
    package_path = f"{BOOTSTRAP_CONFIG['package_storage_path']}/{package_file}"
    
    logger.debug(f"Checking for package {package_file} in lake ...")

    if mssparkutils.fs.exists(package_path): 
        logger.debug(f"Package exists in lake: {package_name}")
        whl_path = f"file:///tmp/{uuid.uuid4().hex}/{package_file}"
        mssparkutils.fs.cp(package_path, whl_path)
        subprocess.check_call([sys.executable, "-m", "pip", f"uninstall {package_name} -y"])
        subprocess.check_call([sys.executable, "-m", "pip", "install", {whl_path}])
        
        # Check if package is now available
        import importlib.util
        spec = importlib.util.find_spec(package_name)
        return spec is not None
    else:
        logger.debug(f"Package does not exist in lake: {package_name}")
        return False

def import_notebooks_into_module(package_name, notebook_names):
    """Import multiple notebooks into a package module"""
    logger.debug(f"Importing notebooks into module - pkg: {package_name}, notebooks: {notebook_names}")

    code = {}
    ordered_notebooks = generate_notebook_graph(notebook_names)
    
    # Load code for all notebooks
    for nb_name in notebook_names:
        nb_code, _ = load_notebook_code(nb_name)
        code[nb_name] = nb_code
    
    imported_modules = {}
    for nb_name in ordered_notebooks:
        if nb_name in code:
            imported_module = import_notebook_as_module(nb_name, code[nb_name], package_name)
            imported_modules[nb_name] = imported_module
        
    return imported_modules

def install_package(package_name, dependencies=None, modules=None):
    """Install a notebook package with dependency handling"""
    
    # Initialize package guard if not exists
    if 'frameworkPackageGuard' not in globals():
        globals()["frameworkPackageGuard"] = {}

    if package_name in globals()["frameworkPackageGuard"]:
        logger.debug(f"Package {package_name} already in package guard")
        return True

    logger.debug(f"Installing package: {package_name}")
    
    use_lake_packages = globals().get("use_lake_packages", False)
    is_interactive = is_interactive_session()
    
    logger.debug(f"use_lake_packages: {use_lake_packages}")

    if is_interactive:
        # Interactive mode: install dependencies only
        if dependencies:
            for dependency in dependencies:
                install_package(dependency)
        globals()["frameworkPackageGuard"][package_name] = True
        return False
    elif not (use_lake_packages and install_package_from_lake(package_name)):
        # Job mode: install dependencies then load from notebooks
        logger.debug(f"Loading {package_name} from notebooks in job session")
        
        if dependencies:
            for dependency in dependencies:
                logger.info(f"Installing dependency: {dependency} for package: {package_name}")
                install_package(dependency, [])
        
        # Get notebooks for this package folder
        folder_nb_names = [item.name for item in get_all_notebooks_for_folder(package_name) 
                          if '_init' not in item.name]
        
        logger.debug(f"Loading notebooks for package {package_name}: {folder_nb_names}")
        import_notebooks_into_module(package_name, folder_nb_names)
        
        globals()["frameworkPackageGuard"][package_name] = True
        return True
    else:
        logger.debug(f"Package {package_name} loaded from lake")
        globals()["frameworkPackageGuard"][package_name] = True
        return True

# =============================================================================
# PHASE 8: PACKAGE REGISTRY INITIALIZATION
# =============================================================================

def initialize_package_registry():
    """Initialize package registry by loading _init notebooks"""
    import importlib.util
    
    packages = get_all_packages()
    logger.debug(f"Found packages: {packages}")
    
    for pkg in packages:
        # Check if package is already installed (from lake)
        spec = importlib.util.find_spec(pkg)
        if spec is not None:
            logger.debug(f"Package already installed: {pkg}")
            continue
            
        # Load the _init notebook to register the package
        nb = f"{pkg}_init" 
        logger.debug(f"Initializing package registry for {pkg} -> {nb}")
        
        try:
            code, _ = load_notebook_code(nb)
            import_notebook_as_module(nb, code)
            notebook_import(nb)
        except Exception as e:
            logger.warning(f"Failed to initialize package {pkg}: {e}")

def load_packages_from_registry():
    """Load packages using the registry for dependency information"""
    # Import NotebookPackages after registry is initialized
    if 'NotebookPackages' not in globals():
        logger.error("NotebookPackages not available - registry may not be initialized")
        return
    
    # Get dependencies from registry
    dependencies = {}
    for nbpkg in NotebookPackages.get_package_names():
        package_def = NotebookPackages.get_package_definition(nbpkg)
        dependencies[nbpkg] = package_def.dependencies

    # Resolve dependency order
    try:
        ordered_nbpkgs = topological_sort(dependencies)
        logger.debug(f"Package load order from registry: {ordered_nbpkgs}")
    except ValueError as e:
        logger.error(f"Package dependency resolution failed: {e}")
        ordered_nbpkgs = list(NotebookPackages.get_package_names())
    
    # Install packages in dependency order
    for nbpkg in ordered_nbpkgs:
        if not bootstrap_state.is_package_loaded(nbpkg):
            logger.debug(f"Installing package from registry: {nbpkg}")
            try:
                install_package(nbpkg)
                bootstrap_state.mark_package_loaded(nbpkg)
            except Exception as e:
                logger.error(f"Failed to install package {nbpkg}: {e}")

# =============================================================================
# BOOTSTRAP ORCHESTRATION
# =============================================================================

def cleanup_globals():
    """Clean up bootstrap-specific globals after completion"""
    globals_to_remove = [
        'bootstrap_state', 'BOOTSTRAP_CONFIG', 'BootstrapState', 
        'BootstrapException', 'create_console_logger', 'safe_bootstrap_operation'
    ]
    
    for global_name in globals_to_remove:
        if global_name in globals():
            del globals()[global_name]

def bootstrap_notebook_framework():
    """
    Main entry point for bootstrap process.
    Call this function to initialize the entire notebook framework.
    """
    global bootstrap_state
    
    # Check if already bootstrapped
    if hasattr(globals(), 'framework_bootstrapped') and globals()['framework_bootstrapped']:
        logger.info("Framework already bootstrapped, skipping...")
        return
    
    logger.info("=== Starting Synapse Notebook Framework Bootstrap ===")
    
    try:
        # Phase 1: Environment Setup
        safe_bootstrap_operation("Environment Cleanup", cleanup_previous_bootstrap)
        safe_bootstrap_operation("Spark Environment Setup", setup_spark_environment)
        safe_bootstrap_operation("Bootstrap Dependencies Installation", install_bootstrap_dependencies)
        
        # Phase 2: Detect execution environment
        bootstrap_state.execution_mode = is_interactive_session()
        logger.info(f"Execution mode: {'Interactive' if bootstrap_state.execution_mode else 'Job'}")
        
        # Phase 3: Synapse Client Initialization
        safe_bootstrap_operation("Synapse Client Initialization", initialize_synapse_client)
        
        # Phase 4: Package Registry Initialization
        safe_bootstrap_operation("Package Registry Initialization", initialize_package_registry)
        
        # Phase 5: Package Loading from Registry
        safe_bootstrap_operation("Package Loading from Registry", load_packages_from_registry)
        
        # Mark as complete
        globals()['framework_bootstrapped'] = True
        bootstrap_state.framework_initialized = True
        
        logger.info("=== Bootstrap Complete - Framework Ready ===")
        
    except BootstrapException:
        logger.error("Bootstrap failed - framework not available")
        raise
    except Exception as e:
        logger.error(f"Unexpected bootstrap error: {str(e)}")
        raise BootstrapException("Unexpected bootstrap failure") from e

# =============================================================================
# BOOTSTRAP EXECUTION
# =============================================================================

# Execute the bootstrap process
bootstrap_notebook_framework()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
