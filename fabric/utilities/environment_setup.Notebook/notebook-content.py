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

%run notebook_framework

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run bootstrap

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run platform_fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# %run notebook_framework
# %run platform_fabric

def get_storage_utils():
    """Get platform storage utilities"""
    import __main__
    return getattr(__main__, 'mssparkutils', None) or getattr(__main__, 'dbutils', None)

def is_kindling_available():
    """Check if kindling is already available"""
    try:
        from kindling.bootstrap import initialize_framework
        return True
    except ImportError:
        return False

def execute_bootstrap_script(bootstrap_config):
    """Execute the main bootstrap script from remote storage"""
    storage_utils = get_storage_utils()
    if not storage_utils:
        raise Exception("Storage utilities not available")
    
    artifacts_path = bootstrap_config.get('artifacts_storage_path')
    if not artifacts_path:
        raise Exception("artifacts_storage_path required in bootstrap config")
    
    script_path = f"{artifacts_path}/scripts/kindling_bootstrap.py"
    
    try:
        print(f"Executing remote bootstrap script: {script_path}")
        content = storage_utils.fs.head(script_path, max_bytes=1000000)
        
        # Execute with bootstrap config available
        exec_globals = globals().copy()
        exec_globals['BOOTSTRAP_CONFIG'] = bootstrap_config
        
        exec(compile(content, script_path, 'exec'), exec_globals)
        
    except Exception as e:
        raise Exception(f"Failed to execute bootstrap script: {str(e)}")

def normalize_bootstrap_config(config):
    """Apply config defaults and transformations"""
    # Convert legacy package_storage_path to artifacts_storage_path
    if config.get('package_storage_path') and not config.get('artifacts_storage_path'):
        config['artifacts_storage_path'] = config['package_storage_path'].rsplit('/', 2)[0]
    
    # Set default platform environment
    if not config.get('platform_environment'):
        config['platform_environment'] = "fabric"
    
    # Convert workspace_endpoint to workspace_id if needed
    if not config.get('workspace_id') and config.get('workspace_endpoint'):
        config['workspace_id'] = config['workspace_endpoint']
    
    return config

def install_required_packages(packages):
    """Install required pip packages"""
    if not packages:
        return True
        
    try:
        import subprocess
        import sys
        
        print(f"Installing packages: {', '.join(packages)}")
        
        pip_args = [sys.executable, "-m", "pip", "install"] + packages + [
            "--disable-pip-version-check",
            "--no-warn-conflicts"
        ]
        
        result = subprocess.run(pip_args, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Package installation failed: {result.stderr}")
            return False
        
        print("Required packages installed successfully")
        return True
        
    except Exception as e:
        print(f"Exception installing packages: {str(e)}")
        return False

def load_kindling_from_notebooks(config):
    """Bootstrap kindling from local notebook packages using already-loaded notebook framework"""
    print("Loading kindling from local notebook packages...")
    
    try:
        # notebook_framework and platform_fabric are already loaded via %run at top
        # Create environment service and notebook loader
        es = FabricService(config, create_console_logger(config))
        nm = create_notebook_loader(es, config)
        
        # Import kindling package notebooks as modules
        # Get all notebooks in kindling folder
        kindling_notebooks = nm.get_notebooks_for_folder("kindling")
        notebook_names = [nb.name for nb in kindling_notebooks if '_init' not in nb.name]
        
        kindling_modules = nm.import_notebooks_into_module("kindling", notebook_names)
        
        print(f"Loaded kindling modules: {list(kindling_modules.keys())}")
        
        # The modules are now available in sys.modules as kindling.notebook_framework, etc.
        
    except Exception as e:
        raise Exception(f"Failed to load kindling from notebooks: {str(e)}")

def bootstrap_notebook(bootstrap_config):
    """Main notebook bootstrap function"""
    print("=== Notebook Bootstrap ===")
    
    try:
        # Normalize config
        config = normalize_bootstrap_config(bootstrap_config)
        
        # Check if kindling is already available
        if is_kindling_available():
            print("Kindling already available")
        elif config.get('use_lake_packages', False):
            print("Kindling not available, executing remote bootstrap...")
            execute_bootstrap_script(config)
        elif config.get('load_local_packages', True):
            print("Kindling not available, loading from local notebook packages...")
            load_kindling_from_notebooks(config)
        else:
            # Neither use_lake_packages nor load_local_packages is True
            print("Minimal mode: using only backend service and notebook manager from %run notebooks")
            
            # Install required packages if specified
            required_packages = config.get('required_packages', [])
            if required_packages:
                print(f"Installing {len(required_packages)} required packages...")
                if not install_required_packages(required_packages):
                    print("Warning: Some required packages failed to install")
            
            # Create environment service and notebook loader using already-loaded classes
            es = FabricService(config, create_console_logger(config))
            nm = create_notebook_loader(es, config)
            
            # Set up minimal framework context
            import __main__
            __main__.framework = type('MinimalFramework', (), {
                'environment_service': es,
                'notebook_loader': nm,
                'config': config
            })()
            __main__.notebook_import = nm.notebook_import
            
            print("Minimal framework setup complete - backend service and notebook manager available")
            return __main__.framework
        
        # Verify kindling is now available (only if we tried to load it)
        if (config.get('use_lake_packages', False) or config.get('load_local_packages', True)) and not is_kindling_available():
            raise Exception("Kindling still not available after bootstrap attempt")
        
        # Hand off to framework initialization (only if kindling is available)
        if is_kindling_available():
            from kindling.bootstrap import initialize_framework
            print("Initializing framework...")
            framework = initialize_framework(config, config.get('app_name'))
        else:
            # We're in minimal mode, framework is already set up above
            framework = __main__.framework
        
        print("Notebook bootstrap complete")
        return framework
        
    except Exception as e:
        print(f"ERROR: Notebook bootstrap failed: {str(e)}")
        raise

# Execute bootstrap if BOOTSTRAP_CONFIG is defined
if 'BOOTSTRAP_CONFIG' in globals():
    bootstrap_notebook(BOOTSTRAP_CONFIG)
else:
    print("INFO: BOOTSTRAP_CONFIG not defined - call bootstrap_notebook(config) manually")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
