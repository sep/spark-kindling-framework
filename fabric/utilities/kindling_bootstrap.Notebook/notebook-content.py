# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import sys
import subprocess
import uuid
from typing import Dict, Any, Optional


def check_kindling_available() -> bool:
    """Check if kindling package is already available"""
    try:
        import kindling
        return True
    except ImportError:
        return False


def check_kindling_in_pool() -> bool:
    """Check if kindling is available via pool dependencies"""
    try:
        # Check if it's in the Python path but not imported yet
        import importlib.util
        spec = importlib.util.find_spec("kindling")
        return spec is not None
    except:
        return False


def detect_platform() -> str:
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
    except:
        return 'unknown'


def get_storage_utils():
    import __main__
    return getattr(__main__, 'mssparkutils', None) or getattr(__main__, 'dbutils', None)


def load_kindling_from_lake(storage_path: str, package_name: str = "kindling") -> bool:
    """Try to load kindling package from data lake storage"""
    try:
        storage_utils = get_storage_utils()
        if not storage_utils:
            print("WARNING: Storage utilities not available for lake loading")
            return False
        
        package_file = f"{package_name}-latest-py3-none-any.whl"
        remote_path = f"{storage_path}/packages/latest/{package_file}"
        
        if not storage_utils.fs.exists(remote_path):
            print(f"INFO: Kindling package not found in lake at {remote_path}")
            return False
        
        temp_dir = f"/tmp/kindling_bootstrap_{uuid.uuid4().hex}"
        local_path = f"file://{temp_dir}/{package_file}"
        
        print(f"INFO: Loading kindling from lake: {remote_path}")
        storage_utils.fs.cp(remote_path, local_path)
        
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            f"{temp_dir}/{package_file}", "--force-reinstall", "--no-deps"
        ])
        
        print("INFO: Successfully loaded kindling from lake")
        return True
    except Exception as e:
        print(f"WARNING: Failed to load kindling from lake: {str(e)}")
        return False


def load_kindling_from_folder(folder_path: str) -> bool:
    """Try to load kindling from a local folder"""
    try:
        import os
        
        if not os.path.exists(folder_path):
            print(f"INFO: Kindling folder not found at {folder_path}")
            return False
        
        # Add folder to Python path
        if folder_path not in sys.path:
            sys.path.insert(0, folder_path)
            print(f"INFO: Added {folder_path} to Python path")
        
        # Try to import kindling
        import kindling
        print("INFO: Successfully loaded kindling from local folder")
        return True
    except Exception as e:
        print(f"WARNING: Failed to load kindling from folder: {str(e)}")
        return False


def load_bootstrap_config() -> Dict[str, Any]:
    import __main__
    config = getattr(__main__, 'bootstrap_config', {})
    
    defaults = {
        'platform_name': None,
        'log_level': 'INFO',
        'load_local_packages': True,
        'use_lake_packages': True,
        'artifacts_storage_path': '',
        'local_kindling_path': './kindling',
        'required_packages': [],
        'ignored_folders': ['temp', 'archive', 'deprecated'],
        'workspace_endpoint': None,
        'is_interactive': None,
        'spark_configs': {}
    }
    
    for key, default_value in defaults.items():
        if key not in config:
            config[key] = default_value
    
    return config


def bootstrap_kindling() -> bool:
    """Bootstrap kindling package through various methods"""
    print("=== Starting Kindling Package Bootstrap ===")
    
    # 1. Check if kindling is already loaded
    if check_kindling_available():
        print("INFO: Kindling package already loaded")
        return True
    
    # 2. Check if kindling is available via pool dependencies
    if check_kindling_in_pool():
        print("INFO: Kindling package available in pool, importing...")
        try:
            import kindling
            print("INFO: Successfully imported kindling from pool")
            return True
        except ImportError as e:
            print(f"WARNING: Failed to import kindling from pool: {str(e)}")
    
    # 3. Load configuration
    config = load_bootstrap_config()
    platform = detect_platform()
    print(f"INFO: Detected platform: {platform}")
    
    # 4. Try to load from lake if enabled and path provided
    if config.get('use_lake_packages', True) and config.get('artifacts_storage_path'):
        if load_kindling_from_lake(config['artifacts_storage_path']):
            return True
    
    # 5. Try to load from local folder if enabled and path provided
    if config.get('load_local_packages', True) and config.get('local_kindling_path'):
        if load_kindling_from_folder(config['local_kindling_path']):
            return True
    
    print("ERROR: Failed to bootstrap kindling package through all methods")
    return False


def minimal_bootstrap():
    """Main bootstrap function"""
    try:
        # Bootstrap kindling package first
        if not bootstrap_kindling():
            raise Exception("Failed to bootstrap kindling package")
        
        # Import and initialize kindling
        try:
            from kindling import bootstrap_framework
        except ImportError as e:
            print(f"ERROR: Could not import kindling framework: {str(e)}")
            raise
        
        # Load config and initialize framework
        config = load_bootstrap_config()
        framework_config = FrameworkConfig.from_globals({'bootstrap_config': config})
        framework = bootstrap_framework(framework_config)
        
        print("=== Kindling Framework Bootstrap Complete ===")
        print(f"Platform: {framework.backend.platform_name}")
        print(f"Interactive mode: {framework.state.is_interactive}")
        print(f"Loaded packages: {len(framework.state.loaded_packages)}")
        
        return framework
    except Exception as e:
        print(f"ERROR: Bootstrap failed: {str(e)}")
        raise

#bootstrap_notebook_framework = minimal_bootstrap

#minimal_bootstrap()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
