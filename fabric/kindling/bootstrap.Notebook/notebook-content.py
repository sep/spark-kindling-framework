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
import importlib.util
from typing import Dict, Any, Optional, Union

notebook_import(".notebook_framework")
notebook_import(".app_framework")
notebook_import(".injection")
notebook_import(".spark_config")

# Spark session and logging utilities
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

def get_or_create_spark_session():
    """Get or create Spark session"""
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
    """Get current Spark log level"""
    return get_or_create_spark_session().sparkContext._jvm.org.apache.log4j.LogManager.getRootLogger().getLevel()

def create_console_logger(config):
    """Create a console logger with configurable log level"""
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

logger = create_console_logger({"log_level":"debug"})

def detect_platform() -> str:
    """Detect the current platform"""
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

def safe_get_global(var_name: str, default: Any = None) -> Any:
    """Safely get a global variable"""
    try:
        import __main__
        return getattr(__main__, var_name, default)
    except Exception:
        return default

def install_bootstrap_dependencies(logger, bootstrap_config):
    """Install packages needed for framework bootstrap"""
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

    for package in bootstrap_config.get('required_packages', []):
        load_if_needed(package)

def initialize_backend_services(platform, config, logger):
    """Initialize platform-specific backend services"""
    import __main__

    env_factory = getattr(__main__, 'kindling_environment_factories').get(
        config.get('platform_environment') or platform
    )
    
    if not env_factory:
        raise Exception(f"No environment factory found for platform: {platform}")
    
    backend = env_factory(config, logger)
    backend.initialize()
    
    setattr(__main__, "platform_environment_service", backend)
    setattr(__main__, "platform_environment", config.get('platform_environment') or platform)

    cfg = get_kindling_service(ConfigService)
    [cfg.set(key, value) for key, value in config.items()]

    return backend

def load_workspace_packages(backend, packages, logger):
    """Load workspace packages using existing NotebookLoader"""
    try:
        loader = get_kindling_service(NotebookManager)
        
        for package_name in packages:
            try:
                notebooks = loader.get_notebooks_for_folder(package_name)
                notebook_names = [nb.name for nb in notebooks if '_init' not in nb.name]
                
                if notebook_names:
                    loaded_modules = loader.import_notebooks_into_module(package_name, notebook_names)
                    logger.info(f"Loaded workspace package: {package_name} ({len(loaded_modules)} modules)")
                    
            except Exception as e:
                logger.warning(f"Failed to load workspace package {package_name}: {str(e)}")
    
    except Exception as e:
        logger.warning(f"Workspace package loading failed: {str(e)}")

def initialize_framework(config: Dict[str, Any], app_name: Optional[str] = None):
    """Linear framework initialization with optional app execution"""
    logger = create_console_logger(config)
    logger.info("Starting framework initialization")
    
    try:
        install_bootstrap_dependencies(logger, config)
        
        platform = detect_platform()
        is_interactive = is_interactive_session(config.get('is_interactive'))
        logger.info(f"Platform: {platform}, Interactive: {is_interactive}")
        
        backend = initialize_backend_services(platform, config, logger)
        logger.info("Backend services initialized")
        
        if config.get('load_local_packages', False):
            # Import here to avoid circular dependency
            from kindling.notebook_framework import NotebookLoader
            
            if hasattr(backend, 'notebook_loader'):
                loader = backend.notebook_loader
            else:
                loader = NotebookLoader(backend, config, logger)
            
            workspace_packages = loader.get_all_packages(
                ignored_folders=config.get('ignored_folders', [])
            )
            load_workspace_packages(backend, workspace_packages, logger)
            logger.info(f"Loaded {len(workspace_packages)} workspace packages")
    
        
        logger.info("Framework initialization complete")
        
        if app_name:
            # Import here to avoid circular dependency
            try:
                logger.info(f"Auto-running app: {app_name}")

                runner = get_kindling_service(AppRunner)
                runner.run_app(app_name)

            except ImportError as e:
                logger.warning(f"Could not run app {app_name}: app framework not available ({e})")
        
        return backend
        
    except Exception as e:
        logger.error(f"Framework initialization failed: {str(e)}")
        raise Exception(f"Framework initialization failed: {str(e)}") from e

def bootstrap_framework(config: Dict[str, Any], logger=None):
    """Backward compatibility wrapper"""
    return initialize_framework(config)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
