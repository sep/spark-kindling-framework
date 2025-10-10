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
notebook_import(".spark_session")
notebook_import(".platform_fabric")
notebook_import(".platform_synapse")
notebook_import(".platform_databricks")
notebook_import(".platform_local")

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

def flatten_dynaconf(data: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """Flatten a nested Dynaconf config into dot-notation keys."""
    items = []
    for key, value in data.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            items.extend(flatten_dynaconf(value, new_key.lower(), sep=sep).items())
        else:
            items.append((new_key.lower(), value))
    return dict(items)


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

def detect_platform(config = None) -> str:       
    # Check for Databricks first - most distinctive
    try:
        # Databricks always has this config
        if spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", None):
            return "databricks"
    except:
        pass
    
    # Check for Synapse-specific config
    try:
        # This is unique to Synapse
        if spark.conf.get("spark.synapse.workspace.name", None):
            return "synapse"
    except:
        pass
    
    # Check for Fabric - notebookutils with specific context
    try:
        import notebookutils
        # Fabric has workspaceId in context
        workspace_id = notebookutils.runtime.context.get("currentWorkspaceId")
        if workspace_id:
            return "fabric"
    except (ImportError, Exception):
        pass
    
    raise RuntimeError("Unable to detect platform (not Synapse, Fabric, or Databricks)")


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

def initialize_platform_services(platform, config, logger):
    svc = PlatformServices.get_service_definition(platform).factory(config,logger)
    get_kindling_service(PlatformServiceProvider).set_service(svc)
    return svc

def load_workspace_packages(platform, packages, logger):
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
    """Linear framework initialization with Dynaconf config loading"""

    print(f"initialize_framework: initial_config = {config}")

    # Extract bootstrap settings
    artifacts_storage_path = config.get('artifacts_storage_path')
    use_lake_packages = config.get('use_lake_packages', True)
    environment = config.get('environment', 'development')
    
    config_files = None
    if use_lake_packages and artifacts_storage_path:
        config_files = download_config_files(
            artifacts_storage_path=artifacts_storage_path,
            environment=environment,
            app_name=app_name
        )
    
    from kindling.spark_config import configure_injector_with_config
    injector = configure_injector_with_config(
        config_files=config_files,
        initial_config=config,  # BOOTSTRAP_CONFIG as overrides
        environment=environment,
        artifacts_storage_path=artifacts_storage_path
    )
    
    config_service = get_kindling_service(ConfigService)
    
    logger = get_kindling_service(PythonLoggerProvider).get_logger("KindlingBootstrap")
    logger.info("Starting framework initialization")
    
    try:
        required_packages = config_service.get('kindling.bootstrap.required_packages', [])
        install_bootstrap_dependencies(logger, {'required_packages': required_packages})
        
        platform = detect_platform(config={'platform_service': config_service.get('kindling.platform.environment')})
        logger.info(f"Platform: {platform}")
        
        platformservice = initialize_platform_services(platform, config_service, logger)
        logger.info("Platform services initialized")
        
        if config_service.get('kindling.bootstrap.load_local', True):
            ignored_folders = config_service.get('kindling.bootstrap.ignored_folders', [])
            workspace_packages = get_kindling_service(NotebookManager).get_all_packages(ignored_folders=ignored_folders)
            load_workspace_packages(platformservice, workspace_packages, logger)
            logger.info(f"Loaded {len(workspace_packages)} workspace packages")
        
        logger.info("Framework initialization complete")
        
        if app_name:
            logger.info(f"Auto-running app: {app_name}")
            runner = get_kindling_service(AppRunner)
            runner.run_app(app_name)
        
        return platformservice
        
    except Exception as e:
        logger.error(f"Framework initialization failed: {str(e)}")
        raise

def bootstrap_framework(config: Dict[str, Any], logger=None):
    """Backward compatibility wrapper"""
    return initialize_framework(config)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
