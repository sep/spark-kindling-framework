import importlib.util
import subprocess
import sys
from typing import Any, Dict, Optional, Union

from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_session import *

from .data_apps import *
from .notebook_framework import *

# Platform modules are imported dynamically based on detected platform
# Don't import them all at module level since platform wheels only include one

# Spark session and logging utilities
level_hierarchy = {
    "ALL": 0,
    "DEBUG": 1,
    "INFO": 2,
    "WARN": 3,
    "WARNING": 3,
    "ERROR": 4,
    "FATAL": 5,
    "OFF": 6,
}


def download_config_files(
    artifacts_storage_path: str, environment: str, app_name: Optional[str] = None
) -> List[str]:
    """
    Download config files from lake to temp location for Dynaconf.

    All files are optional - bootstrap config can provide everything.
    Returns list of local file paths in priority order.
    """
    storage_utils = _get_storage_utils()

    if not storage_utils:
        raise Exception("Storage utilities not available for config loading")

    # Strip trailing slash to prevent double slashes in path construction
    base_path = artifacts_storage_path.rstrip("/")
    print(f"Using artifacts path: {base_path}")

    temp_dir = tempfile.mkdtemp(prefix="kindling_config_")
    temp_path = Path(temp_dir)

    config_files = []

    files_to_download = [
        f"{base_path}/config/settings.yaml",
        f"{base_path}/config/{environment}.yaml",
    ]

    for remote_path in files_to_download:
        filename = remote_path.split("/")[-1]
        local_path = temp_path / f"{len(config_files)}_{filename}"

        try:
            # Use file:// prefix for local filesystem paths with mssparkutils/dbutils
            storage_utils.fs.cp(remote_path, f"file://{str(local_path)}")
            config_files.append(str(local_path))
            print(f"✓ Downloaded: {remote_path}")
        except Exception as e:
            # All config files are optional - continue silently
            print(f"  (Optional config not found: {filename})")

    if not config_files:
        print("No YAML config files found")
        default_config = temp_path / "default_settings.yaml"
        default_config.write_text(_get_minimal_default_config())
        config_files.append(str(default_config))
        print("✓ Using built-in default config (can be overridden by bootstrap)")

    return config_files


def _get_storage_utils():
    """Get platform storage utilities with fallback import chain"""
    import __main__

    # First try to get from __main__ (notebook environment)
    mssparkutils = getattr(__main__, "mssparkutils", None)
    if mssparkutils is not None:
        return mssparkutils

    dbutils = getattr(__main__, "dbutils", None)
    if dbutils is not None:
        return dbutils

    # Fall back to explicit imports (Spark job environment)
    try:
        from notebookutils import mssparkutils

        return mssparkutils
    except ImportError:
        pass

    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark:
            return DBUtils(spark)
    except ImportError:
        pass

    return None


def _get_minimal_default_config() -> str:
    """Minimal default config for when no files are found"""
    return """
default:
  kindling:
    version: "0.1.0"

    bootstrap:
      load_lake: false
      load_local: true
      required_packages: []
      ignored_folders: []

    delta:
      tablerefmode: "forName"
      optimize_write: true

    telemetry:
      logging:
        level: "INFO"
        print: true

    spark_configs: {}
"""


def flatten_dynaconf(data: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
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
    return (
        get_or_create_spark_session()
        .sparkContext._jvm.org.apache.log4j.LogManager.getRootLogger()
        .getLevel()
    )


def create_console_logger(config):
    """Create a console logger with configurable log level"""
    from pyspark.sql import SparkSession

    currentLevel = config.get("log_level", None) or get_spark_log_level()

    def should_log(level):
        level_log_rank = level_hierarchy.get(level.upper(), 2)
        current_log_rank = level_hierarchy.get(currentLevel, 2)
        return level_log_rank >= current_log_rank

    return type(
        "ConsoleLogger",
        (),
        {
            "debug": lambda self, *args, **kwargs: (
                print("DEBUG:", *args) if should_log("DEBUG") else None
            ),
            "info": lambda self, *args, **kwargs: (
                print("INFO:", *args) if should_log("INFO") else None
            ),
            "error": lambda self, *args, **kwargs: (
                print("ERROR:", *args) if should_log("ERROR") else None
            ),
            "warning": lambda self, *args, **kwargs: (
                print("WARNING:", *args) if should_log("WARNING") else None
            ),
        },
    )()


logger = create_console_logger({"log_level": "debug"})


def detect_platform_from_utils():
    try:
        import __main__

        if hasattr(__main__, "dbutils") and getattr(__main__, "dbutils") is not None:
            return "databricks"

        if hasattr(__main__, "mssparkutils") and getattr(__main__, "mssparkutils") is not None:
            return None
    except Exception:
        return None


def detect_platform(config=None) -> str:
    # Check for explicit platform configuration first
    if config and "platform_service" in config:
        explicit_platform = config.get("platform_service")
        if explicit_platform in ["databricks", "fabric", "synapse"]:
            print(f"Platform explicitly set to: {explicit_platform}")
            return explicit_platform

    dp = detect_platform_from_utils()
    if dp:
        print(f"Platform detected from utils: {dp}")
        return dp

    # Only create spark session if we actually need it for detection
    spark = get_or_create_spark_session()
    # Check for Databricks first - most distinctive
    try:
        # Databricks always has this config
        if spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", None):
            return "databricks"
    except:
        pass

    # Check for Fabric BEFORE Synapse - both have mssparkutils and notebookutils
    # Use notebookutils.runtime.context to distinguish
    try:
        import notebookutils

        # Fabric has workspaceId in context
        workspace_id = notebookutils.runtime.context.get("currentWorkspaceId")
        if workspace_id:
            return "fabric"
    except (ImportError, Exception):
        pass

    # Check for Synapse-specific config (check this AFTER Fabric)
    # Note: Fabric may also have some synapse configs since it's built on Synapse infrastructure
    try:
        # This is unique to Synapse
        if spark.conf.get("spark.synapse.workspace.name", None):
            return "synapse"
    except:
        pass

    raise RuntimeError("Unable to detect platform (not Synapse, Fabric, or Databricks)")


def safe_get_global(var_name: str, default: Any = None) -> Any:
    """Safely get a global variable"""
    try:
        import __main__

        return getattr(__main__, var_name, default)
    except Exception:
        return default


def is_framework_initialized() -> bool:
    """
    Check if the Kindling framework has been initialized.

    Returns:
        bool: True if framework is initialized, False otherwise

    Example:
        >>> if not is_framework_initialized():
        >>>     initialize_framework(config)
    """
    try:
        existing_service = get_kindling_service(PlatformServiceProvider).get_service()
        return existing_service is not None
    except Exception:
        return False


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

    for package in bootstrap_config.get("required_packages", []):
        load_if_needed(package)


def initialize_platform_services(platform, config, logger):
    """Initialize platform services by importing the correct platform module dynamically"""

    # Import the platform module dynamically - only the one that exists in this wheel
    platform_module_name = f"kindling.platform_{platform}"
    try:
        import importlib

        importlib.import_module(platform_module_name)
        logger.info(f"Loaded platform module: {platform_module_name}")
    except ImportError as e:
        raise ImportError(
            f"Platform module '{platform_module_name}' not found. "
            f"Make sure you installed the correct kindling wheel for {platform}."
        ) from e

    # Now the platform service should be registered
    svc = PlatformServices.get_service_definition(platform).factory(config, logger)
    get_kindling_service(PlatformServiceProvider).set_service(svc)
    return svc


def load_workspace_packages(platform, packages, logger):
    """Load workspace packages using existing NotebookLoader"""
    try:
        loader = get_kindling_service(NotebookManager)

        for package_name in packages:
            try:
                notebooks = loader.get_notebooks_for_folder(package_name)
                notebook_names = [nb.name for nb in notebooks if "_init" not in nb.name]

                if notebook_names:
                    loaded_modules = loader.import_notebooks_into_module(
                        package_name, notebook_names
                    )
                    logger.info(
                        f"Loaded workspace package: {package_name} ({len(loaded_modules)} modules)"
                    )

            except Exception as e:
                logger.warning(f"Failed to load workspace package {package_name}: {str(e)}")

    except Exception as e:
        logger.warning(f"Workspace package loading failed: {str(e)}")


def initialize_framework(config: Dict[str, Any], app_name: Optional[str] = None):
    """Linear framework initialization with Dynaconf config loading"""

    print(f"initialize_framework: initial_config = {config}")

    # Check if framework is already initialized
    if is_framework_initialized():
        print("Framework already initialized, skipping re-initialization")
        existing_service = get_kindling_service(PlatformServiceProvider).get_service()
        return existing_service

    # Extract bootstrap settings
    artifacts_storage_path = config.get("artifacts_storage_path")
    use_lake_packages = config.get("use_lake_packages", True)
    environment = config.get("environment", "development")

    config_files = None
    if use_lake_packages and artifacts_storage_path:
        config_files = download_config_files(
            artifacts_storage_path=artifacts_storage_path,
            environment=environment,
            app_name=app_name,
        )

    from kindling.spark_config import configure_injector_with_config

    # Check if ConfigService is already properly initialized
    try:
        config_service = get_kindling_service(ConfigService)
        # Check if it's actually initialized (dynaconf will be None if not)
        if hasattr(config_service, "dynaconf") and config_service.dynaconf is not None:
            print("Config service already initialized, skipping configuration")
        else:
            # ConfigService exists but not initialized yet, set it up
            injector = configure_injector_with_config(
                config_files=config_files,
                initial_config=config,  # BOOTSTRAP_CONFIG as overrides
                environment=environment,
                artifacts_storage_path=artifacts_storage_path,
            )
            config_service = get_kindling_service(ConfigService)
    except:
        # ConfigService doesn't exist yet, set it up
        injector = configure_injector_with_config(
            config_files=config_files,
            initial_config=config,  # BOOTSTRAP_CONFIG as overrides
            environment=environment,
            artifacts_storage_path=artifacts_storage_path,
        )
        config_service = get_kindling_service(ConfigService)

    logger = get_kindling_service(PythonLoggerProvider).get_logger("KindlingBootstrap")
    logger.info("Starting framework initialization")

    try:
        required_packages = config_service.get("kindling.bootstrap.required_packages", [])
        install_bootstrap_dependencies(logger, {"required_packages": required_packages})

        # Check for explicit platform in config (supports both nested and flat config keys)
        explicit_platform = (
            config_service.get("kindling.platform.environment")
            or config.get("platform_environment")
            or config.get("platform")
        )
        platform = detect_platform(config={"platform_service": explicit_platform})
        logger.info(f"Platform: {platform}")

        platformservice = initialize_platform_services(platform, config_service, logger)
        logger.info("Platform services initialized")

        # DEBUG: Check what the config value actually is
        load_local_value = config_service.get("kindling.bootstrap.load_local", True)
        logger.info(
            f"DEBUG: kindling.bootstrap.load_local = {load_local_value} (type: {type(load_local_value).__name__})"
        )

        # Also check the original flat key
        load_local_flat = config_service.get("load_local_packages", "NOT_SET")
        logger.info(
            f"DEBUG: load_local_packages (flat) = {load_local_flat} (type: {type(load_local_flat).__name__})"
        )

        if load_local_value:
            ignored_folders = config_service.get("kindling.bootstrap.ignored_folders", [])
            workspace_packages = get_kindling_service(NotebookManager).get_all_packages(
                ignored_folders=ignored_folders
            )
            load_workspace_packages(platformservice, workspace_packages, logger)
            logger.info(f"Loaded {len(workspace_packages)} workspace packages")
        else:
            logger.info("Skipping workspace package loading (load_local=False)")

        logger.info("Framework initialization complete")

        if app_name:
            logger.info(f"Auto-running app: {app_name}")
            print(f"🚀 ATTEMPTING TO RUN APP: {app_name}")
            try:
                runner = get_kindling_service(DataAppRunner)
                print(f"✅ Got DataAppRunner: {type(runner).__name__}")
                print(f"🎯 Calling runner.run_app('{app_name}')")
                runner.run_app(app_name)
                print(f"✅ App '{app_name}' completed successfully")
            except Exception as app_error:
                print(f"❌ App execution failed: {str(app_error)}")
                import traceback

                print(f"📋 App error traceback:")
                traceback.print_exc()
                raise

        return platformservice

    except Exception as e:
        logger.error(f"Framework initialization failed: {str(e)}")
        raise


def bootstrap_framework(config: Dict[str, Any], logger=None):
    """Backward compatibility wrapper"""
    return initialize_framework(config)
