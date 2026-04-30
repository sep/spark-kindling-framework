import importlib.util
import json
import logging
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from kindling.features import get_feature_bool
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


_BOOTSTRAP_STAGE_ID = uuid.uuid4().hex[:12]
_BOOTSTRAP_LOGGER = logging.getLogger("kindling.bootstrap")


def _parse_spark_conf_value(value: Any) -> Any:
    """Parse Spark config string values into native Python types when possible."""
    if not isinstance(value, str):
        return value

    stripped = value.strip()
    lowered = stripped.lower()

    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
        return None

    try:
        return json.loads(stripped)
    except Exception:
        return value


def _get_spark_kindling_config() -> Dict[str, Any]:
    """Read spark.kindling.* keys from SparkConf and map into bootstrap/config keys."""
    try:
        spark = get_or_create_spark_session()
        conf_items = spark.conf.getAll()
        iterator = conf_items.items() if isinstance(conf_items, dict) else conf_items
    except Exception:
        return {}

    mapped: Dict[str, Any] = {}
    prefix = "spark.kindling."
    bootstrap_prefix = "bootstrap."
    bootstrap_aliases = {
        "load_lake": "use_lake_packages",
        "load_local": "load_local_packages",
    }

    for item in iterator:
        try:
            key, raw_value = item
        except Exception:
            continue

        if not isinstance(key, str) or not key.startswith(prefix):
            continue

        suffix = key[len(prefix) :]
        if not suffix:
            continue

        value = _parse_spark_conf_value(raw_value)

        if suffix.startswith(bootstrap_prefix):
            bootstrap_key = suffix[len(bootstrap_prefix) :]
            if not bootstrap_key:
                continue
            mapped[bootstrap_aliases.get(bootstrap_key, bootstrap_key)] = value
        else:
            mapped[f"kindling.{suffix}"] = value

    return mapped


def _merge_with_spark_kindling_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge SparkConf-derived config with explicit config (explicit wins)."""
    spark_kindling_config = _get_spark_kindling_config()
    if not spark_kindling_config:
        return dict(config or {})

    merged = dict(spark_kindling_config)
    merged.update(config or {})
    _BOOTSTRAP_LOGGER.debug(
        "Merged %s SparkConf settings from spark.kindling.*", len(spark_kindling_config)
    )
    return merged


def _normalize_path_value(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _is_volume_path(path: Optional[str]) -> bool:
    return isinstance(path, str) and path.strip().startswith("/Volumes/")


def _is_dbfs_path(path: Optional[str]) -> bool:
    return isinstance(path, str) and path.strip().startswith("dbfs:/")


def _dbfs_uri_to_local_path(path: str) -> str:
    normalized = path.strip()
    if normalized.startswith("dbfs:/"):
        return f"/dbfs{normalized[len('dbfs:'):]}"
    return normalized


def _build_run_scoped_staging_path(root: Optional[str], *parts: str) -> Optional[str]:
    normalized_root = _normalize_path_value(root)
    if not normalized_root:
        return None

    stripped_parts = [
        part.strip("/") for part in parts if isinstance(part, str) and part.strip("/")
    ]
    return "/".join(
        [normalized_root.rstrip("/"), ".kindling-bootstrap", _BOOTSTRAP_STAGE_ID, *stripped_parts]
    )


def _parent_volume_path(path: Optional[str]) -> Optional[str]:
    normalized = _normalize_path_value(path)
    if not _is_volume_path(normalized):
        return None
    parent = str(Path(normalized).parent)
    return parent if _is_volume_path(parent) else None


def _resolve_databricks_staging_path(config_like) -> Optional[str]:
    """Resolve a Databricks bootstrap staging path.

    Preference order:
    1. Explicit `kindling.temp_path` / `temp_path`
    2. Explicit `kindling.databricks.volume_staging_root`
    3. Parent of a volume-backed `kindling.storage.checkpoint_root`
    4. Parent of a volume-backed `kindling.storage.table_root`

    Only volume-backed derived paths are used automatically. If no governed
    staging root can be found, callers should fall back to DBFS/reference mode.
    """

    explicit_temp_path = _normalize_path_value(
        config_like.get("kindling.temp_path") or config_like.get("temp_path")
    )
    if explicit_temp_path:
        return explicit_temp_path

    volumes_enabled = get_feature_bool(config_like, "databricks.volumes_enabled", default=False)
    if not volumes_enabled:
        return None

    explicit_volume_root = _normalize_path_value(
        config_like.get("kindling.databricks.volume_staging_root")
    )
    if explicit_volume_root and _is_volume_path(explicit_volume_root):
        return explicit_volume_root

    checkpoint_root = _normalize_path_value(config_like.get("kindling.storage.checkpoint_root"))
    checkpoint_parent = _parent_volume_path(checkpoint_root)
    if checkpoint_parent:
        return checkpoint_parent

    table_root = _normalize_path_value(config_like.get("kindling.storage.table_root"))
    table_parent = _parent_volume_path(table_root)
    if table_parent:
        return table_parent

    return None


def _resolve_bootstrap_temp_path(config_service, bootstrap_config: Dict[str, Any]) -> Optional[str]:
    if config_service is None:
        return _normalize_path_value(
            bootstrap_config.get("kindling.temp_path") or bootstrap_config.get("temp_path")
        )

    platform_name = (
        config_service.get("kindling.platform.environment")
        or config_service.get("kindling.platform.name")
        or bootstrap_config.get("platform_environment")
        or bootstrap_config.get("platform")
    )

    if str(platform_name).strip().lower() == "databricks":
        resolved = _resolve_databricks_staging_path(config_service)
        if resolved and not _normalize_path_value(config_service.get("kindling.temp_path")):
            config_service.set("kindling.temp_path", resolved)
        config_service.set(
            "kindling.runtime.features.databricks.any_file_required_for_bootstrap",
            not _is_volume_path(resolved),
        )
        return resolved

    return _normalize_path_value(
        config_service.get("kindling.temp_path")
        or bootstrap_config.get("kindling.temp_path")
        or bootstrap_config.get("temp_path")
    )


def _resolve_initial_download_temp_path(
    config: Dict[str, Any], platform: Optional[str]
) -> Optional[str]:
    if str(platform or "").strip().lower() == "databricks":
        return _resolve_databricks_staging_path(config)
    return _normalize_path_value(config.get("kindling.temp_path") or config.get("temp_path"))


def get_temp_path() -> str:
    """
    Get platform-specific temp directory path.

    Detects the platform and returns an appropriate temp directory:
    - Databricks: /tmp/kindling_{uuid} (writable via dbutils, readable at /dbfs/tmp/)
    - Fabric/Synapse/Standalone: tempfile.mkdtemp() result

    Returns:
        Absolute path to temp directory suitable for the current platform
    """
    storage_utils = _get_storage_utils()

    # Check if we're in Databricks by looking for DBUtils
    # Only Databricks has DBUtils - Fabric/Synapse have MsSparkUtils which also has fs
    if storage_utils:
        storage_utils_type = type(storage_utils).__name__
        is_databricks = "DBUtils" in storage_utils_type

        if is_databricks and hasattr(storage_utils, "fs"):
            # Databricks: Use DBFS temp directory
            import uuid

            temp_id = uuid.uuid4().hex[:8]
            return f"/tmp/kindling_{temp_id}"

    # Fabric/Synapse/Standalone: Use standard temp directory
    return tempfile.mkdtemp(prefix="kindling_")


def download_config_files(
    artifacts_storage_path: str,
    environment: str,
    platform: Optional[str] = None,
    workspace_id: Optional[str] = None,
    app_name: Optional[str] = None,
    temp_path: Optional[str] = None,
) -> List[str]:
    """
    Download config files from lake to temp location for Dynaconf.

    All files are optional - bootstrap config can provide everything.
    Returns list of local file paths in priority order (lowest to highest):
      1. settings.yaml (base settings)
      2. platform_{platform}.yaml (platform-specific: fabric, synapse, databricks)
      3. workspace_{workspace_id}.yaml (workspace-specific)
      4. env_{environment}.yaml (environment-specific: dev, prod, etc)
      5. data-apps/{app_name}/settings.yaml (app-specific - highest priority)

    Args:
        artifacts_storage_path: Base path for artifacts storage
        environment: Environment name (dev, prod, etc)
        platform: Platform name (fabric, synapse, databricks)
        workspace_id: Workspace ID for workspace-specific config
        app_name: Optional app name for app-specific config

    Returns:
        List of downloaded config file paths in priority order
    """
    storage_utils = _get_storage_utils()

    if not storage_utils:
        raise Exception("Storage utilities not available for config loading")

    # Strip trailing slash to prevent double slashes in path construction
    base_path = artifacts_storage_path.rstrip("/")
    _BOOTSTRAP_LOGGER.info("Using artifacts path: %s", base_path)
    if platform:
        _BOOTSTRAP_LOGGER.info("Platform: %s", platform)
    if workspace_id:
        _BOOTSTRAP_LOGGER.info("Workspace ID: %s", workspace_id)
    _BOOTSTRAP_LOGGER.info("Environment: %s", environment)

    # Get platform-specific temp path
    temp_dir = get_temp_path()

    # Detect platform by checking storage utils type (more reliable than path format)
    storage_utils = _get_storage_utils()
    storage_utils_type = type(storage_utils).__name__ if storage_utils else "None"
    is_databricks = "DBUtils" in storage_utils_type

    # Use parameter instead of trying to import
    volume_path = temp_path

    # Always create temp_path for Fabric/Synapse (used for default config if needed)
    temp_path = Path(temp_dir)

    if is_databricks:
        if volume_path:
            scoped_volume_path = _build_run_scoped_staging_path(volume_path, "config")
            _BOOTSTRAP_LOGGER.info("Using Volume path for configs: %s", scoped_volume_path)
        else:
            _BOOTSTRAP_LOGGER.info("Using DBFS temp path: %s", temp_dir)
            dbfs_temp_path = temp_dir
    else:
        _BOOTSTRAP_LOGGER.info("Using local temp path: %s", temp_dir)

    config_files = []

    # Build list of config files to download in priority order (lowest to highest)
    files_to_download = [
        (f"{base_path}/config/settings.yaml", "settings.yaml"),
    ]

    # Add platform-specific config if platform is known
    if platform:
        files_to_download.append(
            (f"{base_path}/config/platform_{platform}.yaml", f"platform_{platform}.yaml")
        )

    # Add workspace-specific config if workspace_id is known
    if workspace_id:
        files_to_download.append(
            (f"{base_path}/config/workspace_{workspace_id}.yaml", f"workspace_{workspace_id}.yaml")
        )

    # Add environment config
    files_to_download.append(
        (f"{base_path}/config/env_{environment}.yaml", f"env_{environment}.yaml")
    )

    # Add app-specific settings (highest priority from files)
    # Apps deployed with settings.yaml in their directory get those loaded last
    if app_name:
        files_to_download.append(
            (f"{base_path}/data-apps/{app_name}/settings.yaml", f"app_{app_name}_settings.yaml")
        )

    for remote_path, filename in files_to_download:
        try:
            if is_databricks:
                # Databricks: Use fs.head() + fs.put() for DBFS/Volume writes
                content = storage_utils.fs.head(remote_path, 1024 * 1024)  # 1MB max
                # Decode bytes to string
                text_content = content.decode("utf-8") if isinstance(content, bytes) else content

                if volume_path:
                    # Write to Volume (preferred - accessible as regular file)
                    volume_file = _build_run_scoped_staging_path(
                        volume_path, "config", f"{len(config_files)}_{filename}"
                    )
                    storage_utils.fs.put(volume_file, text_content, overwrite=True)
                    config_files.append(volume_file)
                    _BOOTSTRAP_LOGGER.info("Downloaded config %s to %s", filename, volume_file)
                else:
                    # Fallback to DBFS (requires /dbfs mount)
                    dbfs_file_path = f"{dbfs_temp_path}/{len(config_files)}_{filename}"
                    storage_utils.fs.put(dbfs_file_path, text_content, overwrite=True)
                    local_file_path = f"/dbfs{dbfs_file_path}"
                    config_files.append(local_file_path)
                    _BOOTSTRAP_LOGGER.info("Downloaded config %s to %s", filename, local_file_path)
            else:
                # Fabric/Synapse: Use fs.cp() with file:// prefix (works with relative lakehouse paths)
                local_path = temp_path / f"{len(config_files)}_{filename}"
                storage_utils.fs.cp(remote_path, f"file://{str(local_path)}")
                config_files.append(str(local_path))
                _BOOTSTRAP_LOGGER.info("Downloaded config %s", filename)
        except Exception as e:
            # All config files are optional - log error details for debugging
            error_msg = str(e)
            if "FileNotFoundException" in error_msg or "does not exist" in error_msg.lower():
                _BOOTSTRAP_LOGGER.debug("Optional config not found: %s", filename)
            else:
                _BOOTSTRAP_LOGGER.debug(
                    "Optional config not found: %s (%s: %s)",
                    filename,
                    type(e).__name__,
                    error_msg[:100],
                )

    if not config_files:
        _BOOTSTRAP_LOGGER.info("No YAML config files found")
        default_config = temp_path / "default_settings.yaml"
        default_config.write_text(_get_minimal_default_config())
        config_files.append(str(default_config))
        _BOOTSTRAP_LOGGER.info("Using built-in default config")

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


def _get_workspace_id_for_platform(platform: str) -> Optional[str]:
    """
    Get workspace ID based on detected platform.

    Returns:
        Workspace ID string or None if not available
    """
    if not platform:
        return None

    try:
        if platform == "fabric":
            # Try notebookutils first (Fabric)
            try:
                import notebookutils

                workspace_id = notebookutils.runtime.context.get("currentWorkspaceId")
                if workspace_id:
                    return workspace_id
            except (ImportError, Exception):
                pass

            # Try mssparkutils
            try:
                import __main__

                mssparkutils = getattr(__main__, "mssparkutils", None)
                if mssparkutils and hasattr(mssparkutils.env, "getWorkspaceId"):
                    return mssparkutils.env.getWorkspaceId()
            except Exception:
                pass

        elif platform == "synapse":
            # Synapse uses workspace name, not ID
            # We'll use the workspace name as the identifier
            try:
                spark = get_or_create_spark_session()
                workspace_name = spark.conf.get("spark.synapse.workspace.name", None)
                if workspace_name:
                    return workspace_name
            except Exception:
                pass

            # Try mssparkutils
            try:
                import __main__

                mssparkutils = getattr(__main__, "mssparkutils", None)
                if mssparkutils and hasattr(mssparkutils.env, "getWorkspaceName"):
                    return mssparkutils.env.getWorkspaceName()
            except Exception:
                pass

        elif platform == "databricks":
            # Try to get workspace ID from context
            try:
                import __main__

                dbutils = getattr(__main__, "dbutils", None)
                if dbutils:
                    try:
                        context = dbutils.entry_point.getDbutils().notebook().getContext()
                        workspace_id = context.workspaceId().get()
                        if workspace_id:
                            return workspace_id
                    except Exception:
                        pass
            except Exception:
                pass

            # Try from spark config
            try:
                spark = get_or_create_spark_session()
                workspace_url = spark.conf.get("spark.databricks.workspaceUrl", None)
                if workspace_url:
                    # Extract workspace ID from URL (e.g., "adb-123456789.azuredatabricks.net")
                    # Use the full hostname as identifier
                    return workspace_url.replace(".", "_")
            except Exception:
                pass

    except Exception as e:
        _BOOTSTRAP_LOGGER.warning("Error getting workspace ID for %s: %s", platform, e)

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
      extensions: []
      ignored_folders: []

    delta:
      access_mode: "catalog"
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
    currentLevel = config.get("log_level", None) or get_spark_log_level()
    fallback_logger = logging.getLogger("kindling.bootstrap.console")

    def should_log(level):
        level_log_rank = level_hierarchy.get(level.upper(), 2)
        current_log_rank = level_hierarchy.get(currentLevel, 2)
        return level_log_rank >= current_log_rank

    def log(level, *args):
        if should_log(level):
            fallback_logger.log(
                getattr(logging, level.upper(), logging.INFO), " ".join(map(str, args))
            )

    return type(
        "ConsoleLogger",
        (),
        {
            "debug": lambda self, *args, **kwargs: log("DEBUG", *args),
            "info": lambda self, *args, **kwargs: log("INFO", *args),
            "error": lambda self, *args, **kwargs: log("ERROR", *args),
            "warning": lambda self, *args, **kwargs: log("WARNING", *args),
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
    if config:
        explicit_platform = config.get("platform_service") or config.get("platform")
        if explicit_platform in ["databricks", "fabric", "synapse", "standalone"]:
            _BOOTSTRAP_LOGGER.debug("Platform explicitly set to: %s", explicit_platform)
            return explicit_platform

    dp = detect_platform_from_utils()
    if dp:
        _BOOTSTRAP_LOGGER.debug("Platform detected from utils: %s", dp)
        return dp

    # Only create spark session if we actually need it for detection
    spark = get_or_create_spark_session()
    # Check for Databricks first - most distinctive
    try:
        # Databricks always has this config
        if spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", None):
            return "databricks"
    except Exception:
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
    except Exception:
        pass

    if config and config.get("allow_standalone_fallback"):
        _BOOTSTRAP_LOGGER.debug("Falling back to standalone platform")
        return "standalone"

    raise RuntimeError("Unable to detect platform (not Synapse, Fabric, Databricks, or Standalone)")


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


def install_bootstrap_dependencies(logger, bootstrap_config, artifacts_storage_path=None):
    """Install packages needed for framework bootstrap

    Args:
        logger: Logger instance
        bootstrap_config: Bootstrap configuration dict
        artifacts_storage_path: Path to artifacts storage (for loading extension wheels)
    """

    def get_import_name(package_spec: str) -> str:
        """Convert pip package name to import name (dash to underscore)"""
        # Strip version specifiers
        base_package = package_spec.split(">=")[0].split("==")[0].split("[")[0].strip()
        # Convert dashes to underscores (PEP 8 convention)
        return base_package.replace("-", "_")

    def get_package_name(package_spec: str) -> str:
        """Extract base package name (with dashes) from package spec"""
        try:
            from packaging.requirements import Requirement

            return Requirement(package_spec).name
        except Exception:
            splitters = [">=", "==", "<=", "!=", "~=", ">", "<", "["]
            base = package_spec
            for splitter in splitters:
                if splitter in base:
                    base = base.split(splitter, 1)[0]
            return base.strip()

    def get_version_specifier(package_spec: str):
        """Extract version specifier from package requirement."""
        try:
            from packaging.requirements import Requirement

            return Requirement(package_spec).specifier
        except Exception:
            return None

    def get_installed_version(package_name: str) -> Optional[str]:
        """Get installed package version by distribution name."""
        try:
            from importlib import metadata as importlib_metadata

            return importlib_metadata.version(package_name)
        except Exception:
            try:
                from importlib import metadata as importlib_metadata

                return importlib_metadata.version(package_name.replace("-", "_"))
            except Exception:
                return None

    def is_package_installed(package_spec, check_version: bool = False):
        """Check if package is installed and optionally satisfies requested version."""
        try:
            package_name = get_package_name(package_spec)
            import_name = get_import_name(package_spec)
            spec = importlib.util.find_spec(import_name)
            if spec is None:
                return False

            if not check_version:
                return True

            version_specifier = get_version_specifier(package_spec)
            if not version_specifier:
                return True

            installed_version = get_installed_version(package_name)
            if not installed_version:
                return False

            return version_specifier.contains(installed_version, prereleases=True)
        except ValueError as e:
            if ".__spec__ is None" in str(e):
                return False
            return False

    def load_if_needed(package_spec):
        """Install package (from PyPI), forcing upgrade to override system packages"""
        logger.info(f"Installing package: {package_spec}")
        try:
            # Use --ignore-installed to force installation even if package exists in system site-packages
            # This is critical for platforms like Databricks where system packages may be outdated
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "--ignore-installed", package_spec]
            )
            logger.info(f"Successfully installed package: {package_spec}")

            # CRITICAL: Ensure venv site-packages takes precedence over system site-packages
            # After installing, prioritize the venv path where the package was installed
            import site

            user_site = site.getusersitepackages()
            site_packages = site.getsitepackages()

            # Insert venv paths at the BEGINNING of sys.path (before system paths)
            # This ensures our newly installed packages are found first
            for path in reversed([user_site] + site_packages):
                if path and path not in sys.path:
                    sys.path.insert(0, path)
                    logger.debug(f"Prioritized in sys.path: {path}")
                elif path and path in sys.path:
                    # Move existing path to the front
                    sys.path.remove(path)
                    sys.path.insert(0, path)
                    logger.debug(f"Moved to front of sys.path: {path}")

            # Clear any cached imports of this package to force reload from new location
            package_name = (
                package_spec.split(">")[0].split("=")[0].split("<")[0].split("!")[0].strip()
            )
            module_name = package_name.replace("-", "_")
            if module_name in sys.modules:
                logger.debug(f"Clearing cached import: {module_name}")
                del sys.modules[module_name]

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install {package_spec}: {e}")

    def load_extension_wheel(package_spec, storage_utils, artifacts_path, temp_path=None):
        """Install extension wheel from artifacts storage (like kindling-otel-azure)"""
        if is_package_installed(package_spec, check_version=True):
            import_name = get_import_name(package_spec)
            logger.info(f"Extension already installed: {import_name}")
            return

        if is_package_installed(package_spec, check_version=False):
            logger.info(f"Extension version mismatch, upgrading to satisfy: {package_spec}")

        import os
        import tempfile

        package_name = get_package_name(package_spec)
        # Convert to wheel format: kindling-otel-azure -> kindling_otel_azure
        wheel_prefix = package_name.replace("-", "_")

        # Extract version requirement if present
        version_req = None
        version_operator = None
        if ">=" in package_spec:
            version_req = package_spec.split(">=")[1].strip()
            version_operator = ">="
        elif "==" in package_spec:
            version_req = package_spec.split("==")[1].strip()
            version_operator = "=="

        base_path = artifacts_path.rstrip("/")

        logger.info(f"Installing extension from artifacts: {package_spec}")

        is_databricks = False
        wheel_filename = None
        temp_dir = None

        try:
            # Create temp directory using platform-specific path
            temp_dir = get_temp_path()

            # List directory contents to find the exact wheel filename
            # storage_utils.fs.ls() returns list of file info
            packages_dir = f"{base_path}/packages"
            logger.debug(f"Listing extension directory: {packages_dir}")

            try:
                # List all files in packages directory
                files = storage_utils.fs.ls(packages_dir)

                # Find matching wheel file
                # files is a list of FileInfo objects with .path attribute
                matching_wheels = []
                for file_info in files:
                    file_path = file_info.path if hasattr(file_info, "path") else str(file_info)
                    filename = file_path.split("/")[-1]
                    if not filename.endswith(".whl"):
                        continue

                    wheel_version = None
                    for candidate_prefix in (wheel_prefix, package_name):
                        prefix_with_dash = f"{candidate_prefix}-"
                        prefix_with_platform = f"{candidate_prefix}_"

                        if filename.startswith(prefix_with_dash):
                            remainder = filename[len(prefix_with_dash) :]
                        elif filename.startswith(prefix_with_platform):
                            remainder = filename[len(prefix_with_platform) :]
                            if "-" not in remainder:
                                continue
                            remainder = remainder.split("-", 1)[1]
                        else:
                            continue

                        if "-" not in remainder:
                            continue

                        wheel_version = remainder.split("-", 1)[0]
                        break

                    if not wheel_version:
                        continue

                    # Check version requirement if specified
                    if version_req:
                        try:
                            from packaging import version

                            wheel_ver = version.parse(wheel_version)
                            req_ver = version.parse(version_req)

                            if version_operator == ">=":
                                if wheel_ver >= req_ver:
                                    matching_wheels.append((filename, wheel_version))
                            elif version_operator == "==":
                                if wheel_ver == req_ver:
                                    matching_wheels.append((filename, wheel_version))
                        except Exception:
                            # If version parsing fails, just add it
                            matching_wheels.append((filename, wheel_version))
                    else:
                        # No version requirement, add all matching wheels
                        matching_wheels.append((filename, wheel_version))

                if not matching_wheels:
                    logger.error(f"Extension wheel not found: {package_spec}")
                    return

                # If multiple matches, use the highest version
                if len(matching_wheels) > 1:
                    try:
                        from packaging import version

                        matching_wheels.sort(key=lambda x: version.parse(x[1]), reverse=True)
                    except Exception:
                        pass  # Just use first one if sorting fails

                # Extract filename from tuple
                wheel_filename = matching_wheels[0][0]
                logger.debug(f"Found extension wheel: {wheel_filename}")

                # Download the exact wheel file
                remote_wheel_path = f"{packages_dir}/{wheel_filename}"

                # Detect platform for appropriate download method
                storage_utils_type = type(storage_utils).__name__
                is_databricks = "DBUtils" in storage_utils_type

                if is_databricks:
                    # Databricks: Use Volume or DBFS (same pattern as runtime bootstrap)
                    volume_path = temp_path

                    if volume_path and _is_volume_path(volume_path):
                        # Use Unity Catalog Volume
                        local_path = _build_run_scoped_staging_path(
                            volume_path, "extensions", wheel_filename
                        )
                        logger.debug(f"Downloading extension to volume: {local_path}")
                        storage_utils.fs.cp(remote_wheel_path, local_path, recurse=False)
                    else:
                        # Fallback to DBFS, preserving an explicit DBFS temp path when provided.
                        dbfs_target = (
                            _build_run_scoped_staging_path(
                                volume_path, "extensions", wheel_filename
                            )
                            if _is_dbfs_path(volume_path)
                            else f"dbfs:/tmp/{wheel_filename}"
                        )
                        logger.debug(f"Downloading extension to DBFS: {dbfs_target}")
                        storage_utils.fs.cp(remote_wheel_path, dbfs_target, recurse=False)
                        local_path = _dbfs_uri_to_local_path(dbfs_target)
                else:
                    # Fabric/Synapse: Use file:// with temp directory
                    local_path = os.path.join(temp_dir, wheel_filename)
                    logger.debug(f"Downloading extension: {remote_wheel_path}")
                    storage_utils.fs.cp(remote_wheel_path, f"file://{local_path}")

                if not os.path.exists(local_path):
                    logger.error(f"Failed to download extension wheel: {package_spec}")
                    return

                logger.debug(f"Downloaded wheel ({os.path.getsize(local_path)} bytes)")

                # Install the downloaded wheel
                logger.info(f"Installing {wheel_filename}")
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pip",
                        "install",
                        "--ignore-installed",
                        "--upgrade",
                        "--upgrade-strategy",
                        "eager",  # Upgrade dependencies too (e.g., typing-extensions)
                        local_path,
                        "--disable-pip-version-check",
                    ],
                    capture_output=True,
                    text=True,
                )

                if result.returncode == 0:
                    # Mirror load_if_needed(): make the freshly installed user/venv packages
                    # win over platform system packages and drop cached modules that may
                    # still point at the old Databricks runtime copy.
                    import site

                    user_site = site.getusersitepackages()
                    site_packages = site.getsitepackages()

                    for path in reversed([user_site] + site_packages):
                        if path and path not in sys.path:
                            sys.path.insert(0, path)
                            logger.debug(f"Prioritized in sys.path: {path}")
                        elif path and path in sys.path:
                            sys.path.remove(path)
                            sys.path.insert(0, path)
                            logger.debug(f"Moved to front of sys.path: {path}")

                    stale_modules = [
                        get_import_name(package_spec),
                        "typing_extensions",
                        "azure",
                        "azure.monitor",
                        "azure.monitor.opentelemetry",
                        "opentelemetry",
                    ]
                    for module_name in stale_modules:
                        if module_name in sys.modules:
                            logger.debug(f"Clearing cached import: {module_name}")
                            del sys.modules[module_name]

                    logger.info(f"Successfully installed extension: {package_spec}")
                else:
                    logger.error(f"Failed to install extension {package_spec}: {result.stderr}")
                    logger.debug(f"Extension install stdout: {result.stdout}")

            except Exception as e:
                logger.error(f"Failed to find extension wheel {package_spec}: {e}")
                return
            finally:
                # Cleanup
                if is_databricks and wheel_filename:
                    # Clean up from volume or DBFS
                    try:
                        volume_path = temp_path
                        if volume_path and _is_volume_path(volume_path):
                            cleanup_path = _build_run_scoped_staging_path(
                                volume_path, "extensions", wheel_filename
                            )
                            storage_utils.fs.rm(cleanup_path)
                            logger.debug("Cleaned up volume extension file")
                        else:
                            cleanup_path = (
                                _build_run_scoped_staging_path(
                                    volume_path, "extensions", wheel_filename
                                )
                                if _is_dbfs_path(volume_path)
                                else f"dbfs:/tmp/{wheel_filename}"
                            )
                            storage_utils.fs.rm(cleanup_path)
                            logger.debug("Cleaned up DBFS extension file")
                    except Exception as e:
                        logger.warning(f"Could not clean up temp file: {e}")
                elif temp_dir:
                    # Clean up temp directory for Fabric/Synapse
                    import shutil

                    shutil.rmtree(temp_dir, ignore_errors=True)

        except Exception as e:
            logger.error(f"Failed to install extension {package_spec}: {e}")

    # Install required packages (dependencies - no import, from PyPI)
    required = bootstrap_config.get("required_packages", [])
    logger.info(f"Required packages: {required}")
    for package in required:
        load_if_needed(package)

    # Install and import extensions (Kindling extensions from artifacts storage)
    extensions = bootstrap_config.get("extensions", [])
    logger.info(f"Extensions to load: {extensions}")

    if extensions and artifacts_storage_path:
        # Get storage utils for downloading extension wheels
        storage_utils = _get_storage_utils()
        if not storage_utils:
            logger.warning("Storage utilities not available for extension loading")
        else:
            for extension in extensions:
                # Install the extension from artifacts storage
                logger.info(f"Loading extension: {extension}")
                load_extension_wheel(
                    extension,
                    storage_utils,
                    artifacts_storage_path,
                    temp_path=bootstrap_config.get("temp_path"),
                )

                # Import to trigger @GlobalInjector.singleton_autobind() decorators
                import_name = get_import_name(extension)
                logger.debug(f"Importing extension module: {import_name}")
                try:
                    importlib.import_module(import_name)
                    logger.info(f"Loaded extension: {import_name}")
                except ImportError as e:
                    logger.warning(f"Failed to import extension {import_name}: {e}")
    elif extensions:
        logger.warning("Extensions specified but no artifacts_storage_path for loading wheels")


PLATFORM_EP_GROUP = "spark_kindling.platforms"


def _registered_platform_entry_points():
    """Return {name: EntryPoint} for platforms advertised by installed distributions.

    Platforms register themselves by adding an entry point in the
    ``spark_kindling.platforms`` group pointing at the module that defines the
    ``@PlatformServices.register(name=...)`` factory. Third-party distributions
    can contribute additional platforms the same way without modifying core.
    """
    from importlib.metadata import entry_points

    return {ep.name: ep for ep in entry_points(group=PLATFORM_EP_GROUP)}


def initialize_platform_services(platform, config, logger):
    """Load the platform module, then instantiate and register its service.

    Discovery uses ``importlib.metadata`` entry points in the
    ``spark_kindling.platforms`` group. If the platform isn't advertised via
    entry points, falls back to the legacy ``kindling.platform_{name}``
    module-name convention so in-tree/core platforms still work during
    development or when running from an uninstalled distribution.
    Third-party platforms should register entry points rather than relying
    on this fallback — ``kindling`` is a regular package (not a namespace
    package), so external platform modules cannot plug into
    ``kindling.platform_*`` from outside this distribution.

    On a missing-extras ImportError, surfaces an actionable message telling
    the user which extra to install.
    """
    import importlib

    eps = _registered_platform_entry_points()
    ep = eps.get(platform)

    try:
        if ep is not None:
            ep.load()  # imports the module, triggering @PlatformServices.register
            logger.info(f"Loaded platform module via entry point: {ep.value}")
        else:
            module_name = f"kindling.platform_{platform}"
            importlib.import_module(module_name)
            logger.info(
                f"Loaded platform module by convention: {module_name} "
                f"(no entry point registered; known: {sorted(eps)})"
            )
    except ImportError as exc:
        raise RuntimeError(
            f"Platform '{platform}' is selected but its runtime dependencies "
            f"are not installed. Install with: "
            f"pip install 'spark-kindling[{platform}]'"
        ) from exc

    definition = PlatformServices.get_service_definition(platform)
    if definition is None:
        raise RuntimeError(
            f"Platform module for '{platform}' was loaded but did not register "
            f"a service. Expected @PlatformServices.register(name='{platform}') "
            f"on a factory function in the module."
        )

    try:
        svc = definition.factory(config, logger)
    except ImportError as exc:
        raise RuntimeError(
            f"Platform '{platform}' service construction failed: a required "
            f"dependency is missing. Install with: "
            f"pip install 'spark-kindling[{platform}]'"
        ) from exc

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

    config = _merge_with_spark_kindling_config(dict(config or {}))
    if app_name is None:
        app_name = config.get("app_name")

    # Check if framework is already initialized
    if is_framework_initialized():
        _BOOTSTRAP_LOGGER.debug("Framework already initialized, skipping re-initialization")
        existing_service = get_kindling_service(PlatformServiceProvider).get_service()
        return existing_service

    # Extract bootstrap settings
    artifacts_storage_path = config.get("artifacts_storage_path")
    explicit_platform = config.get("platform_environment") or config.get("platform")
    is_standalone = str(explicit_platform or "").strip().lower() == "standalone"
    use_lake_packages = config.get("use_lake_packages", False if is_standalone else True)
    environment = config.get("environment", "development")
    explicit_config_files = config.get("config_files")
    if explicit_config_files is None:
        config_files = None
    elif isinstance(explicit_config_files, (str, Path)):
        config_files = [str(explicit_config_files)]
    else:
        config_files = [str(path) for path in explicit_config_files]

    # Early platform detection for config loading
    platform = None
    workspace_id = None
    if explicit_platform:
        try:
            platform = detect_platform({"platform_service": explicit_platform})
        except Exception as e:
            _BOOTSTRAP_LOGGER.warning(
                "Could not resolve explicit platform %r: %s", explicit_platform, e
            )
    if use_lake_packages and artifacts_storage_path:
        try:
            # Detect platform early to load platform-specific config
            platform = detect_platform(config={"platform_service": explicit_platform})

            # Get workspace ID based on platform
            workspace_id = _get_workspace_id_for_platform(platform)
        except Exception as e:
            _BOOTSTRAP_LOGGER.warning(
                "Could not detect platform/workspace for config loading: %s", e
            )
            # Continue without platform/workspace-specific configs

    if use_lake_packages and artifacts_storage_path:
        initial_temp_path = _resolve_initial_download_temp_path(config, platform)
        downloaded_config_files = download_config_files(
            artifacts_storage_path=artifacts_storage_path,
            environment=environment,
            platform=platform,
            workspace_id=workspace_id,
            app_name=app_name,
            temp_path=initial_temp_path,
        )
        if config_files:
            config_files = downloaded_config_files + config_files
        else:
            config_files = downloaded_config_files

    from kindling.spark_config import configure_injector_with_config

    # Check if ConfigService is already properly initialized
    try:
        config_service = get_kindling_service(ConfigService)
        # Check if it's actually initialized (dynaconf will be None if not)
        if hasattr(config_service, "dynaconf") and config_service.dynaconf is not None:
            _BOOTSTRAP_LOGGER.debug("Config service already initialized, skipping configuration")
        else:
            # ConfigService exists but not initialized yet, set it up
            injector = configure_injector_with_config(
                config_files=config_files,
                initial_config=config,  # BOOTSTRAP_CONFIG as overrides
                environment=environment,
                artifacts_storage_path=artifacts_storage_path,
                platform=platform,
                workspace_id=workspace_id,
                app_name=app_name,
            )
            config_service = get_kindling_service(ConfigService)
    except Exception:
        # ConfigService doesn't exist yet, set it up
        injector = configure_injector_with_config(
            config_files=config_files,
            initial_config=config,  # BOOTSTRAP_CONFIG as overrides
            environment=environment,
            artifacts_storage_path=artifacts_storage_path,
            platform=platform,
            workspace_id=workspace_id,
            app_name=app_name,
        )
        config_service = get_kindling_service(ConfigService)

    logger = get_kindling_service(PythonLoggerProvider).get_logger("KindlingBootstrap")
    logger.info("Starting framework initialization")

    # Best-effort runtime feature discovery. These values are written into
    # kindling.runtime.features.* and can be overridden by kindling.features.*.
    try:
        from kindling.features import discover_runtime_features

        discover_runtime_features(config_service, logger=logger)
    except Exception as feature_error:
        logger.debug(f"Runtime feature discovery failed (non-fatal): {feature_error}")

    # Helpful diagnostics for system tests / feature-gated behavior.
    # These keys are safe to log (no secrets) and explain why certain Delta operations
    # (like CLUSTER BY AUTO) may be enabled/disabled on a given runtime.
    try:
        dbr_version = config_service.get("kindling.runtime.features.databricks.runtime_version")
        dbr_uc_enabled = config_service.get("kindling.runtime.features.databricks.uc_enabled")
        dbr_volumes_enabled = config_service.get(
            "kindling.runtime.features.databricks.volumes_enabled"
        )
        dbr_any_file = config_service.get(
            "kindling.runtime.features.databricks.any_file_required_for_bootstrap"
        )
        auto_clustering = config_service.get("kindling.runtime.features.delta.auto_clustering")
        auto_clustering_static = config_service.get("kindling.features.delta.auto_clustering")
        logger.info(
            "Runtime features: "
            f"databricks.runtime_version={dbr_version!r} "
            f"databricks.uc_enabled={dbr_uc_enabled!r} "
            f"databricks.volumes_enabled={dbr_volumes_enabled!r} "
            f"databricks.any_file_required_for_bootstrap={dbr_any_file!r} "
            f"delta.auto_clustering={auto_clustering!r} "
            f"(static_override={auto_clustering_static!r})"
        )
    except Exception:
        pass

    try:
        # Read from general kindling config (not bootstrap namespace)
        # bootstrap is just for temp backwards compatibility mapping
        required_packages = config_service.get("kindling.required_packages", [])
        extensions = config_service.get("kindling.extensions", [])

        # Deduplicate extensions - prefer versioned specs over plain names
        # e.g., keep "kindling-otel-azure>=0.3.0a1" instead of "kindling-otel-azure"
        if extensions:
            ext_dict = {}
            for ext in extensions:
                # Extract package name (before any version operator)
                pkg_name = ext.split(">")[0].split("=")[0].split("<")[0].split("!")[0].strip()

                # Check if this extension has a version spec
                has_version = any(op in ext for op in [">=", "<=", "==", "!=", ">", "<", "~="])

                if pkg_name in ext_dict:
                    # If we already have this package, prefer the versioned one
                    existing_has_version = any(
                        op in ext_dict[pkg_name] for op in [">=", "<=", "==", "!=", ">", "<", "~="]
                    )
                    if has_version and not existing_has_version:
                        # Replace plain with versioned
                        ext_dict[pkg_name] = ext
                    # If both versioned or both plain, keep first one
                else:
                    ext_dict[pkg_name] = ext

            extensions = list(ext_dict.values())

        # Build bootstrap config including temp_path from original config
        resolved_temp_path = _resolve_bootstrap_temp_path(config_service, config)
        bootstrap_deps_config = {
            "required_packages": required_packages,
            "extensions": extensions,
            # Resolve a governed staging path when the runtime supports it.
            "temp_path": resolved_temp_path,
        }

        if resolved_temp_path:
            logger.info(f"Bootstrap staging path: {resolved_temp_path}")
        else:
            logger.info("Bootstrap staging path: default platform fallback")

        # Check for explicit platform in config (supports both nested and flat config keys)
        explicit_platform = (
            config_service.get("kindling.platform.environment")
            or config.get("platform_environment")
            or config.get("platform")
        )
        platform = detect_platform(
            config={
                "platform_service": explicit_platform,
                "allow_standalone_fallback": is_standalone,
            }
        )
        logger.info(f"Platform: {platform}")

        install_bootstrap_dependencies_flag = config.get("install_bootstrap_dependencies")
        if install_bootstrap_dependencies_flag is None:
            install_bootstrap_dependencies_flag = platform != "standalone"

        if install_bootstrap_dependencies_flag:
            install_bootstrap_dependencies(
                logger,
                bootstrap_deps_config,
                artifacts_storage_path=artifacts_storage_path,
            )
        else:
            logger.info("Skipping bootstrap dependency installation")

        platformservice = initialize_platform_services(platform, config_service, logger)
        logger.info("Platform services initialized")

        if platform == "standalone":
            from kindling.watermarking import (
                NullWatermarkEntityFinder,
                WatermarkEntityFinder,
            )

            existing_binding = GlobalInjector.get_injector().binder._bindings.get(
                WatermarkEntityFinder
            )
            if existing_binding is None:
                # [implementer] make standalone DI graph constructible without app watermark boilerplate — TASK-20260430-002
                GlobalInjector.bind(WatermarkEntityFinder, NullWatermarkEntityFinder)
                logger.debug("Bound NullWatermarkEntityFinder for standalone platform")

        # Resolve any @secret references now that platform services are available.
        try:
            from kindling.config_loaders import load_secrets_from_provider

            if hasattr(config_service, "dynaconf") and config_service.dynaconf is not None:
                load_secrets_from_provider(config_service.dynaconf, silent=True)
                logger.debug("Resolved @secret references with platform secret provider")
        except Exception as secret_resolution_error:
            logger.warning(f"Secret resolution pass failed: {secret_resolution_error}")

        load_local_default = False if platform == "standalone" else True
        load_local_value = config_service.get("kindling.bootstrap.load_local", load_local_default)
        logger.debug(
            f"kindling.bootstrap.load_local = {load_local_value} "
            f"(type: {type(load_local_value).__name__})"
        )

        load_local_flat = config_service.get("load_local_packages", "NOT_SET")
        logger.debug(
            f"load_local_packages (flat) = {load_local_flat} "
            f"(type: {type(load_local_flat).__name__})"
        )

        if load_local_value:
            ignored_folders = config_service.get("kindling.bootstrap.ignored_folders", [])
            workspace_packages = get_kindling_service(NotebookManager).get_all_packages(
                ignored_folders=ignored_folders
            )
            logger.debug(
                f"Found {len(workspace_packages)} workspace packages: {workspace_packages}"
            )
            load_workspace_packages(platformservice, workspace_packages, logger)
            logger.info(f"Loaded {len(workspace_packages)} workspace packages")
        else:
            logger.info("Skipping workspace package loading (load_local=False)")

        logger.info("Framework initialization complete")

        if app_name:
            logger.info(f"Auto-running app: {app_name}")
            try:
                runner = get_kindling_service(DataAppRunner)
                logger.debug(f"Got DataAppRunner: {type(runner).__name__}")
                runner.run_app(app_name)
                logger.info(f"App '{app_name}' completed successfully")
            except Exception as app_error:
                logger.exception(f"App execution failed: {str(app_error)}")
                raise

        return platformservice

    except Exception as e:
        logger.error(f"Framework initialization failed: {str(e)}")
        raise


def bootstrap_framework(config: Dict[str, Any], logger=None):
    """Backward compatibility wrapper"""
    return initialize_framework(config)
