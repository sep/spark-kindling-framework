import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    print(f"Merged {len(spark_kindling_config)} SparkConf settings from spark.kindling.*")
    return merged


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
    print(f"Using artifacts path: {base_path}")
    if platform:
        print(f"Platform: {platform}")
    if workspace_id:
        print(f"Workspace ID: {workspace_id}")
    print(f"Environment: {environment}")

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
            print(f"Using Volume path for configs: {volume_path}")
        else:
            print(f"Using DBFS temp path: {temp_dir}")
            dbfs_temp_path = temp_dir
    else:
        print(f"Using local temp path: {temp_dir}")

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
                    volume_file = f"{volume_path.rstrip('/')}/{len(config_files)}_{filename}"
                    storage_utils.fs.put(volume_file, text_content, overwrite=True)
                    config_files.append(volume_file)
                    print(f"✓ Downloaded: {filename} to {volume_file}")
                else:
                    # Fallback to DBFS (requires /dbfs mount)
                    dbfs_file_path = f"{dbfs_temp_path}/{len(config_files)}_{filename}"
                    storage_utils.fs.put(dbfs_file_path, text_content, overwrite=True)
                    local_file_path = f"/dbfs{dbfs_file_path}"
                    config_files.append(local_file_path)
                    print(f"✓ Downloaded: {filename} to {local_file_path}")
            else:
                # Fabric/Synapse: Use fs.cp() with file:// prefix (works with relative lakehouse paths)
                local_path = temp_path / f"{len(config_files)}_{filename}"
                storage_utils.fs.cp(remote_path, f"file://{str(local_path)}")
                config_files.append(str(local_path))
                print(f"✓ Downloaded: {filename}")
        except Exception as e:
            # All config files are optional - log error details for debugging
            error_msg = str(e)
            if "FileNotFoundException" in error_msg or "does not exist" in error_msg.lower():
                print(f"  (Optional config not found: {filename})")
            else:
                print(f"  (Optional config not found: {filename})")
                print(f"  Debug: {type(e).__name__}: {error_msg[:100]}")

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
        print(f"⚠️  Error getting workspace ID for {platform}: {e}")

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
        print(f"📥 Installing package: {package_spec}")
        logger.info(f"Installing package: {package_spec}")
        try:
            # Use --ignore-installed to force installation even if package exists in system site-packages
            # This is critical for platforms like Databricks where system packages may be outdated
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "--ignore-installed", package_spec]
            )
            print(f"✅ Successfully installed package: {package_spec}")
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
                    print(f"   📌 Prioritized in sys.path: {path}")
                elif path and path in sys.path:
                    # Move existing path to the front
                    sys.path.remove(path)
                    sys.path.insert(0, path)
                    print(f"   📌 Moved to front of sys.path: {path}")

            # Clear any cached imports of this package to force reload from new location
            package_name = (
                package_spec.split(">")[0].split("=")[0].split("<")[0].split("!")[0].strip()
            )
            module_name = package_name.replace("-", "_")
            if module_name in sys.modules:
                print(f"   🔄 Clearing cached import: {module_name}")
                del sys.modules[module_name]

        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install {package_spec}: {e}")
            logger.error(f"Failed to install {package_spec}: {e}")

    def load_extension_wheel(package_spec, storage_utils, artifacts_path, temp_path=None):
        """Install extension wheel from artifacts storage (like kindling-otel-azure)"""
        if is_package_installed(package_spec, check_version=True):
            import_name = get_import_name(package_spec)
            print(f"✓ Extension already installed: {import_name}")
            logger.info(f"Extension already installed: {import_name}")
            return

        if is_package_installed(package_spec, check_version=False):
            print(f"↻ Extension installed but version does not satisfy '{package_spec}', upgrading")
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

        print(f"📥 Installing extension from artifacts: {package_spec}")
        logger.info(f"Installing extension from artifacts: {package_spec}")

        try:
            # Create temp directory using platform-specific path
            temp_dir = get_temp_path()

            # List directory contents to find the exact wheel filename
            # storage_utils.fs.ls() returns list of file info
            packages_dir = f"{base_path}/packages"
            print(f"   Listing directory: {packages_dir}")

            try:
                # List all files in packages directory
                files = storage_utils.fs.ls(packages_dir)

                # Find matching wheel file
                # files is a list of FileInfo objects with .path attribute
                matching_wheels = []
                for file_info in files:
                    file_path = file_info.path if hasattr(file_info, "path") else str(file_info)
                    filename = file_path.split("/")[-1]

                    # Match: kindling_otel_azure-*.whl or kindling-otel-azure-*.whl
                    if (
                        filename.startswith(wheel_prefix) or filename.startswith(package_name)
                    ) and filename.endswith(".whl"):

                        # Extract version from wheel filename
                        # Format: package_name-version-py3-none-any.whl
                        if filename.startswith(wheel_prefix):
                            # kindling_otel_azure-0.1.0-py3-none-any.whl -> 0.1.0
                            parts = filename[len(wheel_prefix) + 1 :].split("-")
                        else:
                            # kindling-otel-azure-0.1.0-py3-none-any.whl -> 0.1.0
                            parts = filename[len(package_name) + 1 :].split("-")

                        if parts:
                            wheel_version = parts[0]

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
                    print(
                        f"❌ No wheel found matching '{wheel_prefix}' or '{package_name}' in {packages_dir}"
                    )
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
                print(f"   Found wheel: {wheel_filename}")

                # Download the exact wheel file
                remote_wheel_path = f"{packages_dir}/{wheel_filename}"

                # Detect platform for appropriate download method
                storage_utils_type = type(storage_utils).__name__
                is_databricks = "DBUtils" in storage_utils_type

                if is_databricks:
                    # Databricks: Use Volume or DBFS (same pattern as runtime bootstrap)
                    volume_path = temp_path

                    if volume_path:
                        # Use Unity Catalog Volume
                        local_path = f"{volume_path.rstrip('/')}/{wheel_filename}"
                        print(f"   Downloading to volume: {local_path}")
                        storage_utils.fs.cp(remote_wheel_path, local_path, recurse=False)
                    else:
                        # Fallback to DBFS
                        dbfs_path = f"/tmp/{wheel_filename}"
                        print(f"   Downloading to DBFS: dbfs:{dbfs_path}")
                        storage_utils.fs.cp(remote_wheel_path, f"dbfs:{dbfs_path}", recurse=False)
                        local_path = f"/dbfs{dbfs_path}"
                else:
                    # Fabric/Synapse: Use file:// with temp directory
                    local_path = os.path.join(temp_dir, wheel_filename)
                    print(f"   Downloading: {remote_wheel_path}")
                    storage_utils.fs.cp(remote_wheel_path, f"file://{local_path}")

                if not os.path.exists(local_path):
                    print(f"❌ Failed to download wheel to {local_path}")
                    logger.error(f"Failed to download extension wheel: {package_spec}")
                    return

                print(f"   ✅ Downloaded wheel ({os.path.getsize(local_path)} bytes)")

                # Install the downloaded wheel
                print(f"📦 Installing {wheel_filename}...")
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pip",
                        "install",
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
                    print(f"✅ Successfully installed extension: {package_spec}")
                    logger.info(f"Successfully installed extension: {package_spec}")
                else:
                    print(f"❌ Failed to install extension {package_spec}")
                    print(f"   stdout: {result.stdout}")
                    print(f"   stderr: {result.stderr}")
                    logger.error(f"Failed to install extension {package_spec}: {result.stderr}")

            except Exception as e:
                print(f"❌ Failed to list or download extension wheel: {e}")
                logger.error(f"Failed to find extension wheel {package_spec}: {e}")
                return
            finally:
                # Cleanup
                if is_databricks:
                    # Clean up from volume or DBFS
                    try:
                        volume_path = temp_path
                        if volume_path:
                            cleanup_path = f"{volume_path.rstrip('/')}/{wheel_filename}"
                            storage_utils.fs.rm(cleanup_path)
                            print(f"   🧹 Cleaned up volume file")
                        else:
                            dbfs_path = f"/tmp/{wheel_filename}"
                            storage_utils.fs.rm(f"dbfs:{dbfs_path}")
                            print(f"   🧹 Cleaned up DBFS file")
                    except Exception as e:
                        print(f"   ⚠️  Could not clean up temp file: {e}")
                else:
                    # Clean up temp directory for Fabric/Synapse
                    import shutil

                    shutil.rmtree(temp_dir, ignore_errors=True)

        except Exception as e:
            print(f"❌ Failed to install extension {package_spec}: {e}")
            logger.error(f"Failed to install extension {package_spec}: {e}")

    # Install required packages (dependencies - no import, from PyPI)
    required = bootstrap_config.get("required_packages", [])
    print(f"📦 Required packages: {required}")
    logger.info(f"Required packages: {required}")
    for package in required:
        load_if_needed(package)

    # Install and import extensions (Kindling extensions from artifacts storage)
    extensions = bootstrap_config.get("extensions", [])
    print(f"🔌 Extensions to load: {extensions}")
    logger.info(f"Extensions to load: {extensions}")

    if extensions and artifacts_storage_path:
        # Get storage utils for downloading extension wheels
        storage_utils = _get_storage_utils()
        if not storage_utils:
            print(
                "⚠️  Warning: Storage utilities not available, cannot load extensions from artifacts"
            )
            logger.warning("Storage utilities not available for extension loading")
        else:
            for extension in extensions:
                # Install the extension from artifacts storage
                print(f"🔌 Loading extension: {extension}")
                load_extension_wheel(
                    extension,
                    storage_utils,
                    artifacts_storage_path,
                    temp_path=bootstrap_config.get("temp_path"),
                )

                # Import to trigger @GlobalInjector.singleton_autobind() decorators
                import_name = get_import_name(extension)
                print(f"📦 Importing extension module: {import_name}")
                try:
                    importlib.import_module(import_name)
                    print(f"✅ Loaded extension: {import_name}")
                    logger.info(f"Loaded extension: {import_name}")
                except ImportError as e:
                    print(f"❌ Failed to import extension {import_name}: {e}")
                    logger.warning(f"Failed to import extension {import_name}: {e}")
    elif extensions:
        print("⚠️  Warning: Extensions specified but no artifacts_storage_path provided")
        logger.warning("Extensions specified but no artifacts_storage_path for loading wheels")


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

    config = _merge_with_spark_kindling_config(dict(config or {}))
    if app_name is None:
        app_name = config.get("app_name")

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

    # Early platform detection for config loading
    platform = None
    workspace_id = None
    if use_lake_packages and artifacts_storage_path:
        try:
            # Detect platform early to load platform-specific config
            explicit_platform = config.get("platform_environment") or config.get("platform")
            platform = detect_platform(config={"platform_service": explicit_platform})

            # Get workspace ID based on platform
            workspace_id = _get_workspace_id_for_platform(platform)
        except Exception as e:
            print(f"⚠️  Could not detect platform/workspace for config loading: {e}")
            # Continue without platform/workspace-specific configs

    config_files = None
    if use_lake_packages and artifacts_storage_path:
        config_files = download_config_files(
            artifacts_storage_path=artifacts_storage_path,
            environment=environment,
            platform=platform,
            workspace_id=workspace_id,
            app_name=app_name,
            temp_path=config.get("kindling.temp_path") or config.get("temp_path"),
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
                platform=platform,
                workspace_id=workspace_id,
                app_name=app_name,
            )
            config_service = get_kindling_service(ConfigService)
    except:
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

        # Debug: Print entire kindling config section
        print(f"🔍 DEBUG: Full kindling config section:")
        try:
            kindling_config = config_service.get("kindling", {})
            print(f"   Keys in kindling config: {list(kindling_config.keys())}")
            if "extensions" in kindling_config:
                print(f"   kindling.extensions value: {kindling_config['extensions']}")
            else:
                print(f"   ❌ 'extensions' key NOT FOUND in kindling config")
        except Exception as e:
            print(f"   Error reading kindling config: {e}")

        print(f"📦 Required packages: {required_packages}")
        print(f"🔌 Extensions: {extensions}")

        # Build bootstrap config including temp_path from original config
        bootstrap_deps_config = {
            "required_packages": required_packages,
            "extensions": extensions,
            # Pass through from original config (generic temp_path for all platforms)
            "temp_path": config.get("kindling.temp_path") or config.get("temp_path"),
        }

        install_bootstrap_dependencies(
            logger,
            bootstrap_deps_config,
            artifacts_storage_path=artifacts_storage_path,
        )

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
        print(
            f"🔍 DEBUG: kindling.bootstrap.load_local = {load_local_value} (type: {type(load_local_value).__name__})"
        )
        logger.info(
            f"DEBUG: kindling.bootstrap.load_local = {load_local_value} (type: {type(load_local_value).__name__})"
        )

        # Also check the original flat key
        load_local_flat = config_service.get("load_local_packages", "NOT_SET")
        print(
            f"🔍 DEBUG: load_local_packages (flat) = {load_local_flat} (type: {type(load_local_flat).__name__})"
        )
        logger.info(
            f"DEBUG: load_local_packages (flat) = {load_local_flat} (type: {type(load_local_flat).__name__})"
        )

        if load_local_value:
            print(f"📦 Loading workspace packages (load_local=True)...")
            ignored_folders = config_service.get("kindling.bootstrap.ignored_folders", [])
            print(f"📦 Getting all packages from NotebookManager...")
            workspace_packages = get_kindling_service(NotebookManager).get_all_packages(
                ignored_folders=ignored_folders
            )
            print(f"📦 Found {len(workspace_packages)} workspace packages: {workspace_packages}")
            load_workspace_packages(platformservice, workspace_packages, logger)
            logger.info(f"Loaded {len(workspace_packages)} workspace packages")
        else:
            print(f"⏭️  Skipping workspace package loading (load_local=False)")
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
