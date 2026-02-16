import importlib
import os
import subprocess
import sys
import json
from datetime import datetime
from typing import Any, Dict, Union

try:
    from packaging.version import InvalidVersion, Version
except ImportError:
    # Fallback if packaging not available (shouldn't happen in modern Python)
    Version = None
    InvalidVersion = None


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
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if not spark:
            return {}

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

# DEBUG: Track script execution to detect duplicate runs
now = datetime.now()
execution_id = now.strftime("%Y%m%d_%H%M%S_%f")
timestamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
print(f"=" * 80)
print(f"🔍 BOOTSTRAP SCRIPT STARTING - {timestamp}")
print(f"   Execution ID: {execution_id}")
print(f"   Script: {__file__ if '__file__' in globals() else 'unknown'}")
print(f"   PID: {os.getpid()}")
print(f"   Args: {sys.argv}")
print(f"=" * 80)

try:
    import __main__

    if hasattr(__main__, "BOOTSTRAP_CONFIG"):
        # Already defined in notebook, don't parse command line
        BOOTSTRAP_CONFIG = __main__.BOOTSTRAP_CONFIG
        print(f"Using BOOTSTRAP_CONFIG from notebook context")
    else:
        raise AttributeError("Not in notebook context")
except (ImportError, AttributeError):
    # Not in notebook context, parse command line args
    if len(sys.argv) > 1:
        parsed_config = {}
        for arg in sys.argv[1:]:
            if arg.startswith("config:"):
                # Remove 'config:' prefix and split on '='
                key_value = arg[7:]  # Skip 'config:'
                if "=" in key_value:
                    key, value = key_value.split("=", 1)
                    # Try to parse as JSON for complex values, otherwise string
                    try:
                        import json

                        parsed_config[key] = json.loads(value)
                    except:
                        # Convert boolean strings to actual booleans
                        if value.lower() in ("true", "false"):
                            parsed_config[key] = value.lower() == "true"
                        else:
                            parsed_config[key] = value

        # Only set BOOTSTRAP_CONFIG if we found config args
        if parsed_config:
            BOOTSTRAP_CONFIG = parsed_config
            print(f"Parsed BOOTSTRAP_CONFIG from command line: {BOOTSTRAP_CONFIG}")
            # DEBUG: Show types for boolean-like values
            for key, value in parsed_config.items():
                if isinstance(value, bool) or str(value).lower() in ("true", "false"):
                    print(f"  DEBUG: {key} = {value} (type: {type(value).__name__})")


spark_kindling_config = _get_spark_kindling_config()
if spark_kindling_config:
    print(f"Loaded {len(spark_kindling_config)} settings from SparkConf (spark.kindling.*)")

if "BOOTSTRAP_CONFIG" in globals() and isinstance(BOOTSTRAP_CONFIG, dict):
    merged_config = dict(spark_kindling_config)
    merged_config.update(BOOTSTRAP_CONFIG)
    BOOTSTRAP_CONFIG = merged_config
elif spark_kindling_config:
    BOOTSTRAP_CONFIG = spark_kindling_config
    print("Using SparkConf-derived BOOTSTRAP_CONFIG")


def get_storage_utils():
    """Get platform storage utilities with proper fallback chain"""
    import __main__

    # First check __main__ (most reliable when available)
    if hasattr(__main__, "mssparkutils") and getattr(__main__, "mssparkutils") is not None:
        return getattr(__main__, "mssparkutils")

    if hasattr(__main__, "dbutils") and getattr(__main__, "dbutils") is not None:
        return getattr(__main__, "dbutils")

    # Fallback to explicit imports if not in __main__
    try:
        from notebookutils import mssparkutils

        return mssparkutils
    except ImportError:
        pass

    try:
        # For Synapse, might be different import path
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark and hasattr(spark, "sparkContext"):
            sc = spark.sparkContext
            if hasattr(sc, "_jvm"):
                # Try to get mssparkutils from JVM
                try:
                    return sc._jvm.com.microsoft.spark.notebook.MSSparkUtils
                except:
                    pass
    except:
        pass

    try:
        import pyspark.dbutils as dbutils

        return dbutils
    except ImportError:
        pass

    return None


def detect_platform() -> str:
    """Platform detection with config override"""
    # Check if platform is explicitly provided in parsed config
    if "platform" in BOOTSTRAP_CONFIG:
        platform = BOOTSTRAP_CONFIG["platform"].lower()
        print(f"Platform from config: {platform}")
        return platform

    # Fallback to detection logic only if not in config
    print("Platform not in config, detecting...")
    try:
        storage_utils = get_storage_utils()

        if storage_utils is None:
            return "unknown"

        # Check the type/source of storage utilities to determine platform
        utils_str = str(type(storage_utils)).lower()

        if "dbutils" in utils_str:
            return "databricks"
        elif "mssparkutils" in utils_str or "notebookutils" in utils_str:
            # Check environment to distinguish Fabric vs Synapse
            if "fabric" in str(os.environ).lower():
                return "fabric"
            return "synapse"

        return "unknown"
    except Exception:
        return "unknown"


def load_config(
    config_or_filename: Union[Dict, str], artifacts_path: str, storage_utils
) -> Dict[str, Any]:
    """Load config from dict or filename relative to artifacts_path/config/ with environment fallback"""
    if isinstance(config_or_filename, dict):
        return config_or_filename

    if not storage_utils:
        raise Exception("Storage utils required for config file loading")

    # First load base config to get environment setting
    base_config_path = f"{artifacts_path}/config/{config_or_filename}"

    try:
        print(f"Loading base config: {base_config_path}")
        content = storage_utils.fs.head(base_config_path, max_bytes=1024 * 1024).decode("utf-8")
    except Exception as e:
        raise Exception(f"Failed to load base config from {base_config_path}: {str(e)}")

    # Parse base config
    try:
        import yaml

        base_config = yaml.safe_load(content)
    except ImportError:
        import json

        base_config = json.loads(content)

    # Check if environment is specified in base config
    environment = base_config.get("environment")
    if environment:
        base_name = config_or_filename.replace(".yaml", "").replace(".json", "")
        env_filename = f"{base_name}.{environment}.yaml"
        env_config_path = f"{artifacts_path}/config/{env_filename}"

        try:
            print(f"Trying environment-specific config: {env_config_path}")
            env_content = storage_utils.fs.head(env_config_path, max_bytes=1024 * 1024).decode(
                "utf-8"
            )
            print(f"Loaded environment-specific config: {env_filename}")

            # Parse environment config and merge with base
            try:
                env_config = yaml.safe_load(env_content)
            except ImportError:
                env_config = json.loads(env_content)

            # Simple merge - environment config overrides base config
            merged_config = base_config.copy()
            merged_config.update(env_config)
            return merged_config

        except Exception as e:
            print(f"Environment config not found ({env_filename}), using base config")

    return base_config


def is_kindling_available() -> bool:
    """Check if kindling is installed WITHOUT importing it"""
    try:
        import importlib.util
        import sys

        print(f"DEBUG: Python path: {sys.path[:3]}...")

        # Use find_spec to check if module exists without importing it
        spec = importlib.util.find_spec("kindling")

        if spec is not None:
            print("DEBUG: kindling package found (not imported)")
            return True
        else:
            print("DEBUG: kindling package not found")
            return False
    except Exception as e:
        print(f"DEBUG: Error checking kindling availability: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()
        return False


def install_framework_package(
    storage_path: str, package_name: str = "kindling_fabric", version: str = "latest"
) -> bool:
    """Install framework package from datalake storage"""
    try:
        storage_utils = get_storage_utils()
        if not storage_utils:
            print("ERROR: Storage utilities not available")
            return False

        packages_dir = f"{storage_path}/packages/"

        try:
            files = storage_utils.fs.ls(packages_dir)
        except Exception as e:
            print(f"ERROR: Failed to list files in {packages_dir}: {str(e)}")
            return False

        if version == "latest":
            print(f"DEBUG: Looking for files: {package_name}-*.whl")
            matching_files = [
                f
                for f in files
                if f.name.endswith(".whl") and f.name.startswith(f"{package_name}-")
            ]
            if matching_files:

                def extract_version(filename):
                    """Extract version using packaging.version for proper semantic versioning.

                    Handles pre-releases (alpha, beta, rc), dev releases, and post releases.
                    """
                    try:
                        # Get "0.20.1" or "0.20.1-alpha" part
                        version_part = filename.split("-")[1]
                        if Version is not None:
                            return Version(version_part)
                        else:
                            # Fallback to tuple comparison if packaging not available
                            return tuple(map(int, version_part.split(".")))
                    except (IndexError, ValueError, InvalidVersion):
                        # Fallback for malformed versions
                        if Version is not None:
                            return Version("0.0.0")
                        else:
                            return (0, 0, 0)

                matching_files.sort(key=lambda f: extract_version(f.name), reverse=True)
        else:
            # Find specific version
            matching_files = [
                f
                for f in files
                if f.name.endswith(".whl") and f.name.startswith(f"{package_name}-{version}")
            ]

        if not matching_files:
            if version == "latest":
                print(
                    f"INFO: No wheel files found starting with '{package_name}-' in {packages_dir}"
                )
            else:
                print(
                    f"INFO: No wheel files found for '{package_name}-{version}' in {packages_dir}"
                )
            return False

        # Use the first (or latest) matching file
        selected_file = matching_files[0]
        package_file = selected_file.name
        remote_path = selected_file.path

        print(f"Found package file: {package_file}")
        print(f"Installing from: {remote_path}")

        # Install the wheel
        return _install_wheel(remote_path, package_file, storage_utils)

    except Exception as e:
        print(f"ERROR: Failed to install framework package: {str(e)}")
        import traceback

        print(f"Full traceback: {traceback.format_exc()}")
        return False


def install_kindling_from_datalake(config: Dict[str, Any], storage_utils) -> bool:
    """Install kindling from datalake storage"""
    try:
        storage_path = config.get("artifacts_storage_path")
        if not storage_path:
            print("ERROR: artifacts_storage_path not configured")
            return False

        version = config.get("kindling_version", "latest")

        platform = detect_platform()

        # Try specific version first, then fall back to latest
        if version != "latest":
            print(f"Trying specific version: {version}")
            if install_framework_package(storage_path, f"kindling_{platform}", version):
                return True

            print(f"Version {version} not found, falling back to latest")

        # Use latest version
        result = install_framework_package(storage_path, f"kindling_{platform}", "latest")
        if not result:
            print(f"DEBUG: Failed to install from {storage_path}")
            print(f"DEBUG: Full packages path would be: {storage_path}/packages/")

        return result

    except Exception as e:
        print(f"ERROR: Kindling installation failed: {str(e)}")
        return False


def _install_with_debug():
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            local_path,
            "--disable-pip-version-check",
            "--no-warn-conflicts",
            "-vvv",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    output_lines = []
    for line in process.stdout:
        print(line.rstrip())  # Show in real-time
        output_lines.append(line.rstrip())  # Save for later

    process.wait()
    result_code = process.returncode
    full_output = "\n".join(output_lines)


def _install_wheel(remote_path: str, filename: str, storage_utils) -> bool:
    """Install wheel file using platform-appropriate method"""
    import tempfile
    import uuid

    try:
        # CRITICAL: Uninstall ALL kindling packages first to clear cached modules
        print("🧹 Checking for existing kindling packages...")
        list_result = subprocess.run(
            [sys.executable, "-m", "pip", "list"],
            capture_output=True,
            text=True,
        )
        kindling_pkgs = [
            line for line in list_result.stdout.split("\n") if "kindling" in line.lower()
        ]
        if kindling_pkgs:
            print(f"   Found: {kindling_pkgs}")
        else:
            print("   No kindling packages found")

        print("🧹 Uninstalling any existing kindling packages...")
        uninstall_result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "uninstall",
                "-y",  # Yes to all prompts
                "kindling-fabric",
                "kindling-databricks",
                "kindling-synapse",
                "kindling",  # Also uninstall base package if exists
            ],
            capture_output=True,
            text=True,
        )
        if uninstall_result.stdout:
            print(uninstall_result.stdout)

        # Find and physically DELETE the kindling package directory
        print("🧹 Removing kindling package directories...")
        import shutil
        import site

        # Check all possible site-packages locations
        site_dirs = site.getsitepackages()
        if hasattr(site, "getusersitepackages"):
            site_dirs.append(site.getusersitepackages())

        print(f"   Checking {len(site_dirs)} site-packages directories...")
        for site_dir in site_dirs:
            print(f"   Checking: {site_dir}")
            kindling_dir = os.path.join(site_dir, "kindling")
            if os.path.exists(kindling_dir):
                print(f"   🗑️  DELETING: {kindling_dir}")
                shutil.rmtree(kindling_dir, ignore_errors=True)
                print(f"   ✅ Deleted successfully")
            else:
                print(f"   (no kindling directory here)")

        # Also check for .dist-info and .egg-info directories
        print("🧹 Removing kindling metadata directories...")
        for site_dir in site_dirs:
            if not os.path.exists(site_dir):
                continue
            for item in os.listdir(site_dir):
                if "kindling" in item.lower() and (
                    item.endswith(".dist-info") or item.endswith(".egg-info")
                ):
                    metadata_path = os.path.join(site_dir, item)
                    print(f"   🗑️  Deleting: {metadata_path}")
                    shutil.rmtree(metadata_path, ignore_errors=True)

        # Clear sys.modules of any kindling modules
        print("🧹 Clearing kindling from sys.modules...")
        import sys as sys_module

        modules_to_remove = [k for k in sys_module.modules.keys() if "kindling" in k.lower()]
        if modules_to_remove:
            print(f"   Removing {len(modules_to_remove)} modules from sys.modules")
            for mod in modules_to_remove:
                del sys_module.modules[mod]

        # Clear Python bytecode cache
        print("🧹 Clearing Python bytecode cache...")
        import py_compile

        # Just importing py_compile is enough to ensure .pyc handling is initialized
        # Detect platform and use appropriate temp path
        storage_utils_type = type(storage_utils).__name__
        # Only Databricks has DBUtils - Fabric/Synapse have MsSparkUtils which also has fs
        is_databricks = "DBUtils" in storage_utils_type

        if is_databricks:
            # Databricks: Try Unity Catalog Volume first, fallback to DBFS
            print(f"📥 Preparing wheel for installation...")
            try:
                # Check if we have a temp path configured (UC Volume for Databricks)
                # Look for kindling.temp_path first (new convention), fallback to temp_path
                temp_path = None
                if "BOOTSTRAP_CONFIG" in globals():
                    temp_path = BOOTSTRAP_CONFIG.get("kindling.temp_path") or BOOTSTRAP_CONFIG.get(
                        "temp_path"
                    )

                if temp_path:
                    # Use configured temp path (typically UC Volume for Databricks)
                    print(f"   Using Unity Catalog Volume: {temp_path}")
                    volume_file = f"{temp_path.rstrip('/')}/{filename}"

                    print(f"   Source: {remote_path}")
                    print(f"   Volume target: {volume_file}")

                    # Copy to volume
                    storage_utils.fs.cp(remote_path, volume_file, recurse=False)
                    print(f"   ✅ Copied to volume")

                    local_path = volume_file

                else:
                    # Fallback to DBFS /tmp
                    print(f"   Using DBFS (set kindling.temp_path in config to use volumes)")
                    dbfs_temp_path = f"/tmp/{filename}"

                    print(f"   Source: {remote_path}")
                    print(f"   DBFS target: dbfs:{dbfs_temp_path}")

                    # Copy from ABFSS to DBFS (cloud-to-cloud copy)
                    storage_utils.fs.cp(remote_path, f"dbfs:{dbfs_temp_path}", recurse=False)
                    print(f"   ✅ Copied to DBFS")

                    # Access via /dbfs/ mount for pip
                    local_path = f"/dbfs{dbfs_temp_path}"

                # Verify it exists and is readable
                if not os.path.exists(local_path):
                    print(f"ERROR: File not accessible: {local_path}")
                    return False

                file_size = os.path.getsize(local_path)
                print(f"✅ Accessible for pip ({file_size} bytes)")

                # Verify it's a valid zip file (wheels are zip files)
                import zipfile

                if not zipfile.is_zipfile(local_path):
                    print(f"⚠️  WARNING: File is not a valid zip/wheel")
                    # Try to clean up
                    try:
                        if volume_path:
                            storage_utils.fs.rm(volume_file)
                        else:
                            storage_utils.fs.rm(f"dbfs:{dbfs_temp_path}")
                    except:
                        pass
                    return False

            except Exception as e:
                print(f"ERROR: Failed to prepare wheel: {e}")
                import traceback

                traceback.print_exc()
                return False
        else:
            # Fabric/Synapse: Use local temp directory
            import tempfile

            temp_dir = tempfile.mkdtemp(prefix="kindling_bootstrap_")
            local_path = f"{temp_dir}/{filename}"

            print(f"📥 Downloading wheel to: {local_path}")
            storage_utils.fs.cp(remote_path, f"file://{local_path}")

            if not os.path.exists(local_path):
                print(f"ERROR: Failed to copy wheel to {local_path}")
                return False

            print(f"✅ Downloaded {filename} ({os.path.getsize(local_path)} bytes)")

        # Install with pip - simple install, no unnecessary flags
        print(f"📦 Installing {filename}...")
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                local_path,
                "--disable-pip-version-check",
            ],
            capture_output=True,
            text=True,
        )

        # Print stdout (normal output)
        if result.stdout:
            print(result.stdout)

        # Print stderr but filter out non-critical pip dependency warnings
        if result.stderr:
            stderr_lines = result.stderr.split("\n")
            filtered_lines = []
            skip_next = False

            for line in stderr_lines:
                # Skip pip's dependency conflict warnings (not actual errors)
                if "dependency resolver does not currently take into account" in line:
                    skip_next = True
                    continue
                if skip_next and ("requires" in line.lower() or "but you have" in line.lower()):
                    # This is the continuation of the dependency warning
                    continue
                skip_next = False

                # Keep actual errors and other warnings
                if line.strip():
                    filtered_lines.append(line)

            filtered_stderr = "\n".join(filtered_lines).strip()
            if filtered_stderr:
                print(filtered_stderr)

        # DIAGNOSTIC: Show what platform modules actually exist after installation
        print("🔍 Checking installed kindling package contents...")
        import site

        for site_dir in site.getsitepackages():
            kindling_dir = os.path.join(site_dir, "kindling")
            if os.path.exists(kindling_dir):
                print(f"   Found kindling at: {kindling_dir}")
                platform_files = [
                    f
                    for f in os.listdir(kindling_dir)
                    if f.startswith("platform_") and f.endswith(".py")
                ]
                if platform_files:
                    print(f"   📦 Platform modules found: {', '.join(platform_files)}")
                else:
                    print(
                        f"   ✅ No platform_*.py files found (expected for platform-specific wheel)"
                    )
                break

        # CRITICAL: Ensure site-packages is in sys.path for Synapse/Spark environments
        # In some environments, pip install doesn't automatically add to sys.path
        import site

        site_packages_added = False
        for site_dir in site.getsitepackages():
            if site_dir not in sys.path:
                print(f"   📍 Adding to sys.path: {site_dir}")
                sys.path.insert(0, site_dir)
                site_packages_added = True

        if site_packages_added:
            # Force reload of site to pick up new packages
            import importlib

            importlib.invalidate_caches()

        # Cleanup temp directory (only for non-Databricks where we created one)
        if not is_databricks:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            # Clean up Databricks temp file (volume or DBFS)
            try:
                # Check for temp path (new convention)
                temp_path = None
                if "BOOTSTRAP_CONFIG" in globals():
                    temp_path = BOOTSTRAP_CONFIG.get("kindling.temp_path") or BOOTSTRAP_CONFIG.get(
                        "temp_path"
                    )
                if temp_path:
                    volume_file = f"{temp_path.rstrip('/')}/{filename}"
                    storage_utils.fs.rm(volume_file)
                    print(f"   🧹 Cleaned up volume file")
                else:
                    dbfs_temp_path = f"/tmp/{filename}"
                    storage_utils.fs.rm(f"dbfs:{dbfs_temp_path}")
                    print(f"   🧹 Cleaned up DBFS temp file")
            except Exception as e:
                print(f"   ⚠️  Could not clean up temp file: {e}")

        return result.returncode == 0

    except Exception as e:
        print(f"ERROR: Wheel installation failed: {str(e)}")
        return False


def bootstrap(config_or_filename: Union[Dict[str, Any], str], artifacts_path: str = None):
    """Bootstrap - ensures kindling is available and hands off to framework initialization"""
    now = datetime.now()
    execution_id = now.strftime("%Y%m%d_%H%M%S_%f")
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print("=" * 80)
    print(f"🚀 BOOTSTRAP FUNCTION CALLED - {timestamp}")
    print(f"   Execution ID: {execution_id}")
    print(f"   PID: {os.getpid()}")
    print(f"   Config type: {type(config_or_filename).__name__}")
    print(f"   Artifacts path: {artifacts_path}")
    print("=" * 80)
    print("=== Starting Framework Bootstrap ===")

    try:
        # Step 1: Platform detection
        platform = detect_platform()
        storage_utils = get_storage_utils()
        print(f"Platform: {platform}")

        # Step 2: Load configuration
        if isinstance(config_or_filename, dict):
            config = config_or_filename
            artifacts_path = config.get("artifacts_storage_path")
        else:
            if not artifacts_path:
                raise Exception("artifacts_path required when loading config from file")
            config = load_config(config_or_filename, artifacts_path, storage_utils)
            # Set artifacts_path in config for downstream use
            config["artifacts_storage_path"] = artifacts_path

        print("Configuration loaded")

        # Step 3: Ensure kindling availability
        # Default behavior: install from lake only when kindling is missing
        use_lake_packages = config.get("use_lake_packages", True)
        force_reinstall = config.get("force_reinstall", False)
        kindling_available = is_kindling_available()

        if kindling_available and not force_reinstall:
            print("Kindling already available, skipping install (install-if-missing behavior)")
        elif use_lake_packages:
            reason = "force_reinstall=True" if force_reinstall else "kindling not available"
            print(f"Installing kindling from datalake ({reason})...")
            if not install_kindling_from_datalake(config, storage_utils):
                raise Exception("Failed to install kindling from datalake")

            # Verify installation
            if not is_kindling_available():
                raise Exception("Kindling still not available after installation")

            print("Kindling successfully installed and available")
        elif kindling_available:
            print("Kindling already available (use_lake_packages=False)")
        else:
            raise Exception("Kindling not available and use_lake_packages=False")

        # Step 4: Hand off to framework initialization
        now = datetime.now()
        execution_id = now.strftime("%Y%m%d_%H%M%S_%f")
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print("=" * 80)
        print(f"🎯 CALLING FRAMEWORK INITIALIZATION - {timestamp}")
        print(f"   Execution ID: {execution_id}")
        print(f"   App name: {config.get('app_name')}")
        print(f"   PID: {os.getpid()}")
        print("=" * 80)

        from kindling.bootstrap import initialize_framework

        app_name = config.get("app_name")
        framework = initialize_framework(config, app_name)

        complete_now = datetime.now()
        complete_timestamp = complete_now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print("=" * 80)
        print(f"✅ BOOTSTRAP COMPLETE - {complete_timestamp}")
        print(f"   Execution ID: {execution_id}")
        print(f"   Framework initialized successfully")
        print("=" * 80)

        return framework

    except Exception as e:
        print(f"ERROR: Bootstrap failed: {str(e)}")
        import traceback

        print(f"Traceback: {traceback.format_exc()}")
        raise


# Auto-execute bootstrap if BOOTSTRAP_CONFIG is defined AND we haven't run yet
# Guard against recursive execution
if "BOOTSTRAP_CONFIG" in globals() and "bootstrap" in globals():
    now = datetime.now()
    execution_id = now.strftime("%Y%m%d_%H%M%S_%f")
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print("=" * 80)
    print(f"⚡ AUTO-EXECUTE TRIGGERED - {timestamp}")
    print(f"   Execution ID: {execution_id}")
    print(f"   BOOTSTRAP_CONFIG exists: {('BOOTSTRAP_CONFIG' in globals())}")
    print(f"   bootstrap function exists: {('bootstrap' in globals())}")
    print(f"   PID: {os.getpid()}")
    print("=" * 80)

    config = BOOTSTRAP_CONFIG
    artifacts_path = config.get("ARTIFACTS_PATH") if isinstance(config, dict) else None
    bootstrap(config, artifacts_path)

    # Mark end of script execution
    end_now = datetime.now()
    end_timestamp = end_now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print("=" * 80)
    print(f"🏁 BOOTSTRAP SCRIPT FINISHED - {end_timestamp}")
    print(f"   Final Execution ID: {execution_id}")
    print(f"   PID: {os.getpid()}")
    print("=" * 80)
