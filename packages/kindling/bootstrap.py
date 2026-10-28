import importlib
import importlib.util
import json
import logging
import os
import pkgutil
import subprocess
import sys
import tempfile
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from urllib.parse import urlparse

from kindling.features import get_feature_bool
from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_session import *

from .data_apps import *
from .notebook_framework import *
from .package_registrations import (
    REGISTRATION_NAMESPACES,
    import_package_registrations,
    import_registration_namespace,
)

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
_CONFIG_FILES_SOURCE_METADATA_KEY = "_kindling_config_files_source_key"
_SPARK_CONFIG_FILES_KEY = "spark.kindling.bootstrap.config_files"
_LOCAL_PACKAGE_MODULES_ENV = "KINDLING_LOCAL_PACKAGE_MODULES"
_LOCAL_PACKAGE_REGISTRATION_NAMESPACES = REGISTRATION_NAMESPACES
_import_registration_namespace = import_registration_namespace


class _BootstrapPhaseRecorder:
    """Collects bootstrap phase timings before any trace provider can exist.

    Early bootstrap phases predate a usable ConfigService, and the final
    trace-provider identity is only settled once extensions have imported —
    live spans are impossible by construction. Phases are recorded here
    (plain list, no DI, no JVM) and retro-flushed as one span tree at the
    end of a full initialization via SparkTraceProvider.record_span.
    """

    def __init__(self):
        self.phases: List[Dict[str, Any]] = []
        self.start_time: Optional[datetime] = None

    def reset(self, start_time: datetime) -> None:
        self.phases = []
        self.start_time = start_time


_PHASE_RECORDER = _BootstrapPhaseRecorder()


@contextmanager
def _bootstrap_phase(name: str):
    """Record one bootstrap phase's wall-clock window (and error, if any)."""
    entry: Dict[str, Any] = {"name": name, "start": datetime.now(), "end": None, "error": None}
    _PHASE_RECORDER.phases.append(entry)
    try:
        yield
    except Exception as phase_error:
        entry["error"] = str(phase_error)
        raise
    finally:
        entry["end"] = datetime.now()


def _flush_bootstrap_trace(config_service, error: Optional[str] = None) -> None:
    """Retro-record the bootstrap span tree once a provider is resolvable.

    Emits a root kindling.bootstrap/initialize span (a live CM span so the
    per-phase children parent to it and share its trace id on every
    provider) whose true window rides in attributes, plus one record_span
    child per phase with the faithful timestamps. Failures preceding a
    usable ConfigService never reach this point — that tree is lost and
    only stdlib-logged (documented limitation). Flushing must never break
    bootstrap.
    """
    recorder = _PHASE_RECORDER
    if recorder.start_time is None or not recorder.phases:
        return
    try:
        from kindling.spark_trace import SparkTraceProvider
        from kindling.trace_ops import COMPONENT_BOOTSTRAP, tracing_gates

        if not tracing_gates(config_service).minimal:
            return
        tp = GlobalInjector.get(SparkTraceProvider)
        end_time = recorder.phases[-1]["end"] or datetime.now()
        root_details = {
            "started_at": recorder.start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "duration_seconds": f"{(end_time - recorder.start_time).total_seconds():.3f}",
            "phase_count": len(recorder.phases),
        }
        if error:
            root_details["error"] = error
        with tp.span(operation="initialize", component=COMPONENT_BOOTSTRAP, details=root_details):
            for entry in recorder.phases:
                tp.record_span(
                    entry["name"],
                    COMPONENT_BOOTSTRAP,
                    entry["start"],
                    entry["end"] or end_time,
                    error=entry["error"],
                )
    except Exception as flush_error:
        _BOOTSTRAP_LOGGER.debug("Bootstrap trace flush failed (non-fatal): %s", flush_error)
    finally:
        # One flush per recorded initialization — a later failed/aborted
        # init must not re-emit this tree.
        recorder.phases = []
        recorder.start_time = None


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


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    parsed = _parse_spark_conf_value(value)
    if isinstance(parsed, bool):
        return parsed
    return bool(parsed)


def spark_conf_get(spark: Any, key: str) -> Optional[str]:
    """Read one Spark configuration key with restricted-runtime fallbacks."""
    try:
        value = spark.conf.get(key, None)
    except TypeError as exc:
        _BOOTSTRAP_LOGGER.debug(
            "RuntimeConfig.get(key, default) is unavailable for %s: %s", key, exc
        )
        try:
            value = spark.conf.get(key)
        except Exception as fallback_exc:  # noqa: BLE001 - best-effort runtime access
            _BOOTSTRAP_LOGGER.debug(
                "Unable to read Spark configuration key %s: %s", key, fallback_exc
            )
            return None
    except Exception as exc:  # noqa: BLE001 - best-effort runtime access
        _BOOTSTRAP_LOGGER.debug("Unable to read Spark configuration key %s: %s", key, exc)
        return None

    if value is None:
        return None
    return str(value)


def _normalize_spark_conf_items(values: Any) -> Tuple[Tuple[str, Any], ...]:
    if isinstance(values, Mapping):
        return tuple(values.items())
    if isinstance(values, (list, tuple)):
        return tuple(values)
    raise TypeError(f"Unsupported Spark configuration enumeration result: {type(values).__name__}")


def iter_spark_conf_items(spark: Any, extra_keys: Iterable[str] = ()) -> Iterable[Tuple[str, Any]]:
    """Enumerate Spark configuration through all known best-effort tiers."""
    try:
        get_all = getattr(spark.conf, "getAll", None)
        if callable(get_all):
            return _normalize_spark_conf_items(get_all())
    except Exception as exc:  # noqa: BLE001 - py4j-restricted runtimes
        _BOOTSTRAP_LOGGER.debug("Unable to enumerate RuntimeConfig.getAll(): %s", exc)

    try:
        values = spark.sparkContext.getConf().getAll()
        return _normalize_spark_conf_items(values)
    except Exception as exc:  # noqa: BLE001 - py4j-restricted runtimes
        _BOOTSTRAP_LOGGER.debug("Unable to enumerate SparkContext.getConf().getAll(): %s", exc)

    try:
        rows = spark.sql("SET").collect()
        items = tuple((row[0], row[1]) for row in rows if row[0] is not None)
        if items:
            return items
    except Exception as exc:  # noqa: BLE001 - py4j-restricted runtimes
        _BOOTSTRAP_LOGGER.debug("Unable to enumerate configuration via SET: %s", exc)

    explicit_keys = tuple(dict.fromkeys(key for key in extra_keys if key))
    explicit_items = tuple(
        (key, value) for key in explicit_keys if (value := spark_conf_get(spark, key)) is not None
    )
    if explicit_keys:
        _BOOTSTRAP_LOGGER.warning(
            "Unable to enumerate Spark configuration through RuntimeConfig.getAll(), "
            "SparkContext.getConf().getAll(), or SET; read explicit keys %s "
            "(available: %s)",
            explicit_keys,
            tuple(key for key, _ in explicit_items),
        )
    return explicit_items


def map_spark_kindling_items(items: Iterable[Tuple[str, Any]]) -> Dict[str, Any]:
    """Map spark.kindling.* configuration items to Kindling bootstrap/config keys."""
    mapped: Dict[str, Any] = {}
    prefix = "spark.kindling."
    bootstrap_prefix = "bootstrap."
    bootstrap_aliases = {
        "load_lake": "use_lake_packages",
        "load_local": "load_workspace_packages",  # deprecated; canonical is load_workspace_packages
    }

    for item in items:
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
            mapped_key = bootstrap_aliases.get(bootstrap_key, bootstrap_key)
            if mapped_key == "config_files" and isinstance(value, str) and "," in value:
                comma_paths = [part.strip() for part in value.split(",") if part.strip()]
                if comma_paths:
                    _BOOTSTRAP_LOGGER.warning(
                        "SparkConf key '%s' should use a JSON array string for multiple "
                        "config files; treating comma-separated value as %s.",
                        key,
                        comma_paths,
                    )
                    value = comma_paths
            mapped[mapped_key] = value
        else:
            mapped[f"kindling.{suffix}"] = value

    return mapped


def read_spark_kindling_config(spark: Any, extra_keys: Iterable[str] = ()) -> Dict[str, Any]:
    """Read SparkConf-backed spark.kindling.* configuration for shared callers."""
    return map_spark_kindling_items(iter_spark_conf_items(spark, extra_keys=extra_keys))


def _get_spark_kindling_config() -> Dict[str, Any]:
    """Read spark.kindling.* keys from SparkConf and map into bootstrap/config keys."""
    try:
        spark = get_or_create_spark_session()
    except Exception:
        return {}
    return read_spark_kindling_config(spark)


def _load_env_local_package_module_roots() -> List[str]:
    raw_modules = (os.getenv(_LOCAL_PACKAGE_MODULES_ENV) or "").strip()
    if not raw_modules:
        return []

    try:
        parsed = json.loads(raw_modules)
    except json.JSONDecodeError:
        parsed = raw_modules.split(os.pathsep)

    if isinstance(parsed, str):
        parsed = [parsed]

    modules = []
    seen = set()
    for value in parsed if isinstance(parsed, list) else []:
        if not isinstance(value, str):
            continue
        module = value.strip()
        if module and module not in seen:
            seen.add(module)
            modules.append(module)
    return modules


def _load_local_package_module_roots(explicit_roots: Optional[List[str]] = None) -> List[str]:
    """Merge explicit_roots (e.g. initialize_framework's `registration_packages`
    config key) with KINDLING_LOCAL_PACKAGE_MODULES (env var; --local-package /
    the app-run subprocess boundary), de-duplicated, explicit-first."""
    roots: List[str] = []
    seen = set()
    for value in explicit_roots or []:
        if isinstance(value, str) and value.strip():
            stripped = value.strip()
            if stripped not in seen:
                seen.add(stripped)
                roots.append(stripped)
    for value in _load_env_local_package_module_roots():
        if value not in seen:
            seen.add(value)
            roots.append(value)
    return roots


def _import_local_package_registrations(
    logger, registration_packages: Optional[List[str]] = None
) -> None:
    import_package_registrations(logger, _load_local_package_module_roots(registration_packages))


def pending_declaration_derivations() -> Dict[str, str]:
    """Clone/extend derivations whose source is not registered, across both
    registries (``kindling.declaration_derivations``)."""
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry

    pending: Dict[str, str] = {}
    for interface in (DataEntityRegistry, DataPipesRegistry):
        try:
            registry = get_kindling_service(interface)
        except Exception:  # noqa: BLE001 - registry not bound yet
            continue
        reported = getattr(registry, "pending_derivations", lambda: {})()
        if isinstance(reported, dict):  # custom or mocked registries may not implement it
            pending.update(reported)
    return pending


def apply_config_overrides() -> None:
    """Overlay ``datapipes:``/``dataentities:`` config sections onto
    registered pipe/entity metadata.

    Called from ``initialize_framework`` right after local package
    registrations import, and again after post-platform ``@secret``
    resolution so any secret reference inside those sections lands
    resolved rather than as a literal (the managers always rebuild
    metadata from the original registration params, so re-calling is
    idempotent and never accumulates). Also safely re-callable after
    ``config_service.reload()`` for hot reload. The managers keep the
    compiled patterns, so items registered later (workspace packages, app
    ``register_all``, notebook cells) are overlaid at registration.
    """
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry

    config_service = get_kindling_service(ConfigService)
    # Fetch by the registry ABCs: decorators register into the interface
    # singletons, and the injector caches interface and concrete-class
    # bindings separately.
    pipes_registry = get_kindling_service(DataPipesRegistry)
    entity_registry = get_kindling_service(DataEntityRegistry)
    for registry in (pipes_registry, entity_registry):
        overlay = getattr(registry, "apply_config_overrides", None)
        if overlay is None:
            # Custom registry binding without overlay support.
            _BOOTSTRAP_LOGGER.debug(
                "%s does not support config overrides — skipping overlay",
                type(registry).__name__,
            )
            continue
        overlay(config_service)
    _BOOTSTRAP_LOGGER.debug(
        "Config overrides applied to %s pipe(s) and %s entit(y/ies)",
        sum(1 for _ in pipes_registry.get_pipe_ids()),
        sum(1 for _ in entity_registry.get_entity_ids()),
    )


def _merge_with_spark_kindling_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge SparkConf-derived config with explicit config (explicit wins)."""
    spark_kindling_config = _get_spark_kindling_config()
    explicit_config = dict(config or {})
    if not spark_kindling_config:
        return explicit_config

    merged = dict(spark_kindling_config)
    if "config_files" in spark_kindling_config and "config_files" not in explicit_config:
        merged[_CONFIG_FILES_SOURCE_METADATA_KEY] = _SPARK_CONFIG_FILES_KEY
    merged.update(explicit_config)
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


# Logged when Databricks staging that runs before settings are loaded has no
# staging path and falls back to the DBFS root. The first argument names what
# is staged, the second the fallback location.
_DBFS_ROOT_STAGING_WARNING = (
    "No kindling.temp_path in the job parameters; staging %s under dbfs:/tmp "
    "(DBFS root: %s). Settings files are read after this step, so a temp_path there "
    "does not apply here. Pass it as a job parameter "
    "(--param kindling.temp_path=/Volumes/<catalog>/<schema>/<volume>/...) or set "
    "CONFIG__platform_databricks__kindling__temp_path. Workspaces that restrict "
    "DBFS root access will fail at this step."
)


def _warn_if_settings_temp_path_missed_early_staging(
    config_service, bootstrap_config: Dict[str, Any]
) -> None:
    """Warn when settings name a staging path the early Databricks steps never saw.

    The bootstrap script stages the framework wheel, and
    :func:`download_config_files` stages the settings files, before any settings
    file is read. Both see only the bootstrap (job) parameters, so a
    ``kindling.temp_path`` that comes from settings files reaches only later
    staging such as extension wheels.
    """
    if not bootstrap_config.get("artifacts_storage_path"):
        return  # Nothing was staged from artifacts storage.
    if _normalize_path_value(
        bootstrap_config.get("kindling.temp_path") or bootstrap_config.get("temp_path")
    ):
        return  # Early staging used the job parameter.
    settings_temp_path = _normalize_path_value(config_service.get("kindling.temp_path"))
    if not settings_temp_path:
        return
    # Config files may still have gone to a volume root derived from the
    # bootstrap parameters; the framework wheel never uses a derived root.
    early_staged = (
        "the framework wheel"
        if _resolve_databricks_staging_path(bootstrap_config)
        else "the framework wheel and configuration files"
    )
    _BOOTSTRAP_LOGGER.warning(
        "kindling.temp_path=%s comes from settings files, which are read after early "
        "bootstrap staging; it applies only to later staging (such as extension "
        "wheels). No kindling.temp_path was in the job parameters, so early staging "
        "of %s used dbfs:/tmp (DBFS root). Pass it as a job parameter "
        "(--param kindling.temp_path=%s) or set "
        "CONFIG__platform_databricks__kindling__temp_path to stage everything there.",
        settings_temp_path,
        early_staged,
        settings_temp_path,
    )


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
        _warn_if_settings_temp_path_missed_early_staging(config_service, bootstrap_config)
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


def settings_hierarchy(
    environment: str,
    platform: Optional[str] = None,
    workspace_id: Optional[str] = None,
    include_app: bool = True,
) -> List[Tuple[str, str, Optional[str]]]:
    """The settings convention as an ordered table, lowest precedence first.

    Each entry is ``(scope, canonical_name, legacy_name)`` where ``scope`` is
    ``"config"`` (the shared ``config/`` directory) or ``"app"`` (the app's own
    directory) and ``legacy_name`` is the documented pre-``settings.*`` file
    name that is honoured only when the canonical file is absent. Both the
    artifacts-storage download (:func:`download_config_files`) and local
    directory discovery (:func:`resolve_settings_files`) are driven by this
    one table so the two can never disagree.
    """
    entries: List[Tuple[str, str, Optional[str]]] = [("config", "settings.yaml", None)]
    if platform:
        entries.append(("config", f"settings.{platform}.yaml", f"platform_{platform}.yaml"))
    if workspace_id:
        entries.append(("config", f"workspace_{workspace_id}.yaml", None))
    entries.append(("config", f"settings.{environment}.yaml", f"env_{environment}.yaml"))
    if include_app:
        entries.append(("app", "settings.yaml", None))
        if platform:
            entries.append(("app", f"settings.{platform}.yaml", f"app.{platform}.yaml"))
        entries.append(("app", f"settings.{environment}.yaml", f"app.{environment}.yaml"))
    return entries


def resolve_settings_files(
    config_dir: Optional[Any],
    app_dir: Optional[Any],
    environment: str,
    platform: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> List[str]:
    """Apply the settings convention to local directories.

    ``config_dir`` holds the shared overlays and ``app_dir`` the app's own;
    either may be ``None``. Returns the existing files in precedence order
    (see :func:`settings_hierarchy`); callers never spell the list out.
    """
    roots = {
        "config": Path(config_dir).expanduser() if config_dir else None,
        "app": Path(app_dir).expanduser() if app_dir else None,
    }
    resolved: List[str] = []
    for scope, canonical, legacy in settings_hierarchy(
        environment, platform, workspace_id, include_app=roots["app"] is not None
    ):
        root = roots[scope]
        if root is None:
            continue
        for name in (canonical, legacy):
            if name and (root / name).is_file():
                resolved.append(str(root / name))
                break
    return resolved


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
      2. settings.{platform}.yaml (platform-specific: fabric, synapse, databricks)
      3. workspace_{workspace_id}.yaml (workspace-specific)
      4. settings.{environment}.yaml (environment-specific: dev, prod, etc)
      5. data-apps/{app_name}/settings.yaml (app-specific base)
      6. data-apps/{app_name}/settings.{platform}.yaml (app-specific platform)
      7. data-apps/{app_name}/settings.{environment}.yaml (app-specific env)

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
            _BOOTSTRAP_LOGGER.warning(_DBFS_ROOT_STAGING_WARNING, "configuration files", temp_dir)
            dbfs_temp_path = temp_dir
    else:
        _BOOTSTRAP_LOGGER.info("Using local temp path: %s", temp_dir)

    config_files = []

    # Build the download list from the shared convention table (lowest to
    # highest precedence). App overlays come last so they win over
    # workspace/global config; local copies of app files carry a prefix so
    # they never collide with the shared files in the staging directory.
    safe_app_name = str(app_name).replace("/", "_") if app_name else None
    app_config_prefix = f"app_{safe_app_name}_" if safe_app_name else None
    files_to_download: List[Tuple[str, str]] = []
    legacy_fallbacks: Dict[str, Tuple[str, str]] = {}
    for scope, canonical, legacy in settings_hierarchy(
        environment, platform, workspace_id, include_app=bool(app_name)
    ):
        if scope == "config":
            remote_dir, local_prefix = f"{base_path}/config", ""
        else:
            remote_dir, local_prefix = f"{base_path}/data-apps/{app_name}", app_config_prefix
        local_name = f"{local_prefix}{canonical}"
        files_to_download.append((f"{remote_dir}/{canonical}", local_name))
        if legacy:
            legacy_fallbacks[local_name] = (f"{remote_dir}/{legacy}", f"{local_prefix}{legacy}")
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
            fallback = legacy_fallbacks.get(filename)
            if fallback:
                legacy_remote_path, legacy_filename = fallback
                try:
                    if is_databricks:
                        content = storage_utils.fs.head(legacy_remote_path, 1024 * 1024)
                        text_content = (
                            content.decode("utf-8") if isinstance(content, bytes) else content
                        )

                        if volume_path:
                            volume_file = _build_run_scoped_staging_path(
                                volume_path, "config", f"{len(config_files)}_{legacy_filename}"
                            )
                            storage_utils.fs.put(volume_file, text_content, overwrite=True)
                            config_files.append(volume_file)
                        else:
                            dbfs_file_path = (
                                f"{dbfs_temp_path}/{len(config_files)}_{legacy_filename}"
                            )
                            storage_utils.fs.put(dbfs_file_path, text_content, overwrite=True)
                            config_files.append(f"/dbfs{dbfs_file_path}")
                    else:
                        local_path = temp_path / f"{len(config_files)}_{legacy_filename}"
                        storage_utils.fs.cp(legacy_remote_path, f"file://{str(local_path)}")
                        config_files.append(str(local_path))
                    _BOOTSTRAP_LOGGER.info(
                        "Downloaded legacy config %s after %s was not found",
                        legacy_filename,
                        filename,
                    )
                    continue
                except Exception:
                    pass
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
        default_config.parent.mkdir(parents=True, exist_ok=True)
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


def _overlay_workspace_id(workspace_id: Optional[str], platform: Optional[str]) -> Optional[str]:
    """The id that names the workspace_<id>.yaml overlay.

    On Databricks the ``workspace_id`` setting doubles as the REST API host,
    so it may be a URL (the SDK passes the workspace URL). A URL never names
    an overlay file: use the detected workspace id instead, else the host
    with dots replaced by underscores (the same fallback detection uses).
    """
    if not workspace_id or not str(workspace_id).startswith(("http://", "https://")):
        return workspace_id
    detected = _get_workspace_id_for_platform(platform) if platform else None
    if detected and not str(detected).startswith(("http://", "https://")):
        return str(detected)
    host = urlparse(str(workspace_id)).netloc
    return host.replace(".", "_") if host else None


def _get_minimal_default_config() -> str:
    """Minimal default config for when no files are found"""
    return """
default:
  kindling:
    version: "0.1.0"

    bootstrap:
      load_lake: false
      load_workspace_packages: false
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
    """Get current Spark log level, or None when the JVM bridge is unavailable.

    Databricks UC shared/standard access mode and Spark Connect raise
    PySparkAttributeError (an AttributeError subclass) on sparkContext/_jvm
    access; callers fall back to the INFO default.
    """
    try:
        return (
            get_or_create_spark_session()
            .sparkContext._jvm.org.apache.log4j.LogManager.getRootLogger()
            .getLevel()
        )
    except Exception:
        return None


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


_EXT_DIST_PREFIX = "spark-kindling-ext-"
_LEGACY_EXT_DIST_PREFIX = "kindling-ext-"


def normalize_dist_name(name: str) -> str:
    """Normalize a distribution name per PEP 503 (dashes, case)."""
    import re

    return re.sub(r"[-_.]+", "-", name).strip().lower()


def dist_name_candidates(dist_name: str) -> List[str]:
    """Distribution names to try for a requested package: the name itself,
    plus its pre/post-rename alias for kindling extension distributions
    (kindling-ext-* <-> spark-kindling-ext-*), so configs written against
    either naming generation keep resolving during the transition.
    """
    name = normalize_dist_name(dist_name)
    candidates = [name]
    if name.startswith(_EXT_DIST_PREFIX):
        candidates.append(_LEGACY_EXT_DIST_PREFIX + name[len(_EXT_DIST_PREFIX) :])
    elif name.startswith(_LEGACY_EXT_DIST_PREFIX):
        candidates.append(_EXT_DIST_PREFIX + name[len(_LEGACY_EXT_DIST_PREFIX) :])
    return candidates


def find_installed_distribution(dist_name: str):
    """Return (canonical_name, version) for the installed distribution matching
    ``dist_name`` or one of its rename aliases; (None, None) if not installed.
    """
    from importlib import metadata as importlib_metadata

    for candidate in dist_name_candidates(dist_name):
        try:
            dist = importlib_metadata.distribution(candidate)
            return dist.metadata["Name"], dist.version
        except importlib_metadata.PackageNotFoundError:
            continue
        except Exception as e:
            _BOOTSTRAP_LOGGER.debug(f"Metadata lookup failed for {candidate}: {e}")
            continue
    return None, None


def resolve_import_names(dist_name: str) -> List[str]:
    """Import package names provided by a distribution.

    A distribution's import name is not derivable from its pip name
    (spark-kindling-ext-sdp installs ``kindling_ext_sdp``), so resolve from
    installed metadata first: top_level.txt when present, otherwise the
    distribution's file listing. Falls back to the dash-to-underscore
    heuristic over the name and its rename aliases when the distribution
    is not installed or exposes no metadata (e.g. PYTHONPATH dev checkouts).
    """
    from importlib import metadata as importlib_metadata

    resolved: List[str] = []

    for candidate in dist_name_candidates(dist_name):
        try:
            dist = importlib_metadata.distribution(candidate)
        except importlib_metadata.PackageNotFoundError:
            continue
        except Exception as e:
            _BOOTSTRAP_LOGGER.debug(f"Metadata lookup failed for {candidate}: {e}")
            continue

        top_level = None
        try:
            top_level = dist.read_text("top_level.txt")
        except Exception as e:
            _BOOTSTRAP_LOGGER.debug(f"Could not read top_level.txt for {candidate}: {e}")
            top_level = None
        if top_level:
            resolved.extend(line.strip() for line in top_level.splitlines() if line.strip())

        if not resolved:
            try:
                for file in dist.files or []:
                    parts = file.parts
                    if len(parts) > 1 and parts[0].endswith((".dist-info", ".data")):
                        continue
                    if len(parts) > 1 and parts[1] == "__init__.py":
                        resolved.append(parts[0])
                    elif len(parts) == 1 and parts[0].endswith(".py"):
                        resolved.append(parts[0][:-3])
            except Exception:
                pass

        if resolved:
            break

    # Heuristic fallback (and safety net): dash-to-underscore over all aliases.
    for candidate in dist_name_candidates(dist_name):
        resolved.append(candidate.replace("-", "_"))

    seen = set()
    unique = []
    for name in resolved:
        if name and name not in seen:
            seen.add(name)
            unique.append(name)
    return unique


def import_distribution_modules(dist_name: str, logger) -> bool:
    """Import the top-level modules of a distribution so import-time
    registration hooks (DI bindings, provider registration) run.

    Tries every resolved import-name candidate; returns True if at least
    one imported. Modules already present in sys.modules count as loaded.
    """
    imported_any = False
    for import_name in resolve_import_names(dist_name):
        if import_name in sys.modules:
            imported_any = True
            continue
        try:
            if importlib.util.find_spec(import_name) is None:
                continue
        except (ImportError, ValueError):
            continue
        except (
            Exception
        ) as e:  # noqa: BLE001 -- find_spec imports parents; must not abort bootstrap
            logger.warning(
                f"Failed to import extension module {import_name}: {type(e).__name__}: {e}"
            )
            continue
        try:
            importlib.import_module(import_name)
            logger.info(f"Loaded extension module: {import_name}")
            imported_any = True
        except Exception as e:  # noqa: BLE001 -- a broken extension must not abort bootstrap
            logger.warning(
                f"Failed to import extension module {import_name}: {type(e).__name__}: {e}"
            )
    return imported_any


def _is_kindling_distribution(name: str) -> bool:
    normalized = name.strip().lower().replace("_", "-").replace(".", "-")
    return normalized == "spark-kindling" or normalized.startswith("spark-kindling-")


def _extension_requirements(wheel_path) -> Tuple[List[str], List[Any]]:
    """Split a wheel's Requires-Dist into (other requirements to install,
    Kindling requirements to check). Requirements whose marker doesn't apply
    here (extras-only, other Python versions) are dropped."""
    import zipfile

    from packaging.requirements import Requirement

    with zipfile.ZipFile(wheel_path) as wheel:
        metadata_name = next(
            name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = wheel.read(metadata_name).decode("utf-8")

    others: List[str] = []
    kindling: List[Any] = []
    for line in metadata.splitlines():
        if not line.startswith("Requires-Dist:"):
            continue
        requirement = Requirement(line.split(":", 1)[1].strip())
        if requirement.marker is not None and not requirement.marker.evaluate({"extra": ""}):
            continue
        if _is_kindling_distribution(requirement.name):
            kindling.append(requirement)
        else:
            requirement.marker = None
            others.append(str(requirement))
    return others, kindling


class ExtensionCompatibilityError(RuntimeError):
    """An extension's Kindling requirement is not met by the installed
    runtime. Fails the bootstrap rather than being logged and skipped like a
    missing or uninstallable wheel."""


def _installed_kindling_requirements(package_name: str) -> Optional[List[Any]]:
    """The Kindling requirements of an installed extension (by its
    distribution name or a rename alias), or None when it has no installed
    package metadata (e.g. importable files left behind without it)."""
    from importlib.metadata import PackageNotFoundError, requires

    from packaging.requirements import Requirement

    for candidate in dist_name_candidates(package_name):
        try:
            declared = requires(candidate) or []
        except PackageNotFoundError:
            continue
        kindling = []
        for line in declared:
            requirement = Requirement(line)
            if requirement.marker is not None and not requirement.marker.evaluate({"extra": ""}):
                continue
            if _is_kindling_distribution(requirement.name):
                kindling.append(requirement)
        return kindling
    return None


def _require_installed_kindling(package_spec: str, requirements: Iterable[Any]) -> None:
    """Check an extension's Kindling requirements against what's installed.

    Kindling's own distributions are never installed as an extension's
    dependencies: pip would replace the runtime the bootstrap installed
    (from the artifacts store, at kindling_version) with an index copy. An
    extension's major.minor states which Kindling major.minor it works with,
    so a mismatch is an error naming both versions."""
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as installed_version

    for requirement in requirements:
        try:
            installed = installed_version(requirement.name)
        except PackageNotFoundError:
            raise ExtensionCompatibilityError(
                f"{package_spec} requires {requirement}, which is not installed; "
                f"list {requirement.name} before it in kindling.extensions"
            ) from None
        if not requirement.specifier.contains(installed, prereleases=True):
            raise ExtensionCompatibilityError(
                f"{package_spec} requires {requirement}, but {requirement.name} "
                f"{installed} is installed; deploy the extension release that matches "
                f"this Kindling version (kindling runtime deploy --extension)"
            )


def _normalized_distribution_name(dist) -> str:
    return (dist.metadata.get("Name") or "").strip().lower().replace("_", "-")


def _distribution_module_names(dist) -> set:
    """Every module a distribution's files provide, including submodules
    (opentelemetry.util.types) and compiled ones (psutil._psutil_linux)."""
    modules: set = set()
    for file in dist.files or []:
        path = str(file).replace("\\", "/")
        if path.startswith(".."):
            continue
        directory, _, filename = path.rpartition("/")
        if filename.endswith(".py"):
            stem = filename[: -len(".py")]
        elif filename.endswith((".so", ".pyd")):
            # Compiled modules: psutil/_psutil_linux.abi3.so -> psutil._psutil_linux.
            # Missing these left a runtime's old C module cached under an
            # upgraded pure-Python package (psutil on Databricks).
            stem = filename.split(".", 1)[0]
        else:
            continue
        module = f"{directory}/{stem}".strip("/").replace("/", ".")
        if module.endswith(".__init__"):
            module = module[: -len(".__init__")]
        modules.add(module)
    return modules


def _installed_distribution_versions() -> Dict[str, str]:
    """{normalized distribution name: version}, as import would resolve it
    now (the first copy on sys.path wins, like importlib.metadata)."""
    from importlib.metadata import distributions

    versions: Dict[str, str] = {}
    for dist in distributions():
        name = _normalized_distribution_name(dist)
        if name:
            versions.setdefault(name, dist.version)
    return versions


def _installed_distribution_snapshot() -> Dict[str, Tuple[str, frozenset]]:
    """{name: (version, loaded modules)} before an install. The modules are
    the ones currently in sys.modules: an upgrade can remove or rename a
    module, so after the install the new file list no longer names it, yet
    the old copy is still loaded and must be evicted too."""
    from importlib.metadata import distributions

    loaded = set(sys.modules)
    snapshot: Dict[str, Tuple[str, frozenset]] = {}
    for dist in distributions():
        name = _normalized_distribution_name(dist)
        if name and name not in snapshot:
            snapshot[name] = (dist.version, frozenset(_distribution_module_names(dist) & loaded))
    return snapshot


def _modules_of_changed_distributions(before: Mapping[str, Any]) -> set:
    """Module names of the distributions an install added or changed since
    `before` (a _installed_distribution_snapshot): every module of the new
    copy plus the previously loaded modules of the old copy. Unchanged
    distributions are left out: evicting a module that is already current
    would load a second copy of it."""
    from importlib.metadata import distributions

    def version_and_modules(entry):
        if isinstance(entry, tuple):
            return entry
        return entry, frozenset()  # a plain {name: version} mapping

    after = _installed_distribution_versions()
    modules: set = set()
    seen: set = set()
    for dist in distributions():
        name = _normalized_distribution_name(dist)
        if not name or name in seen:
            continue
        seen.add(name)  # the copy import now resolves (first on sys.path)
        old_version, old_modules = version_and_modules(before.get(name, (None, frozenset())))
        if old_version == after.get(name):
            continue
        modules |= _distribution_module_names(dist)
        modules |= set(old_modules)
    return modules


def _evict_stale_modules(logger, modules: Iterable[str]) -> None:
    """Drop cached imports so the next import loads the freshly installed copy.

    A runtime can import a package (e.g. Databricks imports opentelemetry
    before Kindling starts) that an install then upgrades; evicting only the
    top-level name left its submodules cached, so the new code imported
    against the old ones (`cannot import name '_ExtendedAttributes' from
    'opentelemetry.util.types'`).

    Namespace-package parents (`opentelemetry`, `opentelemetry.util`, `azure`)
    are evicted too: they carry only a search path, which otherwise keeps
    pointing at the old copy. Regular packages that are not themselves stale
    (an unchanged `azure.core`) stay cached, so no second copy is loaded, and
    are re-attached to their recreated parent."""
    targets = set(modules)
    for module_name in list(targets):
        parts = module_name.split(".")
        for depth in range(1, len(parts)):
            parent = ".".join(parts[:depth])
            cached = sys.modules.get(parent)
            if cached is not None and getattr(cached, "__file__", None) is None:
                targets.add(parent)  # a namespace package
    evicted_namespaces = []
    for module_name in list(sys.modules):
        if module_name in targets:
            logger.debug(f"Clearing cached import: {module_name}")
            if getattr(sys.modules[module_name], "__file__", None) is None:
                evicted_namespaces.append(module_name)
            del sys.modules[module_name]
    # Re-attach cached children that were kept (e.g. azure.core under a
    # recreated `azure`): import binds `azure.core` through the parent's
    # attribute, which a fresh namespace module does not have.
    for parent in sorted(evicted_namespaces, key=lambda name: name.count(".")):
        children = [
            name
            for name in sys.modules
            if name.startswith(parent + ".") and "." not in name[len(parent) + 1 :]
        ]
        if not children:
            continue
        try:
            fresh = importlib.import_module(parent)
        except Exception as exc:  # noqa: BLE001 -- the next import retries
            logger.debug(f"Could not re-import {parent}: {exc}")
            continue
        for child in children:
            if not hasattr(fresh, child.rsplit(".", 1)[1]):
                setattr(fresh, child.rsplit(".", 1)[1], sys.modules[child])


# Packages a Spark runtime ships that an extension install keeps at the
# installed version (when installed): the runtime's own code and preloaded
# modules depend on them. pip may still add packages the runtime lacks and
# upgrade other ones whose installed version doesn't satisfy a requirement.
_RUNTIME_KEY_DISTRIBUTIONS = frozenset(
    {
        "azure-core",
        "azure-identity",
        "azure-monitor-opentelemetry",
        "msal",
        "numpy",
        "opentelemetry-api",
        "opentelemetry-sdk",
        "opentelemetry-semantic-conventions",
        "pandas",
        "protobuf",
        "py4j",
        "pyarrow",
        "pyspark",
        "typing-extensions",
    }
)

# pip can't replace a package installed without uninstall records (conda,
# distutils) or shadow a site-packages one from the user site.
_PIP_UNINSTALL_FAILURES = (
    "Cannot uninstall",
    "uninstall-no-record-file",
    "lack sys.path precedence",
)


# pip's messages for requirements that can't be met together, e.g. because a
# pinned runtime package conflicts with what the extension needs.
_PIP_RESOLVER_CONFLICTS = (
    "ResolutionImpossible",
    "conflicting dependencies",
    "Could not find a version that satisfies the requirement",
    "No matching distribution found",
)


def _distribution_version(entry) -> Optional[str]:
    """The version from a _installed_distribution_snapshot entry or a plain
    version string."""
    return entry[0] if isinstance(entry, tuple) else entry


def _key_distribution_constraints(versions: Mapping[str, Any]) -> List[str]:
    """pip constraints pinning each installed key distribution at its
    installed version."""
    from packaging.version import InvalidVersion, Version

    constraints = []
    for name in sorted(_RUNTIME_KEY_DISTRIBUTIONS):
        version = _distribution_version(versions.get(name))
        if not version:
            continue
        try:
            Version(version)
        except InvalidVersion:
            continue
        constraints.append(f"{name}=={version}")
    return constraints


def _pip_install_keeping_installed(logger, args: List[str]):
    """Run `pip install <args>`, keeping what the runtime has installed.

    pip keeps every installed version that satisfies the requirements (no
    --upgrade or --ignore-installed), and the runtime's key distributions are
    pinned at their installed versions, so pip picks the highest versions of
    the new requirements that work with them. Fallbacks, each logged:
    - pip can't replace an installed package (no uninstall records, or a
      user-site install under a virtualenv): retry with --ignore-installed;
      the key distributions stay pinned.
    - the requirements can't be met with the key distributions pinned (a
      resolver conflict): retry without the pins; the bootstrap then reports
      what was replaced. Any other failure is returned unchanged.
    """
    pip_install = [sys.executable, "-m", "pip", "install", "--disable-pip-version-check"]
    constraints = _key_distribution_constraints(_installed_distribution_versions())
    ignore_installed = False
    with tempfile.TemporaryDirectory(prefix="kindling_pip_") as work_dir:
        constraints_file = os.path.join(work_dir, "runtime-constraints.txt")
        with open(constraints_file, "w", encoding="utf-8") as handle:
            handle.write("".join(f"{line}\n" for line in constraints))
        while True:
            command = [*pip_install]
            if ignore_installed:
                command.append("--ignore-installed")
            if constraints:
                command += ["-c", constraints_file]
            result = subprocess.run([*command, *args], capture_output=True, text=True)
            if result.returncode == 0:
                return result
            output = f"{result.stdout}\n{result.stderr}"
            if not ignore_installed and any(
                failure in output for failure in _PIP_UNINSTALL_FAILURES
            ):
                logger.warning(
                    "pip could not replace an installed package; retrying with "
                    f"--ignore-installed: {' '.join(args)}"
                )
                ignore_installed = True
            elif constraints and any(conflict in output for conflict in _PIP_RESOLVER_CONFLICTS):
                logger.warning(
                    f"{' '.join(args)} can't be installed with the runtime's "
                    f"{', '.join(constraints)}; retrying without keeping them"
                )
                constraints = []
            else:
                # Anything else (network, a corrupt wheel, disk) is returned as
                # is: retrying unpinned could replace the runtime's packages.
                return result


def _colocate_extended_packages(logger, before: Mapping[str, Any]) -> None:
    """Copy a regular package next to the files an install added into it.

    A distribution can install modules into another distribution's regular
    package (azure-core-tracing-opentelemetry adds
    azure/core/tracing/ext/opentelemetry_span). When pip keeps that package's
    owner where the runtime installed it (Databricks: the system
    site-packages, behind the job's virtualenv), Python resolves the package
    to that directory only and the added modules can't be imported. The owner
    is reinstalled, at the same version, where the install put the new files,
    together with the other distributions that had installed into it there
    (their files would otherwise be hidden behind the new copy).

    Loaded packages of a reinstalled distribution are kept, not evicted, so
    classes already imported from them (an azure.core exception a preloaded
    SDK raises) stay the ones later imports get. The new directory goes first
    on each one's ``__path__``, so its not-yet-imported subpackages (a
    preloaded azure.core, but not azure.core.tracing) load from the new copy,
    next to the added modules."""
    from importlib.metadata import distributions

    owners: Dict[str, Tuple[str, str, str]] = {}  # package dir -> owner
    installed = []
    seen: set = set()
    for dist in distributions():
        name = _normalized_distribution_name(dist)
        if not name or name in seen:
            continue
        seen.add(name)  # the copy import resolves (first on sys.path)
        location = os.path.realpath(str(dist.locate_file("")))
        files = [str(file).replace("\\", "/") for file in dist.files or []]
        installed.append((name, dist.version, location, files))
        for path in files:
            if path.endswith("/__init__.py") and not path.startswith(".."):
                owners.setdefault(path[: -len("/__init__.py")], (name, dist.version, location))

    def owners_of(files):
        for path in files:
            parts = path.split("/")[:-1]
            for depth in range(1, len(parts) + 1):
                owner = owners.get("/".join(parts[:depth]))
                if owner:
                    yield owner

    split: Dict[str, Tuple[str, str]] = {}  # owner -> (version, location)
    for name, version, location, files in installed:
        if _distribution_version(before.get(name)) == version:
            continue  # not added or changed by this install
        for owner in owners_of(files):
            if owner[0] != name and owner[2] != location:
                split[owner[0]] = (owner[1], owner[2])
    if not split:
        return

    relocated = dict(split)  # distribution -> (version, location it is moved from)
    for name, version, location, files in installed:
        if name in relocated:
            continue
        for owner in owners_of(files):
            if owner[0] in split and owner[0] != name and location == split[owner[0]][1]:
                relocated[name] = (version, location)  # installed into the owner there
                break

    pins = [f"{name}=={version}" for name, (version, _location) in sorted(relocated.items())]
    logger.info(f"Reinstalling {', '.join(pins)} next to the packages installed into it")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-deps",
            "--ignore-installed",
            *pins,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.warning(f"Could not reinstall {', '.join(pins)}: {result.stderr}")
        return

    for dist in distributions():
        name = _normalized_distribution_name(dist)
        location = os.path.realpath(str(dist.locate_file("")))
        if name not in relocated or location == relocated[name][1]:
            continue  # not reinstalled, or the original copy
        for path in dist.files or []:
            path = str(path).replace("\\", "/")
            if not path.endswith("/__init__.py") or path.startswith(".."):
                continue
            package = path[: -len("/__init__.py")]
            module = sys.modules.get(package.replace("/", "."))
            search_path = getattr(module, "__path__", None)
            directory = os.path.join(location, package)
            if isinstance(search_path, list) and directory not in search_path:
                search_path.insert(0, directory)


def _complete_runtime_manifest(config_service, platform, logger) -> None:
    """Fill in the runtime manifest once platform services exist and log it
    on one INFO line. Never breaks bootstrap."""
    try:
        from kindling.runtime_manifest import complete_runtime_manifest

        # The session platform init made, if any: never create one here.
        spark = None
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
        except Exception:
            spark = None
        manifest = complete_runtime_manifest(
            installed=_installed_distribution_versions(),
            key_distributions=_RUNTIME_KEY_DISTRIBUTIONS,
            spark=spark,
            config_service=config_service,
            platform=platform,
        )
        if manifest is not None:
            logger.info(manifest.summary())
            _warn_untested_runtime(manifest, logger)
    except Exception as manifest_error:
        logger.warning(f"Runtime manifest could not be completed: {manifest_error}")


def _warn_untested_runtime(manifest, logger) -> None:
    """Warn when the runtime is not one Kindling is system-tested on, or
    differs from the closest one at major.minor."""
    match = manifest.runtime_match
    if match is not None and match.should_warn:
        logger.warning(match.describe(manifest.platform, manifest.platform_runtime_version))


def _record_run(config_service, logger, status: str, error: Optional[str] = None) -> None:
    """Upsert this initialization's ``system.runs`` row when
    ``kindling.runs.enabled`` is on. Never breaks bootstrap."""
    try:
        from kindling.run_records import record_run

        record_run(config_service, status, error=error, logger=logger)
    except Exception as record_error:
        logger.warning(f"Could not record the run in system.runs: {record_error}")


def _emit_runtime_manifest_span(config_service) -> None:
    """A scalar-only ``runtime_manifest`` span for this initialization."""
    try:
        from kindling.runtime_manifest import get_runtime_manifest
        from kindling.spark_trace import SparkTraceProvider
        from kindling.trace_ops import COMPONENT_BOOTSTRAP, tracing_gates

        manifest = get_runtime_manifest()
        if manifest is None or not tracing_gates(config_service).minimal:
            return
        tp = GlobalInjector.get(SparkTraceProvider)
        with tp.span(
            operation="runtime_manifest",
            component=COMPONENT_BOOTSTRAP,
            details=manifest.span_details(),
        ):
            pass
    except Exception as span_error:
        _BOOTSTRAP_LOGGER.debug("Runtime manifest span failed (non-fatal): %s", span_error)


def _report_replaced_distributions(logger, before: Mapping[str, Any], package_spec: str) -> None:
    """Warn about each installed distribution an install changed the version
    of, so a runtime package replaced by an extension shows in the job log,
    log each one it added, and record both in the runtime manifest."""
    after = _installed_distribution_versions()
    added = sorted(name for name in after if name not in before)
    if added:
        logger.info(
            f"Installing {package_spec} added "
            + ", ".join(f"{name} {after[name]}" for name in added)
        )
    try:
        from kindling.runtime_manifest import record_package_changes

        record_package_changes(
            {name: _distribution_version(entry) for name, entry in before.items()},
            after,
            package_spec,
        )
    except Exception as record_error:
        _BOOTSTRAP_LOGGER.debug("Runtime manifest change capture failed: %s", record_error)
    for name in sorted(before):
        old_version = _distribution_version(before[name])
        new_version = after.get(name)
        if new_version is not None and new_version != old_version:
            logger.warning(
                f"Installing {package_spec} replaced installed {name} "
                f"{old_version} with {new_version}"
            )


def install_bootstrap_dependencies(logger, bootstrap_config, artifacts_storage_path=None):
    """Install packages needed for framework bootstrap

    Args:
        logger: Logger instance
        bootstrap_config: Bootstrap configuration dict
        artifacts_storage_path: Path to artifacts storage (for loading extension wheels)
    """

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
        """Get installed package version by distribution name (rename-alias aware)."""
        from importlib import metadata as importlib_metadata

        for candidate in dist_name_candidates(package_name):
            try:
                return importlib_metadata.version(candidate)
            except Exception:
                continue
        return None

    def is_package_installed(package_spec, check_version: bool = False):
        """Check if package is installed and optionally satisfies requested version.

        Presence is judged by distribution metadata first (authoritative even
        when the import name differs from the pip name), then by locating any
        of the distribution's import packages — which covers PYTHONPATH-style
        dev checkouts that have no installed metadata.
        """
        try:
            package_name = get_package_name(package_spec)
            if get_installed_version(package_name) is None:
                found = False
                for import_name in resolve_import_names(package_name):
                    try:
                        if importlib.util.find_spec(import_name) is not None:
                            found = True
                            break
                    except (ImportError, ValueError):
                        continue
                if not found:
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
            versions_before = _installed_distribution_snapshot()
            # Use --ignore-installed to force installation even if package exists in system site-packages
            # This is critical for platforms like Databricks where system packages may be outdated.
            # A required package is an explicit request, so it may replace runtime packages
            # (unlike an extension's requirements); the replacements are reported.
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "-q", "--ignore-installed", package_spec]
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

            _report_replaced_distributions(logger, versions_before, package_spec)

            # Clear any cached imports of this package to force reload from new location
            importlib.invalidate_caches()
            _evict_stale_modules(
                logger,
                set(resolve_import_names(get_package_name(package_spec)))
                | _modules_of_changed_distributions(versions_before),
            )

        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install {package_spec}: {e}")

    def load_extension_wheel(package_spec, storage_utils, artifacts_path, temp_path=None):
        """Install extension wheel from artifacts storage (like kindling-ext-otel-azure)"""
        if is_package_installed(package_spec, check_version=True):
            # Still check it against the runtime: after a runtime upgrade an
            # extension installed earlier may require a different Kindling.
            installed_requirements = _installed_kindling_requirements(
                get_package_name(package_spec)
            )
            if installed_requirements is not None:
                _require_installed_kindling(package_spec, installed_requirements)
                logger.info(f"Extension already installed: {get_package_name(package_spec)}")
                return
            # Importable but without package metadata, so its requirements
            # can't be checked: reinstall it from the artifacts store, which
            # checks the wheel's.
            logger.info(
                f"Extension {get_package_name(package_spec)} is importable but has no "
                "package metadata; reinstalling it to check it against this runtime"
            )
        elif is_package_installed(package_spec, check_version=False):
            logger.info(f"Extension version mismatch, upgrading to satisfy: {package_spec}")

        import os
        import tempfile

        package_name = get_package_name(package_spec)
        # Wheel filenames derive from the distribution name with dashes
        # underscored (kindling-ext-otel-azure -> kindling_ext_otel_azure).
        # Match both forms of the name and of its rename aliases so configs
        # written against either extension naming generation find wheels
        # published under either name in the lake.
        wheel_prefixes: List[str] = []
        for candidate in dist_name_candidates(package_name):
            for prefix in (candidate.replace("-", "_"), candidate):
                if prefix not in wheel_prefixes:
                    wheel_prefixes.append(prefix)

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
                    for candidate_prefix in wheel_prefixes:
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

                # Check the wheel's Kindling requirements against the
                # installed runtime (never install them), install its other
                # requirements, keeping the runtime's packages wherever they
                # satisfy them, and only then the wheel itself (--no-deps): a
                # failed dependency install leaves the extension uninstalled,
                # so the next bootstrap retries both.
                logger.info(f"Installing {wheel_filename}")
                versions_before = _installed_distribution_snapshot()
                other_requirements, kindling_requirements = _extension_requirements(local_path)
                _require_installed_kindling(package_spec, kindling_requirements)
                result = None
                if other_requirements:
                    logger.debug(f"Installing extension dependencies: {other_requirements}")
                    result = _pip_install_keeping_installed(logger, other_requirements)
                if result is None or result.returncode == 0:
                    result = _pip_install_keeping_installed(logger, ["--no-deps", local_path])

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

                    _colocate_extended_packages(logger, versions_before)
                    _report_replaced_distributions(logger, versions_before, package_spec)
                    importlib.invalidate_caches()
                    _evict_stale_modules(
                        logger,
                        set(resolve_import_names(package_name))
                        | {
                            "typing_extensions",
                            "azure",
                            "azure.monitor",
                            "azure.monitor.opentelemetry",
                            "opentelemetry",
                        }
                        | _modules_of_changed_distributions(versions_before),
                    )

                    logger.info(f"Successfully installed extension: {package_spec}")
                else:
                    logger.error(f"Failed to install extension {package_spec}: {result.stderr}")
                    logger.debug(f"Extension install stdout: {result.stdout}")

            except ExtensionCompatibilityError:
                raise
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

        except ExtensionCompatibilityError:
            raise
        except Exception as e:
            logger.error(f"Failed to install extension {package_spec}: {e}")

    # Install required packages (dependencies - no import, from PyPI)
    required = bootstrap_config.get("required_packages") or []
    logger.info(f"Required packages: {required}")
    for package in required:
        load_if_needed(package)

    # Install and import extensions (Kindling extensions from artifacts storage)
    extensions = bootstrap_config.get("extensions") or []
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
                extension_name = get_package_name(extension)
                logger.debug(f"Importing extension modules for: {extension_name}")
                if not import_distribution_modules(extension_name, logger):
                    logger.warning(
                        f"No importable module found for extension {extension_name}; "
                        "import-time registration hooks did not run"
                    )
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


def _apply_spark_configs(config_service, logger) -> None:
    """Push spark_configs entries from the config service to the live Spark session.

    Settings defined under spark_configs in settings.yaml or passed via
    config:spark_configs=... are stored in Dynaconf but have no effect unless
    they are explicitly pushed to SparkConf.  This function does that push.
    """
    try:
        from kindling.spark_session import get_or_create_spark_session

        spark = get_or_create_spark_session()
        if spark is None:
            return

        # spark_configs may be keyed as "spark_configs" (flat) or "SPARK_CONFIGS"
        # (Dynaconf-uppercased).  Try both.
        raw = config_service.get("spark_configs") or config_service.get("SPARK_CONFIGS")
        if not raw or not isinstance(raw, dict):
            return

        applied = []
        for key, value in raw.items():
            try:
                spark.conf.set(key, str(value))
                applied.append(key)
            except Exception as exc:
                logger.warning(f"Could not apply spark_config '{key}': {exc}")

        if applied:
            logger.info(f"Applied {len(applied)} spark_config(s) to session: {applied}")
    except Exception as exc:
        logger.warning(f"spark_configs application failed: {exc}")


def _bind_plain_telemetry_providers(logger=None) -> None:
    """Rebind telemetry providers to the JVM-free implementations.

    Same forced-rebind pattern as kindling_ext_otel_azure._register_providers:
    the concrete SparkLoggerProvider stays bound (dual-bind) so callers that
    resolve the concrete class keep working. The bootstrap has already
    resolved PythonLoggerProvider by now, so the rebind must also evict that
    cached singleton (GlobalInjector.rebind_singleton).

    An interface an extension has since rebound (a second initialize_framework
    after kindling_ext_otel_azure registered its providers) is left alone:
    the extension's module is already imported, so nothing would register
    its override again.
    """
    from kindling.plain_telemetry import (
        PlainPythonLoggerProvider,
        PlainPythonTraceProvider,
    )
    from kindling.spark_log_provider import SparkLoggerProvider
    from kindling.spark_trace import SparkTraceProvider

    for interface, plain in (
        (PythonLoggerProvider, PlainPythonLoggerProvider),
        (SparkLoggerProvider, PlainPythonLoggerProvider),
        (SparkTraceProvider, PlainPythonTraceProvider),
    ):
        override = GlobalInjector.rebound_to(interface)
        if override is not None and override is not plain:
            if logger:
                logger.info(f"Keeping {override.__name__} for {interface.__name__}")
            continue
        GlobalInjector.rebind_singleton(interface, plain)
    if logger:
        logger.info("No py4j JVM bridge detected; using plain-python telemetry providers")


def _resolve_and_validate_secrets(config_service, logger) -> None:
    """Re-run @secret resolution now that platform services exist, and fail
    loudly if anything is still unresolved afterward.

    An earlier resolution pass happens inside ConfigService.initialize()
    (the config_init phase, before platform_init) via Dynaconf's loader
    chain -- at that point there is no SecretProvider to ask yet, so every
    @secret reference is left as a literal, silently, by design. This
    second pass, run after platform_init, is the one that can actually
    succeed.

    If a reference is STILL unresolved here, letting bootstrap complete
    anyway hands a literal "@secret:..." string to whatever config-consuming
    code reads it next -- e.g. the EventHub provider parsing it as a
    connection string, failing with an unrelated-looking KeyError far from
    the real cause. Raising here instead makes the failure immediate and
    actionable, naming only the config paths involved (never secret values).
    """
    from kindling.config_loaders import (
        find_unresolved_secret_references,
        load_secrets_from_provider,
    )

    if not (hasattr(config_service, "dynaconf") and config_service.dynaconf is not None):
        return

    pending = find_unresolved_secret_references(config_service.dynaconf)
    if not pending:
        logger.debug("No @secret references pending resolution")
        return

    logger.info(
        f"Resolving {len(pending)} @secret reference(s) now that platform services "
        f"are available: {', '.join(sorted(pending))}"
    )
    load_secrets_from_provider(config_service.dynaconf, silent=True)
    unresolved = find_unresolved_secret_references(config_service.dynaconf)
    if unresolved:
        raise RuntimeError(
            "Failed to resolve @secret reference(s) at: "
            f"{', '.join(sorted(unresolved))}. Check that the platform secret "
            "provider is reachable (Databricks: kindling.secrets.secret_scope "
            "and dbutils.secrets availability; Fabric/Synapse: Key Vault "
            "configuration) and that the referenced secret(s) exist."
        )
    logger.info(f"Resolved all {len(pending)} @secret reference(s) successfully")


def _resolve_registered_tag_secrets(logger) -> None:
    """Resolve @secret references embedded directly in an entity/pipe's own
    ``tags=`` registration param -- e.g. ``@entity(..., tags={"provider.
    eventhub.connectionString": "@secret:scope:key"})`` written straight in
    app code, as opposed to a ``dataentities:``/``datapipes:``/``entity_tags:``
    config-overlay section.

    These never pass through Dynaconf's config tree at all -- they live only
    in the manager's ``_raw_params`` -- so ``_resolve_and_validate_secrets``
    (which only walks ``config_service.dynaconf``) cannot see or resolve
    them, and neither can the ``dataentities:``-overlay re-apply. Without
    this pass, a secret declared this way stays a literal string forever,
    handed to whatever provider reads it (e.g. EventHub parsing it as a
    connection string), failing the same confusing way @secret resolution
    was originally meant to prevent.

    Silently returns if no SecretProvider is bound (e.g. standalone without
    any registered secrets) -- there is nothing to resolve against, and
    nothing here implies a reference exists that needs one. Raises if any
    resolution actually fails, naming only entity/pipe id + tag key, never
    the attempted value.
    """
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.platform_provider import SecretProvider

    try:
        secret_provider = get_kindling_service(SecretProvider)
    except Exception:
        return

    all_failures = []
    for registry_iface in (DataEntityRegistry, DataPipesRegistry):
        try:
            registry = get_kindling_service(registry_iface)
        except Exception:
            continue
        resolver = getattr(registry, "resolve_secret_tags", None)
        if resolver is None:
            continue
        all_failures.extend(resolver(secret_provider))

    if all_failures:
        raise RuntimeError(
            "Failed to resolve @secret reference(s) declared directly in "
            f"registered tags at: {', '.join(sorted(all_failures))}. Check "
            "that the platform secret provider is reachable and that the "
            "referenced secret(s) exist."
        )


def _config_service_matches_request(config_service, config: Dict[str, Any]) -> bool:
    """True if ``config_service`` is already initialized with the same
    ``environment``/``config_files`` this call is requesting.

    Two separate "already initialized, skip re-init" short-circuits in
    ``initialize_framework`` exist for idempotent re-entry -- e.g.
    re-running a notebook cell that calls ``initialize()`` with an
    unchanged ``BOOTSTRAP_CONFIG`` shouldn't tear down and rebuild Spark/
    platform services or reload Dynaconf. But neither compared anything
    about the *requested* config before skipping, so a second call in the
    same process that explicitly passes a different ``environment``/
    ``config_files`` (a distinct app, a different env in a test, a second
    ``kindling.initialize()`` call) still short-circuited -- silently
    keeping the *first* call's Dynaconf state (and therefore its
    ``entity_tags:``/``dataentities:`` overlays) instead of loading what was
    just asked for. Only short-circuit when the request actually matches
    what is already loaded.
    """
    if not (hasattr(config_service, "dynaconf") and config_service.dynaconf is not None):
        return False

    existing = getattr(config_service, "initial_config", None) or {}

    requested_env = config.get("environment")
    if requested_env is not None and requested_env != existing.get("environment"):
        return False

    requested_files = config.get("config_files")
    if requested_files is not None and requested_files != existing.get("config_files"):
        return False

    for directory_key in ("config_dir", "app_dir"):
        requested_dir = config.get(directory_key)
        if requested_dir is not None and str(requested_dir) != str(existing.get(directory_key)):
            return False

    return True


def _initialized_config_matches(config: Dict[str, Any]) -> bool:
    """Same check as `_config_service_matches_request`, looking up the
    current ConfigService singleton itself (for the outer
    ``is_framework_initialized()`` short-circuit, which runs before any
    local `config_service` reference is in scope)."""
    try:
        existing_config_service = get_kindling_service(ConfigService)
    except Exception:
        return False
    return _config_service_matches_request(existing_config_service, config)


def initialize_framework(config: Dict[str, Any], app_name: Optional[str] = None):
    """Linear framework initialization with Dynaconf config loading"""

    config = _merge_with_spark_kindling_config(dict(config or {}))
    if app_name is None:
        app_name = config.get("app_name")

    raw_registration_packages = config.get("registration_packages")
    if raw_registration_packages is None:
        registration_packages: List[str] = []
    elif isinstance(raw_registration_packages, str):
        registration_packages = [raw_registration_packages]
    else:
        registration_packages = [str(pkg) for pkg in raw_registration_packages]

    # Check if framework is already initialized -- with the config this call
    # is actually requesting. See _initialized_config_matches.
    already_initialized = is_framework_initialized()
    if already_initialized and _initialized_config_matches(config):
        _BOOTSTRAP_LOGGER.debug("Framework already initialized, skipping re-initialization")
        existing_service = get_kindling_service(PlatformServiceProvider).get_service()
        logger_provider = get_kindling_service(PythonLoggerProvider)
        _import_local_package_registrations(
            logger_provider.get_logger("KindlingBootstrap"), registration_packages
        )
        return existing_service

    if already_initialized:
        # Not a caller bug by itself -- a notebook cell can legitimately be
        # re-run with an edited BOOTSTRAP_CONFIG pointing at a new
        # environment -- but it's unusual enough in real (non-test,
        # non-notebook) usage to be worth a visible signal: this exact
        # silently-reusing-stale-config gap is what let a prior call's
        # entity_tags:/dataentities: overlays leak into an unrelated
        # environment's resolved config before this check existed.
        _BOOTSTRAP_LOGGER.warning(
            "initialize_framework() called again with a different environment/"
            "config_files than the currently-initialized configuration "
            "(requesting environment=%r) -- reconfiguring. Expected for a "
            "deliberate re-init (e.g. a notebook cell re-run with edited "
            "settings); if unexpected, a prior kindling.reset() call is "
            "probably missing.",
            config.get("environment"),
        )

    # Full (non-short-circuited) initialization: record phase timings for the
    # retro-flushed bootstrap span tree. Short-circuited re-inits above
    # deliberately record nothing.
    _PHASE_RECORDER.reset(start_time=datetime.now())
    # This initialization's runtime manifest (kindling.runtime_manifest):
    # begun before any install so every package change is recorded.
    from kindling.runtime_manifest import begin_runtime_manifest

    begin_runtime_manifest(
        kindling_version_requested=config.get("kindling_version"), app_name=app_name
    )

    # Extract bootstrap settings
    artifacts_storage_path = config.get("artifacts_storage_path")
    explicit_config_files = config.get("config_files")
    if explicit_config_files is None:
        config_files = None
    elif isinstance(explicit_config_files, (str, Path)):
        config_files = [str(explicit_config_files)]
    else:
        config_files = [str(path) for path in explicit_config_files]
    # Settings are found by convention: callers name the directories that
    # hold them (shared overlays in config_dir, the app's own in app_dir) and
    # bootstrap resolves the ordered file list itself, exactly as it does for
    # artifacts storage. An explicit config_files list still works but is the
    # legacy form.
    settings_config_dir = config.get("config_dir")
    settings_app_dir = config.get("app_dir")
    if config_files and not (settings_config_dir or settings_app_dir):
        _BOOTSTRAP_LOGGER.warning(
            "Bootstrap config 'config_files' is deprecated: pass 'config_dir' (shared "
            "overlays) and/or 'app_dir' (the app's settings) and let Kindling resolve "
            "the settings hierarchy by convention."
        )
    requested_environment = config.get("environment", "development")
    # Platform- and workspace-neutral local files are enough for the early
    # peeks below; the full local hierarchy is resolved once both are known.
    peek_files = list(config_files or []) or resolve_settings_files(
        settings_config_dir, settings_app_dir, requested_environment
    )

    # Explicit config files can name kindling.platform.environment before the
    # download hierarchy is selected. Discovered artifacts-storage files cannot:
    # platform is one of the inputs needed to choose those files, so using them
    # here would be circular.
    settings_platform = peek_settings_value(peek_files, "kindling.platform.environment")
    settings_discover_config_files = peek_settings_value(
        peek_files, "kindling.bootstrap.discover_config_files"
    )
    explicit_platform = (
        config.get("kindling.platform.environment")
        or config.get("platform_environment")
        or config.get("platform")
        or settings_platform
    )
    is_standalone = str(explicit_platform or "").strip().lower() == "standalone"
    use_lake_packages_value = config.get("use_lake_packages")
    if use_lake_packages_value is None:
        use_lake_packages = False if is_standalone else True
    else:
        use_lake_packages = _as_bool(use_lake_packages_value)
    environment = config.get("environment", "development")
    discover_config_files_value = (
        config.get("discover_config_files")
        if config.get("discover_config_files") is not None
        else config.get("kindling.bootstrap.discover_config_files")
    )
    if discover_config_files_value is None:
        discover_config_files_value = settings_discover_config_files
    discover_config_files = _as_bool(
        discover_config_files_value, default=artifacts_storage_path is not None
    )
    explicit_discover_config_files = discover_config_files_value is not None and _as_bool(
        discover_config_files_value
    )
    # Loading packages from artifacts storage relies on the same storage utilities as
    # config discovery. Keep packaged jobs strict so they do not silently start with
    # only explicit config_files when artifact discovery is unavailable.
    strict_config_discovery = explicit_discover_config_files or use_lake_packages

    # Early platform detection for config loading
    platform = None
    workspace_id = config.get("workspace_id")
    if explicit_platform:
        try:
            platform = detect_platform({"platform_service": explicit_platform})
        except Exception as e:
            _BOOTSTRAP_LOGGER.warning(
                "Could not resolve explicit platform %r: %s", explicit_platform, e
            )
    if discover_config_files and artifacts_storage_path:
        try:
            # Detect platform early to load platform-specific config
            platform = detect_platform(config={"platform_service": explicit_platform})

            # Get workspace ID based on platform unless supplied explicitly
            # through bootstrap/SparkConf. The explicit value selects the
            # workspace overlay for scheduled jobs where runtime discovery is
            # unavailable or intentionally avoided.
            if workspace_id is None:
                workspace_id = _get_workspace_id_for_platform(platform)
        except Exception as e:
            _BOOTSTRAP_LOGGER.warning(
                "Could not detect platform/workspace for config loading: %s", e
            )
            # Continue without platform/workspace-specific configs

    # Config downloads (the initial one here and any hot-reload later) stage
    # here; only the bootstrap parameters are known at this point.
    initial_temp_path = _resolve_initial_download_temp_path(config, platform)
    if discover_config_files and artifacts_storage_path:
        with _bootstrap_phase("config_download"):
            if not strict_config_discovery and _get_storage_utils() is None:
                _BOOTSTRAP_LOGGER.warning(
                    "Configuration discovery was enabled by artifacts_storage_path, "
                    "but storage utilities are unavailable; continuing with explicit "
                    "config_files only. Set discover_config_files=true to require "
                    "artifacts-storage discovery."
                )
                downloaded_config_files = []
            else:
                downloaded_config_files = download_config_files(
                    artifacts_storage_path=artifacts_storage_path,
                    environment=environment,
                    platform=platform,
                    workspace_id=_overlay_workspace_id(workspace_id, platform),
                    app_name=app_name,
                    temp_path=initial_temp_path,
                )
            if config_files:
                config_files = downloaded_config_files + config_files
            else:
                config_files = downloaded_config_files

    if settings_config_dir or settings_app_dir:
        # Local directories layer on top of anything discovered in artifacts
        # storage and below an explicit (legacy) config_files list.
        local_settings_files = resolve_settings_files(
            settings_config_dir,
            settings_app_dir,
            environment,
            platform=platform,
            workspace_id=_overlay_workspace_id(workspace_id, platform),
        )
        explicit_files = [str(path) for path in (config.get("config_files") or [])]
        if isinstance(config.get("config_files"), (str, Path)):
            explicit_files = [str(config.get("config_files"))]
        discovered = [path for path in (config_files or []) if path not in explicit_files]
        config_files = discovered + local_settings_files + explicit_files
        _BOOTSTRAP_LOGGER.info(
            "Resolved %d settings file(s) by convention from config_dir=%s app_dir=%s",
            len(local_settings_files),
            settings_config_dir,
            settings_app_dir,
        )

    from kindling.spark_config import configure_injector_with_config

    # Check if ConfigService is already properly initialized with this
    # request's environment/config_files (see _config_service_matches_request
    # -- a stale Dynaconf from a differently-configured prior call in this
    # process must not be reused just because *some* Dynaconf is present).
    with _bootstrap_phase("config_init"):
        try:
            config_service = get_kindling_service(ConfigService)
            if _config_service_matches_request(config_service, config):
                _BOOTSTRAP_LOGGER.debug(
                    "Config service already initialized, skipping configuration"
                )
            else:
                # ConfigService exists but not initialized yet, set it up
                injector = configure_injector_with_config(
                    config_files=config_files,
                    initial_config=config,  # BOOTSTRAP_CONFIG as overrides
                    environment=environment,
                    artifacts_storage_path=artifacts_storage_path,
                    platform=platform,
                    workspace_id=_overlay_workspace_id(workspace_id, platform),
                    app_name=app_name,
                    temp_path=initial_temp_path,
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
                workspace_id=_overlay_workspace_id(workspace_id, platform),
                app_name=app_name,
                temp_path=initial_temp_path,
            )
            config_service = get_kindling_service(ConfigService)

    logger = get_kindling_service(PythonLoggerProvider).get_logger("KindlingBootstrap")
    logger.info("Starting framework initialization")
    declaration_only_value = config_service.get(
        "kindling.bootstrap.declaration_only", config.get("declaration_only")
    )
    declaration_only = _as_bool(declaration_only_value)

    # Best-effort runtime feature discovery. These values are written into
    # kindling.runtime.features.* and can be overridden by kindling.features.*.
    with _bootstrap_phase("feature_discovery"):
        try:
            from kindling.features import discover_runtime_features

            discover_runtime_features(config_service, logger=logger)
        except Exception as feature_error:
            logger.debug(f"Runtime feature discovery failed (non-fatal): {feature_error}")

    # Capability-based telemetry selection: without a py4j JVM bridge (UC
    # shared/standard access mode clusters, Spark Connect) the JVM-backed
    # providers only degrade per call; swap in the plain-python
    # implementations instead. Overridable via kindling.features.spark.jvm_bridge.
    with _bootstrap_phase("telemetry_binding"):
        try:
            if get_feature_bool(config_service, "spark.jvm_bridge", default=True) is False:
                _bind_plain_telemetry_providers(logger)
                logger = get_kindling_service(PythonLoggerProvider).get_logger("KindlingBootstrap")
        except Exception as telemetry_error:
            logger.debug(f"Telemetry provider selection failed (non-fatal): {telemetry_error}")

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

    _bootstrap_error = None
    try:
        # Read from general kindling config (not bootstrap namespace)
        # bootstrap is just for temp backwards compatibility mapping
        required_packages = config_service.get("kindling.required_packages", []) or []
        extensions = config_service.get("kindling.extensions", []) or []

        # Deduplicate extensions - prefer versioned specs over plain names
        # e.g., keep "kindling-ext-otel-azure>=0.3.0a1" instead of "kindling-ext-otel-azure"
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
        if declaration_only:
            install_bootstrap_dependencies_flag = False
        elif install_bootstrap_dependencies_flag is None:
            install_bootstrap_dependencies_flag = platform != "standalone"

        with _bootstrap_phase("dependency_install"):
            if install_bootstrap_dependencies_flag:
                install_bootstrap_dependencies(
                    logger,
                    bootstrap_deps_config,
                    artifacts_storage_path=artifacts_storage_path,
                )
            else:
                logger.info("Skipping bootstrap dependency installation")

        with _bootstrap_phase("platform_init"):
            try:
                platformservice = initialize_platform_services(platform, config_service, logger)
            except Exception as exc:
                if not declaration_only or platform == "standalone":
                    raise
                # SparkLogger takes a single message (no printf-style args).
                logger.warning(
                    f"Platform service '{platform}' could not be constructed during "
                    "declaration-only initialization; falling back to standalone "
                    f"service for declaration-time operations: {exc}"
                )
                platformservice = initialize_platform_services("standalone", config_service, logger)
        logger.info("Platform services initialized")

        with _bootstrap_phase("runtime_manifest"):
            _complete_runtime_manifest(config_service, platform, logger)

        # Attach the watermark aspect: incremental reads and watermark
        # advancement hook into pipe execution via signals rather than being
        # woven into the read/persist strategy. Standalone/local execution
        # deliberately runs without it (full reads, no watermark state),
        # matching the fixture-based local dev workflow. An execution-engine
        # extension that owns its own incrementality (declared via its
        # owns_incrementality capability, surfaced here as config by
        # initialize()) also runs without it — the aspect is simply never
        # registered.
        with _bootstrap_phase("aspect_registration"):
            if (
                not declaration_only
                and platform != "standalone"
                and not config.get("engine_owns_incrementality")
            ):
                from kindling.injection import GlobalInjector
                from kindling.watermarking import WatermarkAspect

                GlobalInjector.get(WatermarkAspect).register()
                logger.info("Watermark aspect registered")

            if not declaration_only:
                # Execution ids for provenance stamps (kindling.provenance);
                # every platform, since standalone runs stamp too. The
                # runtime manifest lists the executor runs made under it.
                try:
                    from kindling.injection import GlobalInjector
                    from kindling.provenance import ProvenanceRunCapture
                    from kindling.runtime_manifest import (
                        RuntimeManifestExecutionRecorder,
                    )
                    from kindling.signaling import SignalProvider

                    GlobalInjector.get(ProvenanceRunCapture).register()
                    RuntimeManifestExecutionRecorder(GlobalInjector.get(SignalProvider)).register()
                except Exception as capture_error:
                    logger.warning(
                        "Provenance run capture not registered; provenance stamps "
                        f"will carry a null execution_id: {capture_error}"
                    )

            # Registration-gated tracing wiring (WatermarkAspect pattern):
            # read kindling.telemetry.tracing.* once and enable provider-op
            # tracing at the registry chokepoint. Runs on every platform —
            # standalone pipe runs must produce span trees too. A config
            # reload does not un-register. Extensions have already imported
            # by now, so the resolved SparkTraceProvider is the final one
            # for this process.
            try:
                from kindling.trace_ops import configure_op_tracing

                configure_op_tracing(config_service, logger)
            except Exception as tracing_error:
                logger.debug(f"Provider op tracing wiring failed (non-fatal): {tracing_error}")

        # Apply spark_configs to the live Spark session.
        # This must happen after the session exists so conf.set() works.
        with _bootstrap_phase("registrations_import"):
            _apply_spark_configs(config_service, logger)
            _import_local_package_registrations(logger, registration_packages)

        # Overlay datapipes:/dataentities: config sections onto everything
        # registered so far; the managers keep the compiled patterns so
        # later registrations (workspace packages, app register_all) are
        # overlaid at registration — before any pipe executes.
        with _bootstrap_phase("config_overlay"):
            apply_config_overrides()
        logger.info("Config overrides applied to registered pipes and entities")

        # Resolve any @secret references now that platform services are available.
        # This is deliberately a hard failure, not a logged warning: an
        # earlier resolution pass (inside ConfigService.initialize(), before
        # platform services exist) always leaves references unresolved by
        # design, silently, because there is no SecretProvider to ask yet.
        # If they are STILL unresolved here -- after platform_init -- passing
        # them through unexamined hands a literal "@secret:..." string to
        # whatever provider reads that config (e.g. the EventHub provider
        # parsing it as a connection string), which fails with a confusing,
        # unrelated error far from the actual cause. Fail here instead, with
        # the config paths (never the secret values) named explicitly.
        with _bootstrap_phase("secret_resolution"):
            _resolve_and_validate_secrets(config_service, logger)

        # Resolve @secret references embedded directly in an entity/pipe's
        # own tags= registration param (as opposed to a config-overlay
        # section) -- these never touch Dynaconf, so the pass above cannot
        # see them. Must run before the resolved_config_overlay re-apply
        # below, since that re-apply rebuilds metadata from _raw_params.
        with _bootstrap_phase("registered_tag_secret_resolution"):
            _resolve_registered_tag_secrets(logger)

        # Re-overlay dataentities:/dataentities-bytag:/datapipes:/datapipes-bytag:
        # config sections now that any @secret references inside them are
        # resolved. The first overlay pass (above) ran before platform
        # services existed, so any @secret literal in those sections got
        # baked verbatim into the registered EntityMetadata/PipeMetadata
        # tags -- apply_config_overrides() always rebuilds from the original
        # registration params (see DataEntityManager.apply_config_overrides),
        # so calling it again here is idempotent and simply picks up the
        # now-resolved config_service state instead of accumulating anything.
        with _bootstrap_phase("resolved_config_overlay"):
            apply_config_overrides()
        logger.debug("Config overrides re-applied after @secret resolution")

        load_workspace_packages_default = False
        if declaration_only:
            load_workspace_packages_value = False
        else:
            load_workspace_packages_value = config_service.get(
                "kindling.bootstrap.load_workspace_packages"
            )
            if load_workspace_packages_value is None:
                load_workspace_packages_value = config_service.get("kindling.bootstrap.load_local")
                if load_workspace_packages_value is not None:
                    logger.warning(
                        "Config key 'kindling.bootstrap.load_local' is deprecated; use "
                        "'kindling.bootstrap.load_workspace_packages' instead."
                    )
            if load_workspace_packages_value is None:
                load_workspace_packages_value = load_workspace_packages_default
        logger.debug(
            f"kindling.bootstrap.load_workspace_packages = {load_workspace_packages_value} "
            f"(type: {type(load_workspace_packages_value).__name__})"
        )

        load_workspace_packages_flat = config_service.get(
            "load_workspace_packages", config_service.get("load_local_packages", "NOT_SET")
        )
        logger.debug(
            f"load_workspace_packages (flat) = {load_workspace_packages_flat} "
            f"(type: {type(load_workspace_packages_flat).__name__})"
        )

        with _bootstrap_phase("workspace_packages"):
            if load_workspace_packages_value:
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
                logger.info("Skipping workspace package loading (load_workspace_packages=False)")
                # Pre-seed the notebook cache so any subsequent call to get_all_notebooks()
                # returns immediately instead of hitting the platform API, which may be
                # unavailable from the Spark cluster in some environments.
                try:
                    loader = get_kindling_service(NotebookManager)
                    if loader._notebook_cache is None:
                        loader._notebook_cache = []
                        loader._folder_cache = set()
                        logger.debug(
                            "Notebook cache pre-seeded empty (load_workspace_packages=False)"
                        )
                except Exception as _pre_seed_err:
                    logger.debug(f"Could not pre-seed notebook cache: {_pre_seed_err}")

        for target, reason in pending_declaration_derivations().items():
            # Every registration phase has run; a clone/extend still waiting
            # for its source will raise on lookup, so say why now. Apps that
            # register later (a Lakeflow selector's register_all) re-check at
            # declaration time.
            logger.warning(f"Declaration derivation for '{target}' is unresolved: {reason}")
        logger.info("Framework initialization complete")
        # Emitted now, not with the bootstrap span tree flushed after an
        # auto-run app finishes: a killed run still leaves its versions.
        _emit_runtime_manifest_span(config_service)
        _record_run(config_service, logger, "running")

        # `app_name` scopes app-specific config overlays. In declaration-only
        # initialization it must not also imply "run this app".
        if app_name and not declaration_only:
            logger.info(f"Auto-running app: {app_name}")
            with _bootstrap_phase("app_run"):
                try:
                    runner = get_kindling_service(DataAppRunner)
                    logger.debug(f"Got DataAppRunner: {type(runner).__name__}")
                    runner.run_app(app_name)
                    logger.warning(f"App '{app_name}' completed successfully")
                    _record_run(config_service, logger, "succeeded")
                except Exception as app_error:
                    logger.exception(f"App execution failed: {str(app_error)}")
                    _record_run(config_service, logger, "failed", error=str(app_error))
                    raise

        return platformservice

    except Exception as e:
        logger.error(f"Framework initialization failed: {str(e)}")
        _bootstrap_error = str(e)
        raise
    finally:
        # Retro-flush the bootstrap span tree now that the final trace
        # provider is settled. On failure the failing phase (and root)
        # carry the error; failures before a usable ConfigService never
        # reach this try and their tree is lost (stdlib logs only).
        _flush_bootstrap_trace(config_service, error=_bootstrap_error)


def bootstrap_framework(config: Dict[str, Any], logger=None):
    """Backward compatibility wrapper"""
    return initialize_framework(config)
