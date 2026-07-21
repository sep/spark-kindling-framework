"""Select and declare a Kindling data app from Lakeflow configuration.

This module is intentionally a declaration-time adapter.  It does not use
``DataAppManager``: Lakeflow must see the selected app's declarations while it
is evaluating the pipeline source, before it builds the dependency graph.

Applications advertise declaration modules through the
``spark_kindling.data_apps`` entry-point group.  The entry-point value is a
module containing a callable ``register_all()`` function.
"""

from __future__ import annotations

import dataclasses
import importlib
import logging
from types import CodeType, ModuleType
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Tuple

from py4j.protocol import Py4JError
from pyspark.sql.utils import AnalysisException

APP_ENTRY_POINT_GROUP = "spark_kindling.data_apps"
DATA_APP_CONFIG_KEY = "kindling.data_app"
ALLOWED_APPS_CONFIG_KEY = "kindling.lakeflow.allowed_apps"
#: Comma-separated pipeline-configuration keys to bridge by point lookup.
#: Restricted runtimes (serverless / shared-access clusters) allow
#: ``spark.conf.get`` on pipeline configuration but block every enumeration
#: surface, so keys beyond the defaults must be named explicitly.
CONFIG_KEYS_CONFIG_KEY = "kindling.lakeflow.config_keys"
#: Comma-separated pipe ids to declare; unset declares every registered pipe.
PIPES_CONFIG_KEY = "kindling.lakeflow.pipes"

_LOGGER = logging.getLogger(__name__)
_CONF_READ_ERRORS = (AttributeError, KeyError, Py4JError, AnalysisException, RuntimeError)
# RuntimeError covers restricted/serverless SparkContext access, where the
# context exists conceptually but is not exposed to the Python evaluation.


class LakeflowAppSelectionError(RuntimeError):
    """Base error for invalid Lakeflow data-app selection."""


class LakeflowAppNotFoundError(LakeflowAppSelectionError):
    """The configured app is not advertised by an installed distribution."""


class LakeflowAppNotAuthorizedError(LakeflowAppSelectionError):
    """The configured app is discovered but not allowlisted."""


class LakeflowAppDeclarationError(LakeflowAppSelectionError):
    """The selected app cannot provide declaration-only registrations."""


class LakeflowAppConflictError(LakeflowAppSelectionError):
    """An app changed an already-registered entity or pipe definition."""


def _registered_data_app_entry_points() -> Dict[str, Any]:
    """Return data-app entry points without importing their target modules."""
    from importlib.metadata import entry_points

    return {ep.name: ep for ep in entry_points(group=APP_ENTRY_POINT_GROUP)}


def _active_spark(spark: Any = None) -> Any:
    if spark is not None:
        return spark

    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return SparkSession.builder.getOrCreate()


def _spark_conf_get(spark: Any, key: str) -> Optional[str]:
    """Read a RuntimeConfig key while remaining compatible with test fakes."""
    try:
        value = spark.conf.get(key, None)
    except TypeError as exc:
        _LOGGER.debug("RuntimeConfig.get(key, default) is unavailable for %s: %s", key, exc)
        try:
            value = spark.conf.get(key)
        except (TypeError, *_CONF_READ_ERRORS) as fallback_exc:
            _LOGGER.debug("Unable to read Spark configuration key %s: %s", key, fallback_exc)
            return None
    except _CONF_READ_ERRORS as exc:
        _LOGGER.debug("Unable to read Spark configuration key %s: %s", key, exc)
        return None

    if value is None:
        return None
    return str(value)


def _spark_conf_items(spark: Any) -> Iterable[Tuple[str, Any]]:
    """Enumerate Spark configuration with PySpark 3.x and Databricks fallbacks.

    Every tier is best-effort: restricted runtimes (serverless / shared-access
    clusters with py4j whitelisting) can fail these calls with exception types
    outside any fixed tuple — enumeration must degrade, never crash
    declaration.
    """

    try:
        get_all = getattr(spark.conf, "getAll", None)
        if callable(get_all):
            values = get_all()
            if isinstance(values, Mapping):
                return tuple(values.items())
            return tuple(values)
    except Exception as exc:  # noqa: BLE001 - best-effort tier
        _LOGGER.debug("Unable to enumerate RuntimeConfig.getAll(): %s", exc)

    try:
        values = spark.sparkContext.getConf().getAll()
        if isinstance(values, Mapping):
            return tuple(values.items())
        return tuple(values)
    except Exception as exc:  # noqa: BLE001 - py4j-restricted runtimes
        _LOGGER.debug("Unable to enumerate SparkContext.getConf().getAll(): %s", exc)

    # Restricted runtimes usually still allow SQL and point lookups even when
    # the py4j conf objects are whitelisted away.
    try:
        rows = spark.sql("SET").collect()
        items = tuple((row[0], row[1]) for row in rows if row[0] is not None)
        if items:
            return items
    except Exception as exc:  # noqa: BLE001 - best-effort tier
        _LOGGER.debug("Unable to enumerate configuration via SET: %s", exc)

    explicit_keys = (DATA_APP_CONFIG_KEY, ALLOWED_APPS_CONFIG_KEY)
    explicit_items = tuple(
        (key, value) for key in explicit_keys if (value := _spark_conf_get(spark, key)) is not None
    )
    _LOGGER.warning(
        "Unable to enumerate Spark configuration through RuntimeConfig.getAll(), "
        "SparkContext.getConf().getAll(), or SET; bridged only explicit keys %s "
        "(available: %s)",
        explicit_keys,
        tuple(key for key, _ in explicit_items),
    )
    return explicit_items


def _pipeline_config_for_kindling(spark: Any, app_name: str) -> Dict[str, Any]:
    """Bridge application Spark configuration into Kindling's ConfigService.

    Lakeflow exposes pipeline configuration through Spark's RuntimeConfig.  A
    normal Kindling initialization also looks for ``spark.kindling.*`` keys,
    but Lakeflow configuration commonly arrives as ``kindling.*`` or
    ``datapipes.*``.  Passing those values explicitly makes them available to
    Dynaconf and therefore to the declaration engine as well.
    """
    config: Dict[str, Any] = {}
    for key, value in _spark_conf_items(spark):
        if not isinstance(key, str):
            continue
        if key.startswith("spark.kindling."):
            config[f"kindling.{key[len('spark.kindling.') :]}"] = value
        elif key.startswith(("kindling.", "datapipes.")):
            config[key] = value

    # These are read before initialization, so make the exact selected values
    # available to ConfigService even when a fake or runtime only exposes get().
    config[DATA_APP_CONFIG_KEY] = app_name
    allowed = _spark_conf_get(spark, ALLOWED_APPS_CONFIG_KEY)
    if allowed is not None:
        config[ALLOWED_APPS_CONFIG_KEY] = allowed

    # Restricted runtimes cannot enumerate configuration at all; bridge any
    # explicitly named keys by point lookup.
    raw_keys = _spark_conf_get(spark, CONFIG_KEYS_CONFIG_KEY)
    if raw_keys:
        for key in (part.strip() for part in str(raw_keys).split(",")):
            if key and key not in config:
                value = _spark_conf_get(spark, key)
                if value is not None:
                    config[key] = value

    # Declaration-time default: SDP owns execution and persistence, and
    # platform services are runtime machinery (workspace scans, token
    # acquisition) that pipeline environments cannot construct — the
    # Databricks platform service fails with "No workspace_id provided" in
    # Lakeflow. An explicit platform in the bridged config wins.
    if "platform" not in config and "kindling.platform.environment" not in config:
        config["platform"] = "standalone"
    return config


def _parse_allowlist(raw: Optional[str]) -> set[str]:
    if raw is None or not raw.strip():
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def _entry_point_module_name(entry_point: Any, app_name: str) -> str:
    value = str(getattr(entry_point, "value", "")).strip()
    module_name, separator, attribute = value.partition(":")
    if not module_name:
        raise LakeflowAppDeclarationError(
            f"Lakeflow data app '{app_name}' has an invalid entry point value {value!r}. "
            "Expected a module exposing register_all()."
        )
    if separator and attribute not in {"", "register_all"}:
        raise LakeflowAppDeclarationError(
            f"Lakeflow data app '{app_name}' entry point {value!r} must target "
            "the module or its register_all function."
        )
    return module_name


def _load_declaration_module(
    entry_point: Any, app_name: str
) -> Tuple[ModuleType, Callable[[], Any]]:
    module_name = _entry_point_module_name(entry_point, app_name)
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise LakeflowAppDeclarationError(
            f"Unable to import declaration module '{module_name}' for Lakeflow "
            f"data app '{app_name}'. The module must be available in the "
            "pipeline environment and must perform declaration-only work."
        ) from exc

    register_all = getattr(module, "register_all", None)
    if not callable(register_all):
        raise LakeflowAppDeclarationError(
            f"Lakeflow data app '{app_name}' module '{module_name}' does not "
            "expose a callable register_all(). Provide a declaration-only "
            "register_all() entrypoint."
        )
    return module, register_all


def _callable_signature(value: Callable[..., Any]) -> Tuple[Any, ...]:
    code = getattr(value, "__code__", None)
    if code is None:
        return ("callable", value.__class__.__module__, value.__class__.__qualname__)

    closure = getattr(value, "__closure__", None) or ()
    closure_values = tuple(_stable_signature(cell.cell_contents) for cell in closure)
    return (
        "callable",
        getattr(value, "__module__", None),
        getattr(value, "__qualname__", None),
        _code_signature(code),
        _stable_signature(getattr(value, "__defaults__", None)),
        _stable_signature(getattr(value, "__kwdefaults__", None)),
        closure_values,
    )


def _code_signature(code: CodeType) -> Tuple[Any, ...]:
    """Return a structural signature that is stable across module imports."""
    return (
        "code",
        code.co_code,
        code.co_names,
        tuple(_stable_signature(constant) for constant in code.co_consts),
        code.co_varnames,
        code.co_freevars,
        code.co_cellvars,
        code.co_argcount,
        code.co_posonlyargcount,
        code.co_kwonlyargcount,
        code.co_flags,
    )


def _stable_signature(value: Any) -> Any:
    if isinstance(value, CodeType):
        return _code_signature(value)
    if callable(value):
        return _callable_signature(value)
    if dataclasses.is_dataclass(value):
        return (
            value.__class__.__module__,
            value.__class__.__qualname__,
            tuple(
                (field.name, _stable_signature(getattr(value, field.name)))
                for field in dataclasses.fields(value)
            ),
        )
    if isinstance(value, Mapping):
        return tuple(
            sorted(
                (
                    repr(key),
                    _stable_signature(item),
                )
                for key, item in value.items()
            )
        )
    if isinstance(value, (list, tuple)):
        return tuple(_stable_signature(item) for item in value)
    if isinstance(value, set):
        return tuple(sorted(_stable_signature(item) for item in value))
    try:
        hash(value)
    except Exception:
        return repr(value)
    return value


def _registry_snapshot() -> Dict[str, Dict[str, Any]]:
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.injection import GlobalInjector

    entity_registry = GlobalInjector.get(DataEntityRegistry)
    pipe_registry = GlobalInjector.get(DataPipesRegistry)
    return {
        "entity": {
            entity_id: _stable_signature(entity_registry.get_entity_definition(entity_id))
            for entity_id in entity_registry.get_entity_ids()
        },
        "pipe": {
            pipe_id: _stable_signature(pipe_registry.get_pipe_definition(pipe_id))
            for pipe_id in pipe_registry.get_pipe_ids()
        },
    }


def _raise_on_conflicts(
    before: Mapping[str, Mapping[str, Any]],
    after: Mapping[str, Mapping[str, Any]],
    app_name: str,
) -> None:
    for kind in ("entity", "pipe"):
        for item_id, previous in before[kind].items():
            current = after[kind].get(item_id)
            if current is not None and current != previous:
                raise LakeflowAppConflictError(
                    f"Lakeflow data app '{app_name}' re-registered {kind} "
                    f"'{item_id}' with different content. Existing registrations "
                    "must be identical or use distinct IDs."
                )


def declare_from_pipeline_config(spark: Any = None) -> Any:
    """Resolve, register, and declare the configured Lakeflow data app.

    The lifecycle is deliberately fixed: read and authorize metadata, bridge
    Spark configuration, initialize Kindling, import/register the app, then
    declare the pipeline as the final operation.
    """
    spark = _active_spark(spark)
    app_name = (_spark_conf_get(spark, DATA_APP_CONFIG_KEY) or "").strip()
    if not app_name:
        raise LakeflowAppSelectionError(
            f"Lakeflow requires a non-empty Spark configuration value for "
            f"'{DATA_APP_CONFIG_KEY}'. Set it to a registered Kindling data app name."
        )

    entry_points = _registered_data_app_entry_points()
    discovered = sorted(entry_points)
    entry_point = entry_points.get(app_name)
    if entry_point is None:
        discovered_text = ", ".join(discovered) if discovered else "<none>"
        raise LakeflowAppNotFoundError(
            f"Unknown Lakeflow data app '{app_name}'. Discovered apps: {discovered_text}. "
            f"Check '{DATA_APP_CONFIG_KEY}' and install the app distribution."
        )

    allowed = _parse_allowlist(_spark_conf_get(spark, ALLOWED_APPS_CONFIG_KEY))
    if allowed and app_name not in allowed:
        raise LakeflowAppNotAuthorizedError(
            f"Lakeflow data app '{app_name}' was discovered but is not authorized by "
            f"'{ALLOWED_APPS_CONFIG_KEY}'. Allowed apps: {', '.join(sorted(allowed))}."
        )

    import kindling

    kindling.initialize(
        config=_pipeline_config_for_kindling(spark, app_name),
        engine="databricks_sdp",
    )

    before = _registry_snapshot()
    _, register_all = _load_declaration_module(entry_point, app_name)
    try:
        register_all()
    except LakeflowAppSelectionError:
        raise
    except Exception as exc:
        raise LakeflowAppDeclarationError(
            f"Declaration registration failed for Lakeflow data app '{app_name}'. "
            "register_all() must only register Kindling entities and pipes; "
            "dependency installation and imperative execution are unsupported."
        ) from exc

    after = _registry_snapshot()
    _raise_on_conflicts(before, after, app_name)

    # An app may register more pipes than the pipeline should declare —
    # notably alternative lowerings over the same declarations (the temporal
    # chain pipes vs the per-declaration pipes, which self-reference by
    # design and rightly fail SDP validation). kindling.lakeflow.pipes
    # names the subset to declare; unset declares everything.
    pipe_ids = None
    raw_pipes = _spark_conf_get(spark, PIPES_CONFIG_KEY)
    if raw_pipes:
        pipe_ids = [part.strip() for part in str(raw_pipes).split(",") if part.strip()]

    _LOGGER.info(
        "Declaring Lakeflow graph for Kindling data app '%s' (pipes: %s)",
        app_name,
        pipe_ids or "all",
    )
    return kindling.declare_pipeline(pipe_ids=pipe_ids)
