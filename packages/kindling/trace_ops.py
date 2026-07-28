"""Provider-operation tracing at the entity-provider registry chokepoint.

Every entity provider resolution funnels through
``EntityProviderRegistry.get_provider``; wrapping the resolved instance's op
methods there yields provider-wide span coverage without editing any provider.
Wrappers are installed as instance attributes shadowing the bound methods —
the object identity is unchanged, so ``isinstance`` capability probes,
``hasattr`` checks, and cached-singleton semantics all still hold.

Also home to the tracing config keys, level ordering, and span naming
constants shared by the direct instrumentation seams (component =
``kindling.<area>``; operation = short stable verb; ids/counts go in span
attributes, never in names, keeping the exported span-name set small).

Wiring is registration-gated at bootstrap (like WatermarkAspect): the config
keys are read once at initialization and a later config reload does not
un-register tracing.
"""

import functools
from typing import Any, Callable, Dict, Optional, Tuple

from kindling.features import _coerce_bool

# Config keys (kindling.telemetry.* family; read once at bootstrap wiring).
TRACING_ENABLED_KEY = "kindling.telemetry.tracing.enabled"
TRACING_LEVEL_KEY = "kindling.telemetry.tracing.level"

# Tracing levels: each level includes everything below it.
LEVEL_MINIMAL = "minimal"
LEVEL_STANDARD = "standard"
LEVEL_VERBOSE = "verbose"
_LEVEL_ORDER = {LEVEL_MINIMAL: 0, LEVEL_STANDARD: 1, LEVEL_VERBOSE: 2}

# Span component names (component = kindling.<area>; see docs/contributing/
# logging_tracing.md). Entity-provider ops use COMPONENT_ENTITY_PREFIX plus
# the provider_type, e.g. "kindling.entity.delta".
COMPONENT_ENTITY_PREFIX = "kindling.entity"
COMPONENT_PIPES = "kindling.pipes"
COMPONENT_WATERMARK = "kindling.watermark"
COMPONENT_CONFIG = "kindling.config"
COMPONENT_MIGRATION = "kindling.migration"
COMPONENT_BOOTSTRAP = "kindling.bootstrap"
COMPONENT_STREAMING = "kindling.streaming"
COMPONENT_ORCHESTRATOR = "kindling.orchestrator"
COMPONENT_INGESTION = "kindling.ingestion"
COMPONENT_APPS = "kindling.apps"
COMPONENT_DEPLOY = "kindling.deploy"
COMPONENT_CLI = "kindling.cli"

# Provider op methods wrapped when present on a resolved instance: the
# capability-ABC methods plus the de-facto delta ops callers probe with
# hasattr (merge_to_entity is in no ABC). Cheap existence/version probes are
# deliberately untraced.
TRACED_PROVIDER_OPS = (
    "read_entity",
    "read_entity_as_stream",
    "read_entity_changes",
    "read_entity_since_version",
    "read_entity_as_of",
    "write_to_entity",
    "append_to_entity",
    "merge_to_entity",
    "replace_entity",
    "ensure_entity_table",
    "ensure_destination",
    "append_as_stream",
    "merge_as_stream",
)
UNTRACED_PROVIDER_OPS = (
    "check_entity_exists",
    "get_entity_version",
)

_WRAP_MARKER = "_kindling_op_tracing_wrapped"


def level_at_least(level: str, minimum: str) -> bool:
    """True when `level` enables spans gated at `minimum` (unknown → standard)."""
    return (
        _LEVEL_ORDER.get(str(level).lower(), _LEVEL_ORDER[LEVEL_STANDARD]) >= _LEVEL_ORDER[minimum]
    )


def read_tracing_settings(config_service) -> Tuple[bool, str]:
    """Read (enabled, level) with features-style bool coercion.

    ConfigService.get does no type coercion and the SparkConf path returns
    strings, so "false"/"0" must coerce like kindling.features values do.
    """
    enabled = _coerce_bool(config_service.get(TRACING_ENABLED_KEY, True))
    if enabled is None:
        enabled = True
    level = str(config_service.get(TRACING_LEVEL_KEY, LEVEL_STANDARD) or LEVEL_STANDARD).lower()
    if level not in _LEVEL_ORDER:
        level = LEVEL_STANDARD
    return enabled, level


def whitelist_details(mapping: Optional[Dict[str, Any]], keys) -> Dict[str, Any]:
    """Copy only whitelisted keys into span details.

    Span attributes are whitelisted per seam (ids, provider_type, durations,
    counts, booleans, cursors) — never entity/pipe tags wholesale, which may
    carry credential references.
    """
    if not mapping:
        return {}
    return {key: mapping[key] for key in keys if key in mapping}


def _entity_id_from_args(args, kwargs) -> Optional[str]:
    """Best-effort entity id: first argument shaped like EntityMetadata."""
    for value in list(args) + list(kwargs.values()):
        entity_id = getattr(value, "entityid", None)
        if entity_id is not None:
            return entity_id
    return None


def _make_op_wrapper(
    original: Callable, trace_provider, component: str, op_name: str, provider_type: str
) -> Callable:
    @functools.wraps(original)
    def traced_op(*args, **kwargs):
        details = {"provider_type": provider_type}
        entity_id = _entity_id_from_args(args, kwargs)
        if entity_id is not None:
            details["entity_id"] = entity_id
        with trace_provider.span(
            operation=op_name, component=component, details=details, reraise=True
        ):
            return original(*args, **kwargs)

    return traced_op


def wrap_provider_ops(instance, trace_provider, provider_type: str):
    """Shadow the instance's op methods with tracing wrappers.

    Idempotent (marker attribute); returns the same object. Streaming ops
    span only the stream *setup* call — the query they return runs on after
    the span closes, and per-micro-batch work must never produce spans (see
    DeltaEntityProvider._merge_batch's deliberate wrapper bypass).
    """
    if getattr(instance, _WRAP_MARKER, False):
        return instance
    try:
        setattr(instance, _WRAP_MARKER, True)
    except AttributeError:
        # Instances that reject attribute writes (__slots__/proxies) stay
        # unwrapped; tracing must never break provider resolution.
        return instance
    component = f"{COMPONENT_ENTITY_PREFIX}.{provider_type}"
    for op_name in TRACED_PROVIDER_OPS:
        original = getattr(instance, op_name, None)
        if original is None or not callable(original):
            continue
        wrapper = _make_op_wrapper(original, trace_provider, component, op_name, provider_type)
        setattr(instance, op_name, wrapper)
    return instance


def configure_op_tracing(
    config_service,
    logger=None,
    registry=None,
    trace_provider=None,
    legacy_provider=None,
) -> bool:
    """Bootstrap wiring: enable provider-op tracing when config allows.

    Provider ops are standard-level spans: nothing is wrapped at
    ``minimal``. The keyword seams exist for tests; production resolves
    everything via GlobalInjector.
    """
    enabled, level = read_tracing_settings(config_service)
    if not enabled or not level_at_least(level, LEVEL_STANDARD):
        if logger is not None:
            logger.debug(f"Provider op tracing not enabled (enabled={enabled}, level={level})")
        return False

    from kindling.injection import GlobalInjector

    if trace_provider is None:
        from kindling.spark_trace import SparkTraceProvider

        trace_provider = GlobalInjector.get(SparkTraceProvider)
    if registry is None:
        from kindling.entity_provider_registry import EntityProviderRegistry

        registry = GlobalInjector.get(EntityProviderRegistry)

    registry.enable_op_tracing(trace_provider, level)

    # The legacy EntityProvider singleton reaches consumers (self.ep in the
    # stage processor, watermark manager, and file ingestion) via DI without
    # passing through the registry. It is the same delta instance the
    # registry would serve; wrapping is idempotent either way.
    if legacy_provider is None:
        from kindling.data_entities import EntityProvider

        legacy_provider = GlobalInjector.get(EntityProvider)
    wrap_provider_ops(legacy_provider, trace_provider, provider_type="delta")

    if logger is not None:
        logger.info(f"Provider op tracing enabled (level={level})")
    return True
