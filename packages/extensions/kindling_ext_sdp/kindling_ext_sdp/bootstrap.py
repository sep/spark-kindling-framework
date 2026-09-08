"""SDP-mode bootstrap surface: guard installation and ``declare_pipeline``.

The entry-point shape from the proposal:

.. code-block:: python

    import kindling
    kindling.initialize(engine="sdp")   # activates SDP mode (guard, no
                                        # watermark aspect)
    from my_app import register_all
    register_all()
    kindling.declare_pipeline()         # delegates here

Ordering matters (Phase-1 decision, ``sdp_engine_phase1_notes.md``):
``declare_pipeline()`` must be the LAST step — after registrations and
after the post-registration config overlay — or engine keys like
``dataset_type`` bake in stale values. Entity tags are overlay-safe
automatically (``get_entity_definition()`` merges config tag overrides at
retrieval time); the per-pipe engine config is resolved here, from the
fully-overlaid config, at declaration time.
"""

from typing import Any, Dict, List, Optional

from kindling.entity_naming import (
    ENTITY_NAMING_TAG,
    GLOBAL_NAMING_KEY,
    TableNamingPolicy,
    sdp_mode_for,
)
from kindling_ext_sdp.declaration_plan import DeclarationPlan
from kindling_ext_sdp.guard_provider import SdpWriteGuardProvider
from kindling_ext_sdp.oss_engine import OssSdpEngine

SDP_DATASET_NAMING_KEY = "kindling.sdp.dataset_naming"
SDP_DATASET_NAMING_DIVERGENCE_KEY = "kindling.sdp.dataset_naming_divergence"
STORAGE_COLLISION_CHECK_KEY = "kindling.storage.collision_check"


def activate_sdp_mode() -> None:
    """Install the write-inert provider guard for this process.

    Called by ``kindling.initialize(engine="sdp")`` after core framework
    initialization. Idempotent: the registry treats re-installing the same
    decorator as a no-op (and refuses to stack a different one).
    """
    from kindling.entity_provider_registry import EntityProviderRegistry
    from kindling.injection import GlobalInjector

    registry = GlobalInjector.get(EntityProviderRegistry)
    registry.set_provider_decorator(SdpWriteGuardProvider)


def resolve_engine_config(config_service, pipe_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Resolve ``datapipes.<pipeid>.engine`` blocks from the overlaid config."""
    engine_config: Dict[str, Dict[str, Any]] = {}
    for pipe_id in pipe_ids:
        block = config_service.get(f"datapipes.{pipe_id}.engine", None)
        if block:
            engine_config[pipe_id] = dict(block)
    return engine_config


def _clean_config_value(value: object) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _project_dataset_naming(shared_naming: TableNamingPolicy) -> str:
    try:
        return sdp_mode_for(shared_naming.global_mode)
    except ValueError:
        return "normalized"


def _entity_ids_for_selected_pipes(
    entity_registry, pipe_registry, selected: List[str]
) -> List[str]:
    ids: List[str] = []
    seen = set()
    for pipe_id in selected:
        pipe = pipe_registry.get_pipe_definition(pipe_id)
        if pipe is None:
            continue
        candidates = []
        if pipe.output_entity_id:
            candidates.append(pipe.output_entity_id)
        candidates.extend(pipe.input_entity_ids or ())
        for entity_id in candidates:
            if not entity_id or entity_id in seen:
                continue
            entity = entity_registry.get_entity_definition(entity_id)
            if entity is None:
                continue
            seen.add(entity_id)
            ids.append(entity_id)
    return ids


def _has_entity_table_naming_policy(entity_registry, pipe_registry, selected: List[str]) -> bool:
    for entity_id in _entity_ids_for_selected_pipes(entity_registry, pipe_registry, selected):
        entity = entity_registry.get_entity_definition(entity_id)
        tags = (entity.tags if entity is not None else None) or {}
        if _clean_config_value(tags.get(ENTITY_NAMING_TAG)) is not None:
            return True
    return False


def _needs_entity_name_mapper(
    config_service, entity_registry, pipe_registry, selected: List[str]
) -> bool:
    if _clean_config_value(config_service.get(GLOBAL_NAMING_KEY)) is not None:
        return True
    if _has_entity_table_naming_policy(entity_registry, pipe_registry, selected):
        return True
    collision_check = _clean_config_value(config_service.get(STORAGE_COLLISION_CHECK_KEY))
    return collision_check is not None and collision_check.lower() != "off"


def _external_read_resolvers(entity_registry, entity_name_mapper):
    def _resolved_name(entity_id: str) -> str:
        entity = entity_registry.get_entity_definition(entity_id)
        if entity is None:
            return entity_id
        return entity_name_mapper.get_table_name(entity)

    return (
        lambda spark, entity_id: spark.table(_resolved_name(entity_id)),
        lambda spark, entity_id: spark.readStream.table(_resolved_name(entity_id)),
    )


def _resolve_dataset_naming(config_service):
    shared_naming = TableNamingPolicy.from_config_value(config_service.get(GLOBAL_NAMING_KEY))
    raw_dataset_naming = config_service.get(SDP_DATASET_NAMING_KEY, None)
    configured_dataset_naming = _clean_config_value(raw_dataset_naming)
    dataset_naming_explicit = configured_dataset_naming is not None
    if dataset_naming_explicit:
        dataset_naming = configured_dataset_naming
    else:
        dataset_naming = _project_dataset_naming(shared_naming)
    return shared_naming, dataset_naming, dataset_naming_explicit


def _resolve_entity_name_mapper(config_service, entity_registry, pipe_registry, selected):
    if not _needs_entity_name_mapper(config_service, entity_registry, pipe_registry, selected):
        return None, None, None
    from kindling.data_entities import EntityNameMapper
    from kindling.injection import GlobalInjector

    name_resolver = GlobalInjector.get(EntityNameMapper)
    external_read_resolver, external_stream_read_resolver = _external_read_resolvers(
        entity_registry, name_resolver
    )
    return name_resolver, external_read_resolver, external_stream_read_resolver


def _pipeline_services():
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.injection import GlobalInjector
    from kindling.spark_config import ConfigService

    return (
        GlobalInjector.get(DataEntityRegistry),
        GlobalInjector.get(DataPipesRegistry),
        GlobalInjector.get(ConfigService),
    )


def declare_pipeline(
    pipe_ids: Optional[List[str]] = None,
    dp_module: Any = None,
    engine_factory: Any = None,
) -> DeclarationPlan:
    """Build, validate, and declare the pipeline from the live registries.

    ``engine_factory(entity_registry, pipe_registry, engine_config=...,
    dp_module=..., dataset_naming=...)`` constructs the concrete engine; defaults to
    :class:`OssSdpEngine`. Adapter packages (``kindling_ext_databricks``)
    pass their own engine class here and reuse everything else.

    Returns the validated plan (useful for logging/inspection). Raises
    ``DeclarationValidationError`` with every accumulated issue when any
    selected pipe cannot be declared safely.
    """
    entity_registry, pipe_registry, config_service = _pipeline_services()

    selected = list(pipe_ids) if pipe_ids is not None else list(pipe_registry.get_pipe_ids())
    shared_naming, dataset_naming, dataset_naming_explicit = _resolve_dataset_naming(config_service)
    name_resolver, external_read_resolver, external_stream_read_resolver = (
        _resolve_entity_name_mapper(config_service, entity_registry, pipe_registry, selected)
    )
    engine = (engine_factory or OssSdpEngine)(
        entity_registry,
        pipe_registry,
        engine_config=resolve_engine_config(config_service, selected),
        dp_module=dp_module,
        dataset_naming=dataset_naming,
        shared_naming=shared_naming,
        dataset_naming_explicit=dataset_naming_explicit,
        dataset_naming_divergence=config_service.get(SDP_DATASET_NAMING_DIVERGENCE_KEY, "error"),
        external_read_resolver=external_read_resolver,
        external_stream_read_resolver=external_stream_read_resolver,
        name_resolver=name_resolver,
    )
    plan = engine.build_plan(selected)
    engine.declare_pipeline(plan)
    return plan
