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

from kindling_sdp.declaration_plan import DeclarationPlan
from kindling_sdp.guard_provider import SdpWriteGuardProvider
from kindling_sdp.oss_engine import OssSdpEngine


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


def declare_pipeline(
    pipe_ids: Optional[List[str]] = None,
    dp_module: Any = None,
) -> DeclarationPlan:
    """Build, validate, and declare the pipeline from the live registries.

    Returns the validated plan (useful for logging/inspection). Raises
    ``DeclarationValidationError`` with every accumulated issue when any
    selected pipe cannot be declared safely.
    """
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.injection import GlobalInjector
    from kindling.spark_config import ConfigService

    entity_registry = GlobalInjector.get(DataEntityRegistry)
    pipe_registry = GlobalInjector.get(DataPipesRegistry)
    config_service = GlobalInjector.get(ConfigService)

    selected = list(pipe_ids) if pipe_ids is not None else list(pipe_registry.get_pipe_ids())
    engine = OssSdpEngine(
        entity_registry,
        pipe_registry,
        engine_config=resolve_engine_config(config_service, selected),
        dp_module=dp_module,
    )
    plan = engine.build_plan(selected)
    engine.declare_pipeline(plan)
    return plan
