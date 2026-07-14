from . import bootstrap  # This imports all platform modules
from . import data_apps  # Renamed from app_framework
from . import (
    cache_optimizer,
    common_transforms,
    data_entities,
    data_pipes,
    entity_provider,
    entity_provider_csv,
    entity_provider_delta,
    entity_provider_eventhub,
    entity_provider_memory,
    entity_provider_registry,
    entity_resolution,
    execution_orchestrator,
    execution_strategy,
    file_ingestion,
    generation_executor,
    injection,
    job_deployment,
    notebook_framework,
    pip_manager,
    pipe_graph,
    pipe_streaming,
    platform_provider,
    signaling,
    simple_read_persist_strategy,
    simple_stage_processor,
    spark_config,
    spark_jdbc,
    spark_log,
    spark_log_provider,
    spark_session,
    spark_trace,
    streaming_orchestrator,
    test_framework,
    watermarking,
)
from .bootstrap import (
    bootstrap_framework,
    initialize_framework,
    is_framework_initialized,
)
from .data_entities import KindlingNotInitializedError

#: The engine extension activated by initialize(engine=...), if any. Core
#: knows only the extension contract, never a specific engine.
_active_engine_extension = None


def initialize(config=None, app_name=None, engine=None):
    """Public initialize entrypoint for pre-installed kindling usage.

    ``engine="<name>"`` selects an alternative execution engine provided
    by an extension package: the module ``kindling_<name>`` must expose an
    ``engine_extension()`` factory (see ``_load_engine_extension`` for the
    contract). Core has no knowledge of specific engines — e.g.
    ``engine="sdp"`` resolves to whatever the separately-installed
    ``kindling_sdp`` package provides. Follow with registrations, then
    ``kindling.declare_pipeline()`` as the final step for declarative
    engines.
    """
    global _active_engine_extension

    config = dict(config or {})
    if app_name is not None:
        config["app_name"] = app_name
    if engine is not None:
        config["engine"] = engine

    # Resolve the engine extension BEFORE framework initialization: a
    # missing or broken extension package must fail with the framework
    # untouched, not half-initialized.
    extension = None
    if config.get("engine"):
        extension = _load_engine_extension(config["engine"])
        config["engine_owns_incrementality"] = bool(
            getattr(extension, "owns_incrementality", False)
        )

    result = initialize_framework(config)

    if extension is not None:
        extension.activate()
        _active_engine_extension = extension
    return result


def _load_engine_extension(engine_name):
    """Resolve an execution-engine extension by naming convention.

    The contract: engine ``<name>`` is provided by an importable module
    ``kindling_<name>`` exposing a zero-arg ``engine_extension()`` factory.
    The returned object must provide ``activate()`` (called once, after
    framework initialization) and may provide ``owns_incrementality``
    (bool — the engine manages incremental reads itself, so the watermark
    aspect is not registered) and ``declare_pipeline(pipe_ids=None)``.

    Loading is side-effect free: nothing is activated here.
    """
    import importlib

    module_name = f"kindling_{engine_name}"
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ImportError(
            f"engine='{engine_name}' requires the '{module_name}' package. "
            "Install the extension that provides this engine alongside "
            "kindling."
        ) from exc
    factory = getattr(module, "engine_extension", None)
    if factory is None:
        raise TypeError(
            f"'{module_name}' does not provide an engine_extension() "
            f"factory, so it cannot be used as engine='{engine_name}'."
        )
    return factory()


def declare_pipeline(pipe_ids=None):
    """Declare the registered pipes through the active engine extension.

    Must run AFTER all registrations and the post-registration config
    overlay — the last step of the entry point. Requires an engine that
    supports pipeline declaration, selected via ``initialize(engine=...)``.
    """
    if _active_engine_extension is None:
        raise RuntimeError(
            "declare_pipeline() requires a declarative execution engine — "
            "call initialize(engine='<name>') first."
        )
    declare = getattr(_active_engine_extension, "declare_pipeline", None)
    if declare is None:
        raise TypeError(
            f"The active engine extension ({_active_engine_extension}) "
            "does not support pipeline declaration."
        )
    return declare(pipe_ids=pipe_ids)
