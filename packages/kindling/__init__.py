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


def initialize(config=None, app_name=None, engine=None):
    """Public initialize entrypoint for pre-installed kindling usage.

    ``engine="sdp"`` selects the Spark Declarative Pipelines execution
    mode (requires the ``kindling_sdp`` package): the watermark aspect is
    never registered (SDP owns incrementality) and entity-provider write
    paths are guarded (SDP owns persistence). Follow with registrations,
    then ``kindling.declare_pipeline()`` as the final step.
    """
    config = dict(config or {})
    if app_name is not None:
        config["app_name"] = app_name
    if engine is not None:
        config["engine"] = engine

    # Resolve SDP availability BEFORE framework initialization: a missing
    # kindling_sdp package or an unsupported engine name must fail with the
    # framework untouched, not half-initialized in SDP mode.
    activate = None
    if config.get("engine") in ("sdp", "databricks_sdp"):
        activate = _resolve_sdp_activation(config["engine"])

    result = initialize_framework(config)

    if activate is not None:
        activate()
    return result


def _resolve_sdp_activation(engine_name):
    """Validate SDP-mode availability and return the activation callable.

    Raises (ImportError / ValueError) without side effects so callers can
    check availability before any framework state is created.
    """
    try:
        from kindling_sdp.bootstrap import SUPPORTED_ENGINES, activate_sdp_mode
    except ImportError as exc:
        raise ImportError(
            f"engine='{engine_name}' requires the kindling_sdp package. "
            "Install it alongside kindling to use SDP execution mode."
        ) from exc
    if engine_name not in SUPPORTED_ENGINES:
        raise ValueError(
            f"engine='{engine_name}' is not available yet "
            f"(supported: {', '.join(SUPPORTED_ENGINES)}). The Databricks "
            "adapter arrives with kindling_databricks_sdp."
        )
    return activate_sdp_mode


def declare_pipeline(pipe_ids=None):
    """Declare the registered pipes as an SDP pipeline (SDP mode only).

    Must run AFTER all registrations and the post-registration config
    overlay — the last step of the entry point.
    """
    try:
        from kindling_sdp.bootstrap import declare_pipeline as _declare
    except ImportError as exc:
        raise ImportError(
            "declare_pipeline() requires the kindling_sdp package and " "initialize(engine='sdp')."
        ) from exc
    return _declare(pipe_ids=pipe_ids)
