from . import bootstrap  # This imports all platform modules
from . import data_apps  # Renamed from app_framework
from . import (
    common_transforms,
    data_entities,
    data_pipes,
    entity_provider,
    entity_provider_csv,
    entity_provider_delta,
    entity_provider_eventhub,
    entity_provider_memory,
    entity_provider_registry,
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
    test_framework,
    watermarking,
)
from .bootstrap import (
    bootstrap_framework,
    initialize_framework,
    is_framework_initialized,
)
from .platform_provider import PlatformAPI


def initialize(config=None, app_name=None):
    """Public initialize entrypoint for pre-installed kindling usage."""
    config = dict(config or {})
    if app_name is not None:
        config["app_name"] = app_name

    return initialize_framework(config, app_name=config.get("app_name"))
