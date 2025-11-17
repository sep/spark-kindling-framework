from . import bootstrap  # This imports all platform modules
from . import data_apps  # Renamed from app_framework
from . import (
    common_transforms,
    data_entities,
    data_pipes,
    delta_entity_provider,
    file_ingestion,
    injection,
    job_deployment,
    notebook_framework,
    pip_manager,
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
