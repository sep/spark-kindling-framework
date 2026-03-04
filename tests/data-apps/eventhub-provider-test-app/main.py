#!/usr/bin/env python3
"""Event Hub provider system test app.

Validates EventHubEntityProvider through the framework registry by executing:
- provider resolution by provider_type
- check_entity_exists
- batch read with marker detection
- streaming read with marker detection
"""

import sys
import time

from kindling.data_entities import DataEntities, DataEntityRegistry, EntityMetadata
from kindling.data_pipes import DataPipes
from kindling.entity_provider import is_streamable
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.execution_strategy import ExecutionPlanGenerator
from kindling.generation_executor import GenerationExecutor
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.platform_provider import PlatformServiceProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from kindling.watermarking import WatermarkEntityFinder
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


class SimpleWatermarkEntityFinder(WatermarkEntityFinder):
    """Minimal WatermarkEntityFinder for system-test DI wiring."""

    def __init__(self):
        from types import SimpleNamespace

        watermark_schema = StructType(
            [
                StructField("watermark_id", StringType(), False),
                StructField("source_entity_id", StringType(), False),
                StructField("reader_id", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("last_version_processed", IntegerType(), False),
                StructField("last_execution_id", StringType(), False),
            ]
        )

        self.watermark_entity = SimpleNamespace(
            entityid="system.watermarks",
            name="watermarks",
            schema=watermark_schema,
            partition_columns=[],
            merge_columns=["watermark_id"],
            tags={"provider_type": "delta"},
        )

    def get_watermark_entity_for_entity(self, _context: str):
        return self.watermark_entity

    def get_watermark_entity_for_layer(self, _layer: str):
        return self.watermark_entity


def _ensure_test_watermark_finder_bound() -> None:
    GlobalInjector.bind(WatermarkEntityFinder, SimpleWatermarkEntityFinder)


def _emit(logger, test_id: str, test_name: str, passed: bool, details: str = "") -> None:
    status = "PASSED" if passed else "FAILED"
    suffix = f" {details}" if details else ""
    line = f"TEST_ID={test_id} test={test_name} status={status}{suffix}"
    logger.info(line)
    print(line, flush=True)


def _get_logger():
    logger_provider = get_kindling_service(SparkLoggerProvider)
    return logger_provider.get_logger("eventhub-provider-test-app")


def _get_test_id(config_service: ConfigService) -> str:
    value = config_service.get("test_id")
    return str(value) if value else "unknown"


def _build_eventhub_entity(
    connection_string: str,
    eventhub_name: str,
    consumer_group: str,
    starting_position: str,
):
    return EntityMetadata(
        entityid=f"stream.eventhub_provider_test.{starting_position}",
        name=f"eventhub_provider_test_{starting_position}",
        partition_columns=[],
        merge_columns=[],
        tags={
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": connection_string,
            "provider.eventhub.name": eventhub_name,
            "provider.eventhub.consumerGroup": consumer_group,
            "provider.startingPosition": starting_position,
            "provider.maxEventsPerTrigger": "500",
        },
        schema=None,
    )


def _find_marker_in_batch(provider, entity, marker: str, attempts: int = 6, delay_s: int = 5):
    marker_count = 0
    total_count = 0

    for _ in range(attempts):
        batch_df = provider.read_entity(entity).selectExpr("cast(body as string) as body_str")
        total_count = batch_df.count()
        marker_count = batch_df.filter(col("body_str").contains(marker)).count()
        if marker_count > 0:
            break
        time.sleep(delay_s)

    return marker_count, total_count


def _find_marker_in_stream(spark, provider, entity, marker: str, query_name: str):
    marker_count = 0
    query = None

    try:
        stream_df = provider.read_entity_as_stream(entity).selectExpr("cast(body as string) as body_str")
        filtered = stream_df.filter(col("body_str").contains(marker))

        query = filtered.writeStream.format("memory").queryName(query_name).outputMode("append").start()

        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                marker_count = spark.sql(f"SELECT count(1) AS c FROM {query_name}").collect()[0]["c"]
            except Exception:
                marker_count = 0

            if marker_count > 0:
                break

            time.sleep(5)

        return marker_count
    finally:
        if query and query.isActive:
            query.stop()


def _resolve_pipe_paths(config_service: ConfigService, test_id: str) -> tuple[str, str]:
    platform = str(config_service.get("kindling.platform.name") or "").lower()
    if platform == "databricks":
        temp_root = str(config_service.get("kindling.temp_path") or "/tmp").rstrip("/")
        base = f"{temp_root}/eventhub_pipe_test_{test_id}"
        return f"{base}/sink", f"{base}/checkpoints"

    table_root = str(config_service.get("kindling.storage.table_root") or "Tables").rstrip("/")
    checkpoint_root = str(config_service.get("kindling.storage.checkpoint_root") or "Files/checkpoints").rstrip(
        "/"
    )
    return f"{table_root}/eventhub_pipe_test_{test_id}/sink", f"{checkpoint_root}/eventhub_pipe_test_{test_id}"


def _cleanup_pipe_paths(logger, sink_path: str, checkpoint_base: str):
    try:
        platform_service = get_kindling_service(PlatformServiceProvider).get_service()
    except Exception:
        return
    if platform_service is None:
        return

    for path in [sink_path, checkpoint_base]:
        try:
            if platform_service.exists(path):
                platform_service.delete(path, recurse=True)
        except Exception as e:
            logger.warning(f"Cleanup failed for path '{path}': {e}")


def _run_pipe_to_table(
    logger,
    config_service: ConfigService,
    marker: str,
    connection_string: str,
    eventhub_name: str,
    consumer_group: str,
    test_id: str,
) -> tuple[bool, int]:
    _ensure_test_watermark_finder_bound()

    source_entity_id = f"stream.eventhub_pipe_source_{test_id}"
    sink_entity_id = f"stream.eventhub_pipe_sink_{test_id}"
    pipe_id = f"eventhub_to_delta_{test_id}"

    sink_path, checkpoint_base = _resolve_pipe_paths(config_service, test_id)

    sink_schema = StructType(
        [
            StructField("body_str", StringType(), True),
            StructField("ingested_at", TimestampType(), True),
        ]
    )

    DataEntities.entity(
        entityid=source_entity_id,
        name=f"eventhub_pipe_source_{test_id}",
        partition_columns=[],
        merge_columns=[],
        tags={
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": connection_string,
            "provider.eventhub.name": eventhub_name,
            "provider.eventhub.consumerGroup": consumer_group,
            "provider.startingPosition": "latest",
            "provider.maxEventsPerTrigger": "500",
        },
        schema=None,
    )

    DataEntities.entity(
        entityid=sink_entity_id,
        name=f"eventhub_pipe_sink_{test_id}",
        partition_columns=[],
        merge_columns=[],
        tags={
            "provider_type": "delta",
            "provider.access_mode": "forPath",
            "provider.path": sink_path,
        },
        schema=sink_schema,
    )

    source_key = source_entity_id.replace(".", "_")

    @DataPipes.pipe(
        pipeid=pipe_id,
        name=pipe_id,
        input_entity_ids=[source_entity_id],
        output_entity_id=sink_entity_id,
        output_type="append",
        tags={"processing_mode": "streaming"},
    )
    def eventhub_to_delta_pipe(**frames):
        stream_df = frames[source_key]
        return stream_df.selectExpr("cast(body as string) as body_str").withColumn(
            "ingested_at", current_timestamp()
        )

    plan_generator = get_kindling_service(ExecutionPlanGenerator)
    executor = get_kindling_service(GenerationExecutor)
    plan = plan_generator.generate_streaming_plan([pipe_id])
    result = executor.execute_streaming(
        plan, streaming_options={"base_checkpoint_path": checkpoint_base}
    )

    query = result.streaming_queries.get(pipe_id)
    if query is None:
        _cleanup_pipe_paths(logger, sink_path, checkpoint_base)
        raise RuntimeError("Streaming query was not created for eventhub_to_delta pipe")

    entity_registry = get_kindling_service(DataEntityRegistry)
    provider_registry = get_kindling_service(EntityProviderRegistry)
    sink_entity = entity_registry.get_entity_definition(sink_entity_id)
    sink_provider = provider_registry.get_provider_for_entity(sink_entity)

    marker_count = 0
    deadline = time.time() + 120

    try:
        while time.time() < deadline:
            if query and query.isActive:
                try:
                    query.processAllAvailable()
                except Exception:
                    pass
            try:
                sink_df = sink_provider.read_entity(sink_entity)
                marker_count = sink_df.filter(col("body_str").contains(marker)).count()
                if marker_count > 0:
                    break
            except Exception:
                pass
            time.sleep(5)
    finally:
        if query and query.isActive:
            query.stop()
        _cleanup_pipe_paths(logger, sink_path, checkpoint_base)

    return marker_count > 0, marker_count


def main() -> int:
    logger = _get_logger()
    config_service = get_kindling_service(ConfigService)
    spark = get_or_create_spark_session()

    test_id = _get_test_id(config_service)
    start_line = f"TEST_ID={test_id} status=STARTED component=eventhub_provider"
    logger.info(start_line)
    print(start_line, flush=True)

    connection_string = config_service.get("kindling.eventhub_test.connection_string")
    eventhub_name = config_service.get("kindling.eventhub_test.eventhub_name")
    consumer_group = str(config_service.get("kindling.eventhub_test.consumer_group") or "$Default")
    marker = str(config_service.get("kindling.eventhub_test.marker") or "")

    if not connection_string or not eventhub_name or not marker:
        _emit(
            logger,
            test_id,
            "config_validation",
            False,
            "missing_required_kindling_eventhub_test_values=true",
        )
        final_line = f"TEST_ID={test_id} status=COMPLETED result=FAILED"
        logger.info(final_line)
        print(final_line, flush=True)
        return 1

    batch_entity = _build_eventhub_entity(
        connection_string=connection_string,
        eventhub_name=eventhub_name,
        consumer_group=consumer_group,
        starting_position="earliest",
    )
    stream_entity = _build_eventhub_entity(
        connection_string=connection_string,
        eventhub_name=eventhub_name,
        consumer_group=consumer_group,
        starting_position="latest",
    )

    registry = get_kindling_service(EntityProviderRegistry)
    provider = registry.get_provider_for_entity(batch_entity)
    _emit(logger, test_id, "provider_resolution", provider is not None, "provider_type=eventhub")

    exists = provider.check_entity_exists(batch_entity)
    _emit(logger, test_id, "check_entity_exists", exists)

    if not exists:
        final_line = f"TEST_ID={test_id} status=COMPLETED result=FAILED"
        logger.info(final_line)
        print(final_line, flush=True)
        return 1

    batch_marker_count, batch_total_count = _find_marker_in_batch(provider, batch_entity, marker)
    batch_ok = batch_marker_count > 0
    _emit(
        logger,
        test_id,
        "batch_read",
        batch_ok,
        f"marker_count={batch_marker_count} total_count={batch_total_count}",
    )

    stream_ok = False
    stream_marker_count = 0
    pipe_ok = False
    pipe_marker_count = 0

    if is_streamable(provider):
        query_name = f"eventhub_provider_{test_id.replace('-', '_')}"
        stream_marker_count = _find_marker_in_stream(
            spark=spark,
            provider=provider,
            entity=stream_entity,
            marker=marker,
            query_name=query_name,
        )
        stream_ok = stream_marker_count > 0

    _emit(logger, test_id, "stream_read", stream_ok, f"marker_count={stream_marker_count}")

    try:
        pipe_ok, pipe_marker_count = _run_pipe_to_table(
            logger=logger,
            config_service=config_service,
            marker=marker,
            connection_string=connection_string,
            eventhub_name=eventhub_name,
            consumer_group=consumer_group,
            test_id=test_id,
        )
    except Exception as e:
        pipe_ok = False
        _emit(logger, test_id, "pipe_to_table", pipe_ok, f"error={e}")
    else:
        _emit(logger, test_id, "pipe_to_table", pipe_ok, f"marker_count={pipe_marker_count}")

    passed = all([batch_ok, stream_ok, pipe_ok])
    final_result = "PASSED" if passed else "FAILED"
    final_line = f"TEST_ID={test_id} status=COMPLETED result={final_result}"
    logger.info(final_line)
    print(final_line, flush=True)

    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
