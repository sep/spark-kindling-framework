#!/usr/bin/env python3
"""
Streaming Pipes Test App

Tests the Unified DAG Orchestrator in streaming mode:
- Define entities and pipes via framework decorators
- Bind a concrete EntityPathLocator (the one ABC users must provide)
- Execute streaming plan via GenerationExecutor
- Verify data flows through bronze → silver → gold

Pipeline: rate source → bronze (Delta) → silver (Delta) → gold (Delta)
"""

import sys
import time

from kindling.data_entities import (
    DataEntities,
    DataEntityRegistry,
    EntityNameMapper,
    EntityPathLocator,
)
from kindling.data_pipes import DataPipes
from kindling.execution_strategy import ExecutionPlanGenerator
from kindling.generation_executor import GenerationExecutor
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from kindling.watermarking import WatermarkEntityFinder
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---- Init ----

logger_provider = get_kindling_service(SparkLoggerProvider)
logger = logger_provider.get_logger("streaming-pipes-test-app")

config_service = get_kindling_service(ConfigService)
test_id = str(config_service.get("test_id") or "unknown")

msg = f"TEST_ID={test_id} status=STARTED component=streaming_orchestrator"
logger.info(msg)
print(msg, flush=True)

test_results = {}
streaming_queries = []

try:
    spark = get_or_create_spark_session()

    # Read platform-specific paths from config (injected at app publish time)
    table_root = config_service.get("kindling.storage.table_root", "Tables")
    checkpoint_root = config_service.get("kindling.storage.checkpoint_root", "Files/checkpoints")

    base_path = f"{table_root}/streaming_pipes_test_{test_id}"
    chk_base = f"{checkpoint_root}/streaming_pipes_{test_id}"

    bronze_path = f"{base_path}/bronze"
    silver_path = f"{base_path}/silver"
    gold_path = f"{base_path}/gold"

    # ---- Bind EntityPathLocator (user-provided ABC) ----
    # EntityPathLocator has no framework default — users bind their own.
    # This implementation uses provider.path tags set on entity definitions.

    class TagBasedPathLocator(EntityPathLocator):
        """Resolves table paths from entity provider.path tags."""

        def get_table_path(self, entity):
            path = entity.tags.get("provider.path")
            if path:
                return path
            # Fallback: derive from entity id
            entity_id = entity.entityid if hasattr(entity, "entityid") else str(entity)
            if "." in entity_id:
                layer, name = entity_id.split(".", 1)
                return f"{base_path}/{layer}/{name}"
            return f"{base_path}/{entity_id}"

    class SimpleEntityNameMapper(EntityNameMapper):
        """Maps entity to a table name derived from its entityid."""

        def get_table_name(self, entity):
            entity_id = entity.entityid if hasattr(entity, "entityid") else str(entity)
            return entity_id.replace(".", "_")

    class SimpleWatermarkEntityFinder(WatermarkEntityFinder):
        """Provides watermark entities for streaming test - minimal implementation."""

        def __init__(self):
            # Define watermark entity schema (used by WatermarkManager)
            self.watermark_schema = StructType(
                [
                    StructField("watermark_id", StringType(), False),
                    StructField("source_entity_id", StringType(), False),
                    StructField("reader_id", StringType(), False),
                    StructField("timestamp", TimestampType(), False),
                    StructField("last_version_processed", IntegerType(), False),
                    StructField("last_execution_id", StringType(), False),
                ]
            )

            # Create a dummy watermark entity
            # NOTE: This test uses streaming mode, so watermarks aren't actually used,
            # but WatermarkEntityFinder is required by dependency injection
            from types import SimpleNamespace

            self.watermark_entity = SimpleNamespace(
                entityid="system.watermarks",
                name="watermarks",
                schema=self.watermark_schema,
                partition_columns=[],
                merge_columns=["watermark_id"],
                tags={"provider_type": "delta"},
            )

        def get_watermark_entity_for_entity(self, _context: str):
            return self.watermark_entity

        def get_watermark_entity_for_layer(self, _layer: str):
            return self.watermark_entity

    GlobalInjector.bind(EntityPathLocator, TagBasedPathLocator)
    GlobalInjector.bind(EntityNameMapper, SimpleEntityNameMapper)
    GlobalInjector.bind(WatermarkEntityFinder, SimpleWatermarkEntityFinder)

    # ---- Define entity schemas ----

    bronze_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("value", StringType(), True),
        ]
    )

    silver_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("value", StringType(), True),
            StructField("processed_at", TimestampType(), True),
        ]
    )

    gold_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("value", StringType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("enriched_at", TimestampType(), True),
        ]
    )

    # ---- Define entities ----

    DataEntities.entity(
        entityid="stream.bronze",
        name="bronze_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags={"provider_type": "delta", "provider.path": bronze_path},
        schema=bronze_schema,
    )

    DataEntities.entity(
        entityid="stream.silver",
        name="silver_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags={"provider_type": "delta", "provider.path": silver_path},
        schema=silver_schema,
    )

    DataEntities.entity(
        entityid="stream.gold",
        name="gold_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags={"provider_type": "delta", "provider.path": gold_path},
        schema=gold_schema,
    )

    msg = f"TEST_ID={test_id} test=entity_definitions status=PASSED"
    logger.info(msg)
    print(msg, flush=True)
    test_results["entity_definitions"] = True

    # ---- Define pipes ----
    # Bronze is the source entity (pre-seeded with streaming data)
    # Pipes transform: bronze → silver → gold

    @DataPipes.pipe(
        pipeid="bronze_to_silver",
        name="bronze_to_silver",
        input_entity_ids=["stream.bronze"],
        output_entity_id="stream.silver",
        output_type="append",
        tags={"processing_mode": "streaming"},
    )
    def bronze_to_silver(df):
        """Add processing timestamp to bronze events."""
        return df.withColumn("processed_at", current_timestamp()).withColumn(
            "event_id", col("value").cast(StringType())
        )

    @DataPipes.pipe(
        pipeid="silver_to_gold",
        name="silver_to_gold",
        input_entity_ids=["stream.silver"],
        output_entity_id="stream.gold",
        output_type="append",
        tags={"processing_mode": "streaming"},
    )
    def silver_to_gold(df):
        """Enrich silver events for gold layer."""
        return df.withColumn("enriched_at", current_timestamp())

    msg = f"TEST_ID={test_id} test=pipe_definitions status=PASSED"
    logger.info(msg)
    print(msg, flush=True)
    test_results["pipe_definitions"] = True

    # ---- Create tables by writing empty DataFrames (using framework) ----

    from kindling.data_entities import EntityProvider

    entity_provider = get_kindling_service(EntityProvider)
    entity_registry = get_kindling_service(DataEntityRegistry)

    bronze_entity = entity_registry.get_entity_definition("stream.bronze")
    silver_entity = entity_registry.get_entity_definition("stream.silver")
    gold_entity = entity_registry.get_entity_definition("stream.gold")

    # Write empty DataFrames to create Delta tables at specified paths
    entity_provider.write_to_entity(spark.createDataFrame([], bronze_schema), bronze_entity)
    entity_provider.write_to_entity(spark.createDataFrame([], silver_schema), silver_entity)
    entity_provider.write_to_entity(spark.createDataFrame([], gold_schema), gold_entity)

    # ---- Execute streaming pipeline via Orchestrator FIRST ----
    # Start downstream queries (silver_to_gold, bronze_to_silver) - they wait for data

    plan_generator = get_kindling_service(ExecutionPlanGenerator)
    executor = get_kindling_service(GenerationExecutor)

    pipe_ids = ["bronze_to_silver", "silver_to_gold"]
    plan = plan_generator.generate_streaming_plan(pipe_ids)

    # Pass checkpoint base path for streaming queries
    streaming_options = {"base_checkpoint_path": chk_base}
    result = executor.execute_streaming(plan, streaming_options=streaming_options)

    ok = result.all_succeeded
    num_queries = len(result.streaming_queries) if hasattr(result, "streaming_queries") else 0
    msg = (
        f"TEST_ID={test_id} test=executor_streaming "
        f"status={'PASSED' if ok else 'FAILED'} "
        f"success={result.success_count} failed={result.failed_count} "
        f"queries={num_queries}"
    )
    logger.info(msg)
    print(msg, flush=True)
    test_results["executor_streaming"] = ok

    # Track executor's streaming queries for cleanup
    if hasattr(result, "streaming_queries"):
        for pipe_id, q in result.streaming_queries.items():
            msg = f"TEST_ID={test_id} streaming_query pipe={pipe_id} query_id={q.id if q else 'None'} active={q.isActive if q else False}"
            logger.info(msg)
            print(msg)
            if q:
                streaming_queries.append(q)

    # ---- Helper: Wait for queries to process data ----
    def wait_for_query_progress(queries, expected_rows, timeout_seconds=60, poll_interval=2):
        """Monitor streaming queries and wait until they've processed expected rows."""
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            all_processed = True
            for q in queries:
                if not q.isActive:
                    continue
                progress = q.lastProgress
                if progress:
                    num_input = progress.get("numInputRows", 0)
                    msg = f"TEST_ID={test_id} query={q.id} batch={progress.get('batchId')} input_rows={num_input}"
                    logger.debug(msg)
                    # Check if this query has processed enough data
                    if num_input < expected_rows:
                        all_processed = False
                else:
                    all_processed = False

            if all_processed:
                msg = f"TEST_ID={test_id} queries_processed=true elapsed={time.time() - start_time:.1f}s"
                logger.info(msg)
                print(msg)
                return True

            time.sleep(poll_interval)

        # Timeout reached
        msg = f"TEST_ID={test_id} queries_timeout=true elapsed={time.time() - start_time:.1f}s"
        logger.warning(msg)
        print(msg)
        return False

    # ---- Simulate streaming by appending batches to bronze ----
    # Streaming queries are active and will pick up new data automatically
    from datetime import datetime

    # Small delay to let queries initialize checkpoints
    time.sleep(3)

    # Append batches of data to bronze over time using framework
    total_rows = 0
    for batch_num in range(5):
        batch_time = datetime.now()
        batch_data = [
            {
                "event_id": str(batch_num * 20 + i),
                "timestamp": batch_time,
                "value": str(batch_num * 20 + i),
            }
            for i in range(20)
        ]
        batch_df = spark.createDataFrame(batch_data)
        entity_provider.append_to_entity(batch_df, bronze_entity)
        total_rows += 20

        msg = f"TEST_ID={test_id} bronze_batch={batch_num + 1} rows=20 total={total_rows} appended=true"
        logger.info(msg)
        print(msg)

        # Wait between batches to simulate streaming
        time.sleep(3)

    # Wait for streaming queries to process all data (with timeout)
    msg = f"TEST_ID={test_id} waiting_for_queries=true expected_rows={total_rows} timeout=60s"
    logger.info(msg)
    print(msg)

    queries_processed = wait_for_query_progress(
        streaming_queries, expected_rows=total_rows, timeout_seconds=60
    )
    if not queries_processed:
        msg = f"TEST_ID={test_id} warning=queries_did_not_process_all_data"
        logger.warning(msg)
        print(msg)

    # ---- Verify data in each layer ----

    for path, label in [
        (bronze_path, "bronze_data"),
        (silver_path, "silver_data"),
        (gold_path, "gold_data"),
    ]:
        count = spark.read.format("delta").load(path).count()
        ok = count > 0
        msg = f"TEST_ID={test_id} test={label} status={'PASSED' if ok else 'FAILED'} count={count}"
        logger.info(msg)
        print(msg, flush=True)
        test_results[label] = ok

    # ---- Stop streaming queries gracefully in reverse order ----
    # NOTE: Framework should provide executor.stop_streaming_queries() that handles this
    # Reverse order ensures downstream queries finish processing before upstream stops

    msg = f"TEST_ID={test_id} stopping_queries=true count={len(streaming_queries)}"
    logger.info(msg)
    print(msg)

    for q in reversed(streaming_queries):
        try:
            if q.isActive:
                query_id = q.id
                # Process any remaining data
                q.processAllAvailable()
                # Stop gracefully
                q.stop()
                msg = f"TEST_ID={test_id} query_stopped=true query_id={query_id}"
                logger.info(msg)
                print(msg)
        except Exception as e:
            msg = f"TEST_ID={test_id} query_stop_failed=true query_id={q.id} error={str(e)}"
            logger.warning(msg)
            print(msg)

    streaming_queries.clear()
    time.sleep(2)  # Allow final cleanup

    msg = f"TEST_ID={test_id} test=queries_stopped status=PASSED"
    logger.info(msg)
    print(msg, flush=True)
    test_results["queries_stopped"] = True

    # ---- Cleanup: Delete test tables and checkpoints ----
    msg = f"TEST_ID={test_id} cleanup=starting"
    logger.info(msg)
    print(msg)

    from kindling.platform_provider import PlatformServiceProvider

    platform_service_provider = get_kindling_service(PlatformServiceProvider)
    platform_service = platform_service_provider.get_service()

    for path, label in [
        (bronze_path, "bronze"),
        (silver_path, "silver"),
        (gold_path, "gold"),
        (chk_base, "checkpoints"),
    ]:
        try:
            if platform_service.exists(path):
                platform_service.delete(path, recurse=True)
            msg = f"TEST_ID={test_id} cleanup={label} status=deleted"
            logger.info(msg)
            print(msg)
        except Exception as cleanup_error:
            msg = f"TEST_ID={test_id} cleanup={label} status=failed error={str(cleanup_error)}"
            logger.warning(msg)
            print(msg)

except Exception as e:
    msg = f"TEST_ID={test_id} status=FAILED error={str(e)}"
    logger.error(msg, include_traceback=True)
    print(msg)
    import traceback

    traceback.print_exc()
    test_results["exception"] = False

    # Stop any running queries on failure
    for q in streaming_queries:
        try:
            q.stop()
        except Exception:
            pass

# ---- Summary ----

overall = all(test_results.values())
status = "PASSED" if overall else "FAILED"
msg = f"TEST_ID={test_id} status=COMPLETED result={status}"
logger.info(msg)
print(msg, flush=True)

print(f"\n{'='*60}")
print(f"TEST SUMMARY - {status}")
print(f"{'='*60}")
for name, passed in test_results.items():
    icon = "PASSED" if passed else "FAILED"
    print(f"  {name}: {icon}")
print(f"{'='*60}\n")

sys.exit(0 if overall else 1)
