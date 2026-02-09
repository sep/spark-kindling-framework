#!/usr/bin/env python3
"""
Streaming Pipes Test App

Tests the Unified DAG Orchestrator in streaming mode:
- Define entities and pipes via framework decorators
- Seed bronze via entity provider + create_mock_stream() helper
- Execute streaming plan via GenerationExecutor
- Verify data flows through bronze → silver → gold

Pipeline: mock stream → bronze (seed) → silver → gold (orchestrated)
"""

import sys
import time

from kindling.data_entities import DataEntities, DataEntityRegistry, EntityPathLocator
from kindling.data_pipes import DataPipes
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.execution_strategy import ExecutionPlanGenerator
from kindling.generation_executor import GenerationExecutor
from kindling.injection import get_kindling_service
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.test_framework import create_mock_stream
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType

# ---- Init ----

logger_provider = get_kindling_service(SparkLoggerProvider)
logger = logger_provider.get_logger("streaming-pipes-test-app")

config_service = get_kindling_service(ConfigService)
test_id = str(config_service.get("test_id") or "unknown")

msg = f"TEST_ID={test_id} status=STARTED component=streaming_orchestrator"
logger.info(msg)
print(msg)

test_results = {}

try:
    # Checkpoint base from config — set per-platform in config hierarchy
    chk_base = config_service.get("checkpoint_path") or f"checkpoints/streaming_pipes_{test_id}"
    config_service.set("base_checkpoint_path", f"{chk_base}/pipes")

    # ---- Define entities ----

    @DataEntities.entity(
        entityid="stream.bronze",
        name="bronze_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags={"provider_type": "delta"},
    )
    class BronzeEvents:
        pass

    @DataEntities.entity(
        entityid="stream.silver",
        name="silver_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags={"provider_type": "delta"},
    )
    class SilverEvents:
        pass

    @DataEntities.entity(
        entityid="stream.gold",
        name="gold_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags={"provider_type": "delta"},
    )
    class GoldEvents:
        pass

    msg = f"TEST_ID={test_id} test=entity_definitions status=PASSED"
    logger.info(msg)
    print(msg)
    test_results["entity_definitions"] = True

    # ---- Define pipes ----

    @DataPipes.pipe(
        pipeid="bronze_to_silver",
        input_entity_ids=["stream.bronze"],
        output_entity_id="stream.silver",
        tags={"processing_mode": "streaming"},
    )
    def bronze_to_silver(df):
        """Add processing timestamp to bronze events."""
        return df.withColumn("processed_at", current_timestamp()).withColumn(
            "event_id", col("value").cast(StringType())
        )

    @DataPipes.pipe(
        pipeid="silver_to_gold",
        input_entity_ids=["stream.silver"],
        output_entity_id="stream.gold",
        tags={"processing_mode": "streaming"},
    )
    def silver_to_gold(df):
        """Enrich silver events for gold layer."""
        return df.withColumn("enriched_at", current_timestamp())

    msg = f"TEST_ID={test_id} test=pipe_definitions status=PASSED"
    logger.info(msg)
    print(msg)
    test_results["pipe_definitions"] = True

    # ---- Seed bronze via entity provider ----
    # create_mock_stream() is a test helper that creates a Spark rate source.
    # In production, bronze would be fed by EventHub, Kafka, etc.

    entity_registry = get_kindling_service(DataEntityRegistry)
    provider_registry = get_kindling_service(EntityProviderRegistry)
    entity_path_locator = get_kindling_service(EntityPathLocator)

    bronze_entity = entity_registry.get_entity_definition("stream.bronze")
    bronze_provider = provider_registry.get_provider_for_entity(bronze_entity)
    bronze_path = entity_path_locator.get_table_path(bronze_entity)

    mock_stream = create_mock_stream(rows_per_second=10)
    bronze_seed = mock_stream.select(
        col("value").cast(StringType()).alias("event_id"),
        col("timestamp"),
        col("value"),
    )

    bronze_query = bronze_provider.append_as_stream(
        bronze_entity, bronze_seed, f"{chk_base}/seed"
    ).start(bronze_path)

    msg = f"TEST_ID={test_id} test=bronze_seed status=STARTED query_id={bronze_query.id}"
    logger.info(msg)
    print(msg)
    test_results["bronze_seed"] = True

    # Let some data land in bronze
    time.sleep(5)

    # ---- Execute streaming plan via GenerationExecutor ----

    plan_generator = get_kindling_service(ExecutionPlanGenerator)
    executor = get_kindling_service(GenerationExecutor)

    pipe_ids = ["bronze_to_silver", "silver_to_gold"]
    plan = plan_generator.generate_streaming_plan(pipe_ids)
    result = executor.execute_streaming(plan)

    ok = result.all_succeeded
    msg = (
        f"TEST_ID={test_id} test=executor_streaming "
        f"status={'PASSED' if ok else 'FAILED'} "
        f"success={result.success_count} failed={result.failed_count}"
    )
    logger.info(msg)
    print(msg)
    test_results["executor_streaming"] = ok

    # ---- Wait for data to flow through ----

    print(f"TEST_ID={test_id} waiting_for_pipeline=true duration=15s")
    time.sleep(15)

    # ---- Verify data in each layer ----

    for entity_id, label in [
        ("stream.bronze", "bronze_data"),
        ("stream.silver", "silver_data"),
        ("stream.gold", "gold_data"),
    ]:
        entity = entity_registry.get_entity_definition(entity_id)
        provider = provider_registry.get_provider_for_entity(entity)
        count = provider.read_entity(entity).count()
        ok = count > 0
        msg = f"TEST_ID={test_id} test={label} status={'PASSED' if ok else 'FAILED'} count={count}"
        logger.info(msg)
        print(msg)
        test_results[label] = ok

    # ---- Stop all streaming queries ----

    bronze_query.stop()
    for q in result.streaming_queries.values():
        q.stop()
    time.sleep(2)

    msg = f"TEST_ID={test_id} test=queries_stopped status=PASSED"
    logger.info(msg)
    print(msg)
    test_results["queries_stopped"] = True

except Exception as e:
    msg = f"TEST_ID={test_id} status=FAILED error={str(e)}"
    logger.error(msg, exc_info=True)
    print(msg)
    import traceback

    traceback.print_exc()
    test_results["exception"] = False

# ---- Summary ----

overall = all(test_results.values())
status = "PASSED" if overall else "FAILED"
msg = f"TEST_ID={test_id} status=COMPLETED result={status}"
logger.info(msg)
print(msg)

print(f"\n{'='*60}")
print(f"TEST SUMMARY - {status}")
print(f"{'='*60}")
for name, passed in test_results.items():
    icon = "PASSED" if passed else "FAILED"
    print(f"  {name}: {icon}")
print(f"{'='*60}\n")

sys.exit(0 if overall else 1)
