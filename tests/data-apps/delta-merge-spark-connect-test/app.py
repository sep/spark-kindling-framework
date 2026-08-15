#!/usr/bin/env python3
"""
Delta Merge / Spark Connect Regression Test App

Regression coverage for the kindling.trace_ops._entity_id_from_args bug:
provider-op tracing used to call getattr(value, "entityid", None) on every
argument passed to a traced op, including the DataFrame argument to
merge_to_entity(df, entity). On a genuine Spark Connect DataFrame, an
unrecognized attribute name is not a clean AttributeError -- it can trigger
a schema-resolution RPC that fails outside an active session/API-URL
context (e.g. "No api url found in local command context").

This app calls merge_to_entity(df, entity) directly through the traced
provider registry -- the same call shape SCD1MergeStrategy.apply() drives --
with provider-op tracing running at its real default (standard) level, so
whatever DataFrame class the job's actual Spark session hands back (classic
or Spark Connect, depending on cluster access mode) flows through the exact
code path that was broken.

Emits TEST_ID= markers so the system test harness can validate outcome from
streamed stdout.
"""

import sys

from pyspark.sql.types import LongType, StringType, StructField, StructType

from kindling.data_entities import DataEntities, DataEntityRegistry
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.platform_provider import PlatformServiceProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_session import get_or_create_spark_session

logger = get_kindling_service(PythonLoggerProvider).get_logger("delta-merge-spark-connect-test")
config_service = get_kindling_service(ConfigService)
test_id = str(config_service.get("test_id") or "unknown").replace("-", "_")

platform_service = get_kindling_service(PlatformServiceProvider).get_service()
platform_name = platform_service.get_platform_name() if platform_service else "unknown"

msg = f"TEST_ID={test_id} status=STARTED platform={platform_name}"
logger.info(msg)
print(msg, flush=True)

spark = get_or_create_spark_session()
print(
    f"TEST_ID={test_id} spark_session_class={type(spark).__module__}.{type(spark).__name__}",
    flush=True,
)

TELEMETRY_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("reading_ts", LongType(), False),
        StructField("value", LongType(), True),
    ]
)

DataEntities.entity(
    entityid="staging.device_telemetry",
    name=f"device_telemetry_{test_id}",
    merge_columns=["device_id", "reading_ts"],
    tags={"provider_type": "delta"},
    schema=TELEMETRY_SCHEMA,
    partition_columns=[],
)

entity = GlobalInjector.get(DataEntityRegistry).get_entity_definition("staging.device_telemetry")
provider = GlobalInjector.get(EntityProviderRegistry).get_provider_for_entity(entity)
print(
    f"TEST_ID={test_id} provider_class={type(provider).__module__}.{type(provider).__name__}",
    flush=True,
)

try:
    initial_rows = [("device-1", 1, 10), ("device-2", 1, 20)]
    initial_df = spark.createDataFrame(initial_rows, TELEMETRY_SCHEMA)
    print(
        f"TEST_ID={test_id} df_class={type(initial_df).__module__}.{type(initial_df).__name__}",
        flush=True,
    )
    provider.merge_to_entity(initial_df, entity)
    print(f"TEST_ID={test_id} test=initial_merge status=PASSED", flush=True)

    # Second merge exercises the update-matched / insert-unmatched branches of
    # SCD1MergeStrategy: device-1 matches and updates, device-3 is new.
    update_rows = [("device-1", 1, 99), ("device-3", 1, 30)]
    provider.merge_to_entity(spark.createDataFrame(update_rows, TELEMETRY_SCHEMA), entity)

    result = {row["device_id"]: row["value"] for row in provider.read_entity(entity).collect()}
    expected = {"device-1": 99, "device-2": 20, "device-3": 30}
    if result != expected:
        raise AssertionError(f"unexpected merge result: {result} (expected {expected})")
    print(f"TEST_ID={test_id} test=keyed_merge_updates_matched_rows status=PASSED", flush=True)

    print(f"TEST_ID={test_id} status=COMPLETED result=PASSED", flush=True)
except Exception as exc:
    logger.error(f"TEST_ID={test_id} status=COMPLETED result=FAILED error={exc}")
    print(f"TEST_ID={test_id} status=COMPLETED result=FAILED error={exc}", flush=True)
    import traceback

    traceback.print_exc()
    sys.exit(1)
