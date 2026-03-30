#!/usr/bin/env python3
"""
Streaming Pipes Test App

Tests the Unified DAG Orchestrator in streaming mode:
- Define entities and pipes via framework decorators
- Use platform-provided default EntityPathLocator and EntityNameMapper bindings
- Execute streaming plan via ExecutionOrchestrator
- Verify data flows through bronze → silver → gold

Pipeline: bronze stream → silver stream → gold stream (joined with static lookup)
"""

import sys
import time

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kindling.data_entities import DataEntities, DataEntityRegistry
from kindling.data_pipes import DataPipes
from kindling.execution_orchestrator import ExecutionOrchestrator
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.platform_provider import PlatformServiceProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from kindling.watermarking import WatermarkEntityFinder

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
    lookup_path = f"{base_path}/lookup"
    partition_test_path = f"{base_path}/partition_test"
    clustering_test_path = f"{base_path}/clustering_test"

    platform_service = get_kindling_service(PlatformServiceProvider).get_service()
    platform_name = platform_service.get_platform_name() if platform_service else "unknown"
    is_databricks = platform_name == "databricks"
    is_synapse = platform_name == "synapse"
    table_name_prefix = f"streaming_pipes_test_{str(test_id).replace('-', '_')}"
    table_catalog = None
    table_schema = None

    def _quote_table_identifier(table_name: str) -> str:
        parts = [part.strip() for part in table_name.split(".") if part.strip()]
        if not parts:
            raise ValueError("Table name cannot be empty")
        return ".".join([f"`{part.replace('`', '``')}`" for part in parts])

    if is_databricks:
        table_catalog = config_service.get("kindling.storage.table_catalog")
        table_schema = config_service.get("kindling.storage.table_schema")

        # Reuse catalog/schema from the configured UC volume path when available.
        if not table_catalog or not table_schema:
            temp_path = str(config_service.get("kindling.temp_path", "") or "")
            temp_parts = [part for part in temp_path.split("/") if part]
            if len(temp_parts) >= 3 and temp_parts[0].lower() == "volumes":
                table_catalog = table_catalog or temp_parts[1]
                table_schema = table_schema or temp_parts[2]

        if not table_catalog or not table_schema:
            current_context = spark.sql(
                "SELECT current_catalog() AS catalog, current_schema() AS schema"
            ).first()
            table_catalog = table_catalog or current_context["catalog"]
            table_schema = table_schema or current_context["schema"]

    if is_synapse:
        # Synapse convention: 2-part identifiers (<database>.<table>) work broadly.
        # Allow an explicit catalog override when the engine supports it.
        table_catalog = config_service.get("kindling.storage.table_catalog")
        table_schema = config_service.get("kindling.storage.table_schema")
        if not table_schema:
            current_context = spark.sql("SELECT current_database() AS schema").first()
            table_schema = current_context["schema"]

    def _qualified_table_name(schema: str, leaf: str) -> str:
        if table_catalog:
            return f"{table_catalog}.{schema}.{leaf}"
        return f"{schema}.{leaf}"

    def _entity_tags(layer_name: str, path: str):
        if is_databricks:
            return {
                "provider_type": "delta",
                "provider.access_mode": "forName",
                "provider.table_name": f"{table_catalog}.{table_schema}.{table_name_prefix}_{layer_name}",
            }
        if is_synapse:
            return {
                "provider_type": "delta",
                "provider.access_mode": "forName",
                "provider.table_name": _qualified_table_name(
                    table_schema, f"{table_name_prefix}_{layer_name}"
                ),
            }
        return {"provider_type": "delta", "provider.path": path}

    def _partition_test_tags(path: str):
        # Always test file partitioning via forPath to avoid platform catalog differences.
        return {
            "provider_type": "delta",
            "provider.access_mode": "forPath",
            "provider.path": path,
        }

    def _clustering_test_tags(path: str):
        # Prefer name-based clustering on engines that support it.
        if is_databricks:
            return {
                "provider_type": "delta",
                "provider.access_mode": "forName",
                "provider.table_name": f"{table_catalog}.{table_schema}.{table_name_prefix}_clustering_test",
            }
        if is_synapse:
            # Use the system test schema (pre-created with LOCATION) so managed tables can be created by name.
            return {
                "provider_type": "delta",
                "provider.access_mode": "forName",
                "provider.table_name": _qualified_table_name(
                    table_schema, f"{table_name_prefix}_clustering_test"
                ),
            }
        return {
            "provider_type": "delta",
            "provider.access_mode": "forPath",
            "provider.path": path,
        }

    def _get_row_field(row, field_name: str):
        if row is None:
            return None
        try:
            d = row.asDict(recursive=True)
        except Exception:
            d = dict(row) if isinstance(row, dict) else {}
        for k, v in d.items():
            if str(k).lower() == str(field_name).lower():
                return v
        return None

    def _describe_detail_for_path(path: str):
        escaped = str(path).replace("`", "``")
        return spark.sql(f"DESCRIBE DETAIL delta.`{escaped}`").first()

    def _describe_detail_for_table(table_name: str):
        return spark.sql(f"DESCRIBE DETAIL {_quote_table_identifier(table_name)}").first()

    def _coerce_cluster_columns(raw_value, default_cols):
        """Coerce a config value into the cluster_columns shape expected by DataEntities.entity."""
        if raw_value is None:
            return list(default_cols)
        if isinstance(raw_value, str):
            return [raw_value]
        try:
            return [str(v) for v in list(raw_value)]
        except Exception:
            return list(default_cols)

    def _is_auto_cluster_columns(cluster_cols) -> bool:
        if cluster_cols is None:
            return False
        if isinstance(cluster_cols, str):
            vals = [cluster_cols]
        else:
            try:
                vals = list(cluster_cols)
            except Exception:
                vals = [cluster_cols]
        return len(vals) == 1 and str(vals[0]).strip().lower() == "auto"

    def _auto_clustering_enabled() -> bool:
        try:
            from kindling.features import get_feature_bool

            return get_feature_bool(config_service, "delta.auto_clustering", default=False) is True
        except Exception:
            return False

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
            StructField("category", StringType(), True),
        ]
    )

    lookup_schema = StructType(
        [
            StructField("lookup_key", StringType(), True),
            StructField("category", StringType(), True),
        ]
    )

    # ---- Define entities ----

    DataEntities.entity(
        entityid="stream.bronze",
        name="bronze_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags=_entity_tags("bronze", bronze_path),
        schema=bronze_schema,
    )

    DataEntities.entity(
        entityid="stream.silver",
        name="silver_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags=_entity_tags("silver", silver_path),
        schema=silver_schema,
    )

    DataEntities.entity(
        entityid="stream.gold",
        name="gold_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags=_entity_tags("gold", gold_path),
        schema=gold_schema,
    )

    DataEntities.entity(
        entityid="stream.lookup",
        name="lookup_events",
        partition_columns=[],
        merge_columns=["lookup_key"],
        tags=_entity_tags("lookup", lookup_path),
        schema=lookup_schema,
    )

    # ---- Delta layout tests: partitioning + clustering ----

    layout_schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("pdate", StringType(), False),
            StructField("value", StringType(), True),
        ]
    )

    DataEntities.entity(
        entityid="system.delta_partitioning",
        name="delta_partitioning",
        partition_columns=["pdate"],
        merge_columns=["id"],
        tags=_partition_test_tags(partition_test_path),
        schema=layout_schema,
    )

    # Intentionally specify both partition_columns and cluster_columns to validate
    # provider behavior: prefer clustering and skip partitionBy().
    # In system tests, allow overriding cluster columns via config so we can validate
    # "auto" liquid clustering behavior without cloning the app.
    cluster_cols_v1 = _coerce_cluster_columns(
        config_service.get("kindling.system_tests.streaming_pipes.cluster_columns"), ["id"]
    )
    DataEntities.entity(
        entityid="system.delta_clustering",
        name="delta_clustering",
        partition_columns=["pdate"],
        cluster_columns=cluster_cols_v1,
        merge_columns=["id"],
        tags=_clustering_test_tags(clustering_test_path),
        schema=layout_schema,
    )

    # Same destination as delta_clustering, but a different clustering spec to validate updates.
    cluster_cols_v2 = _coerce_cluster_columns(
        config_service.get("kindling.system_tests.streaming_pipes.cluster_columns_v2"), ["pdate"]
    )
    DataEntities.entity(
        entityid="system.delta_clustering_v2",
        name="delta_clustering_v2",
        partition_columns=["pdate"],
        cluster_columns=cluster_cols_v2,
        merge_columns=["id"],
        tags=_clustering_test_tags(clustering_test_path),
        schema=layout_schema,
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
        input_entity_ids=["stream.silver", "stream.lookup"],
        output_entity_id="stream.gold",
        output_type="append",
        tags={"processing_mode": "streaming"},
    )
    def silver_to_gold(stream_silver, stream_lookup):
        """Enrich silver events for gold layer using static lookup data."""
        return (
            stream_silver.join(
                stream_lookup,
                stream_silver["value"] == stream_lookup["lookup_key"],
                "left",
            )
            .drop("lookup_key")
            .withColumn("enriched_at", current_timestamp())
        )

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
    lookup_entity = entity_registry.get_entity_definition("stream.lookup")
    partition_entity = entity_registry.get_entity_definition("system.delta_partitioning")
    clustering_entity = entity_registry.get_entity_definition("system.delta_clustering")
    clustering_entity_v2 = entity_registry.get_entity_definition("system.delta_clustering_v2")

    # Write empty DataFrames to create Delta tables at specified paths
    entity_provider.write_to_entity(spark.createDataFrame([], bronze_schema), bronze_entity)
    entity_provider.write_to_entity(spark.createDataFrame([], silver_schema), silver_entity)
    entity_provider.write_to_entity(spark.createDataFrame([], gold_schema), gold_entity)
    entity_provider.write_to_entity(spark.createDataFrame([], lookup_schema), lookup_entity)

    # ---- Validate partitioning ----
    try:
        entity_provider.ensure_entity_table(partition_entity)
        detail = _describe_detail_for_path(partition_test_path)
        partition_cols = _get_row_field(detail, "partitionColumns") or []
        ok = list(partition_cols) == ["pdate"]
        msg = (
            f"TEST_ID={test_id} test=delta_partitioning status={'PASSED' if ok else 'FAILED'} "
            f"partitionColumns={partition_cols}"
        )
        logger.info(msg)
        print(msg, flush=True)
        test_results["delta_partitioning"] = ok
    except Exception as e:
        msg = (
            f"TEST_ID={test_id} test=delta_partitioning status=FAILED error={type(e).__name__}:{e}"
        )
        logger.error(msg)
        print(msg, flush=True)
        test_results["delta_partitioning"] = False

    # ---- Validate clustering ----
    try:
        auto_requested = _is_auto_cluster_columns(
            getattr(clustering_entity, "cluster_columns", None)
        )
        auto_enabled = _auto_clustering_enabled()

        entity_provider.ensure_entity_table(clustering_entity)

        if auto_requested and not auto_enabled:
            ok = False
            partition_cols, clustering_cols = [], []
        else:
            # Always validate that clustering config prevents file partitioning.
            clustering_table_name = (clustering_entity.tags or {}).get("provider.table_name")
            if clustering_table_name:
                detail = _describe_detail_for_table(clustering_table_name)
            else:
                detail = _describe_detail_for_path(clustering_test_path)

            partition_cols = _get_row_field(detail, "partitionColumns") or []
            clustering_cols = _get_row_field(detail, "clusteringColumns") or []

            if auto_requested:
                # For AUTO we don't assert exact engine-chosen columns; we just ensure we did not
                # physically partition files even though partition_columns were provided.
                ok = list(partition_cols) == []
            else:
                ok = list(partition_cols) == [] and (
                    "id" in [str(c).lower() for c in list(clustering_cols)]
                )

        msg = (
            f"TEST_ID={test_id} test=delta_clustering status={'PASSED' if ok else 'FAILED'} "
            f"partitionColumns={partition_cols} clusteringColumns={clustering_cols} "
            f"auto_requested={auto_requested} auto_enabled={auto_enabled}"
        )
        logger.info(msg)
        print(msg, flush=True)
        test_results["delta_clustering"] = ok
    except Exception as e:
        auto_requested = _is_auto_cluster_columns(
            getattr(clustering_entity, "cluster_columns", None)
        )
        auto_enabled = _auto_clustering_enabled()

        if auto_requested and not auto_enabled:
            # Expected: provider should reject auto clustering when feature is not enabled.
            msg = (
                f"TEST_ID={test_id} test=delta_clustering status=PASSED "
                f"auto_requested=true auto_enabled=false expected_rejection=true "
                f"error={type(e).__name__}:{e}"
            )
            logger.info(msg)
            print(msg, flush=True)
            test_results["delta_clustering"] = True
        else:
            msg = (
                f"TEST_ID={test_id} test=delta_clustering status=FAILED "
                f"auto_requested={auto_requested} auto_enabled={auto_enabled} "
                f"error={type(e).__name__}:{e}"
            )
            logger.error(msg)
            print(msg, flush=True)
            test_results["delta_clustering"] = False

    # ---- Validate clustering updates ----
    try:
        auto_requested = _is_auto_cluster_columns(
            getattr(clustering_entity_v2, "cluster_columns", None)
        )
        auto_enabled = _auto_clustering_enabled()

        entity_provider.ensure_entity_table(clustering_entity_v2)

        if auto_requested and not auto_enabled:
            ok = False
            partition_cols, clustering_cols = [], []
        else:
            clustering_table_name = (clustering_entity_v2.tags or {}).get("provider.table_name")
            if clustering_table_name:
                detail = _describe_detail_for_table(clustering_table_name)
            else:
                detail = _describe_detail_for_path(clustering_test_path)

            partition_cols = _get_row_field(detail, "partitionColumns") or []
            clustering_cols = _get_row_field(detail, "clusteringColumns") or []

            if auto_requested:
                ok = list(partition_cols) == []
            else:
                ok = list(partition_cols) == []
                desired = {"pdate"}
                actual = {str(c).lower() for c in list(clustering_cols)}
                ok = ok and (actual == desired)

        msg = (
            f"TEST_ID={test_id} test=delta_clustering_update status={'PASSED' if ok else 'FAILED'} "
            f"partitionColumns={partition_cols} clusteringColumns={clustering_cols} "
            f"auto_requested={auto_requested} auto_enabled={auto_enabled}"
        )
        logger.info(msg)
        print(msg, flush=True)
        test_results["delta_clustering_update"] = ok
    except Exception as e:
        auto_requested = _is_auto_cluster_columns(
            getattr(clustering_entity_v2, "cluster_columns", None)
        )
        auto_enabled = _auto_clustering_enabled()

        if auto_requested and not auto_enabled:
            msg = (
                f"TEST_ID={test_id} test=delta_clustering_update status=PASSED "
                f"auto_requested=true auto_enabled=false expected_rejection=true "
                f"error={type(e).__name__}:{e}"
            )
            logger.info(msg)
            print(msg, flush=True)
            test_results["delta_clustering_update"] = True
        else:
            msg = (
                f"TEST_ID={test_id} test=delta_clustering_update status=FAILED "
                f"auto_requested={auto_requested} auto_enabled={auto_enabled} "
                f"error={type(e).__name__}:{e}"
            )
            logger.error(msg)
            print(msg, flush=True)
            test_results["delta_clustering_update"] = False

    # Seed static lookup data (read as direct/batch input while silver is streaming)
    lookup_rows = [{"lookup_key": str(i), "category": f"group_{i % 5}"} for i in range(100)]
    lookup_df = spark.createDataFrame(lookup_rows, lookup_schema)
    # Lookup table already exists from the empty-table bootstrap write above.
    # Append seed rows to avoid DELTA_PATH_EXISTS on platforms that disallow
    # implicit overwrite when saving to an existing Delta path.
    entity_provider.append_to_entity(lookup_df, lookup_entity)

    msg = f"TEST_ID={test_id} test=lookup_data status=PASSED count={len(lookup_rows)}"
    logger.info(msg)
    print(msg, flush=True)
    test_results["lookup_data"] = True

    # ---- Execute streaming pipeline via Orchestrator FIRST ----
    # Start downstream queries (silver_to_gold, bronze_to_silver) - they wait for data

    orchestrator = get_kindling_service(ExecutionOrchestrator)

    pipe_ids = ["bronze_to_silver", "silver_to_gold"]

    # Pass checkpoint base path for streaming queries
    streaming_options = {"base_checkpoint_path": chk_base}
    result = orchestrator.execute_streaming(pipe_ids, streaming_options=streaming_options)

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

    for entity, label in [
        (bronze_entity, "bronze_data"),
        (silver_entity, "silver_data"),
        (gold_entity, "gold_data"),
    ]:
        count = entity_provider.read_entity(entity).count()
        ok = count > 0
        msg = f"TEST_ID={test_id} test={label} status={'PASSED' if ok else 'FAILED'} count={count}"
        logger.info(msg)
        print(msg, flush=True)
        test_results[label] = ok

    gold_df = entity_provider.read_entity(gold_entity)
    matched_lookup_rows = gold_df.filter(col("category").isNotNull()).count()
    multi_input_ok = matched_lookup_rows > 0
    msg = (
        f"TEST_ID={test_id} test=multi_input_lookup_join "
        f"status={'PASSED' if multi_input_ok else 'FAILED'} "
        f"matched_rows={matched_lookup_rows}"
    )
    logger.info(msg)
    print(msg, flush=True)
    test_results["multi_input_lookup_join"] = multi_input_ok

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

    if is_databricks:
        for entity, label in [
            (bronze_entity, "bronze_table"),
            (silver_entity, "silver_table"),
            (gold_entity, "gold_table"),
            (lookup_entity, "lookup_table"),
        ]:
            try:
                table_name = (entity.tags or {}).get("provider.table_name")
                if table_name:
                    spark.sql(f"DROP TABLE IF EXISTS {_quote_table_identifier(table_name)}")
                msg = f"TEST_ID={test_id} cleanup={label} status=dropped"
                logger.info(msg)
                print(msg)
            except Exception as cleanup_error:
                msg = (
                    f"TEST_ID={test_id} cleanup={label} status=failed "
                    f"error={str(cleanup_error)}"
                )
                logger.warning(msg)
                print(msg)

        try:
            if platform_service.exists(chk_base):
                platform_service.delete(chk_base, recurse=True)
            msg = f"TEST_ID={test_id} cleanup=checkpoints status=deleted"
            logger.info(msg)
            print(msg)
        except Exception as cleanup_error:
            msg = (
                f"TEST_ID={test_id} cleanup=checkpoints status=failed "
                f"error={str(cleanup_error)}"
            )
            logger.warning(msg)
            print(msg)
    else:
        for path, label in [
            (bronze_path, "bronze"),
            (silver_path, "silver"),
            (gold_path, "gold"),
            (lookup_path, "lookup"),
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
