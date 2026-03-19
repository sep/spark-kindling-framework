#!/usr/bin/env python3
"""Streaming Orchestrator system test app.

Exercises StreamingOrchestrator as the primary streaming runtime entrypoint:
- define streaming entities and pipes via normal Kindling abstractions
- start streaming through StreamingOrchestrator.run(...)
- feed source data after startup
- stop the active query from a controller thread after sink progress
- verify orchestrator signals and successful completion markers
"""

import sys
import threading
import time
from datetime import datetime
from types import SimpleNamespace

from kindling.data_entities import DataEntities, DataEntityRegistry, EntityProvider
from kindling.data_pipes import DataPipes
from kindling.execution_strategy import ExecutionPlanGenerator
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.platform_provider import PlatformServiceProvider
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from kindling.streaming_listener import KindlingStreamingListener
from kindling.streaming_orchestrator import StreamingOrchestrator
from kindling.streaming_query_manager import StreamingQueryManager
from kindling.watermarking import WatermarkEntityFinder
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


class SimpleWatermarkEntityFinder(WatermarkEntityFinder):
    """Minimal WatermarkEntityFinder for system-test DI wiring."""

    def __init__(self):
        watermark_schema = StructType(
            [
                StructField("watermark_id", StringType(), False),
                StructField("source_entity_id", StringType(), False),
                StructField("reader_id", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("last_version_processed", StringType(), True),
                StructField("last_execution_id", StringType(), True),
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


def _emit(logger, test_id: str, test_name: str, passed: bool, details: str = "") -> None:
    status = "PASSED" if passed else "FAILED"
    suffix = f" {details}" if details else ""
    line = f"TEST_ID={test_id} test={test_name} status={status}{suffix}"
    logger.info(line)
    print(line, flush=True)


def _emit_signal(logger, test_id: str, signal_name: str) -> None:
    line = f"TEST_ID={test_id} signal={signal_name} received=true"
    logger.info(line)
    print(line, flush=True)


def _flush_azure_monitor(logger) -> None:
    """Best-effort flush/shutdown for Azure Monitor-backed telemetry export."""
    try:
        from kindling_otel_azure.config import AzureMonitorConfig

        logger.info("Azure Monitor extension detected - flushing telemetry...")
        print("Azure Monitor extension detected - flushing telemetry...", flush=True)

        flush_success = AzureMonitorConfig.force_flush(timeout_millis=10000)
        if flush_success:
            logger.info("Azure Monitor telemetry flush completed")
            print("Azure Monitor telemetry flush completed", flush=True)
        else:
            logger.warning("Azure Monitor telemetry flush timed out")
            print("Azure Monitor telemetry flush timed out", flush=True)

        AzureMonitorConfig.shutdown()
        logger.info("Azure Monitor shutdown complete")
        print("Azure Monitor shutdown complete", flush=True)
    except ImportError:
        logger.info("Azure Monitor extension not loaded; skipping telemetry flush")
    except Exception as exc:
        logger.warning(f"Azure Monitor telemetry flush failed: {type(exc).__name__}:{exc}")
        print(
            f"Azure Monitor telemetry flush failed: {type(exc).__name__}:{exc}",
            flush=True,
        )


def _get_test_id(config_service: ConfigService) -> str:
    value = config_service.get("test_id")
    return str(value) if value else "unknown"


def _detect_platform(config_service: ConfigService) -> str:
    try:
        platform_service = get_kindling_service(PlatformServiceProvider).get_service()
        if platform_service:
            name = platform_service.get_platform_name()
            if name:
                return str(name).lower()
    except Exception:
        pass
    return str(config_service.get("platform") or "fabric").lower()


def _extract_volume_namespace(temp_path: str) -> tuple[str | None, str | None]:
    parts = [part for part in str(temp_path or "").split("/") if part]
    if len(parts) >= 3 and parts[0].lower() == "volumes":
        return parts[1], parts[2]
    return None, None


def _resolve_table_namespace(spark, config_service: ConfigService, platform_name: str):
    if platform_name == "databricks":
        catalog = config_service.get("kindling.storage.table_catalog")
        schema = config_service.get("kindling.storage.table_schema")
        if not catalog or not schema:
            temp_catalog, temp_schema = _extract_volume_namespace(
                config_service.get("kindling.temp_path")
            )
            catalog = catalog or temp_catalog
            schema = schema or temp_schema
        if not catalog or not schema:
            context = spark.sql(
                "SELECT current_catalog() AS catalog, current_schema() AS schema"
            ).first()
            catalog = catalog or context["catalog"]
            schema = schema or context["schema"]
        return catalog, schema

    if platform_name == "synapse":
        catalog = config_service.get("kindling.storage.table_catalog")
        schema = config_service.get("kindling.storage.table_schema")
        if not schema:
            schema = spark.sql("SELECT current_database() AS schema").first()["schema"]
        return catalog, schema

    return None, None


def _resolve_runtime_paths(config_service: ConfigService, platform_name: str, test_id: str):
    if platform_name == "databricks":
        checkpoint_root = config_service.get("kindling.storage.checkpoint_root")
        table_root = config_service.get("kindling.storage.table_root")
        temp_root = config_service.get("kindling.temp_path")

        if checkpoint_root and table_root:
            base_tables = f"{str(table_root).rstrip('/')}/streaming_orchestrator_{test_id}"
            return {
                "source_path": f"{base_tables}/source",
                "sink_path": f"{base_tables}/sink",
                "checkpoint_root": (
                    f"{str(checkpoint_root).rstrip('/')}/streaming_orchestrator_{test_id}"
                ),
                "temp_root": str(temp_root or table_root).rstrip("/"),
            }

        temp_root = str(temp_root or "/tmp").rstrip("/")
        return {
            "source_path": f"{temp_root}/streaming_orchestrator_{test_id}/source",
            "sink_path": f"{temp_root}/streaming_orchestrator_{test_id}/sink",
            "checkpoint_root": f"{temp_root}/streaming_orchestrator_{test_id}/checkpoints",
            "temp_root": temp_root,
        }

    table_root = str(config_service.get("kindling.storage.table_root") or "Tables").rstrip("/")
    checkpoint_root = str(
        config_service.get("kindling.storage.checkpoint_root") or "Files/checkpoints"
    ).rstrip("/")
    return {
        "source_path": f"{table_root}/streaming_orchestrator_{test_id}/source",
        "sink_path": f"{table_root}/streaming_orchestrator_{test_id}/sink",
        "checkpoint_root": f"{checkpoint_root}/streaming_orchestrator_{test_id}",
        "temp_root": table_root,
    }


def _entity_tags(
    platform_name: str,
    path: str,
    table_leaf: str,
    table_catalog: str | None,
    table_schema: str | None,
):
    if platform_name == "databricks" and table_catalog and table_schema:
        return {
            "provider_type": "delta",
            "provider.access_mode": "forName",
            "provider.table_name": f"{table_catalog}.{table_schema}.{table_leaf}",
        }
    if platform_name == "synapse" and table_schema:
        table_name = f"{table_schema}.{table_leaf}"
        if table_catalog:
            table_name = f"{table_catalog}.{table_name}"
        return {
            "provider_type": "delta",
            "provider.access_mode": "forName",
            "provider.table_name": table_name,
        }
    return {
        "provider_type": "delta",
        "provider.access_mode": "forPath",
        "provider.path": path,
    }


# Initialize


def main() -> int:
    GlobalInjector.bind(WatermarkEntityFinder, SimpleWatermarkEntityFinder)

    config_service = get_kindling_service(ConfigService)
    logger_provider = get_kindling_service(SparkLoggerProvider)
    logger = logger_provider.get_logger("streaming-orchestrator-test-app")
    test_id = _get_test_id(config_service)

    start_line = f"TEST_ID={test_id} status=STARTED component=streaming_orchestrator"
    logger.info(start_line)
    print(start_line, flush=True)

    test_results = {}

    try:
        spark = get_or_create_spark_session()
        _emit(logger, test_id, "spark_session", True)
        test_results["spark_session"] = True

        # Enable trace printing so streaming span events appear in stdout
        config_service.set("print_trace", True)

        platform_name = _detect_platform(config_service)
        runtime_paths = _resolve_runtime_paths(config_service, platform_name, test_id)
        table_catalog, table_schema = _resolve_table_namespace(spark, config_service, platform_name)
        table_prefix = f"streaming_orchestrator_{str(test_id).replace('-', '_')}"

        source_schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("value", StringType(), True),
                StructField("event_time", TimestampType(), True),
            ]
        )
        sink_schema = StructType(
            [
                StructField("event_id", StringType(), True),
                StructField("value", StringType(), True),
                StructField("event_time", TimestampType(), True),
                StructField("processed_at", TimestampType(), True),
            ]
        )

        source_entity_id = f"stream.orchestrator_source_{test_id}"
        sink_entity_id = f"stream.orchestrator_sink_{test_id}"
        pipe_id = f"streaming_orchestrator_pipe_{test_id}"

        DataEntities.entity(
            entityid=source_entity_id,
            name=f"orchestrator_source_{test_id}",
            partition_columns=[],
            merge_columns=["event_id"],
            tags=_entity_tags(
                platform_name,
                runtime_paths["source_path"],
                f"{table_prefix}_source",
                table_catalog,
                table_schema,
            ),
            schema=source_schema,
        )

        DataEntities.entity(
            entityid=sink_entity_id,
            name=f"orchestrator_sink_{test_id}",
            partition_columns=[],
            merge_columns=["event_id"],
            tags=_entity_tags(
                platform_name,
                runtime_paths["sink_path"],
                f"{table_prefix}_sink",
                table_catalog,
                table_schema,
            ),
            schema=sink_schema,
        )

        _emit(logger, test_id, "entity_definitions", True, f"platform={platform_name}")
        test_results["entity_definitions"] = True

        source_key = source_entity_id.replace(".", "_")

        @DataPipes.pipe(
            pipeid=pipe_id,
            name=pipe_id,
            input_entity_ids=[source_entity_id],
            output_entity_id=sink_entity_id,
            output_type="append",
            tags={"processing_mode": "streaming"},
        )
        def source_to_sink_pipe(**frames):
            stream_df = frames[source_key]
            return stream_df.withColumn("processed_at", current_timestamp())

        _emit(logger, test_id, "pipe_definitions", True, f"pipe_id={pipe_id}")
        test_results["pipe_definitions"] = True

        entity_provider = get_kindling_service(EntityProvider)
        entity_registry = get_kindling_service(DataEntityRegistry)
        source_entity = entity_registry.get_entity_definition(source_entity_id)
        sink_entity = entity_registry.get_entity_definition(sink_entity_id)

        entity_provider.write_to_entity(spark.createDataFrame([], source_schema), source_entity)
        entity_provider.write_to_entity(spark.createDataFrame([], sink_schema), sink_entity)
        _emit(logger, test_id, "entity_bootstrap", True)
        test_results["entity_bootstrap"] = True

        signal_provider = get_kindling_service(SignalProvider)
        orchestrator = get_kindling_service(StreamingOrchestrator)
        query_manager = get_kindling_service(StreamingQueryManager)
        plan_generator = get_kindling_service(ExecutionPlanGenerator)
        plan = plan_generator.generate_streaming_plan([pipe_id])

        _emit(logger, test_id, "plan_generation", True, f"generations={len(plan.generations)}")
        test_results["plan_generation"] = True

        signal_hits: list[str] = []
        signal_lock = threading.Lock()
        controller_state = {"query_seen": False, "stop_passed": False, "sink_rows": 0}
        producer_state = {"batches": 0, "rows": 0}

        def track_signal(signal_name):
            def _handler(sender, **kwargs):
                del sender, kwargs
                with signal_lock:
                    signal_hits.append(signal_name)
                _emit_signal(logger, test_id, signal_name)

            return _handler

        for signal_name in [
            "streaming.orchestrator_started",
            "streaming.orchestrator_completed",
            "streaming.query_started",
            "streaming.spark_query_started",
        ]:
            signal_provider.create_signal(signal_name).connect(
                track_signal(signal_name), weak=False
            )

        def produce_source_batches():
            time.sleep(4)
            for batch_num in range(3):
                batch_rows = [
                    {
                        "event_id": f"{test_id}-{batch_num}-{row_num}",
                        "value": f"value_{batch_num}_{row_num}",
                        "event_time": datetime.now(),
                    }
                    for row_num in range(5)
                ]
                batch_df = spark.createDataFrame(batch_rows, source_schema)
                entity_provider.append_to_entity(batch_df, source_entity)
                producer_state["batches"] += 1
                producer_state["rows"] += len(batch_rows)
                line = (
                    f"TEST_ID={test_id} producer=batch_written batch={batch_num + 1} "
                    f"rows={len(batch_rows)} total_rows={producer_state['rows']}"
                )
                logger.info(line)
                print(line, flush=True)
                time.sleep(2)

        def stop_when_sink_receives_data():
            deadline = time.time() + 180
            while time.time() < deadline:
                status = orchestrator.get_status()
                query_id = status.query_ids_by_pipe.get(pipe_id)
                if query_id:
                    controller_state["query_seen"] = True
                    try:
                        sink_rows = entity_provider.read_entity(sink_entity).count()
                    except Exception:
                        sink_rows = 0
                    controller_state["sink_rows"] = sink_rows
                    if sink_rows > 0:
                        query_info = query_manager.get_query_status(query_id)
                        if query_info.spark_query is not None:
                            try:
                                query_info.spark_query.processAllAvailable()
                            except Exception:
                                pass
                        query_manager.stop_query(query_id, await_termination=True)
                        controller_state["stop_passed"] = True
                        _emit(
                            logger,
                            test_id,
                            "controller_stop",
                            True,
                            f"sink_rows={sink_rows} query_id={query_id}",
                        )
                        return
                time.sleep(2)

            _emit(
                logger,
                test_id,
                "controller_stop",
                False,
                f"query_seen={controller_state['query_seen']} sink_rows={controller_state['sink_rows']}",
            )

        producer_thread = threading.Thread(target=produce_source_batches, daemon=True)
        controller_thread = threading.Thread(target=stop_when_sink_receives_data, daemon=True)
        producer_thread.start()
        controller_thread.start()

        run_started = time.time()
        result = orchestrator.run(
            plan,
            streaming_options={"base_checkpoint_path": runtime_paths["checkpoint_root"]},
            timeout=240,
            poll_interval=1.0,
        )
        run_duration = time.time() - run_started

        producer_thread.join(timeout=15)
        controller_thread.join(timeout=15)

        _emit(
            logger,
            test_id,
            "source_batches",
            producer_state["batches"] >= 1 and producer_state["rows"] >= 5,
            f"batches={producer_state['batches']} rows={producer_state['rows']}",
        )
        test_results["source_batches"] = (
            producer_state["batches"] >= 1 and producer_state["rows"] >= 5
        )

        sink_count = entity_provider.read_entity(sink_entity).count()
        _emit(
            logger,
            test_id,
            "orchestrator_run",
            result.all_succeeded and controller_state["stop_passed"] and sink_count > 0,
            (
                f"duration_s={run_duration:.2f} sink_rows={sink_count} "
                f"success={result.success_count} failed={result.failed_count}"
            ),
        )
        test_results["orchestrator_run"] = (
            result.all_succeeded and controller_state["stop_passed"] and sink_count > 0
        )

        required_signals = {
            "streaming.orchestrator_started",
            "streaming.orchestrator_completed",
            "streaming.query_started",
        }
        got_required_signals = required_signals.issubset(set(signal_hits))
        _emit(
            logger,
            test_id,
            "orchestrator_signals",
            got_required_signals,
            f"signals={sorted(set(signal_hits))}",
        )
        test_results["orchestrator_signals"] = got_required_signals

        # --- Streaming listener observability validation ---
        listener = get_kindling_service(KindlingStreamingListener)
        metrics = listener.get_metrics()

        # Listener should have processed events during the streaming lifecycle
        listener_processed = metrics["events_processed"] > 0
        # All query spans should be cleaned up after orchestrator shutdown
        orphaned_spans = len(listener._query_spans)
        _emit(
            logger,
            test_id,
            "listener_metrics",
            listener_processed and orphaned_spans == 0,
            (
                f"processed={metrics['events_processed']} "
                f"dropped={metrics['events_dropped']} "
                f"orphaned_spans={orphaned_spans}"
            ),
        )
        test_results["listener_metrics"] = listener_processed and orphaned_spans == 0

        all_passed = all(test_results.values())
        final_line = (
            f"TEST_ID={test_id} status=COMPLETED result={'PASSED' if all_passed else 'FAILED'}"
        )
        logger.info(final_line)
        print(final_line, flush=True)
        _flush_azure_monitor(logger)
        return 0 if all_passed else 1

    except Exception as exc:
        error_line = f"TEST_ID={test_id} status=FAILED error={type(exc).__name__}:{exc}"
        logger.error(error_line, include_traceback=True)
        print(error_line, flush=True)
        _flush_azure_monitor(logger)
        return 1


if __name__ == "__main__":
    sys.exit(main())
