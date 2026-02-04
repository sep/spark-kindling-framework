#!/usr/bin/env python3
"""
Streaming System Test App

Tests the complete streaming stack:
- KindlingStreamingListener (event capture)
- StreamingQueryManager (lifecycle management)
- StreamingHealthMonitor (health detection)
- StreamingRecoveryManager (auto-recovery)

This app is deployed and run on Fabric, Databricks, or Synapse.
"""

import sys
import time

from kindling.injection import get_kindling_service
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from kindling.streaming_health_monitor import StreamingHealthMonitor
from kindling.streaming_listener import KindlingStreamingListener
from kindling.streaming_query_manager import StreamingQueryManager
from kindling.streaming_recovery_manager import StreamingRecoveryManager
from pyspark.sql.functions import col


def get_test_id():
    """Extract test ID from config"""
    try:
        from kindling.spark_config import ConfigService

        config_service = get_kindling_service(ConfigService)
        test_id = config_service.get("test_id")
        if test_id:
            return str(test_id)
    except Exception as e:
        print(f"ERROR getting test_id: {e}")
    return "unknown"


def get_logger():
    """Get logger from Kindling framework via DI"""
    logger_provider = get_kindling_service(SparkLoggerProvider)
    return logger_provider.get_logger("streaming-test-app")


def create_test_stream(spark, rate=5):
    """Create a test streaming DataFrame using rate source"""
    return spark.readStream.format("rate").option("rowsPerSecond", rate).load()


def create_memory_sink_query(spark, stream_df, query_name, checkpoint_location):
    """Create a streaming query that writes to memory sink"""
    return (
        stream_df.writeStream.format("memory")
        .queryName(query_name)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )


# Initialize
logger = get_logger()
test_id = get_test_id()

# Log test start
msg = f"TEST_ID={test_id} status=STARTED component=streaming_system"
logger.info(msg)
print(msg)

test_results = {}

try:
    # Get Spark session
    spark = get_or_create_spark_session()

    msg = f"TEST_ID={test_id} test=spark_session status=PASSED"
    logger.info(msg)
    print(msg)
    test_results["spark_session"] = True

    # Get streaming components via DI (they handle their own dependency injection)
    from kindling.signaling import SignalProvider

    # Only get SignalProvider for signal subscription (top-level app usage)
    signal_provider = get_kindling_service(SignalProvider)

    msg = f"TEST_ID={test_id} test=di_services status=PASSED"
    logger.info(msg)
    print(msg)
    test_results["di_services"] = True

    # Get streaming components via DI (singletons ensure same instances throughout)
    streaming_listener = get_kindling_service(KindlingStreamingListener)
    streaming_listener.start()

    # Register listener with Spark
    spark.streams.addListener(streaming_listener)

    msg = f"TEST_ID={test_id} test=streaming_listener status=STARTED"
    logger.info(msg)
    print(msg)

    # Get query manager via DI (singleton)
    query_manager = get_kindling_service(StreamingQueryManager)

    msg = f"TEST_ID={test_id} test=query_manager status=CREATED"
    logger.info(msg)
    print(msg)

    # Get health monitor via DI (singleton)
    health_monitor = get_kindling_service(StreamingHealthMonitor)
    health_monitor.start()

    msg = f"TEST_ID={test_id} test=health_monitor status=STARTED"
    logger.info(msg)
    print(msg)

    # Get recovery manager via DI (singleton)
    recovery_manager = get_kindling_service(StreamingRecoveryManager)
    recovery_manager.start()

    msg = f"TEST_ID={test_id} test=recovery_manager status=STARTED"
    logger.info(msg)
    print(msg)
    test_results["components_started"] = True

    # Track signals
    emitted_signals = []

    def track_signal(signal_name):
        def handler(sender, **kwargs):
            emitted_signals.append(signal_name)
            print(f"TEST_ID={test_id} signal={signal_name} received=true")

        return handler

    # Subscribe to lifecycle signals
    # IMPORTANT: Use weak=False to prevent garbage collection of handler closures
    for signal_name in [
        "streaming.query_started",
        "streaming.spark_query_started",
        "streaming.spark_query_progress",
    ]:
        signal = signal_provider.create_signal(signal_name)
        signal.connect(track_signal(signal_name), weak=False)

    # Register and start a streaming query
    # Use lakehouse path for checkpoint (works on all platforms)
    import time

    checkpoint_suffix = int(time.time())
    checkpoint_dir = f"Files/checkpoints/streaming_test_{test_id}_{checkpoint_suffix}"

    def builder(spark, config):
        df = create_test_stream(spark, rate=10)
        return create_memory_sink_query(spark, df, "test_streaming", checkpoint_dir)

    query_manager.register_query("test_streaming", builder, {})

    msg = f"TEST_ID={test_id} test=query_registration status=PASSED"
    logger.info(msg)
    print(msg)
    test_results["query_registration"] = True

    # Start the query
    query_info = query_manager.start_query("test_streaming")
    assert query_info.is_active, "Query should be active after start"

    msg = f"TEST_ID={test_id} test=query_start status=PASSED query_id={query_info.spark_query_id}"
    logger.info(msg)
    print(msg)
    test_results["query_start"] = True

    # Wait for query to process batches and emit signals
    print(f"TEST_ID={test_id} waiting_for_batches=true duration=5s")
    time.sleep(5)

    # Verify signals were emitted
    if "streaming.query_started" in emitted_signals:
        msg = f"TEST_ID={test_id} test=signal_query_started status=PASSED"
        logger.info(msg)
        print(msg)
        test_results["signal_query_started"] = True
    else:
        msg = f"TEST_ID={test_id} test=signal_query_started status=FAILED"
        logger.error(msg)
        print(msg)
        test_results["signal_query_started"] = False

    if "streaming.spark_query_started" in emitted_signals:
        msg = f"TEST_ID={test_id} test=signal_spark_query_started status=PASSED"
        logger.info(msg)
        print(msg)
        test_results["signal_spark_query_started"] = True
    else:
        msg = f"TEST_ID={test_id} test=signal_spark_query_started status=FAILED"
        logger.error(msg)
        print(msg)
        test_results["signal_spark_query_started"] = False

    # Check listener metrics
    listener_metrics = streaming_listener.get_metrics()
    events_processed = listener_metrics["events_processed"]

    if events_processed > 0:
        msg = f"TEST_ID={test_id} test=listener_events status=PASSED count={events_processed}"
        logger.info(msg)
        print(msg)
        test_results["listener_events"] = True
    else:
        msg = f"TEST_ID={test_id} test=listener_events status=FAILED count=0"
        logger.error(msg)
        print(msg)
        test_results["listener_events"] = False

    # Check health monitor
    health_state = health_monitor.get_health_status(query_info.spark_query_id)
    if health_state is not None:
        msg = f"TEST_ID={test_id} test=health_monitoring status=PASSED"
        logger.info(msg)
        print(msg)
        test_results["health_monitoring"] = True
    else:
        msg = f"TEST_ID={test_id} test=health_monitoring status=FAILED"
        logger.error(msg)
        print(msg)
        test_results["health_monitoring"] = False

    # Stop the query
    query_manager.stop_query("test_streaming", await_termination=False)
    time.sleep(1)

    msg = f"TEST_ID={test_id} test=query_stop status=PASSED"
    logger.info(msg)
    print(msg)
    test_results["query_stop"] = True

    # Stop components
    streaming_listener.stop()
    health_monitor.stop()
    recovery_manager.stop()

    msg = f"TEST_ID={test_id} test=components_cleanup status=PASSED"
    logger.info(msg)
    print(msg)
    test_results["components_cleanup"] = True

except Exception as e:
    msg = f"TEST_ID={test_id} status=FAILED error={str(e)}"
    logger.error(msg)
    print(msg)
    import traceback

    traceback.print_exc()
    test_results["exception"] = False

# Overall result
overall = all(test_results.values())
overall_status = "PASSED" if overall else "FAILED"
msg = f"TEST_ID={test_id} status=COMPLETED result={overall_status}"
logger.info(msg)
print(msg)

# Exit with appropriate code
sys.exit(0 if overall else 1)
