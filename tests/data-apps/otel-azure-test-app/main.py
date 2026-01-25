#!/usr/bin/env python3
"""
Azure Monitor OpenTelemetry Extension Test App

Tests that the kindling-otel-azure extension correctly overrides
the default trace and log providers with Azure Monitor versions.
"""
import sys
import time
from datetime import datetime

from kindling.injection import get_kindling_service
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import *
from kindling.spark_trace import SparkTraceProvider


def get_logger():
    """Get logger from Kindling framework"""
    logger_provider = get_kindling_service(SparkLoggerProvider)
    return logger_provider.get_logger("otel-azure-test")


def test_provider_override(logger):
    """Test 1: Verify Azure Monitor providers are registered"""
    logger.info("TEST 1: Checking provider registration...")

    # Get the registered providers from DI container
    trace_provider = get_kindling_service(SparkTraceProvider)
    log_provider = get_kindling_service(SparkLoggerProvider)

    trace_class = f"{trace_provider.__class__.__module__}.{trace_provider.__class__.__name__}"
    log_class = f"{log_provider.__class__.__module__}.{log_provider.__class__.__name__}"

    # CRITICAL: Also print() so it appears in stdout/stderr for Fabric
    print(f"PROVIDER_CHECK: Trace Provider: {trace_class}")
    print(f"PROVIDER_CHECK: Log Provider: {log_class}")

    logger.info(f"Trace Provider: {trace_class}")
    logger.info(f"Log Provider: {log_class}")

    # Check if Azure Monitor providers are active
    is_azure_trace = "AzureMonitor" in trace_class
    is_azure_log = "AzureMonitor" in log_class

    if is_azure_trace and is_azure_log:
        print("✅ TEST 1 PASSED: Azure Monitor providers are active")
        logger.info("✅ TEST 1 PASSED: Azure Monitor providers are active")
        return True
    else:
        print(f"❌ TEST 1 FAILED: Expected Azure Monitor providers")
        print(f"   Trace is Azure: {is_azure_trace}, Log is Azure: {is_azure_log}")
        logger.error(f"❌ TEST 1 FAILED: Expected Azure Monitor providers")
        logger.error(f"   Trace is Azure: {is_azure_trace}")
        logger.error(f"   Log is Azure: {is_azure_log}")
        return False


def test_trace_generation(logger):
    """Test 2: Generate spans and verify tracer works"""
    logger.info("TEST 2: Testing trace generation...")

    try:
        trace_provider = get_kindling_service(SparkTraceProvider)
        tracer = trace_provider.get_tracer("otel-azure-test")

        # Generate a test span
        with tracer.start_as_current_span("test_operation") as span:
            span.set_attribute("test.attribute", "test_value")
            span.set_attribute("test.timestamp", datetime.utcnow().isoformat())

            # Nested span
            with tracer.start_as_current_span("nested_operation") as nested_span:
                nested_span.set_attribute("nested.data", "nested_value")
                time.sleep(0.1)  # Simulate work

            logger.info(f"Generated span: {span.get_span_context().span_id}")

        logger.info("✅ TEST 2 PASSED: Trace generation successful")
        return True

    except Exception as e:
        logger.error(f"❌ TEST 2 FAILED: {str(e)}")
        return False


def test_log_generation(logger):
    """Test 3: Generate logs at various levels"""
    logger.info("TEST 3: Testing log generation...")

    try:
        # Generate logs at different levels
        logger.debug("DEBUG level log message")
        logger.info("INFO level log message")
        logger.warning("WARNING level log message")
        logger.error("ERROR level log message")

        # Log with structured data (as formatted string since SparkLogger doesn't support extra)
        logger.info(
            f"Structured log: custom_field=custom_value, timestamp={datetime.utcnow().isoformat()}, test_id=otel-azure-test"
        )

        logger.info("✅ TEST 3 PASSED: Log generation successful")
        return True

    except Exception as e:
        logger.error(f"❌ TEST 3 FAILED: {str(e)}")
        return False


def test_trace_and_log_correlation(logger):
    """Test 4: Verify traces and logs are correlated"""
    logger.info("TEST 4: Testing trace/log correlation...")

    try:
        trace_provider = get_kindling_service(SparkTraceProvider)
        tracer = trace_provider.get_tracer("otel-azure-test")

        with tracer.start_as_current_span("correlated_operation") as span:
            span_context = span.get_span_context()
            trace_id = format(span_context.trace_id, "032x")
            span_id = format(span_context.span_id, "016x")

            # Log within span context
            logger.info(f"Log within span - trace_id: {trace_id}, span_id: {span_id}")
            logger.info("This log should be correlated with the trace")

        logger.info("✅ TEST 4 PASSED: Trace/log correlation test completed")
        return True

    except Exception as e:
        logger.error(f"❌ TEST 4 FAILED: {str(e)}")
        return False


# Main execution
logger = get_logger()

logger.info("=" * 60)
logger.info("Azure Monitor OpenTelemetry Extension Test")
logger.info("=" * 60)

results = {
    "provider_override": test_provider_override(logger),
    "trace_generation": test_trace_generation(logger),
    "log_generation": test_log_generation(logger),
    "trace_log_correlation": test_trace_and_log_correlation(logger),
}

# Summary
logger.info("=" * 60)
logger.info("Test Summary:")
for test_name, passed in results.items():
    status = "✅ PASSED" if passed else "❌ FAILED"
    logger.info(f"  {test_name}: {status}")

all_passed = all(results.values())
logger.info("=" * 60)
if all_passed:
    logger.info("🎉 ALL TESTS PASSED")
else:
    logger.error("💥 SOME TESTS FAILED")

# CRITICAL: Force flush spans before exit to ensure they're exported to Application Insights
logger.info("Flushing spans to Application Insights...")
try:
    from kindling_otel_azure.config import AzureMonitorConfig

    flush_success = AzureMonitorConfig.force_flush(timeout_millis=5000)  # 5 second timeout
    if flush_success:
        logger.info("✅ Spans flushed successfully")
    else:
        logger.warning("⚠️  Flush timed out - some spans may not be exported")
except Exception as e:
    logger.error(f"❌ Error flushing spans: {e}")

logger.info("Shutting down OpenTelemetry...")
try:
    AzureMonitorConfig.shutdown()
    logger.info("✅ OpenTelemetry shutdown complete")
except Exception as e:
    logger.error(f"❌ Error during shutdown: {e}")

sys.exit(0 if all_passed else 1)
