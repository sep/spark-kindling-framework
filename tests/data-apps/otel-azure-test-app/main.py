#!/usr/bin/env python3
"""
Azure Monitor OpenTelemetry Extension Test App

Tests that the kindling-otel-azure extension correctly overrides
the default trace and log providers with Azure Monitor versions.

CRITICAL: This test requires the Kindling bootstrap system to load the extension.
Without bootstrap, the extension will not be installed or loaded.
"""

import sys
import time
from datetime import datetime

# Print early to confirm app is starting
print("=" * 80)
print("OTEL-AZURE-TEST-APP STARTING")
print("=" * 80)

try:
    from kindling.injection import get_kindling_service
    from kindling.spark_log_provider import SparkLoggerProvider
    from kindling.spark_session import *
    from kindling.spark_trace import SparkTraceProvider

    print("✅ Kindling imports successful")
except Exception as e:
    print(f"❌ Failed to import Kindling: {e}")
    sys.exit(1)


def get_logger():
    """Get logger from Kindling framework"""
    try:
        print("Attempting to get logger from Kindling framework...")
        logger_provider = get_kindling_service(SparkLoggerProvider)
        logger = logger_provider.get_logger("otel-azure-test")
        print("✅ Logger obtained successfully")
        return logger
    except Exception as e:
        print(f"❌ Failed to get logger: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


def log_extension_version(logger):
    """Log the version of the kindling-otel-azure extension that was loaded"""
    try:
        import kindling_otel_azure

        version = kindling_otel_azure.__version__
        # Print to stdout (captured by Fabric/Databricks)
        print(f"EXTENSION_VERSION: kindling-otel-azure {version}")
        # Also log via framework
        logger.info(f"Extension loaded: kindling-otel-azure version {version}")
        return version
    except ImportError:
        print("⚠️  kindling-otel-azure not found - extension not loaded")
        logger.warning("kindling-otel-azure not found - extension not loaded")
        return None
    except AttributeError:
        print("⚠️  kindling-otel-azure version not available")
        logger.warning("kindling-otel-azure version not available")
        return None


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
    """Test 2: Generate spans and verify Kindling tracing API works."""
    logger.info("TEST 2: Testing trace generation...")

    try:
        trace_provider = get_kindling_service(SparkTraceProvider)

        with trace_provider.span(
            component="otel-azure-test",
            operation="test_operation",
            details={
                "test.attribute": "test_value",
                "test.timestamp": datetime.utcnow().isoformat(),
            },
            reraise=True,
        ):
            with trace_provider.span(
                component="otel-azure-test",
                operation="nested_operation",
                details={"nested.data": "nested_value"},
                reraise=True,
            ):
                time.sleep(0.1)  # Simulate work

            logger.info("Generated test_operation span with nested_operation child span")

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
        from opentelemetry import trace

        trace_provider = get_kindling_service(SparkTraceProvider)
        with trace_provider.span(
            component="otel-azure-test",
            operation="correlated_operation",
            details={},
            reraise=True,
        ):
            span_context = trace.get_current_span().get_span_context()
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
print("\n" + "=" * 80)
print("MAIN EXECUTION STARTING")
print("=" * 80)

try:
    logger = get_logger()
except Exception as e:
    print(f"❌ FATAL: Could not initialize logger: {e}")
    sys.exit(1)

logger.info("=" * 60)
logger.info("Azure Monitor OpenTelemetry Extension Test")
logger.info("=" * 60)

# Log extension version first
extension_version = log_extension_version(logger)
if extension_version:
    logger.info(f"✅ Extension version check passed: {extension_version}")
    print(f"✅ Extension package found: kindling-otel-azure {extension_version}")
else:
    logger.warning("⚠️  Extension version could not be determined")
    print("⚠️  Extension package NOT found - extension may not be installed")

# Check if extension is actually active (providers registered)
try:
    trace_provider = get_kindling_service(SparkTraceProvider)
    log_provider = get_kindling_service(SparkLoggerProvider)

    trace_class = f"{trace_provider.__class__.__module__}.{trace_provider.__class__.__name__}"
    log_class = f"{log_provider.__class__.__module__}.{log_provider.__class__.__name__}"

    print(f"CURRENT_PROVIDERS: Trace={trace_class}, Log={log_class}")
    logger.info(f"Current Providers: Trace={trace_class}, Log={log_class}")
except Exception as e:
    print(f"❌ Failed to get providers: {e}")
    logger.error(f"Failed to get providers: {e}")

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
