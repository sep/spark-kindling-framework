#!/usr/bin/env python3
"""
Complex Tracing System Test

Generates complex span hierarchies with variable durations to test telemetry systems.
Works with both default Kindling telemetry and Azure Monitor OpenTelemetry extension.

This test simulates realistic data processing patterns with:
- Nested operations of varying complexity
- Variable execution times
- Parallel-style operations (sequential but simulating parallel work)
- Error handling and recovery
- Span attributes and annotations
"""
import random
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
    return logger_provider.get_logger("complex-tracing-test")


def simulate_data_validation(tracer, logger, record_count):
    """Simulate data validation with variable duration"""
    with tracer.start_as_current_span("validate_data") as span:
        span.set_attribute("record_count", record_count)
        span.set_attribute("validation.type", "schema_and_quality")

        logger.info(f"Validating {record_count} records...")

        # Simulate validation work (10-50ms per 1000 records)
        duration_ms = random.randint(10, 50) * (record_count // 1000)
        time.sleep(duration_ms / 1000.0)

        # Simulate finding some issues
        invalid_count = random.randint(0, record_count // 100)
        span.set_attribute("validation.invalid_records", invalid_count)
        span.set_attribute("validation.success_rate", (record_count - invalid_count) / record_count)

        logger.info(f"Validation complete: {invalid_count} invalid records found")
        return invalid_count


def simulate_data_transformation(tracer, logger, record_count, batch_size=1000):
    """Simulate data transformation in batches"""
    with tracer.start_as_current_span("transform_data") as span:
        span.set_attribute("record_count", record_count)
        span.set_attribute("batch_size", batch_size)

        num_batches = (record_count + batch_size - 1) // batch_size
        span.set_attribute("batch_count", num_batches)

        logger.info(f"Transforming {record_count} records in {num_batches} batches...")

        for batch_num in range(num_batches):
            with tracer.start_as_current_span(f"transform_batch_{batch_num}") as batch_span:
                current_batch_size = min(batch_size, record_count - (batch_num * batch_size))
                batch_span.set_attribute("batch.number", batch_num)
                batch_span.set_attribute("batch.record_count", current_batch_size)

                # Simulate transformation work (5-20ms per batch)
                duration_ms = random.randint(5, 20)
                time.sleep(duration_ms / 1000.0)

                # Occasionally simulate a slow batch
                if random.random() < 0.1:  # 10% chance
                    logger.warning(f"Batch {batch_num} is slow, applying optimization...")
                    batch_span.set_attribute("batch.slow", True)
                    time.sleep(random.randint(50, 100) / 1000.0)

                batch_span.set_attribute("batch.status", "completed")

        logger.info(f"Transformation complete: {num_batches} batches processed")


def simulate_data_enrichment(tracer, logger, record_count):
    """Simulate data enrichment with external lookups"""
    with tracer.start_as_current_span("enrich_data") as span:
        span.set_attribute("record_count", record_count)

        logger.info(f"Enriching {record_count} records...")

        # Simulate multiple enrichment sources
        enrichment_sources = ["customer_db", "product_catalog", "geo_location"]

        for source in enrichment_sources:
            with tracer.start_as_current_span(f"lookup_{source}") as lookup_span:
                lookup_span.set_attribute("source.name", source)
                lookup_span.set_attribute("source.record_count", record_count)

                # Simulate lookup time (varies by source)
                if source == "customer_db":
                    time.sleep(random.randint(30, 60) / 1000.0)
                    lookup_span.set_attribute("source.cache_hit_rate", 0.85)
                elif source == "product_catalog":
                    time.sleep(random.randint(20, 40) / 1000.0)
                    lookup_span.set_attribute("source.cache_hit_rate", 0.92)
                else:  # geo_location
                    time.sleep(random.randint(40, 80) / 1000.0)
                    lookup_span.set_attribute("source.cache_hit_rate", 0.78)

                lookup_span.set_attribute("source.status", "completed")
                logger.info(f"Enriched from {source}")

        span.set_attribute("enrichment.sources_used", len(enrichment_sources))
        logger.info("Enrichment complete")


def simulate_data_aggregation(tracer, logger, record_count):
    """Simulate data aggregation with grouping and calculations"""
    with tracer.start_as_current_span("aggregate_data") as span:
        span.set_attribute("record_count", record_count)

        logger.info(f"Aggregating {record_count} records...")

        # Simulate grouping
        with tracer.start_as_current_span("group_by_operation") as group_span:
            group_span.set_attribute("group_by.columns", "customer_id,product_category,date")
            time.sleep(random.randint(40, 80) / 1000.0)

            num_groups = record_count // random.randint(5, 20)
            group_span.set_attribute("group_by.result_count", num_groups)
            logger.info(f"Grouped into {num_groups} groups")

        # Simulate calculations
        calculations = ["sum", "avg", "count", "min", "max"]
        for calc in calculations:
            with tracer.start_as_current_span(f"calculate_{calc}") as calc_span:
                calc_span.set_attribute("calculation.type", calc)
                time.sleep(random.randint(10, 30) / 1000.0)
                calc_span.set_attribute("calculation.status", "completed")

        span.set_attribute("aggregation.calculations_performed", len(calculations))
        logger.info("Aggregation complete")


def simulate_error_handling(tracer, logger):
    """Simulate error handling within spans"""
    with tracer.start_as_current_span("error_prone_operation") as span:
        span.set_attribute("operation.type", "network_request")

        try:
            logger.info("Attempting potentially failing operation...")
            time.sleep(random.randint(20, 40) / 1000.0)

            # Simulate occasional failure
            if random.random() < 0.3:  # 30% chance of failure
                raise Exception("Simulated network timeout")

            span.set_attribute("operation.status", "success")
            logger.info("Operation succeeded")
            return True

        except Exception as e:
            span.set_attribute("operation.status", "failed")
            span.set_attribute("error.type", type(e).__name__)
            span.set_attribute("error.message", str(e))
            logger.error(f"Operation failed: {e}")

            # Simulate retry
            with tracer.start_as_current_span("retry_operation") as retry_span:
                logger.info("Retrying operation...")
                time.sleep(random.randint(30, 60) / 1000.0)
                retry_span.set_attribute("retry.attempt", 1)
                retry_span.set_attribute("retry.status", "success")
                logger.info("Retry succeeded")

            return False


def run_complex_pipeline(tracer, logger, pipeline_id):
    """Run a complete data processing pipeline"""
    with tracer.start_as_current_span("data_pipeline") as pipeline_span:
        pipeline_span.set_attribute("pipeline.id", pipeline_id)
        pipeline_span.set_attribute("pipeline.start_time", datetime.utcnow().isoformat())

        # Simulate different dataset sizes
        record_count = random.choice([5000, 10000, 25000, 50000])
        pipeline_span.set_attribute("pipeline.record_count", record_count)

        logger.info(f"=" * 60)
        logger.info(f"Starting pipeline {pipeline_id} with {record_count} records")
        logger.info(f"=" * 60)

        # Stage 1: Validation
        invalid_count = simulate_data_validation(tracer, logger, record_count)
        pipeline_span.set_attribute("stage.validation.invalid_records", invalid_count)

        # Stage 2: Transformation
        simulate_data_transformation(tracer, logger, record_count)

        # Stage 3: Enrichment
        simulate_data_enrichment(tracer, logger, record_count)

        # Stage 4: Aggregation
        simulate_data_aggregation(tracer, logger, record_count)

        # Stage 5: Error handling (demonstrates recovery)
        error_handled = simulate_error_handling(tracer, logger)
        pipeline_span.set_attribute("stage.error_handling.recovery", error_handled)

        pipeline_span.set_attribute("pipeline.end_time", datetime.utcnow().isoformat())
        pipeline_span.set_attribute("pipeline.status", "completed")

        logger.info(f"Pipeline {pipeline_id} completed successfully")
        return True


# Main execution
logger = get_logger()
trace_provider = get_kindling_service(SparkTraceProvider)
tracer = trace_provider.get_tracer("complex-tracing-test")

logger.info("=" * 60)
logger.info("Complex Tracing System Test")
logger.info("=" * 60)

# Log provider information
trace_class = f"{trace_provider.__class__.__module__}.{trace_provider.__class__.__name__}"
logger.info(f"Trace Provider: {trace_class}")

# Create top-level span for entire test
with tracer.start_as_current_span("tracing_system_test") as test_span:
    test_span.set_attribute("test.type", "complex_tracing")
    test_span.set_attribute("test.start_time", datetime.utcnow().isoformat())

    # Run multiple pipelines to create rich telemetry
    num_pipelines = 3
    test_span.set_attribute("test.pipeline_count", num_pipelines)

    pipeline_results = []
    for i in range(num_pipelines):
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Running Pipeline {i + 1} of {num_pipelines}")
        logger.info(f"{'=' * 60}\n")

        result = run_complex_pipeline(tracer, logger, f"pipeline-{i + 1}")
        pipeline_results.append(result)

        # Small delay between pipelines
        if i < num_pipelines - 1:
            time.sleep(0.1)

    test_span.set_attribute("test.end_time", datetime.utcnow().isoformat())
    test_span.set_attribute("test.pipelines_completed", sum(pipeline_results))
    test_span.set_attribute("test.status", "completed")

# Summary
logger.info("=" * 60)
logger.info("Test Summary:")
logger.info(f"  Pipelines run: {num_pipelines}")
logger.info(f"  Pipelines completed: {sum(pipeline_results)}")
logger.info(f"  Trace provider: {trace_class}")
logger.info("=" * 60)

# Flush if Azure Monitor extension is loaded
try:
    from kindling_otel_azure.config import AzureMonitorConfig

    logger.info("Azure Monitor extension detected - flushing spans...")
    flush_success = AzureMonitorConfig.force_flush(timeout_millis=10000)
    if flush_success:
        logger.info("✅ Spans flushed successfully to Azure Monitor")
    else:
        logger.warning("⚠️  Flush timed out - some spans may not be exported")

    AzureMonitorConfig.shutdown()
    logger.info("✅ Azure Monitor shutdown complete")
except ImportError:
    logger.info("Using default Kindling telemetry (no Azure Monitor extension)")

logger.info("🎉 Complex tracing test completed successfully!")
sys.exit(0)
