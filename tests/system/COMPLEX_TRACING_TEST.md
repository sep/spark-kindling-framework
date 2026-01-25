# Complex Tracing System Test

## Overview

A comprehensive test app that generates realistic, complex span hierarchies with variable durations to validate telemetry systems. Works with **both** default Kindling telemetry and Azure Monitor OpenTelemetry extension.

## Files Created

```
tests/
├── data-apps/
│   └── complex-tracing-test/
│       ├── main.py                    # Test app with complex span patterns
│       ├── settings.yaml              # Default config (no extension)
│       ├── settings-azure.yaml        # Azure Monitor config
│       └── README.md                  # App documentation
├── system/
│   ├── test_complex_tracing.py        # System tests for both modes
│   └── queries/
│       └── complex_tracing_analysis.kql  # Application Insights queries
```

## What It Tests

### Span Hierarchy (5 levels deep)
```
tracing_system_test (2-5s)
└── data_pipeline (600-1500ms)
    ├── validate_data (50-200ms)
    ├── transform_data (100-500ms)
    │   ├── transform_batch_0 (10-30ms)
    │   ├── transform_batch_1 (10-30ms)
    │   └── ... (5-50 batches)
    ├── enrich_data (90-180ms)
    │   ├── lookup_customer_db (30-60ms)
    │   ├── lookup_product_catalog (20-40ms)
    │   └── lookup_geo_location (40-80ms)
    ├── aggregate_data (90-200ms)
    │   ├── group_by_operation (40-80ms)
    │   └── calculate_{sum,avg,count,min,max} (10-30ms each)
    └── error_prone_operation (20-100ms)
        └── retry_operation (30-60ms, conditional)
```

### Realistic Patterns
- **Variable durations**: 5-100ms per operation
- **Slow batches**: 10% chance of 50-100ms delay
- **Error handling**: 30% failure rate with retry
- **Different dataset sizes**: 5K, 10K, 25K, 50K records
- **Rich attributes**: Record counts, cache hit rates, batch numbers, statuses

### Telemetry Volume
- **Per test run**: ~150-300 spans across 3 pipelines
- **Per pipeline**: 50-100 spans depending on batch count
- **Total duration**: 2-5 seconds

## Running Tests

### Test with Default Telemetry
```bash
cd /workspace
poetry run pytest -v tests/system/test_complex_tracing.py::TestComplexTracing::test_default_telemetry
```

### Test with Azure Monitor Extension
```bash
# Set environment variable
export APPLICATIONINSIGHTS_CONNECTION_STRING="InstrumentationKey=..."

# Run test
poetry run pytest -v tests/system/test_complex_tracing.py::TestComplexTracing::test_azure_monitor_telemetry
```

### Run Both Tests
```bash
poetry run pytest -v tests/system/test_complex_tracing.py
```

## Verification in Application Insights

Use queries from `tests/system/queries/complex_tracing_analysis.kql`:

```kql
// Quick verification - should show 3 pipelines
dependencies
| where timestamp > ago(1h)
| where name == "data_pipeline"
| project timestamp, operation_Id, duration, customDimensions
| order by timestamp desc
```

Expected results:
- **3 pipeline runs** (one per test execution)
- **Variable durations** based on random dataset sizes
- **50-100 child spans per pipeline**
- **Proper parent-child relationships** via operation_Id/operation_ParentId

## Key Features

✅ **Platform agnostic** - Works with default telemetry or Azure Monitor
✅ **Realistic patterns** - Simulates actual data processing workloads
✅ **Rich metadata** - Extensive span attributes for analysis
✅ **Error scenarios** - Tests error handling and retry logic
✅ **Performance variation** - Random delays simulate real-world conditions
✅ **Scalable** - Configurable dataset sizes and pipeline count

## Architecture

The test demonstrates proper use of Kindling's tracing API:
- Uses `SparkTraceProvider` from DI container
- Works with any provider implementation (default or extension)
- Properly handles span context propagation
- Includes flush/shutdown for Azure Monitor
- Logs are written via SparkLoggerProvider for consistency

This validates that the telemetry abstraction works correctly regardless of the underlying implementation.
