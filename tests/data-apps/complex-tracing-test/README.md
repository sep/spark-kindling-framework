# Complex Tracing Test App

Test app that generates complex span hierarchies with variable durations.

## Purpose

Tests telemetry systems (default Kindling or Azure Monitor extension) with realistic workload patterns:
- Nested span hierarchies (4-5 levels deep)
- Variable execution times (simulating real work)
- Multiple sequential operations (simulating parallel work)
- Error handling and retry patterns
- Rich span attributes and metadata

## What It Tests

### Pipeline Stages
1. **Data Validation** - Schema and quality checks
2. **Data Transformation** - Batch processing with variable performance
3. **Data Enrichment** - Multiple external lookups
4. **Data Aggregation** - Grouping and calculations
5. **Error Handling** - Failure simulation and recovery

### Span Characteristics
- **Variable durations**: 5ms to 100ms per operation
- **Deep nesting**: Up to 5 levels (test → pipeline → stage → batch → operation)
- **Rich attributes**: Record counts, batch numbers, status codes, error details
- **Realistic patterns**: Slow batches, cache hit rates, retry logic

## Usage

### With Default Kindling Telemetry
No extension required - uses built-in SparkTraceProvider.

### With Azure Monitor Extension
Add to settings.yaml:
```yaml
extensions:
  - kindling-otel-azure>=0.1.0

extension_config:
  azure_monitor:
    connection_string: "InstrumentationKey=..."
```

## Expected Telemetry

**Per test run:**
- 1 top-level test span
- 3 pipeline spans (configurable)
- ~50-100 operation spans total
- Variable total duration: 2-5 seconds

**Span hierarchy example:**
```
tracing_system_test (2-5s)
└── data_pipeline (600-1500ms)
    ├── validate_data (50-200ms)
    ├── transform_data (100-500ms)
    │   ├── transform_batch_0 (10-30ms)
    │   ├── transform_batch_1 (10-30ms)
    │   └── ... (more batches)
    ├── enrich_data (90-180ms)
    │   ├── lookup_customer_db (30-60ms)
    │   ├── lookup_product_catalog (20-40ms)
    │   └── lookup_geo_location (40-80ms)
    ├── aggregate_data (90-200ms)
    │   ├── group_by_operation (40-80ms)
    │   ├── calculate_sum (10-30ms)
    │   ├── calculate_avg (10-30ms)
    │   └── ... (more calculations)
    └── error_prone_operation (20-100ms)
        └── retry_operation (30-60ms, conditional)
```

## Verification

### Query Application Insights
```kql
dependencies
| where timestamp > ago(1h)
| where name in ("data_pipeline", "validate_data", "transform_data", "enrich_data")
| summarize count() by name, bin(duration, 100)
| order by name, duration asc
```

### Check for specific patterns
```kql
dependencies
| where timestamp > ago(1h)
| where name startswith "transform_batch_"
| extend batchNum = toint(extract("transform_batch_(\\d+)", 1, name))
| summarize avg(duration), max(duration) by batchNum
| order by batchNum asc
```
