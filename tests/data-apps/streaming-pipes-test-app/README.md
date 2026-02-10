# Streaming Pipes Test App

System test application that validates streaming with `SimpleReadPersistStrategy` orchestrator.

## Purpose

Tests the complete streaming data pipeline pattern using Kindling framework components:

- **Entity definitions** with tag-based configuration
- **Pipe definitions** with streaming transformations
- **SimplePipeStreamOrchestrator** for streaming query management
- **SimpleReadPersistStrategy** for persist operations
- **Signal emissions** throughout the pipeline
- **Multi-layer data flow** (bronze → silver → gold)

## Architecture

```
Rate Source (10 rows/sec)
    ↓
Bronze Layer (raw events)
    ↓ [bronze_to_silver pipe]
Silver Layer (processed events with metadata)
    ↓ [silver_to_gold pipe]
Gold Layer (aggregated metrics by time window)
```

### Entities

1. **bronze_events**: Raw streaming events from rate source
   - Columns: event_id, timestamp, value, date
   - Provider: Delta (forPath mode)
   - Partitioned by: date

2. **silver_events**: Processed events with processing metadata
   - Columns: event_id, timestamp, event_value, processed_at, date
   - Provider: Delta (forPath mode)
   - Partitioned by: date

3. **gold_metrics**: Aggregated metrics by 1-minute time window
   - Columns: window_start, window_end, event_count, total_value, date
   - Provider: Delta (forPath mode)
   - Partitioned by: date

### Pipes

1. **bronze_to_silver**: Add processing metadata
   - Input: bronze_events
   - Output: silver_events
   - Transformation: Add processed_at timestamp, cast event_id

2. **silver_to_gold**: Aggregate by time window
   - Input: silver_events
   - Output: gold_metrics
   - Transformation: 1-minute windowing with watermark, count and sum aggregations

## What's Tested

### Component Integration
- ✅ Entity definitions registered correctly
- ✅ Pipe definitions registered correctly
- ✅ DI services resolved successfully
- ✅ SimplePipeStreamOrchestrator starts streaming queries
- ✅ SimpleReadPersistStrategy handles persist operations

### Data Flow
- ✅ Bronze layer receives data from source
- ✅ Silver layer processes bronze data
- ✅ Gold layer aggregates silver data
- ✅ Data counts validated at each layer

### Signal Emissions
- ✅ `persist.before_persist` emitted before writes
- ✅ `persist.after_persist` emitted after successful writes
- ✅ `persist.watermark_saved` emitted after watermark updates
- ✅ `streaming.query_started` emitted when queries start
- ✅ `streaming.query_stopped` emitted when queries stop

### Platform Compatibility
- ✅ Works on Fabric (lakehouse paths)
- ✅ Works on Databricks (DBFS paths)
- ✅ Works on Synapse (workspace storage paths)

## Running Tests

### Run on specific platform:
```bash
# Fabric
poe test-system --platform fabric --test streaming_pipes

# Databricks
poe test-system --platform databricks --test streaming_pipes

# Synapse
poe test-system --platform synapse --test streaming_pipes
```

### Run on all platforms:
```bash
poe test-system --test streaming_pipes
```

## Expected Output

The app produces structured log output with TEST_ID markers for validation:

```
TEST_ID=abc123 status=STARTED component=streaming_pipes_orchestrator
TEST_ID=abc123 test=spark_session status=PASSED
TEST_ID=abc123 test=entity_definitions status=PASSED
TEST_ID=abc123 test=pipe_definitions status=PASSED
TEST_ID=abc123 test=di_services status=PASSED
TEST_ID=abc123 test=bronze_source_stream status=STARTED
TEST_ID=abc123 test=bronze_silver_pipe status=STARTED
TEST_ID=abc123 test=silver_gold_pipe status=STARTED
TEST_ID=abc123 test=bronze_data status=PASSED count=50
TEST_ID=abc123 test=silver_data status=PASSED count=50
TEST_ID=abc123 test=gold_data status=PASSED count=5
TEST_ID=abc123 signal=persist.before_persist pipe_id=bronze_to_silver
TEST_ID=abc123 signal=persist.after_persist pipe_id=bronze_to_silver
TEST_ID=abc123 test=queries_stopped status=PASSED
TEST_ID=abc123 status=COMPLETED result=PASSED
```

## Configuration

The app uses `settings.yaml` for framework configuration:

```yaml
kindling:
  version: "0.4.2"
  delta:
    tablerefmode: "forPath"
  telemetry:
    logging:
      level: INFO
```

Platform-specific settings (checkpoint paths, table paths) are determined dynamically in code based on the platform.

## Notes

- **Streaming duration**: Runs for ~15 seconds total (5s bronze + 10s pipeline processing)
- **Rate source**: 10 rows/second = ~50 events in bronze, ~50 in silver
- **Gold aggregation**: ~5 time windows (1-minute windows with some delay)
- **Cleanup**: All streaming queries are stopped before exit
- **Error handling**: Exceptions are caught, logged, and result in non-zero exit code

## Related Tests

- `tests/system/core/test_streaming_system.py` - Tests streaming query manager components
- `tests/system/core/test_platform_job_deployment.py` - Tests basic job deployment
- `tests/integration/test_entity_config_overrides_simple.py` - Tests entity configuration
