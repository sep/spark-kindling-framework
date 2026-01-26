# Signal Quick Reference - Kindling Framework

**Quick lookup for all signal opportunities across Kindling providers**

---

## Signal Naming Convention

**Django-style prefix pattern** (established in ConfigService):
- `pre_{operation}` - Before operation starts
- `post_{operation}` - After successful completion
- `{operation}_failed` - When operation fails
- `{state}_changed` - When state transitions

---

## Complete Signal Catalog

### 🔥 CRITICAL - DataPipesExecuter

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `datapipes.pre_run` | Before run_datapipes() | pipe_ids, pipe_count, run_id |
| `datapipes.post_run` | After successful run | success_count, failed_count, duration_seconds |
| `datapipes.run_failed` | When run fails | failed_pipe, error, error_type |
| `datapipes.pre_pipe` | Before individual pipe | pipe_id, pipe_name, pipe_index |
| `datapipes.post_pipe` | After pipe completes | pipe_id, duration_seconds, rows_processed |
| `datapipes.pipe_failed` | When pipe fails | pipe_id, error, error_type |
| `datapipes.pipe_skipped` | When pipe skipped | pipe_id, skip_reason |

**Primary Use Cases**: Pipeline orchestration, error monitoring, performance tracking

---

### 🔥 CRITICAL - DataEntityManager

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `entity.pre_ensure_table` | Before table creation | entity_id, entity_name |
| `entity.post_ensure_table` | After table created | entity_id, table_exists |
| `entity.ensure_failed` | Table creation fails | entity_id, error |
| `entity.pre_merge` | Before merge operation | entity_id, merge_keys, source_row_count |
| `entity.post_merge` | After merge completes | rows_inserted, rows_updated, rows_deleted |
| `entity.merge_failed` | Merge fails | entity_id, error |
| `entity.pre_append` | Before append | entity_id, row_count |
| `entity.post_append` | After append | entity_id, rows_appended |
| `entity.append_failed` | Append fails | entity_id, error |
| `entity.pre_write` | Before write | entity_id, mode |
| `entity.post_write` | After write | entity_id, rows_written |
| `entity.write_failed` | Write fails | entity_id, error |
| `entity.schema_changed` | Schema evolution | added_columns, removed_columns, modified_columns |
| `entity.version_updated` | Version increment | old_version, new_version, reason |

**Primary Use Cases**: Data governance, audit trails, schema evolution tracking

---

### 🔥 CRITICAL - FileIngestionProcessor

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `file_ingestion.pre_process` | Before batch processing | path, pattern, file_count, batch_id |
| `file_ingestion.post_process` | After batch completes | success_files, failed_files, total_rows |
| `file_ingestion.process_failed` | Batch fails | error, failed_file, processed_files |
| `file_ingestion.pre_file` | Before individual file | filename, entry_id, file_size_bytes |
| `file_ingestion.post_file` | After file processed | filename, rows_processed, duration_seconds |
| `file_ingestion.file_failed` | File fails | filename, error |
| `file_ingestion.file_moved` | File moved | source_path, dest_path |
| `file_ingestion.batch_built` | DF batch plan built | entry_id, file_count |
| `file_ingestion.batch_written` | Batch written | entry_id, rows_written |

**Primary Use Cases**: File monitoring, error recovery, performance tracking

---

### ⚡ HIGH - WatermarkManager

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `watermark.pre_get` | Before get_watermark() | entity_id |
| `watermark.watermark_found` | Watermark retrieved | watermark_value, version, age_hours |
| `watermark.watermark_missing` | No watermark (first run) | entity_id |
| `watermark.pre_save` | Before save_watermark() | entity_id, new_watermark |
| `watermark.post_save` | After save | old_watermark, new_watermark, watermark_delta |
| `watermark.save_failed` | Save fails | entity_id, error |
| `watermark.watermark_advanced` | Watermark moves forward | advancement_seconds |
| `watermark.watermark_reset` | Watermark reset | old_watermark, reason |
| `watermark.version_changed` | Version changes | old_version, new_version |

**Primary Use Cases**: CDC monitoring, incremental load tracking, stale data detection

---

### ⚡ HIGH - DataAppManager

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `app.pre_package` | Before .kda creation | app_name, version, target_platform |
| `app.post_package` | After packaging | kda_file, kda_size_bytes |
| `app.package_failed` | Packaging fails | app_name, error |
| `app.pre_deploy` | Before deployment | app_name, kda_file, target_environment |
| `app.post_deploy` | After deployment | deploy_path, file_count |
| `app.deploy_failed` | Deployment fails | app_name, error |
| `app.pre_run` | Before app execution | app_name |
| `app.post_run` | After app completes | app_name, duration_seconds |
| `app.run_failed` | App fails | app_name, error |
| `app.installed` | App installed | app_name, version |
| `app.uninstalled` | App removed | app_name |

**Primary Use Cases**: Deployment monitoring, lifecycle tracking, audit trails

---

### ⚡ HIGH - PipManager

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `pip.pre_install` | Before install | packages, package_count, operation_id |
| `pip.post_install` | After install | success_count, cached_packages, downloaded_packages |
| `pip.install_failed` | Install fails | failed_package, error |
| `pip.pre_wheel` | Before wheel operation | operation |
| `pip.post_wheel` | After wheel | wheel_file |
| `pip.wheel_failed` | Wheel fails | error |
| `pip.package_installing` | Before specific package | package |
| `pip.package_installed` | After package installed | package, cached, dependencies_installed |
| `pip.package_failed` | Package fails | package, error |

**Primary Use Cases**: Dependency monitoring, performance tracking, security audit

---

### ⚡ HIGH - NotebookLoader

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `notebooks.packages_discovered` | After package scan | packages, package_count |
| `notebooks.pre_build` | Before wheel build | package_name, notebook_count |
| `notebooks.post_build` | After build | wheel_file, wheel_size_bytes |
| `notebooks.build_failed` | Build fails | package_name, error |
| `notebooks.notebook_loaded` | Notebook loaded | notebook_name |
| `notebooks.notebook_executed` | Notebook executed | notebook_name |
| `notebooks.notebook_failed` | Notebook fails | notebook_name, error |

**Primary Use Cases**: Build monitoring, error tracking, audit logging

---

### ⚡ HIGH - StageProcessor

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `stage.pre_execute` | Before stage execution | stage, layer, pipe_count |
| `stage.post_execute` | After execution | pipes_executed, duration_seconds |
| `stage.execute_failed` | Execution fails | stage, error |
| `stage.watermark_ensured` | Watermark entity created | layer |
| `stage.pipes_started` | Before pipes run | pipe_ids |
| `stage.pipes_completed` | After pipes done | pipe_count |

**Primary Use Cases**: Stage monitoring, watermark tracking, pipeline coordination

---

### ⚡ HIGH - Bootstrap

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `bootstrap.started` | Bootstrap begins | app_name, environment |
| `bootstrap.platform_detected` | Platform identified | platform, workspace_id, spark_version |
| `bootstrap.config_loaded` | Config files loaded | config_files, config_count |
| `bootstrap.dependencies_installed` | Packages installed | framework_wheels, lake_packages, pypi_packages |
| `bootstrap.framework_ready` | Initialization complete | duration_seconds |
| `bootstrap.failed` | Bootstrap fails | failed_phase, error |

**Primary Use Cases**: Initialization monitoring, config validation, error alerting

---

### 📊 MODERATE - SparkSessionProvider

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `session.created` | Session created | app_name, spark_version, config |
| `session.config_changed` | Config updated | changed_configs, old_values |
| `session.stopped` | Session stopped | app_name |

**Primary Use Cases**: Session lifecycle, configuration tracking

---

### 📊 MODERATE - PlatformService

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `platform.file_read` | File read | platform, file_path, file_size_bytes |
| `platform.file_written` | File written | platform, file_path |
| `platform.file_deleted` | File deleted | platform, file_path |
| `platform.notebook_read` | Notebook read | platform, notebook_name |

**Primary Use Cases**: Storage monitoring, performance analysis, audit trail

**Warning**: High volume - consider sampling

---

### 📊 MODERATE - SparkTraceProvider

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `trace.span_started` | Span created | span_id, component, operation |
| `trace.span_ended` | Span closed | span_id, duration_seconds |
| `trace.span_failed` | Span failed | span_id, error |

**Primary Use Cases**: Trace aggregation, external monitoring

**Note**: Spans already provide tracing - signals for external notification only

---

### 💤 LOW PRIORITY (Skip Signals)

- **SparkLoggerProvider** - Too simple, no state changes
- **SignalProvider** - Meta-level, would be circular

---

## Common Payload Patterns

### Pre-Operation
```python
{
    "operation_id": "uuid",           # Unique operation identifier
    "entity_id": "customers",         # What's being operated on
    "count_estimate": 1000,           # Estimated size
    "timestamp": "2025-01-20T10:00:00Z"
}
```

### Post-Operation (Success)
```python
{
    "operation_id": "uuid",
    "entity_id": "customers",
    "rows_processed": 1000,           # Actual results
    "duration_seconds": 45.2,         # Timing
    "timestamp": "2025-01-20T10:00:45Z"
}
```

### Failed Operation
```python
{
    "operation_id": "uuid",
    "entity_id": "customers",
    "error": "Table not found",       # Concise error
    "error_type": "AnalysisException", # Exception class
    "duration_seconds": 2.1,          # Time before failure
    "timestamp": "2025-01-20T10:00:02Z"
}
```

---

## Observer Pattern Examples

### Basic Observer
```python
from kindling.signaling import SignalProvider

signal_provider = SignalProvider()

def on_pipeline_complete(sender, **kwargs):
    pipe_count = kwargs.get('pipe_count', 0)
    duration = kwargs.get('duration_seconds', 0)
    print(f"Pipeline completed: {pipe_count} pipes in {duration}s")

signal_provider.connect('datapipes.post_run', on_pipeline_complete)
```

### Error Monitoring Observer
```python
def on_operation_failed(sender, **kwargs):
    operation = kwargs.get('operation', 'unknown')
    error = kwargs.get('error', 'Unknown error')
    error_type = kwargs.get('error_type', 'Exception')

    # Send alert
    send_alert(f"{operation} failed: {error_type} - {error}")

    # Log to monitoring system
    log_error(operation, error_type, error)

# Subscribe to all failure signals
signal_provider.connect('datapipes.run_failed', on_operation_failed)
signal_provider.connect('entity.merge_failed', on_operation_failed)
signal_provider.connect('file_ingestion.process_failed', on_operation_failed)
```

### Performance Monitoring Observer
```python
from collections import defaultdict

performance_stats = defaultdict(list)

def track_performance(sender, **kwargs):
    operation = sender  # Signal name
    duration = kwargs.get('duration_seconds', 0)
    performance_stats[operation].append(duration)

    # Calculate rolling average
    avg_duration = sum(performance_stats[operation]) / len(performance_stats[operation])

    if duration > avg_duration * 2:
        send_alert(f"{operation} took {duration}s (avg: {avg_duration}s)")

# Track all post-operations
signal_provider.connect('datapipes.post_run', track_performance)
signal_provider.connect('entity.post_merge', track_performance)
signal_provider.connect('file_ingestion.post_process', track_performance)
```

### Audit Trail Observer
```python
import json
from datetime import datetime

def audit_log(sender, **kwargs):
    log_entry = {
        'signal': sender,
        'timestamp': kwargs.get('timestamp', datetime.utcnow().isoformat()),
        'user': get_current_user(),
        'payload': kwargs
    }

    # Write to audit log
    with open('/logs/audit.jsonl', 'a') as f:
        f.write(json.dumps(log_entry) + '\n')

# Audit all entity write operations
signal_provider.connect('entity.post_merge', audit_log)
signal_provider.connect('entity.post_append', audit_log)
signal_provider.connect('entity.post_write', audit_log)
```

---

## Testing Patterns

### Unit Test with Mock Signals
```python
from unittest.mock import Mock, MagicMock

def test_pipeline_emits_signals(setup_method):
    # Setup
    mock_signal_provider = Mock()
    mock_pre_run = MagicMock()
    mock_post_run = MagicMock()

    mock_signal_provider.signal.side_effect = lambda name: {
        'datapipes.pre_run': mock_pre_run,
        'datapipes.post_run': mock_post_run
    }[name]

    # Exercise
    executer = DataPipesExecuter(signal_provider=mock_signal_provider)
    executer.run_datapipes(['pipe1', 'pipe2'])

    # Verify
    mock_pre_run.send.assert_called_once()
    mock_post_run.send.assert_called_once()

    # Check payload
    post_call_kwargs = mock_post_run.send.call_args[1]
    assert post_call_kwargs['pipe_count'] == 2
    assert 'duration_seconds' in post_call_kwargs
```

### Integration Test with Real Signals
```python
def test_pipeline_signals_integration(setup_method):
    # Setup
    signal_provider = BlinkerSignalProvider()
    received_signals = []

    def record_signal(sender, **kwargs):
        received_signals.append((sender, kwargs))

    signal_provider.connect('datapipes.pre_run', record_signal)
    signal_provider.connect('datapipes.post_run', record_signal)

    # Exercise
    executer = DataPipesExecuter(signal_provider=signal_provider)
    executer.run_datapipes(['pipe1'])

    # Verify
    assert len(received_signals) == 2
    assert received_signals[0][0] == 'datapipes.pre_run'
    assert received_signals[1][0] == 'datapipes.post_run'
```

---

## Performance Guidelines

### Low Volume (<100/min)
- All signals - emit normally
- No optimization needed

### Medium Volume (100-1000/min)
- Individual file signals - emit all
- Consider lazy payload construction
- Monitor observer performance

### High Volume (>1000/min)
- Individual file signals - consider sampling (every 10th file)
- Platform operations - aggregate or sample
- Batch signals into periodic summaries

### Optimization: Lazy Payloads
```python
def emit_signal_lazy(signal_name, payload_fn):
    """Only build payload if observers exist"""
    signal = signal_provider.signal(signal_name)

    if signal.has_receivers_for(signal_provider):
        payload = payload_fn()
        signal.send(signal_provider, **payload)
```

---

## Documentation Template

```python
class MyService:
    """
    Service description.

    Signals Emitted:
        my_service.pre_operation (before operation):
            - entity_id (str): Entity being operated on
            - param1 (int): Operation parameter

        my_service.post_operation (after success):
            - entity_id (str): Entity operated on
            - result_count (int): Number of results
            - duration_seconds (float): Operation duration

        my_service.operation_failed (on failure):
            - entity_id (str): Entity that failed
            - error (str): Error message
            - error_type (str): Exception class name

    Usage:
        >>> signal_provider = BlinkerSignalProvider()
        >>> signal_provider.connect('my_service.post_operation', my_callback)
        >>> service = MyService(signal_provider)
        >>> service.do_operation()  # Emits signals
    """
```

---

## Next Steps

1. **Review Evaluation**: See `signal_opportunities_evaluation.md` for detailed analysis
2. **Prioritize**: Choose providers based on business needs
3. **Implement Phase 1**: Start with DataPipesExecuter, DataEntityManager, FileIngestionProcessor
4. **Test Thoroughly**: Use patterns from this guide
5. **Document**: Follow template for each provider
6. **Monitor**: Track signal performance impact
7. **Iterate**: Gather feedback and adjust

---

## References

- **Full Evaluation**: `/workspace/docs/signal_opportunities_evaluation.md`
- **Config Reload Implementation**: `/workspace/packages/kindling/spark_config.py`
- **Config Reload Tests**: `/workspace/tests/unit/test_config_reload.py`
- **Signaling Framework**: `/workspace/packages/kindling/signaling.py`
- **Blinker Documentation**: https://blinker.readthedocs.io/

---

**Document Version**: 1.0
**Last Updated**: January 2025
**Status**: Evaluation Complete - No Implementation Yet
