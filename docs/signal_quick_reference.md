# Signal Quick Reference - Kindling Framework

Current reference for implemented in-process signals.

- Last validated against code: 2026-02-26
- Source modules: `packages/kindling/*.py`

## Status

Signal emission is implemented and used across core runtime components.

## Signal Naming

Preferred pattern:
- `{namespace}.before_{operation}`
- `{namespace}.after_{operation}`
- `{namespace}.{operation}_failed`
- `{namespace}.{state}` (for event/state style domains like streaming)

Compatibility note:
- Config reload uses legacy names: `config.pre_reload`, `config.post_reload`, `config.reload_failed`.

## Implemented Signals

### Data Pipes (`data_pipes.py`)
- `datapipes.before_run`
- `datapipes.after_run`
- `datapipes.run_failed`
- `datapipes.before_pipe`
- `datapipes.after_pipe`
- `datapipes.pipe_failed`
- `datapipes.pipe_skipped`

### Entity Registry + Provider (`data_entities.py`, `entity_provider_delta.py`)
- `entity.registered`
- `entity.before_ensure_table`
- `entity.after_ensure_table`
- `entity.ensure_failed`
- `entity.before_merge`
- `entity.after_merge`
- `entity.merge_failed`
- `entity.before_append`
- `entity.after_append`
- `entity.append_failed`
- `entity.before_write`
- `entity.after_write`
- `entity.write_failed`
- `entity.before_read`
- `entity.after_read`

### File Ingestion (`file_ingestion.py`)
- `file_ingestion.before_process`
- `file_ingestion.after_process`
- `file_ingestion.process_failed`
- `file_ingestion.before_file`
- `file_ingestion.after_file`
- `file_ingestion.file_failed`
- `file_ingestion.file_moved`
- `file_ingestion.batch_written`

### Generation Orchestrator (`generation_executor.py`)
- `orchestrator.execution_started`
- `orchestrator.execution_completed`
- `orchestrator.execution_failed`
- `orchestrator.generation_started`
- `orchestrator.generation_completed`
- `orchestrator.pipe_started`
- `orchestrator.pipe_completed`
- `orchestrator.pipe_failed`
- `orchestrator.pipe_skipped`
- `orchestrator.cache_recommended`
- `orchestrator.cache_hit`
- `orchestrator.cache_miss`

### Stage Processing (`simple_stage_processor.py`)
- `stage.before_execute`
- `stage.after_execute`
- `stage.execute_failed`

### Watermarking (`watermarking.py`, `simple_read_persist_strategy.py`)
- `watermark.before_get`
- `watermark.watermark_found`
- `watermark.watermark_missing`
- `watermark.before_save`
- `watermark.after_save`
- `watermark.save_failed`
- `watermark.before_read_changes`
- `watermark.after_read_changes`
- `watermark.no_new_data`
- `persist.before_persist`
- `persist.after_persist`
- `persist.persist_failed`
- `persist.watermark_saved`

### App + Job Deployment (`job_deployment.py`)
- `app.before_deploy`
- `app.after_deploy`
- `app.deploy_failed`
- `job.before_create`
- `job.after_create`
- `job.create_failed`
- `job.before_run`
- `job.after_run`
- `job.run_failed`
- `job.before_cancel`
- `job.after_cancel`
- `job.cancel_failed`
- `job.status_checked`

### Config Reload (`spark_config.py`)
- `config.pre_reload`
- `config.post_reload`
- `config.reload_failed`

### Streaming (`streaming_listener.py`, `streaming_query_manager.py`, `streaming_health_monitor.py`, `streaming_recovery_manager.py`)
- `streaming.spark_query_started`
- `streaming.spark_query_progress`
- `streaming.spark_query_terminated`
- `streaming.query_registered`
- `streaming.query_started`
- `streaming.query_stopped`
- `streaming.query_failed`
- `streaming.query_restarted`
- `streaming.query_healthy`
- `streaming.query_unhealthy`
- `streaming.query_exception`
- `streaming.query_stalled`
- `streaming.recovery_attempted`
- `streaming.recovery_succeeded`
- `streaming.recovery_failed`
- `streaming.recovery_exhausted`

## Subscribe Example

```python
from kindling.signaling import BlinkerSignalProvider

signals = BlinkerSignalProvider()

# Ensure signal exists, then connect
sig = signals.create_signal("datapipes.after_run")


def on_pipeline_completed(sender, **payload):
    print(payload.get("success_count"), payload.get("duration_seconds"))


sig.connect(on_pipeline_completed)
```

## Emission Behavior

- Components using `SignalEmitter` lazily create signals on first emit.
- If no signal provider is configured for a component, emit calls are no-ops.
