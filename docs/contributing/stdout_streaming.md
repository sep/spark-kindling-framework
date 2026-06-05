# Real-Time Stdout Log Streaming for Spark Jobs

## Overview

Kindling provides real-time stdout log streaming for running Spark jobs across all supported platforms:
- **Microsoft Fabric**
- **Azure Synapse Analytics**
- **Databricks**

This feature enables you to monitor job execution as it happens, without waiting for the job to complete.

## Platform-Specific Implementations

While the API is consistent across platforms, each platform accesses stdout logs differently:

### Microsoft Fabric

Uses the Fabric Spark REST API to stream logs via Livy sessions:

1. **Get Livy Session Info**: Retrieves session ID and Spark application ID
2. **Poll Driver Log API**: Queries stdout logs with byte-based offsets
3. **Incremental Reading**: Tracks byte position to only read new content

**Endpoint Pattern:**
```
GET https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/sparkJobDefinitions/{job_id}/livySessions/{livy_id}/applications/{app_id}/logs?type=driver&fileName=stdout&isDownload=true&isPartial=true&offset={bytes}&size={chunk_size}
```

### Azure Synapse Analytics

Reads stdout logs from diagnostic emitter storage in ADLS Gen2:

1. **Get Batch Status**: Monitors Livy batch job status
2. **Extract Application ID**: Retrieves Spark application ID from batch details
3. **Read from Storage**: Accesses stdout file in diagnostic logs directory
4. **Incremental Reading**: Tracks byte position in the stdout file

**Log Path Pattern:**
```
logs/{workspace}.{pool}.{batch_id}/driver/spark-logs/stdout
```

### Databricks

Uses the Jobs API to retrieve stdout from run output:

1. **Monitor Run Status**: Tracks job lifecycle state
2. **Get Run Output**: Calls get_run_output API for stdout/stderr
3. **Incremental Parsing**: Tracks line count to detect new output
4. **Run-Isolated Logs**: Each run has isolated output (last 5MB)

**API Pattern:**
```
GET {workspace_url}/api/2.1/jobs/runs/get-output?run_id={run_id}
```

## Usage

### Basic Example (Platform-Agnostic)

```python
# Works with FabricAPI, SynapseAPI, or DatabricksAPI
from kindling.platform_fabric import FabricAPI  # or SynapseAPI, DatabricksAPI

# Initialize API client
api = FabricAPI(
    workspace_id="your-workspace-id",
    lakehouse_id="your-lakehouse-id",
    storage_account="your-storage-account",
    container="artifacts",
)

# Deploy and run a job
job_result = api.deploy_spark_job(app_files, job_config)
run_id = api.run_job(job_result["job_id"])

# Stream stdout logs with custom callback
def log_handler(line):
    print(f"[APP] {line}")

logs = api.stream_stdout_logs(
    job_id=job_result["job_id"],
    run_id=run_id,
    callback=log_handler,
    poll_interval=5.0,
    max_wait=300.0
)

print(f"Captured {len(logs)} log lines")
```

### Using the Test Helper

The `StdoutStreamValidator` provides convenient validation methods:

```python
from tests.system.test_helpers import StdoutStreamValidator

# Create validator (works with any platform API)
validator = StdoutStreamValidator(api_client)

# Stream with automatic line capture
validator.stream_with_callback(
    job_id=job_id,
    run_id=run_id,
    print_lines=True  # Print as they arrive
)

# Validate bootstrap execution
bootstrap_results = validator.validate_bootstrap_execution()
# Returns: {"bootstrap_start": True, "framework_init": True, "config_loaded": True}

# Validate extension loading
extension_results = validator.validate_extension_loading("kindling-otel-azure")
# Returns: {"extension_install": True, "extension_success": True}

# Validate test app execution
test_results = validator.validate_test_app_execution(test_id="abc123")
# Returns: {"test_markers": True, "test_id_match": True}

# Print summary
validator.print_validation_summary(bootstrap_results, "Bootstrap Validation")
```

### Integration with Tests

```python
import pytest

def test_my_spark_app(platform_client, stdout_validator):
    """Test that monitors stdout during execution (works on all platforms)"""
    client, platform_name = platform_client

    # Deploy job
    result = client.deploy_spark_job(app_files, config)
    job_id = result["job_id"]

    # Start job
    run_id = client.run_job(job_id)

    # Stream and validate logs
    stdout_validator.stream_with_callback(
        job_id=job_id,
        run_id=run_id,
        print_lines=True,
        poll_interval=5.0
    )

    # Validate execution
    bootstrap_results = stdout_validator.validate_bootstrap_execution()
    assert bootstrap_results["bootstrap_start"], "Bootstrap not found in stdout"
    assert bootstrap_results["framework_init"], "Framework init not found in stdout"
```

## API Reference

### `stream_stdout_logs(job_id, run_id, callback=None, poll_interval=5.0, max_wait=300.0)`

Stream stdout logs from a running Spark job in real-time.

**Available on:** `FabricAPI`, `SynapseAPI`, `DatabricksAPI`

**Parameters:**

- `job_id` (str): Spark job definition ID
- `run_id` (str): Job run/instance ID
- `callback` (callable, optional): Function called for each new log line. Signature: `callback(line: str)`
- `poll_interval` (float): Seconds between polls (default: 5.0)
- `max_wait` (float): Maximum seconds to wait for job to start (default: 300.0)

**Returns:**

- `List[str]`: Complete list of all log lines retrieved

**Behavior:**

1. Polls job status until it starts running
2. Attempts to get Livy session info (livy_id, app_id)
3. Polls stdout endpoint for new content
4. Calls callback for each new line
5. Continues until job completes (COMPLETED, FAILED, CANCELLED)

### `get_livy_session_info(job_id, run_id)`

Get Livy session information for a running Spark job.

**Parameters:**

- `job_id` (str): Spark job definition ID
- `run_id` (str): Job run/instance ID

**Returns:**

- `Dict[str, Any]` or `None`: Dictionary with keys:
  - `livy_id`: Livy session ID
  - `app_id`: Spark application ID
  - `state`: Session state
  - `job_id`: Job definition ID
  - `run_id`: Job run ID

Returns `None` if session info is not available.

## Implementation Details

### Position Tracking

The system maintains an internal position counter to track how many lines have been read. Each poll requests logs starting from `from={position}` to avoid re-reading content.

```python
# Internal position tracking
last_position = 0
new_logs = _read_stdout_chunk(job_id, run_id, session_info, last_position)
last_position += len(new_logs)
```

### Job State Handling

The streaming loop handles different job states:

- **NOTSTARTED/PENDING/STARTING**: Waits and retries
- **INPROGRESS/RUNNING**: Attempts to get session info and read logs
- **COMPLETED/FAILED/CANCELLED**: Performs final read and exits

### Error Handling

The system gracefully handles:

- Session info not available (waits and retries)
- 404 errors from logs endpoint (logs not ready yet)
- HTTP errors during streaming (logs and continues)
- Job status check failures (logs and continues)

### Timeout Management

Two timeouts are enforced:

1. **max_wait**: Maximum time to wait for job to start running
2. **Natural timeout**: Exits when job reaches terminal state

## Limitations

### Fabric API Requirements

The stdout streaming requires:

1. Job must be running (not just queued)
2. Livy session must be established
3. Application ID must be available
4. REST API endpoint must be accessible

### Timing Considerations

- Logs may not be available immediately when job starts
- There's typically a 5-10 second delay before stdout appears
- Final log lines may take a few seconds after job completes

### Content Limitations

- Only stdout is captured (not stderr or log4j logs)
- Maximum chunk size may be limited by API
- Very verbose applications may experience truncation

## Comparison with Diagnostic Emitter Logs

| Feature | Stdout Streaming | Diagnostic Emitters |
|---------|-----------------|---------------------|
| **Timing** | Real-time during execution | Available after completion |
| **Source** | Application stdout | Log4j logs |
| **Content** | print() statements | app_logger.info() calls |
| **API** | Spark REST API | ADLS storage |
| **Use Case** | Live monitoring | Post-mortem analysis |

**Recommendation**: Use both!
- Stdout streaming for real-time monitoring during tests
- Diagnostic emitters for complete log history after completion

## Troubleshooting

### "Could not get session info - will retry"

**Cause**: Livy session not established yet

**Solution**:
- Wait longer (increase max_wait)
- Check job actually started (not stuck in queue)
- Verify job definition is valid

### "No new logs after 30 seconds"

**Cause**: Application not writing to stdout

**Solution**:
- Use `print()` statements in your application
- Check application is actually running (not hung)
- Verify Spark driver is active

### "HTTP 404 from logs endpoint"

**Cause**: Logs not ready or endpoint not available

**Solution**:
- Normal during job startup - system will retry
- If persistent, check Fabric API permissions
- Verify workspace and job IDs are correct

## Future Enhancements

Potential improvements:

1. **Stderr Streaming**: Add parallel stderr monitoring
2. **Log4j Streaming**: Stream Log4j logs in addition to stdout
3. **Pattern Matching**: Filter logs by regex patterns
4. **Batching**: Buffer lines and deliver in batches
5. **Compression**: Handle compressed log responses
6. **Multiplexing**: Monitor multiple jobs simultaneously

## See Also

- [Fabric REST API Documentation](https://learn.microsoft.com/en-us/rest/api/fabric/)
- [Spark Livy API](https://livy.apache.org/docs/latest/rest-api.html)
- [System Testing Guide](testing.md)
- [Platform API Architecture](platform_api_architecture.md)
