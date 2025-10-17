# System Tests - Platform Testing via App Framework

System tests verify the Kindling framework on actual platforms (Databricks, Synapse, Fabric) by publishing test apps that run as Spark jobs.

## Architecture

```
tests/system/
├── apps/                           # Test apps (deployable as Spark jobs)
│   ├── azure-storage-test/
│   │   ├── app.yaml               # Base config
│   │   ├── app.databricks.yaml    # Databricks-specific config
│   │   ├── app.synapse.yaml       # Synapse-specific config
│   │   ├── app.fabric.yaml        # Fabric-specific config
│   │   └── main.py                # Test app entry point
│   ├── data-pipeline-test/
│   │   ├── app.yaml
│   │   ├── app.databricks.yaml
│   │   ├── app.synapse.yaml
│   │   ├── app.fabric.yaml
│   │   └── main.py
│   └── performance-test/
│       ├── app.yaml
│       └── main.py
├── runners/                        # Local test runners
│   ├── databricks_runner.py       # Deploy and run on Databricks
│   ├── synapse_runner.py          # Deploy and run on Synapse
│   └── fabric_runner.py           # Deploy and run on Fabric
├── shared/                         # Shared test utilities
│   ├── test_result_collector.py   # Collect results from platform runs
│   ├── platform_authenticator.py  # Platform authentication
│   └── test_data_generator.py     # Generate test datasets
└── conftest.py                     # pytest configuration
```

## Test App Structure

Each test app follows the Kindling App Framework pattern:
- `app.yaml` - Base configuration with test parameters
- `app.{platform}.yaml` - Platform-specific overrides
- `main.py` - Entry point that runs tests and reports results

## Usage

### Run Single Platform Test
```bash
python -m pytest tests/system/test_databricks.py::test_azure_storage -v
```

### Run All Platform Tests
```bash
python -m pytest tests/system/ -v
```

### Run Specific Test App Manually
```bash
# Deploy to Databricks and run
python tests/system/runners/databricks_runner.py azure-storage-test

# Deploy to Synapse and run  
python tests/system/runners/synapse_runner.py data-pipeline-test
```

## Test Results

Test apps write structured results to platform storage, which are then collected and validated by the local test runners.

Results format:
```json
{
  "test_name": "azure-storage-test",
  "platform": "databricks", 
  "status": "passed|failed|error",
  "execution_time": 45.2,
  "details": {
    "spark_version": "3.4.0",
    "cluster_id": "1234-567890-abc123",
    "test_steps": [
      {"step": "create_test_data", "status": "passed", "duration": 5.1},
      {"step": "write_to_delta", "status": "passed", "duration": 12.3},
      {"step": "read_and_validate", "status": "passed", "duration": 8.7}
    ]
  },
  "metrics": {
    "rows_processed": 1000000,
    "data_size_mb": 45.2,
    "memory_usage_gb": 2.1
  },
  "errors": []
}
```