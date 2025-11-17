# Synapse System Test Setup Guide

## Overview

The Synapse system tests validate end-to-end job deployment and execution on Azure Synapse Analytics, similar to the Fabric tests.

## Current Status

✅ **COMPLETE**: The `SynapseAPI` class is fully implemented with all methods working via Synapse REST API.

The tests are **ready to run** once you configure the required environment variables.

## Required Environment Variables

Add these to your `tests/system/.env` file:

```bash
# Synapse Workspace Configuration (REQUIRED)
SYNAPSE_WORKSPACE_NAME=your-workspace-name     # e.g., "mysynapseworkspace"
SYNAPSE_SPARK_POOL_NAME=your-spark-pool-name   # e.g., "spark31"

# Azure Authentication (REQUIRED for Synapse)
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-service-principal-client-id
AZURE_CLIENT_SECRET=your-service-principal-secret
```

### Finding Your Values

1. **Workspace Name**:
   - Go to Azure Portal → Your Synapse Workspace
   - The name is shown at the top (NOT the full URL)
   - Example: If URL is `https://mysynapseworkspace.dev.azuresynapse.net`, use `mysynapseworkspace`

2. **Spark Pool Name**:
   - In Synapse Studio → Manage → Apache Spark pools
   - Use the pool name (e.g., `spark31`, `sparkpool1`)

3. **Service Principal**:
   - Create an Azure AD App Registration
   - Grant it `Synapse Contributor` role on the workspace
   - Use the Application (client) ID, Tenant ID, and create a client secret

## Test Structure

### Test App Location
```
tests/system/apps/synapse-test-app/
├── main.py         # Test application (5 tests)
└── README.md       # App documentation
```

### Test File
```
tests/system/test_synapse_job_deployment.py
```

### Test Classes

1. **TestSynapseJobDeployment** - Core deployment tests
   - `test_deploy_app_as_job` - Deploy app as Synapse Spark job
   - `test_run_and_monitor_job` - Execute and monitor job (5-min timeout)
   - `test_job_cancellation` - Cancel running job
   - `test_job_with_invalid_config` - Validate error handling

2. **TestSynapseJobResults** - Result validation
   - `test_job_output_files` - Verify output file creation

## Running Tests

### Run All Synapse Tests
```bash
cd /workspace
pytest tests/system/test_synapse_job_deployment.py -v -s -m synapse
```

### Run Specific Test
```bash
pytest tests/system/test_synapse_job_deployment.py::TestSynapseJobDeployment::test_run_and_monitor_job -v -s
```

### Skip Cleanup (for debugging)
```python
@pytest.mark.skip_cleanup
def test_something(...):
    ...
```

## Implementation Status

✅ All `SynapseAPI` methods are implemented:

- [x] `create_spark_job()` - Create Spark job definition (stored locally)
- [x] `upload_files()` - Upload files to ADLS Gen2 via Azure Storage SDK
- [x] `update_job_files()` - Update job config with file paths
- [x] `run_job()` - Execute Spark batch job via Livy API
- [x] `get_job_status()` - Check job execution status via Livy API
- [x] `cancel_job()` - Cancel running job
- [x] `delete_job()` - Delete job definition
- [x] `list_spark_jobs()` - List existing Spark batch jobs

### API Documentation References

- [Synapse Spark Batch API](https://docs.microsoft.com/en-us/rest/api/synapse/data-plane/spark-batch)
- [ADLS Gen2 REST API](https://docs.microsoft.com/en-us/rest/api/storageservices/data-lake-storage-gen2)
- [Azure Identity](https://docs.microsoft.com/en-us/python/api/azure-identity/)

## Differences from Fabric Tests

| Aspect | Fabric | Synapse |
|--------|--------|---------|
| Auth | Service Principal OR `az login` | Service Principal ONLY |
| Workspace ID | UUID | Workspace name (string) |
| Storage | OneLake (Fabric abstraction) | ADLS Gen2 (direct) |
| Job API | Fabric Items API | Spark Batch Jobs API |
| Bootstrap | Files/scripts/kindling_bootstrap.py | scripts/kindling_bootstrap.py |

## Test App Execution

The `synapse-test-app` runs 5 tests when deployed:

1. ✅ **Spark Session** - Create session and check version
2. ✅ **Spark Operations** - DataFrame operations and aggregations
3. ✅ **Framework Available** - Verify kindling-synapse wheel installed
4. ✅ **Storage Access** - Test mssparkutils filesystem operations
5. ✅ **Results Saved** - Persist JSON results to local and ADLS

Results saved to:
- Local: `/tmp/synapse_job_test_results.json`
- ADLS: `{STORAGE_OUTPUT_PATH}/synapse_job_test_results.json`

## Next Steps

1. ✅ Create test structure (DONE)
2. ✅ Create test app (DONE)
3. ✅ Implement `SynapseAPI` methods in `packages/kindling/platform_synapse.py` (DONE)
4. ✅ Remove `@pytest.mark.skip` from test classes (DONE)
5. ⏳ Configure environment variables in `.env` file
6. ⏳ Deploy kindling_bootstrap.py to ADLS
7. ⏳ Run tests and validate
8. ⏳ Document results

## See Also

- `test_fabric_job_deployment.py` - Similar test structure for Fabric
- `packages/kindling/platform_synapse.py` - SynapseAPI implementation (TODO)
- `runtime/scripts/kindling_bootstrap.py` - Bootstrap script used by jobs
