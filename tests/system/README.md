# System Tests

This directory contains system-level integration tests for the Kindling framework.
These tests validate end-to-end functionality on real platforms (Fabric, Databricks, Synapse).

## Overview

System tests differ from unit tests:
- **Unit Tests**: Fast, isolated, use mocks
- **System Tests**: Slow, integrated, use real platforms

## Test Structure

```
tests/system/
├── README.md                        # This file
├── conftest.py                      # Pytest configuration
├── test_fabric_job_deployment.py   # Fabric job deployment tests
├── apps/                            # Test applications
│   └── fabric-job-test/             # Simple test app
│       ├── main.py                  # Test app entry point
│       ├── app.yaml                 # Base config
│       └── app.fabric.yaml          # Fabric-specific config
└── runners/                         # Test runners (examples)
    └── fabric_runner.py             # Example Fabric runner
```

## Prerequisites

### For Fabric Tests

1. **Microsoft Fabric Workspace**
   - Fabric capacity with Spark enabled
   - Lakehouse created in workspace

2. **Authentication** (choose one):

   **Option A: Azure CLI (Recommended for local development)**
   ```bash
   # Login with your Azure account
   az login

   # Set required environment variables
   export FABRIC_WORKSPACE_ID="your-workspace-id"
   export FABRIC_LAKEHOUSE_ID="your-lakehouse-id"
   ```

   **Option B: Service Principal (CI/CD)**
   ```bash
   # Set all environment variables
   export FABRIC_WORKSPACE_ID="your-workspace-id"
   export FABRIC_LAKEHOUSE_ID="your-lakehouse-id"
   export AZURE_TENANT_ID="your-tenant-id"
   export AZURE_CLIENT_ID="your-client-id"
   export AZURE_CLIENT_SECRET="your-client-secret"
   ```

   Note: The test framework uses `DefaultAzureCredential` which automatically tries:
   1. Service principal (if env vars set)
   2. Managed identity (if running in Azure)
   3. Azure CLI (`az login`)
   4. Other credential types

3. **Required Permissions**
   - Fabric workspace contributor role
   - Lakehouse read/write access

### For Databricks Tests

1. **Databricks Workspace**
   - Cluster or serverless compute available

2. **Environment Variables**
   ```bash
   export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
   export DATABRICKS_TOKEN="your-access-token"
   ```

### For Synapse Tests

1. **Azure Synapse Workspace**
   - Spark pool created

2. **Environment Variables**
   ```bash
   export SYNAPSE_WORKSPACE_NAME="your-workspace-name"
   export AZURE_TENANT_ID="your-tenant-id"
   export AZURE_CLIENT_ID="your-client-id"
   export AZURE_CLIENT_SECRET="your-client-secret"
   ```

## Running Tests

### Run All System Tests
```bash
pytest tests/system/ -v -s
```

### Run Fabric Tests Only
```bash
pytest tests/system/ -v -s -m fabric
```

### Run Specific Test
```bash
pytest tests/system/test_fabric_job_deployment.py::TestFabricJobDeployment::test_deploy_app_as_job -v -s
```

### Skip Slow Tests
```bash
pytest tests/system/ -v -m "not slow"
```

### Run with Custom Timeout
```bash
TEST_TIMEOUT=600 pytest tests/system/ -v -s
```

## Test Markers

Tests use pytest markers to categorize:

- `@pytest.mark.fabric` - Requires Fabric platform
- `@pytest.mark.databricks` - Requires Databricks platform
- `@pytest.mark.synapse` - Requires Synapse platform
- `@pytest.mark.azure` - Requires Azure cloud (platform-agnostic across Azure-backed platforms)
- `@pytest.mark.aws` - Requires AWS cloud
- `@pytest.mark.gcp` - Requires Google Cloud
- `@pytest.mark.system` - System-level integration test
- `@pytest.mark.slow` - Takes significant time to run

## Configuration

### Pytest Configuration

System tests are configured via `conftest.py`:
- Automatically adds packages to Python path
- Skips tests when credentials are missing
- Provides common fixtures for platform access

### Test Timeouts

Default timeouts can be overridden via environment variables:

```bash
# Job execution timeout (default: 600 seconds)
export TEST_TIMEOUT=900

# Status poll interval (default: 10 seconds)
export POLL_INTERVAL=15
```

## Writing New System Tests

### Basic Structure

```python
import pytest

@pytest.mark.fabric
@pytest.mark.system
@pytest.mark.slow
class TestMyFeature:
    """System tests for my feature"""

    def test_something(self, fabric_deployer, test_app_path, job_config):
        """Test description"""
        # Deploy job
        result = fabric_deployer.deploy_as_job(
            app_path=str(test_app_path),
            job_config=job_config
        )

        # Run assertions
        assert result["job_id"]

        # Cleanup
        # ...
```

### Best Practices

1. **Always Clean Up**: Use try/finally to clean up resources
2. **Skip Gracefully**: Tests should skip when credentials missing
3. **Use Fixtures**: Leverage shared fixtures from conftest.py
4. **Document Requirements**: Clearly document environment variables needed
5. **Handle Timeouts**: Set reasonable timeouts for operations
6. **Validate Results**: Check actual execution results, not just completion

## Test Applications

Test apps in `apps/` directory are simple Spark applications used for testing.

### Creating a Test App

1. Create directory: `tests/system/apps/my-test-app/`
2. Add `main.py` with Spark application code
3. Add `app.yaml` with base configuration
4. Add `app.<platform>.yaml` for platform-specific config

Example `main.py`:
```python
#!/usr/bin/env python3
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("MyTest").getOrCreate()
    # Your test logic here
    print("Test completed successfully!")

if __name__ == "__main__":
    main()
```

## Troubleshooting

### Tests Are Skipped

**Issue**: All tests show "SKIPPED"
**Cause**: Missing environment variables
**Solution**: Set required environment variables (see Prerequisites)

### Job Deployment Fails

**Issue**: Deployment fails with authentication error
**Cause**: Invalid or expired credentials
**Solution**:
- Verify service principal has correct permissions
- Check credentials are not expired
- Ensure workspace ID and lakehouse ID are correct

### Job Times Out

**Issue**: Test fails with timeout error
**Cause**: Job takes longer than expected
**Solution**:
- Increase TEST_TIMEOUT environment variable
- Check job logs for errors
- Verify Spark cluster has sufficient resources

### Import Errors

**Issue**: "ModuleNotFoundError: No module named 'kindling'"
**Cause**: Packages not in Python path
**Solution**:
- Run pytest from workspace root
- conftest.py should automatically add packages to path
- Verify packages/kindling/ directory exists

## CI/CD Integration

### GitHub Actions Example

```yaml
name: System Tests

on: [push, pull_request]

jobs:
  fabric-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -r requirements.txt
      - run: pytest tests/system/ -v -m fabric
        env:
          FABRIC_WORKSPACE_ID: ${{ secrets.FABRIC_WORKSPACE_ID }}
          FABRIC_LAKEHOUSE_ID: ${{ secrets.FABRIC_LAKEHOUSE_ID }}
          AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
          AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
          AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
```

## Additional Resources

- [Job Deployment Documentation](../../docs/job_deployment.md)
- [Testing Documentation](../../docs/testing.md)
- [Fabric API Documentation](https://learn.microsoft.com/fabric/)
- [Databricks Jobs API](https://docs.databricks.com/api/workspace/jobs)
- [Synapse Spark API](https://learn.microsoft.com/azure/synapse-analytics/)

## Support

For issues or questions:
1. Check existing documentation
2. Review error logs carefully
3. Verify all prerequisites are met
4. Open an issue on GitHub with details
