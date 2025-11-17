# Platform-Specific Testing Guide

The Kindling framework includes a robust mechanism for running platform-specific tests using pytest markers. This allows you to selectively run tests for specific cloud platforms (Synapse, Databricks, Fabric) or standalone environments.

## Overview

Tests can be marked with platform-specific markers to indicate which platform they require:

- `@pytest.mark.synapse` - Tests for Azure Synapse Analytics
- `@pytest.mark.databricks` - Tests for Databricks
- `@pytest.mark.fabric` - Tests for Microsoft Fabric
- `@pytest.mark.standalone` - Tests for standalone Spark or local environments
- `@pytest.mark.system` - General system tests (auto-applied to platform-marked tests)
- `@pytest.mark.requires_azure` - Tests requiring Azure credentials/SDK

## Quick Start

### Run Only Unit Tests (No System Tests)

```bash
poetry run pytest tests/unit/ -v
```

### Skip All System Tests

```bash
poetry run pytest tests/ --skip-system
```

### Run Tests for a Specific Platform

```bash
# Synapse only
poetry run pytest tests/ --platform=synapse -v

# Databricks only
poetry run pytest tests/ --platform=databricks -v

# Fabric only
poetry run pytest tests/ --platform=fabric -v

# Standalone only
poetry run pytest tests/ --platform=standalone -v
```

### Require Platform Environment Variables

Skip tests if platform environment variables are not configured:

```bash
poetry run pytest tests/system/ --require-platform-env --platform=synapse
```

## Marking Tests as Platform-Specific

### Mark All Tests in a Module

Use `pytestmark` at the module level:

```python
import pytest

# Mark all tests in this file as Synapse-specific
pytestmark = [pytest.mark.synapse, pytest.mark.system, pytest.mark.requires_azure]

class TestSynapseFeatures:
    def test_synapse_deployment(self):
        # This test will only run with --platform=synapse
        pass
```

### Mark Individual Tests

```python
@pytest.mark.synapse
@pytest.mark.requires_azure
def test_synapse_spark_pool():
    # Synapse-specific test
    pass

@pytest.mark.databricks
def test_databricks_cluster():
    # Databricks-specific test
    pass

@pytest.mark.fabric
def test_fabric_lakehouse():
    # Fabric-specific test
    pass

@pytest.mark.standalone
def test_local_spark():
    # Standalone/local Spark test
    pass
```

### Mark Test Classes

```python
@pytest.mark.synapse
class TestSynapseIntegration:
    """All tests in this class are Synapse-specific"""

    def test_feature_1(self):
        pass

    def test_feature_2(self):
        pass
```

## Required Environment Variables

Platform-specific tests may require environment variables to be set. Use `--require-platform-env` to automatically skip tests if variables are missing.

### Synapse

```bash
export SYNAPSE_WORKSPACE_NAME="your-workspace"
export SYNAPSE_SPARK_POOL_NAME="your-pool"
export AZURE_SUBSCRIPTION_ID="sub-id"
export AZURE_RESOURCE_GROUP="rg-name"
```

### Databricks

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
```

### Fabric

```bash
export FABRIC_WORKSPACE_ID="workspace-id"
export FABRIC_LAKEHOUSE_ID="lakehouse-id"
```

## Examples

### Development Workflow

```bash
# 1. Run unit tests during development (fast, no cloud required)
poetry run pytest tests/unit/ -v

# 2. Run integration tests with local Spark
poetry run pytest tests/integration/ -v

# 3. Before deploying to Synapse, run Synapse-specific tests
export SYNAPSE_WORKSPACE_NAME="dev-workspace"
export SYNAPSE_SPARK_POOL_NAME="dev-pool"
# ... set other env vars ...
poetry run pytest tests/system/ --platform=synapse --require-platform-env -v

# 4. Run all tests except system tests (for CI without cloud access)
poetry run pytest tests/ --skip-system
```

### CI/CD Pipeline

```yaml
# Example GitHub Actions workflow
test-unit:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v3
    - name: Run unit tests
      run: poetry run pytest tests/unit/ -v

test-synapse:
  runs-on: ubuntu-latest
  needs: test-unit
  steps:
    - uses: actions/checkout@v3
    - name: Set up Azure credentials
      run: |
        echo "SYNAPSE_WORKSPACE_NAME=${{ secrets.SYNAPSE_WORKSPACE }}" >> $GITHUB_ENV
        # ... other env vars ...
    - name: Run Synapse system tests
      run: poetry run pytest tests/system/ --platform=synapse --require-platform-env -v
```

## Test Discovery

The pytest configuration in `pyproject.toml` includes all test directories:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
markers = [
    "synapse: Synapse-specific system tests",
    "databricks: Databricks-specific system tests",
    "fabric: Fabric-specific system tests",
    "standalone: Standalone-specific tests",
    "system: System tests requiring cloud infrastructure",
    "requires_azure: Tests requiring Azure credentials and SDK",
]
```

## Converting Existing Tests

If you have non-pytest test files (with `if __name__ == "__main__"`), convert them to pytest format:

### Before (Old Style)

```python
class MyTest:
    def __init__(self):
        self.temp_dir = None

    def setup(self):
        self.temp_dir = tempfile.mkdtemp()

    def teardown(self):
        shutil.rmtree(self.temp_dir)

    def test_something(self):
        # test code
        pass

if __name__ == "__main__":
    test = MyTest()
    test.setup()
    test.test_something()
    test.teardown()
```

### After (Pytest Style)

```python
import pytest

pytestmark = [pytest.mark.synapse, pytest.mark.system]

@pytest.fixture(scope="class")
def temp_dir():
    """Fixture for temporary directory"""
    temp = tempfile.mkdtemp()
    yield temp
    shutil.rmtree(temp)

class TestMy:
    def test_something(self, temp_dir):
        # test code using temp_dir fixture
        pass

# Run with: pytest tests/system/test_my.py --platform=synapse -v
```

## Tips and Best Practices

1. **Use Fixtures**: Convert setup/teardown methods to pytest fixtures
2. **Mark Early**: Add platform markers when writing new tests
3. **Group Tests**: Use test classes to group related platform-specific tests
4. **Environment Checks**: Use `--require-platform-env` in CI/CD to fail fast
5. **Documentation**: Document required environment variables in test docstrings
6. **Skip Gracefully**: Use `pytest.skip()` when optional dependencies are missing

## Troubleshooting

### Tests Not Running

```bash
# Check which tests are collected
poetry run pytest tests/ --collect-only --platform=synapse

# Verify markers are registered
poetry run pytest --markers | grep "synapse:"
```

### Missing Dependencies

If Azure SDK is not installed, system tests will be skipped automatically:

```python
try:
    from azure.mgmt.synapse import SynapseManagementClient
except ImportError:
    pytestmark = pytest.mark.skip("Azure SDK not available")
```

### Platform Filter Not Working

Ensure tests have the correct marker:

```bash
# List all tests with synapse marker
poetry run pytest tests/ -m synapse --collect-only
```

## Reference

- `tests/conftest.py` - Pytest configuration and marker definitions
- `pyproject.toml` - Test discovery and marker registration
- `tests/unit/` - Unit tests (no platform markers needed)
- `tests/integration/` - Integration tests (may have platform markers)
- `tests/system/` - System tests (platform markers required)

## Support

For issues or questions:
- Check test output with `-v` flag for verbose information
- Use `--collect-only` to see which tests will run
- Review test markers with `pytest --markers`
- Check environment variables are set correctly
