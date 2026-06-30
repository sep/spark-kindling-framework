# Kindling Testing Guide

A comprehensive guide to testing the Spark Kindling Framework.

## Quick Start

### Run Tests

```bash
# All tests
poe test

# Unit tests only
poe test-unit

# Integration tests only
poe test-integration

# Quick tests (unit + integration, no system tests)
poe test-quick

# With coverage report
poe test-coverage

# System tests (flexible filtering)
poe test-system                                    # All platforms, all tests
poe test-system --platform fabric                   # Only Fabric tests
poe test-system --platform synapse                  # Only Synapse tests
poe test-system --platform databricks               # Only Databricks tests
poe test-system --test deploy_app_as_job           # Specific test, all platforms
poe test-system --platform fabric --test deploy    # Fabric + deploy tests

# Extension tests (flexible filtering)
poe test-extension                                          # Azure Monitor on all platforms
poe test-extension --extension azure-monitor --platform fabric  # Azure Monitor on Fabric only

# Cleanup tests resources (flexible filtering)
poe cleanup                              # Clean all platforms + old packages
poe cleanup --platform fabric            # Clean Fabric only
poe cleanup --platform synapse           # Clean Synapse only
poe cleanup --platform databricks        # Clean Databricks only
poe cleanup --skip-packages              # Clean all platforms, skip package cleanup

# Deploy wheels (flexible filtering)
poe deploy                                # Deploy all platforms from local dist/
poe deploy --platform fabric              # Deploy Fabric only from local dist/
poe deploy --release                      # Deploy all from latest GitHub release
poe deploy --release 0.4.0                # Deploy all from specific release
poe deploy --platform fabric --release    # Deploy Fabric from latest release

# Specific test file (use pytest directly)
pytest tests/integration/test_data_pipes_integration.py -v
```

### System Test Filtering

The `poe test-system` command uses pytest's `-k` filtering under the hood:

- **No filters**: Runs all system tests on all platforms (fabric, synapse, databricks)
- **`--platform <name>`**: Filters by platform marker (fabric, synapse, databricks)
- **`--test <pattern>`**: Filters by test name pattern (e.g., `deploy`, `monitor`, etc.)
- **Combined filters**: Both filters are AND'ed together

Examples:
```bash
# Run all tests on all platforms
poe test-system

# Run only Fabric tests
poe test-system --platform fabric

# Run only deployment tests on all platforms
poe test-system --test deploy_app_as_job

# Run specific test on specific platform
poe test-system --platform fabric --test deploy_app_as_job
```

---

## Test Structure

```
tests/
├── conftest.py                          # Root fixtures
├── spark_test_helper.py                 # Spark session utilities
├── azure_token_provider.py              # Azure token helper for tests
├── test_infrastructure_proposal.md      # Proposed test utilities
├── unit/                                # Unit tests (isolated components)
│   ├── test_app_framework.py
│   ├── test_bootstrap_*.py              # Bootstrap-related tests
│   ├── test_cli*.py                     # CLI tests
│   ├── test_data_entities.py
│   ├── test_data_pipes.py
│   ├── test_execution_*.py              # Execution strategy/orchestrator tests
│   ├── test_platform_*.py              # Per-platform unit tests
│   ├── test_scd*.py                     # SCD (slowly-changing dimension) tests
│   ├── test_streaming_*.py             # Streaming pipeline tests
│   └── ...                             # Many more component-level tests
├── integration/                         # Integration tests (real Spark, no cloud)
│   ├── README.md                        # Integration test philosophy
│   ├── run_tests.md                     # How to run integration tests
│   ├── spark_java_compatibility.md      # Java/Spark compatibility guide
│   ├── test_config_integration.py
│   ├── test_csv_entity_provider.py
│   ├── test_data_pipes_integration.py
│   ├── test_deployment_service.py
│   ├── test_entity_config_overrides_simple.py
│   ├── test_execution_strategy_integration.py
│   ├── test_generation_executor_integration.py
│   ├── test_kda_deployment.py
│   ├── test_kda_packaging.py
│   ├── test_kda_storage.py
│   └── test_pipe_graph_integration.py
├── system/                              # System tests (require cloud infrastructure)
│   └── core/                           # Core system tests, parametrized by platform
├── local-project/                       # Local-project smoke tests
└── data-apps/                           # Sample data apps used by tests
```

---

## Testing Philosophy

### Unit Tests
- **Scope**: Individual components in isolation
- **Dependencies**: Mocked external services
- **Speed**: Fast (< 1 second per test)
- **Purpose**: Verify component logic and edge cases

### Integration Tests
- **Scope**: Multiple components working together
- **Dependencies**: Real Spark sessions, in-memory data stores
- **Speed**: Slower (a few seconds per test)
- **Purpose**: Verify end-to-end workflows and component interaction

### System Tests
- **Scope**: Full end-to-end on live cloud platforms
- **Dependencies**: Cloud credentials (Fabric, Synapse, Databricks)
- **Speed**: Minutes per test
- **Purpose**: Validate real deployments and cloud-specific behavior

---

## Common Fixtures

### Spark Session

```python
@pytest.fixture(scope="module")
def spark():
    """Spark session for integration tests"""
    from tests.spark_test_helper import get_local_spark_session

    spark = get_local_spark_session(
        "KindlingIntegrationTests",
        delta_support=True
    )
    yield spark
    spark.stop()
```

### Temporary Workspace

```python
@pytest.fixture
def temp_workspace():
    """Temporary workspace, auto-cleaned"""
    import tempfile
    import shutil
    from pathlib import Path

    temp_dir = Path(tempfile.mkdtemp(prefix="kindling_test_"))
    yield temp_dir
    shutil.rmtree(temp_dir)
```

### Mock Services

```python
@pytest.fixture
def mock_logger():
    """Mock logger for testing"""
    from unittest.mock import MagicMock
    return MagicMock()

@pytest.fixture
def mock_trace_provider():
    """Mock trace provider"""
    from unittest.mock import MagicMock
    mock = MagicMock()
    mock.span.return_value.__enter__ = MagicMock()
    mock.span.return_value.__exit__ = MagicMock()
    return mock
```

---

## Testing Patterns

### Pattern 1: Unit Test with Mocks

```python
from unittest.mock import MagicMock
from kindling.data_pipes import DataPipesManager

def test_pipe_registration():
    """Test pipe registration with mocked dependencies"""
    mock_logger = MagicMock()
    manager = DataPipesManager(mock_logger)

    # Register pipe
    manager.register_pipe(
        pipeid="test_pipe",
        name="Test Pipe",
        execute=lambda df: df,
        tags={},
        input_entity_ids=["bronze.input"],
        output_entity_id="silver.output",
        output_type="table"
    )

    # Verify registration
    pipe_def = manager.get_pipe_definition("test_pipe")
    assert pipe_def is not None
    assert pipe_def.name == "Test Pipe"
```

### Pattern 2: Integration Test with Real Spark

```python
from kindling.data_entities import DataEntities
from kindling.data_pipes import DataPipes, DataPipesExecuter
from kindling.injection import GlobalInjector
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

def test_end_to_end_pipeline(spark):
    """Test complete pipeline execution"""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("product", StringType(), True),
        StructField("amount", DoubleType(), True),
    ])

    DataEntities.entity(
        entityid="bronze.sales",
        name="Bronze Sales",
        partition_columns=[],
        merge_columns=["id"],
        tags={"layer": "bronze"},
        schema=schema
    )

    DataEntities.entity(
        entityid="silver.sales_summary",
        name="Sales Summary",
        partition_columns=[],
        merge_columns=["product"],
        tags={"layer": "silver"},
        schema=schema
    )

    @DataPipes.pipe(
        pipeid="aggregate",
        name="Aggregate Sales",
        tags={},
        input_entity_ids=["bronze.sales"],
        output_entity_id="silver.sales_summary",
        output_type="table",
    )
    def aggregate(bronze_sales):
        from pyspark.sql import functions as F
        return bronze_sales.groupBy("product").agg(
            F.sum("amount").alias("total_amount")
        )

    executor = GlobalInjector.get(DataPipesExecuter)
    executor.run_datapipes(["aggregate"])
```

### Pattern 3: Testing with In-Memory Storage

```python
class TestEntityReadPersistStrategy:
    """In-memory strategy for testing"""

    def __init__(self, spark):
        self.spark = spark
        self.data_store = {}      # Input data
        self.written_data = {}    # Output data

    def create_pipe_entity_reader(self, pipe):
        """Return DataFrame from memory"""
        def reader(entity, usewm):
            return self.data_store.get(entity.entityid)
        return reader

    def create_pipe_persist_activator(self, pipe):
        """Capture output to memory"""
        def persister(df):
            self.written_data[pipe.output_entity_id] = df
        return persister
```

---

## Test Commands Reference

```bash
# Poe task runner commands (recommended)
poe test              # Run all tests (unit + integration + system)
poe test-unit         # Run unit tests only
poe test-integration  # Run integration tests only
poe test-quick        # Run unit + integration (skip system tests)
poe test-coverage     # Run tests with HTML coverage report (output: test-results/htmlcov/)

# System tests (require cloud credentials) - flexible filtering
poe test-system                                    # All tests on all platforms
poe test-system --platform fabric                   # Fabric tests only
poe test-system --platform synapse                  # Synapse tests only
poe test-system --platform databricks               # Databricks tests only
poe test-system --test deploy_app_as_job           # Specific test on all platforms
poe test-system --platform fabric --test deploy    # Fabric + deploy tests

# Direct pytest commands (for advanced usage)
pytest tests/ -v                    # All tests
pytest tests/unit/ -v               # Unit tests
pytest tests/integration/ -v        # Integration tests

# Run with coverage report
pytest tests/ --cov=kindling --cov-report=html:test-results/htmlcov --cov-report=term-missing

# Run specific test class
pytest tests/unit/test_data_pipes.py::TestDataPipesRegistration -v

# Run specific test method
pytest tests/integration/test_data_pipes_integration.py::TestDataPipesRegistrationAndExecution::test_register_and_execute_simple_pipe -v

# Run tests matching pattern
pytest tests/ -k "pipe" -v

# Stop on first failure
pytest tests/ -x

# Show print statements
pytest tests/ -v -s

# Run in parallel (requires pytest-xdist)
pytest tests/ -n auto
```

---

## Environment Setup

### Required Dependencies

```bash
# Testing framework
pip install pytest pytest-cov

# Already in pyproject.toml
pip install -e ".[dev]"
```

### Java/Spark Compatibility

- **Current**: Java 21 + PySpark 3.5.5 (verified compatible)
- **Previous issue**: Java 21 + PySpark 3.4.3 (incompatible)
- See: `tests/integration/spark_java_compatibility.md`

---

## Common Issues & Solutions

### Issue: Tests fail with Delta Lake errors
**Solution**: Ensure Delta Spark is installed and Spark config includes Delta extensions
```python
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

### Issue: Tests are slow
**Solution**:
1. Use module-scoped Spark sessions instead of function-scoped
2. Use in-memory data stores instead of file I/O
3. Keep test data small

### Issue: FileNotFoundError in tests
**Solution**: Use temp_workspace fixture and clean up properly

### Issue: Import errors for Azure packages
**Solution**: Install azure dependencies or mock them in unit tests

---

## Testing Best Practices

### DO
- Use `poe test-*` commands for standard test runs
- Use fixtures for common setup/teardown
- Test one thing per test function
- Use descriptive test names (`test_<what>_<when>_<expected>`)
- Clean up resources in teardown
- Use temporary directories for file operations
- Test both success and failure paths
- Mock external dependencies in unit tests
- Use real Spark in integration tests for realistic behavior
- Isolate tests (no shared state)
- Run `poe test-quick` before committing code

### DON'T
- Test external services (cloud APIs) in unit tests
- Leave test data behind
- Write tests that depend on execution order
- Hard-code paths or credentials
- Test implementation details
- Skip cleanup in teardown
- Use real file I/O when in-memory works
- Commit temporary test files

---

## Additional Resources

- **Integration Test Guide**: `tests/integration/README.md`
- **Run Integration Tests**: `tests/integration/run_tests.md`
- **Test Infrastructure Proposal**: `tests/test_infrastructure_proposal.md`
- **Java/Spark Compatibility**: `tests/integration/spark_java_compatibility.md`
- **pytest Documentation**: https://docs.pytest.org/
- **PySpark Testing**: https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html
