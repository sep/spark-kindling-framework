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

# Specific test file (use pytest directly)
pytest tests/integration/test_data_pipes_integration.py -v
```

### Current Test Status

**Unit Tests**
- 24 of 25 tests passing
- Coverage: 26% overall, 57% on platform_local.py

**Integration Tests**
- 10 data_pipes integration tests - all passing ✅
- Coverage: 91% on data_pipes.py with integration tests

---

## Test Structure

```
tests/
├── conftest.py                          # Root fixtures
├── spark_test_helper.py                 # Spark session utilities
├── unit/                                # Unit tests (isolated components)
│   ├── conftest.py
│   ├── test_platform_local.py          # 25 tests for LocalService
│   ├── test_data_entities.py
│   ├── test_data_pipes.py
│   └── ...
├── integration/                         # Integration tests (end-to-end)
│   ├── conftest.py
│   ├── README.md                        # Integration test philosophy
│   ├── run_tests.md                     # How to run integration tests
│   ├── spark_java_compatibility.md      # Java/Spark compatibility guide
│   ├── test_data_pipes_integration.py   # 10 tests, all passing
│   └── ...
└── test_infrastructure_proposal.md      # Proposed test utilities
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
        pipe_id="test_pipe",
        name="Test Pipe",
        transform=lambda df: df
    )

    # Verify registration
    pipe_def = manager.get_pipe_definition("test_pipe")
    assert pipe_def is not None
    assert pipe_def.name == "Test Pipe"
```

### Pattern 2: Integration Test with Real Spark

```python
from kindling.data_entities import DataEntityManager
from kindling.data_pipes import DataPipesManager, DataPipesExecutor

def test_end_to_end_pipeline(spark):
    """Test complete pipeline execution"""
    # Setup
    entity_registry = DataEntityManager()
    pipes_registry = DataPipesManager(mock_logger)

    # Register entity
    entity_registry.register_entity(
        entity_id="bronze.sales",
        layer="bronze",
        entity_type="fact"
    )

    # Create test data
    test_df = spark.createDataFrame([
        (1, "Product A", 100.0),
        (2, "Product B", 200.0)
    ], ["id", "product", "amount"])

    # Register and execute pipe
    @pipes_registry.register_transform_pipe(
        pipe_id="aggregate",
        name="Aggregate Sales",
        output_entity="silver.sales_summary"
    )
    def aggregate(bronze_sales):
        from pyspark.sql import functions as F
        return bronze_sales.groupBy("product").agg(
            F.sum("amount").alias("total_amount")
        )

    # Execute
    executor = DataPipesExecutor(
        entity_registry=entity_registry,
        pipes_registry=pipes_registry,
        entity_provider=in_memory_provider,
        strategy=test_strategy
    )
    executor.run_datapipes(["aggregate"])

    # Verify
    result = in_memory_provider.read_entity("silver.sales_summary")
    assert result.count() == 2
```

### Pattern 3: Testing with In-Memory Storage

```python
class TestEntityReadPersistStrategy:
    """In-memory strategy for testing"""

    def __init__(self, spark):
        self.spark = spark
        self.data_store = {}      # Input data
        self.written_data = {}    # Output data

    def create_pipe_entity_reader(self, entity, pipe):
        """Return DataFrame from memory"""
        def reader():
            return self.data_store.get(entity.entity_id)
        return reader

    def create_pipe_persist_activator(self, entity, pipe):
        """Capture output to memory"""
        def persister(df):
            self.written_data[entity.entity_id] = df
        return persister
```

---

## Test Coverage Goals

### Priority 1: Core Components (Target: 80%+)
- ✅ `data_pipes.py` - 91% with integration tests
- ✅ `platform_local.py` - 57% with unit tests
- 🔄 `data_entities.py` - In progress
- 🔄 `injection.py` - Planned

### Priority 2: Data Layer (Target: 70%+)
- 🔄 `delta_entity_provider.py`
- 🔄 `watermarking.py`
- 🔄 `simple_read_persist_strategy.py`

### Priority 3: Processing & Observability (Target: 60%+)
- 🔄 `simple_stage_processor.py`
- 🔄 `file_ingestion.py`
- 🔄 `spark_log.py`
- 🔄 `spark_trace.py`

---

## Testing Best Practices

### DO ✅
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

### DON'T ❌
- Test external services (cloud APIs) in unit tests
- Leave test data behind
- Write tests that depend on execution order
- Hard-code paths or credentials
- Test implementation details
- Skip cleanup in teardown
- Use real file I/O when in-memory works
- Commit temporary test files

---

## Test Commands Reference

```bash
# Poe task runner commands (recommended)
poe test              # Run all tests (unit + integration + system)
poe test-unit         # Run unit tests only
poe test-integration  # Run integration tests only
poe test-quick        # Run unit + integration (skip system tests)
poe test-coverage     # Run tests with HTML coverage report

# System tests (require cloud credentials)
poe test-system           # All system tests
poe test-system-fabric    # Fabric-specific system tests
poe test-system-synapse   # Synapse-specific system tests
poe test-system-databricks # Databricks-specific system tests

# Direct pytest commands (for advanced usage)
pytest tests/ -v                    # All tests
pytest tests/unit/ -v               # Unit tests
pytest tests/integration/ -v        # Integration tests

# Run with coverage report
pytest tests/ --cov=packages/kindling --cov-report=html --cov-report=term-missing

# Run specific test class
pytest tests/unit/test_platform_local.py::TestLocalServiceFileOperations -v

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

- **Current**: Java 21 + PySpark 3.5.5 ✅
- **Previous issue**: Java 21 + PySpark 3.4.3 ❌ (incompatible)
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

## Next Steps

1. **Expand unit test coverage** for core components
2. **Add integration tests** for watermarking and Delta operations
3. **Create test utilities module** (see `test_infrastructure_proposal.md`)
4. **Set up CI/CD** with automated test runs
5. **Document test patterns** specific to each component type

---

## Additional Resources

- **Integration Test Guide**: `tests/integration/README.md`
- **Run Integration Tests**: `tests/integration/run_tests.md`
- **Test Infrastructure Proposal**: `tests/test_infrastructure_proposal.md`
- **Java/Spark Compatibility**: `tests/integration/spark_java_compatibility.md`
- **pytest Documentation**: https://docs.pytest.org/
- **PySpark Testing**: https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html
