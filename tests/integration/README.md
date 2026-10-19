# Data Pipes Integration Tests

## Overview

Integration tests for data pipes test **end-to-end workflows** with real or realistic components, unlike unit tests which mock everything. They verify that the entire system works together correctly.

## Key Differences: Unit vs Integration Tests

### Unit Tests (`tests/unit/test_data_pipes.py`)
- ✅ Mock all dependencies (Spark, registries, logger)
- ✅ Test individual components in isolation
- ✅ Fast execution (~1-2 seconds)
- ✅ Can run anywhere without external dependencies
- ✅ Focus on code correctness and edge cases
- ✅ 40 tests achieving 94% coverage

### Integration Tests (`tests/integration/test_data_pipes_integration.py`)
- 🔄 Use **real Spark sessions and DataFrames**
- 🔄 Test complete workflows from registration → execution → output
- 🔄 Slower execution (~5-30 seconds per test)
- 🔄 Requires Spark environment
- 🔄 Focus on system behavior and data flow
- 🔄 10 comprehensive end-to-end scenarios

## Integration Test Categories

### 1. **Complete Registration and Execution** (`TestDataPipesRegistrationAndExecution`)

Tests the full lifecycle:
```python
- Register entities → Create data → Define pipe → Execute → Verify output
```

**Scenarios:**
- **Simple pipe** with single input (aggregation)
- **Multi-input pipes** (joins between datasets)
- **Pipe chains** where output of one feeds another
- **Decorator-based registration** using `@DataPipes.pipe`

**What's Being Tested:**
- Entity metadata registration works with real schemas
- Real Spark DataFrames are transformed correctly
- Data flows through the entire pipeline
- Output is written to the correct location
- Business logic produces expected results

### 2. **Edge Cases and Error Handling** (`TestPipeExecutionEdgeCases`)

**Scenarios:**
- **Missing input data** - pipes skip gracefully
- **Empty DataFrames** - transformations handle zero rows
- **Multiple independent pipes** - parallel execution

**What's Being Tested:**
- Robustness when data is unavailable
- Empty dataset handling
- Execution orchestration for multiple pipes

### 3. **Observability Integration** (`TestTraceIntegration`)

**Scenarios:**
- Trace span creation during execution
- Span hierarchy (outer + inner spans)
- Metadata propagation (component, operation, details)

**What's Being Tested:**
- Tracing instrumentation is properly called
- Span context includes correct metadata
- Observability hooks don't interfere with execution

### 4. **Complex Transformations** (`TestComplexTransformations`)

**Scenarios:**
- **Window functions** with partitioning and ranking
- **Advanced Spark operations** (aggregations, filters, joins)

**What's Being Tested:**
- Real Spark transformation logic
- Complex business rules
- Performance with realistic data volumes

### 5. **Entity Key Transformation** (`TestEntityKeyTransformation`)

**Scenarios:**
- Entity IDs with dots (`bronze.raw.sales`)
- Parameter name conversion (`bronze_raw_sales`)

**What's Being Tested:**
- Naming conventions are correctly applied
- Function signatures match transformed names
- Data flows with correct parameter mapping

## Test Strategy

### Test Implementation Pattern

```python
class TestEntityReadPersistStrategy(EntityReadPersistStrategy):
    """In-memory implementation for testing"""

    def __init__(self, spark, data_store):
        self.data_store = {}  # entity_id -> DataFrame
        self.written_data = {}  # Capture outputs

    def create_pipe_entity_reader(self, pipe):
        """Read from in-memory store"""
        def reader(entity, is_first):
            return self.data_store.get(entity.entityid)
        return reader

    def create_pipe_persist_activator(self, pipe):
        """Write to in-memory store for verification"""
        def activator(df):
            self.written_data[pipe.output_entity_id] = df
            df.count()  # Trigger action
        return activator
```

**Key Design Choices:**
1. **In-memory data store** - No file I/O, faster execution
2. **Real DataFrames** - Actual Spark transformations
3. **Output capture** - Verify results programmatically
4. **Fixture isolation** - Each test gets fresh state

### Typical Test Flow

```python
def test_pipe_execution(spark, entity_registry, pipes_registry):
    # 1. Setup: Create test data
    df = spark.createDataFrame([...], schema)
    data_store = {"bronze.sales": df}

    # 2. Define transformation
    def my_transformation(bronze_sales):
        return bronze_sales.filter("amount > 100")

    # 3. Register pipe
    pipes_registry.register_pipe(
        "my_pipe",
        execute=my_transformation,
        input_entity_ids=["bronze.sales"],
        output_entity_id="silver.sales"
    )

    # 4. Execute
    executor = DataPipesExecuter(...)
    executor.run_datapipes(["my_pipe"])

    # 5. Verify results
    result = strategy.written_data["silver.sales"]
    assert result.count() == expected_count
    assert result.collect()[0]["amount"] > 100
```

## Real-World Integration Test Scenarios

### Scenario 1: Data Quality Pipeline
```python
def test_data_quality_pipeline():
    """
    Bronze (raw) → Silver (cleaned) → Gold (aggregated)
    """
    # Bronze: Raw data with nulls, duplicates
    # Silver: Deduplicated, filled nulls
    # Gold: Business metrics
```

### Scenario 2: Multi-Source Join
```python
def test_multi_source_enrichment():
    """
    Sales + Customers + Products → Enriched Sales
    """
    # Verify join keys work correctly
    # Verify all expected columns present
    # Verify no data loss
```

### Scenario 3: Incremental Processing
```python
def test_incremental_load():
    """
    Read watermark → Process new data → Update watermark
    """
    # Verify only new records processed
    # Verify watermark advances correctly
```

### Scenario 4: Error Recovery
```python
def test_pipe_skip_on_missing_data():
    """
    Missing input → Skip gracefully → Log warning
    """
    # Verify no exception raised
    # Verify appropriate logging
    # Verify downstream not affected
```

## Running Integration Tests

### Prerequisites
- Apache Spark installed
- PySpark accessible
- Sufficient memory (2GB+)
- A Java runtime matching the selected Spark lane; use the repository devcontainer
  and [current testing guide](../../docs/contributing/testing.md) rather than the historical Java troubleshooting matrix.

### Execution

```bash
# Run all integration tests
poe test-integration

# Run specific test class
PYTEST_ADDOPTS="-k TestDataPipesRegistrationAndExecution" poe test-integration

# Run with coverage
poe test-integration  # repository coverage defaults apply

# Run single test
PYTEST_ADDOPTS="-k test_register_and_execute_simple_pipe" poe test-integration
```

### Environment Setup

For environments where Spark requires specific configuration:

```python
@pytest.fixture(scope="module")
def spark():
    """Configure Spark for test environment"""
    spark = SparkSession.builder \
        .appName("IntegrationTest") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()

    yield spark
    spark.stop()
```

## Benefits of Integration Tests

### 1. **Catch Integration Issues**
- Component A and B work individually but fail together
- Configuration mismatches
- Data format incompatibilities

### 2. **Validate Real Transformations**
- Spark SQL correctness
- Join logic accuracy
- Aggregation results
- Window function behavior

### 3. **Performance Insights**
- Identify slow operations
- Optimize Spark configurations
- Test with realistic data volumes

### 4. **Confidence in Releases**
- End-to-end workflows validated
- Real-world scenarios covered
- Regression prevention

## Best Practices

### ✅ DO:
- Use realistic test data that represents production scenarios
- Test complete workflows, not just happy paths
- Verify both success and failure scenarios
- Use in-memory strategies to avoid I/O overhead
- Test with small datasets (10-1000 rows) for speed
- Separate integration tests from unit tests
- Use module-scoped fixtures for Spark sessions

### ❌ DON'T:
- Create actual files/tables unless testing I/O specifically
- Use production-sized datasets (millions of rows)
- Test internal implementation details (that's for unit tests)
- Run integration tests on every code save (too slow)
- Mock Spark DataFrames (defeats the purpose)
- Leave Spark sessions running (always cleanup)

## Test Data Patterns

### Pattern 1: Minimal Representative Data
```python
# Just enough to verify logic
sales_data = spark.createDataFrame([
    (1, "Widget", 100),
    (2, "Gadget", 200),
], ["id", "product", "amount"])
```

### Pattern 2: Edge Case Data
```python
# Include boundaries
edge_cases = spark.createDataFrame([
    (1, None, 0),      # Null product, zero amount
    (2, "", -100),     # Empty string, negative
    (3, "A" * 1000, 1e9),  # Very long, very large
], ["id", "product", "amount"])
```

### Pattern 3: Multi-Entity Data
```python
# Related datasets for joins
sales = spark.createDataFrame([...])
customers = spark.createDataFrame([...])
products = spark.createDataFrame([...])
```

## Debugging Integration Tests

### When Tests Fail:

1. **Check Spark session** - Is it starting correctly?
2. **Inspect DataFrames** - Use `df.show()` to see actual data
3. **Verify schemas** - Schema mismatches cause cryptic errors
4. **Check logs** - Spark logs reveal execution issues
5. **Run interactively** - Use `PYTEST_ADDOPTS="--pdb" poe test-integration` to debug

### Common Issues:

- **Java version** - Spark 3.x requires Java 8 or 11
- **Memory** - Increase driver/executor memory
- **Partitions** - Too many partitions slow down small tests
- **Cleanup** - Ensure Spark sessions are stopped

## Future Enhancements

### Additional Test Scenarios:
- [ ] Streaming pipe execution
- [ ] Delta Lake merge operations
- [ ] Partition pruning optimization
- [ ] Schema evolution handling
- [ ] Concurrent pipe execution
- [ ] Resource cleanup verification
- [ ] Performance benchmarking
- [ ] Large dataset handling (1M+ rows)

### Test Infrastructure:
- [ ] Docker-based Spark cluster for CI/CD
- [ ] Shared test data fixtures
- [ ] Performance regression tracking
- [ ] Test data generators
- [ ] Visual result comparison tools

## Summary

Integration tests for data pipes provide **critical validation** that components work together correctly. While unit tests ensure individual pieces are correct, integration tests prove the **entire system functions as expected** with real data and transformations.

The test suite demonstrates:
- ✅ Complete pipe registration and execution workflows
- ✅ Multi-input pipe orchestration (joins)
- ✅ Pipe chaining (output → input)
- ✅ Decorator-based registration
- ✅ Edge case handling (missing data, empty DataFrames)
- ✅ Tracing integration
- ✅ Complex Spark transformations (window functions, aggregations)
- ✅ Entity naming conventions

**Next Steps:** Adapt these tests to your specific environment and add scenarios that match your production workflows.
