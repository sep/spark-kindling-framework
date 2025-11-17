# Integration Tests - Quick Run Guide

## Quick Commands

```bash
# Run all integration tests
pytest tests/integration/test_data_pipes_integration.py -v

# Run specific test
pytest tests/integration/test_data_pipes_integration.py::TestDataPipesRegistrationAndExecution::test_register_and_execute_simple_pipe -v

# Run with coverage
pytest tests/integration/test_data_pipes_integration.py -v --cov=src/kindling/data_pipes --cov-report=term-missing
```

## Available Test Classes

1. **TestDataPipesRegistrationAndExecution**
   - `test_register_and_execute_simple_pipe` - Basic aggregation pipeline
   - `test_pipe_with_multiple_inputs` - Join multiple datasets
   - `test_pipe_chain_execution` - Chain pipes (bronze → silver → gold)
   - `test_pipe_execution_with_decorator` - Using @DataPipes.pipe decorator

2. **TestPipeExecutionEdgeCases**
   - `test_pipe_execution_with_missing_input_entity` - Graceful skip
   - `test_pipe_execution_with_empty_dataframe` - Handle empty data
   - `test_multiple_pipes_execution_in_sequence` - Multiple independent pipes

3. **TestTraceIntegration**
   - `test_pipe_execution_creates_trace_spans` - Observability integration

4. **TestComplexTransformations**
   - `test_pipe_with_window_functions` - Window functions with ranking

5. **TestEntityKeyTransformation**
   - `test_entity_keys_with_dots_become_underscores` - Entity naming conventions

## Run Individual Test Classes

```bash
# Registration and execution tests
pytest tests/integration/test_data_pipes_integration.py::TestDataPipesRegistrationAndExecution -v

# Edge cases
pytest tests/integration/test_data_pipes_integration.py::TestPipeExecutionEdgeCases -v

# Trace integration
pytest tests/integration/test_data_pipes_integration.py::TestTraceIntegration -v

# Complex transformations
pytest tests/integration/test_data_pipes_integration.py::TestComplexTransformations -v

# Entity key transformation
pytest tests/integration/test_data_pipes_integration.py::TestEntityKeyTransformation -v
```

## Useful Options

```bash
# Show print statements
pytest ... -s

# Stop on first failure
pytest ... -x

# Show full traceback
pytest ... --tb=long

# Show only test names (quiet)
pytest ... -q

# Run tests matching pattern
pytest ... -k "chain"

# Drop into debugger on failure
pytest ... --pdb

# Show slowest tests
pytest ... --durations=10
```

## Common Workflows

### Development Workflow
```bash
# Make changes to code...

# Run specific test you're working on
pytest tests/integration/test_data_pipes_integration.py::TestDataPipesRegistrationAndExecution::test_pipe_chain_execution -v -s

# If it passes, run all tests
pytest tests/integration/test_data_pipes_integration.py -v
```

### Debug a Failing Test
```bash
# Run with full output and debugger
pytest tests/integration/test_data_pipes_integration.py::TestName::test_name -v -s --tb=long --pdb
```

### Check Coverage
```bash
# Run all integration tests with coverage
pytest tests/integration/test_data_pipes_integration.py -v --cov=src/kindling --cov-report=html

# Open coverage report
open output/htmlcov/index.html  # macOS
xdg-open output/htmlcov/index.html  # Linux
```

## Environment Requirements

- ✅ Java 21 (OpenJDK 21.0.8)
- ✅ PySpark 3.5.5
- ✅ Delta Spark 3.3.2
- ✅ Dependencies: injector, dynaconf, azure-storage-blob, azure-identity, azure-core

## Troubleshooting

### "No module named 'injector'"
```bash
pip install injector dynaconf azure-storage-blob azure-identity azure-core
```

### "Java/Spark compatibility error"
- Ensure Java 21 is installed
- Ensure PySpark 3.5.5 is installed
- Run: `java -version` and `python -c "import pyspark; print(pyspark.__version__)"`

### Tests hang or timeout
- Check Spark session is created correctly
- Reduce data volume in tests
- Check system resources (memory/CPU)

### Coverage not working
```bash
pip install pytest-cov
```

## Examples

### Run one test with output
```bash
cd /workspace
pytest tests/integration/test_data_pipes_integration.py::TestDataPipesRegistrationAndExecution::test_register_and_execute_simple_pipe -v -s
```

### Run all tests with coverage
```bash
cd /workspace
pytest tests/integration/test_data_pipes_integration.py -v --cov=src/kindling/data_pipes --cov-report=term-missing
```

### Debug failing test
```bash
cd /workspace
pytest tests/integration/test_data_pipes_integration.py::TestName::test_name -v --tb=long --pdb
```

---

**Last Updated**: October 15, 2025
**Status**: All 10 tests passing ✅
