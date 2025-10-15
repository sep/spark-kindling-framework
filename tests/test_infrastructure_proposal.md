# Test Infrastructure Design Document

## Overview

Based on analysis of the Kindling framework, this document proposes a comprehensive set of test services and utilities to support both unit and integration testing.

## Current State Analysis

### Existing Test Infrastructure

**Notebook Testing (Fabric/Synapse)**
- `test_framework.Notebook` - Comprehensive mock framework for notebook testing
- Mock implementations of all major services
- Works well for %run-based notebook testing

**Unit Testing (Python)**
- `tests/unit/` - Individual component tests with mocks
- `tests/conftest.py` - Shared fixtures
- Good coverage for isolated components

**Integration Testing (Python)**
- `tests/integration/` - End-to-end tests with real Spark
- `TestEntityReadPersistStrategy` - In-memory implementation for data pipes
- Limited infrastructure for other components

### Gaps Identified

1. **No reusable in-memory EntityProvider** for testing
2. **No test watermark service** that avoids Delta table dependencies
3. **No test file ingestion** mock for file-based pipeline testing
4. **No test data builders** for creating realistic test data
5. **No test fixtures library** for common scenarios
6. **Inconsistent patterns** between notebook and Python testing

---

## Proposed Test Services

### 1. **InMemoryEntityProvider**

**Purpose**: Test EntityProvider that stores entities in memory without file/Delta dependencies.

**Features**:
- Store DataFrames in memory by entity ID
- Track versions for each entity
- Support merge, append, and write operations
- No file system or Delta table required
- Fast execution for tests

```python
class InMemoryEntityProvider(EntityProvider):
    """In-memory entity provider for testing"""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session or get_spark_session()
        self.entities = {}  # entity_id -> list of (version, DataFrame)
        self.current_versions = {}  # entity_id -> int
    
    def read_entity(self, entity):
        """Read latest version"""
        
    def write_to_entity(self, df, entity):
        """Write new version"""
        
    def merge_to_entity(self, df, entity):
        """Merge using entity merge columns"""
        
    def append_to_entity(self, df, entity):
        """Append data"""
        
    def get_entity_version(self, entity):
        """Get current version number"""
        
    def read_entity_since_version(self, entity, since_version):
        """Read changes since version"""
        
    def check_entity_exists(self, entity):
        """Check if entity has data"""
        
    def clear_entity(self, entity_id):
        """Clear entity for test cleanup"""
        
    def get_all_versions(self, entity_id):
        """Get all versions for assertions"""
```

**Location**: `tests/test_helpers/in_memory_entity_provider.py`

---

### 2. **InMemoryWatermarkService**

**Purpose**: Watermark service that stores watermarks in memory instead of Delta tables.

**Features**:
- Track watermarks without file dependencies
- Support all watermark operations
- Easy assertions on watermark state
- Fast test execution

```python
class InMemoryWatermarkService(WatermarkService):
    """In-memory watermark service for testing"""
    
    def __init__(self, entity_provider=None):
        self.watermarks = {}  # (source_id, reader_id) -> watermark data
        self.ep = entity_provider  # Optional for read_current_entity_changes
    
    def get_watermark(self, source_entity_id, reader_id):
        """Get watermark value"""
        
    def save_watermark(self, source_entity_id, reader_id, 
                      last_version_processed, last_execution_id):
        """Save watermark"""
        
    def read_current_entity_changes(self, entity, pipe):
        """Read changes since last watermark"""
        
    def clear_watermark(self, source_entity_id, reader_id):
        """Clear for test cleanup"""
        
    def get_all_watermarks(self):
        """Get all watermarks for assertions"""
```

**Location**: `tests/test_helpers/in_memory_watermark_service.py`

---

### 3. **TestDataBuilder**

**Purpose**: Fluent API for creating realistic test data with common patterns.

**Features**:
- Builder pattern for DataFrames
- Pre-defined entity types (sales, customers, products, etc.)
- Date/time generation helpers
- Realistic fake data generation
- Relationship builders (parent/child entities)

```python
class TestDataBuilder:
    """Builder for creating test DataFrames with realistic data"""
    
    def __init__(self, spark):
        self.spark = spark
        self.data = []
        self.schema = None
    
    @classmethod
    def sales_data(cls, spark, num_rows=10):
        """Create sales transaction data"""
        
    @classmethod
    def customer_data(cls, spark, num_rows=5):
        """Create customer dimension data"""
        
    @classmethod  
    def product_data(cls, spark, num_rows=8):
        """Create product dimension data"""
        
    @classmethod
    def time_series_data(cls, spark, start_date, end_date, freq='D'):
        """Create time series data"""
        
    def with_null_values(self, columns, null_percentage=0.1):
        """Add null values to specific columns"""
        
    def with_duplicates(self, key_columns, duplicate_count=2):
        """Add duplicate rows based on keys"""
        
    def with_partition_skew(self, partition_column):
        """Create skewed partition distribution"""
        
    def build(self):
        """Return the DataFrame"""
```

**Location**: `tests/test_helpers/test_data_builder.py`

---

### 4. **TestFixtureLibrary**

**Purpose**: Pre-built test fixtures for common scenarios.

**Features**:
- Entity metadata fixtures
- Pipe metadata fixtures
- Sample entity registries
- Common test configurations

```python
# Entity Fixtures
@pytest.fixture
def bronze_sales_entity():
    """Bronze layer sales entity metadata"""
    
@pytest.fixture
def silver_sales_entity():
    """Silver layer sales entity metadata"""
    
@pytest.fixture
def gold_sales_summary_entity():
    """Gold layer aggregated entity metadata"""

# Registry Fixtures
@pytest.fixture
def populated_entity_registry():
    """Entity registry with common bronze/silver/gold entities"""
    
@pytest.fixture
def populated_pipes_registry():
    """Pipes registry with sample transformations"""

# Data Fixtures
@pytest.fixture
def sample_sales_df(spark):
    """Sample sales DataFrame"""
    
@pytest.fixture
def sample_customer_df(spark):
    """Sample customer DataFrame"""

# Strategy Fixtures
@pytest.fixture
def in_memory_strategy(spark):
    """Complete in-memory read-persist strategy"""
```

**Location**: `tests/test_helpers/fixtures.py`

---

### 5. **TestPipelineBuilder**

**Purpose**: Builder for creating complete test pipelines with entities and pipes.

**Features**:
- Fluent API for pipeline definition
- Automatic entity registration
- Automatic pipe registration
- Built-in test data generation

```python
class TestPipelineBuilder:
    """Builder for creating complete test pipelines"""
    
    def __init__(self, spark, entity_registry, pipes_registry):
        self.spark = spark
        self.entity_registry = entity_registry
        self.pipes_registry = pipes_registry
        self.entities = []
        self.pipes = []
        
    def add_bronze_entity(self, entity_id, schema, test_data=None):
        """Add bronze entity with optional test data"""
        
    def add_silver_entity(self, entity_id, schema):
        """Add silver entity"""
        
    def add_transformation_pipe(self, pipe_id, name, transform_func,
                               input_entities, output_entity):
        """Add transformation pipe"""
        
    def with_test_data(self, entity_id, data):
        """Add test data for entity"""
        
    def build(self):
        """Register all entities and pipes"""
        
    def get_test_data_store(self):
        """Get dictionary of entity_id -> DataFrame for testing"""
```

**Location**: `tests/test_helpers/test_pipeline_builder.py`

---

### 6. **MockServicesFactory**

**Purpose**: Factory for creating consistently mocked services.

**Features**:
- Pre-configured mocks with sensible defaults
- Easy customization for specific tests
- Consistent mock behavior across tests

```python
class MockServicesFactory:
    """Factory for creating test mocks"""
    
    @staticmethod
    def create_logger_provider():
        """Create mock logger provider"""
        
    @staticmethod
    def create_trace_provider():
        """Create mock trace provider with span support"""
        
    @staticmethod
    def create_config_service(config_dict=None):
        """Create mock config service"""
        
    @staticmethod
    def create_entity_name_mapper(mapping=None):
        """Create mock entity name mapper"""
        
    @staticmethod
    def create_path_locator(base_path="/tmp/test"):
        """Create mock path locator"""
        
    @staticmethod
    def create_full_mock_stack(spark=None):
        """Create complete set of mocked services"""
```

**Location**: `tests/test_helpers/mock_services.py`

---

### 7. **AssertionHelpers**

**Purpose**: Custom assertions for common test patterns.

**Features**:
- DataFrame comparison utilities
- Entity state assertions
- Watermark assertions
- Pipe execution assertions

```python
class DataFrameAssertions:
    """Custom assertions for DataFrames"""
    
    @staticmethod
    def assert_dataframes_equal(df1, df2, check_schema=True, check_order=False):
        """Assert two DataFrames are equal"""
        
    @staticmethod
    def assert_schema_equal(df, expected_schema):
        """Assert DataFrame schema matches"""
        
    @staticmethod
    def assert_row_count(df, expected_count):
        """Assert DataFrame row count"""
        
    @staticmethod
    def assert_column_values(df, column, expected_values):
        """Assert column contains expected values"""

class EntityAssertions:
    """Assertions for entity state"""
    
    @staticmethod
    def assert_entity_exists(entity_provider, entity_id):
        """Assert entity exists"""
        
    @staticmethod
    def assert_entity_version(entity_provider, entity_id, expected_version):
        """Assert entity version"""
        
    @staticmethod
    def assert_entity_row_count(entity_provider, entity_id, expected_count):
        """Assert entity has expected rows"""

class WatermarkAssertions:
    """Assertions for watermarks"""
    
    @staticmethod
    def assert_watermark_set(watermark_service, source_id, reader_id):
        """Assert watermark exists"""
        
    @staticmethod
    def assert_watermark_value(watermark_service, source_id, reader_id, expected_value):
        """Assert watermark value"""
```

**Location**: `tests/test_helpers/assertions.py`

---

### 8. **IntegrationTestBase**

**Purpose**: Base class for integration tests with common setup.

**Features**:
- Automatic Spark session management
- Common fixture setup
- Cleanup utilities
- Test isolation

```python
class IntegrationTestBase:
    """Base class for integration tests"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Spark session for integration tests"""
        
    @pytest.fixture
    def entity_registry(self):
        """Fresh entity registry for each test"""
        
    @pytest.fixture
    def pipes_registry(self, mock_logger_provider):
        """Fresh pipes registry for each test"""
        
    @pytest.fixture
    def in_memory_entity_provider(self, spark):
        """In-memory entity provider"""
        
    @pytest.fixture
    def in_memory_watermark_service(self):
        """In-memory watermark service"""
        
    @pytest.fixture
    def test_pipeline_builder(self, spark, entity_registry, pipes_registry):
        """Pipeline builder for tests"""
        
    @pytest.fixture
    def mock_services(self):
        """Complete set of mock services"""
```

**Location**: `tests/test_helpers/integration_test_base.py`

---

## Directory Structure

```
tests/
├── test_helpers/              # NEW: Test infrastructure
│   ├── __init__.py
│   ├── in_memory_entity_provider.py
│   ├── in_memory_watermark_service.py
│   ├── test_data_builder.py
│   ├── test_pipeline_builder.py
│   ├── mock_services.py
│   ├── assertions.py
│   ├── fixtures.py
│   └── integration_test_base.py
├── unit/
│   ├── conftest.py           # UPDATED: Import test_helpers
│   ├── test_data_pipes.py
│   ├── test_data_entities.py
│   └── ...
├── integration/
│   ├── conftest.py           # UPDATED: Import test_helpers
│   ├── test_data_pipes_integration.py
│   └── ...
└── conftest.py               # UPDATED: Root fixtures
```

---

## Usage Examples

### Example 1: Simple Unit Test with Mocks

```python
from tests.test_helpers.mock_services import MockServicesFactory

def test_pipe_registration():
    # Use factory to create mocks
    mocks = MockServicesFactory.create_full_mock_stack()
    
    registry = DataPipesManager(mocks['logger_provider'])
    registry.register_pipe("test_pipe", ...)
    
    assert registry.get_pipe_definition("test_pipe") is not None
```

### Example 2: Integration Test with In-Memory Providers

```python
from tests.test_helpers import (
    InMemoryEntityProvider,
    InMemoryWatermarkService,
    TestDataBuilder
)

def test_pipe_execution_with_watermarks(spark):
    # Setup providers
    entity_provider = InMemoryEntityProvider(spark)
    watermark_service = InMemoryWatermarkService(entity_provider)
    
    # Create test data
    sales_df = TestDataBuilder.sales_data(spark, num_rows=100)
    
    # Execute pipeline
    ...
    
    # Assertions
    assert watermark_service.get_watermark("bronze.sales", "my_pipe") == 1
```

### Example 3: Integration Test with Pipeline Builder

```python
from tests.test_helpers import TestPipelineBuilder, DataFrameAssertions

def test_complete_pipeline(spark, entity_registry, pipes_registry):
    # Build pipeline
    builder = TestPipelineBuilder(spark, entity_registry, pipes_registry)
    builder.add_bronze_entity("bronze.sales", sales_schema, test_data=sales_data)
    builder.add_silver_entity("silver.clean_sales", clean_schema)
    builder.add_transformation_pipe("cleanse", "Cleanse Sales", 
                                   cleanse_func, ["bronze.sales"], 
                                   "silver.clean_sales")
    builder.build()
    
    # Execute
    executor.run_datapipes(["cleanse"])
    
    # Assert
    DataFrameAssertions.assert_row_count(
        entity_provider.read_entity("silver.clean_sales"), 
        100
    )
```

---

## Implementation Priority

### Phase 1: Core Infrastructure (Week 1)
1. ✅ InMemoryEntityProvider
2. ✅ InMemoryWatermarkService  
3. ✅ MockServicesFactory
4. ✅ Basic fixtures

### Phase 2: Builders and Helpers (Week 2)
5. ✅ TestDataBuilder
6. ✅ TestPipelineBuilder
7. ✅ AssertionHelpers

### Phase 3: Integration (Week 3)
8. ✅ IntegrationTestBase
9. ✅ Update existing tests to use new infrastructure
10. ✅ Documentation and examples

---

## Benefits

1. **Consistency**: Same patterns across unit and integration tests
2. **Speed**: In-memory providers are much faster than Delta tables
3. **Isolation**: Tests don't interfere with each other
4. **Maintainability**: Centralized test infrastructure
5. **Reusability**: Common patterns available to all tests
6. **Clarity**: Tests focus on business logic, not test setup
7. **Coverage**: Easier to test edge cases and error scenarios

---

## Migration Strategy

### For Existing Tests
1. Keep existing tests working
2. Gradually migrate to new infrastructure
3. Create side-by-side examples
4. Document migration patterns

### For New Tests
1. Use new infrastructure by default
2. Provide templates and examples
3. Include in test documentation

---

## Next Steps

1. **Review and feedback** on this proposal
2. **Create prototype** of InMemoryEntityProvider
3. **Test with existing integration tests**
4. **Iterate based on feedback**
5. **Full implementation** following priority order
6. **Documentation and training**

---

**Status**: Proposal  
**Date**: October 15, 2025  
**Author**: GitHub Copilot  
**Version**: 1.0
