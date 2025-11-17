# Data Entities Framework Documentation

## Overview

The Data Entities Framework provides a declarative way to define, register, and manage data entities (typically Delta Lake tables) in a Spark-based data processing environment. It works seamlessly with the Data Pipes Framework to provide a complete data pipeline solution with automatic entity discovery, versioning, and CRUD operations.

## Core Concepts

### Data Entities
Data entities represent structured datasets, typically Delta Lake tables, with defined schemas, partitioning strategies, and merge behaviors. Each entity is registered with metadata that controls how it's stored, accessed, and updated.

### Entity Operations
The framework supports comprehensive data operations including reading, writing, merging, appending, and version management through a pluggable provider system.

### Registry System
All entities are registered in a central registry that enables discovery, metadata lookup, and automated pipeline construction.

## Public Interfaces

### 1. EntityMetadata

```python
@dataclass
class EntityMetadata:
    entityid: str
    name: str
    partition_columns: List[str]
    merge_columns: List[str]
    tags: Dict[str, str]
    schema: Any
```

**Purpose**: Defines comprehensive metadata for a data entity.

**Fields**:
- `entityid`: Unique identifier for the entity
- `name`: Human-readable name for the entity
- `partition_columns`: Columns used for partitioning the underlying table
- `merge_columns`: Columns used as keys for merge operations (typically primary keys)
- `tags`: Key-value pairs for categorization and metadata
- `schema`: Spark schema definition for the entity

### 2. @DataEntities.entity() Decorator

```python
@DataEntities.entity(
    entityid="domain.entity_name",
    name="Human Readable Entity Name",
    partition_columns=["date", "region"],
    merge_columns=["id"],
    tags={"domain": "sales", "tier": "bronze"},
    schema=schema_definition
)
```

**Purpose**: Decorator to register entity definitions with the framework.

**Parameters**: All parameters correspond to `EntityMetadata` fields.

**Usage Notes**:
- All parameters are required
- Returns `None` (used for registration side effects only)
- Must be called at module level for proper registration
- Schema can be a Spark StructType or string representation

### 3. EntityPathLocator (Abstract)

```python
class EntityPathLocator(ABC):
    @abstractmethod
    def get_table_path(self, entity):
        """Get the storage path for an entity"""
        pass
```

**Purpose**: Abstract interface for determining where entity data is stored.

**Implementation Required**: Define how entity IDs map to storage paths (e.g., S3, HDFS, local filesystem).

**Usage**: Typically implemented to support different storage backends or path conventions.

### 4. EntityNameMapper (Abstract)

```python
class EntityNameMapper(ABC):
    @abstractmethod
    def get_table_name(self, entity):
        """Get the table name for an entity"""
        pass
```

**Purpose**: Abstract interface for mapping entity IDs to table names.

**Implementation Required**: Define naming conventions for database tables/views.

**Usage**: Enables flexible naming strategies (e.g., environment prefixes, schema organization).

### 5. EntityProvider (Abstract)

```python
class EntityProvider(ABC):
    @abstractmethod
    def ensure_entity_table(self, entity):
        """Ensure the entity table exists, create if necessary"""
        pass

    @abstractmethod
    def check_entity_exists(self, entity):
        """Check if entity table exists"""
        pass

    @abstractmethod
    def merge_to_entity(self, df, entity):
        """Merge DataFrame into entity using merge columns"""
        pass

    @abstractmethod
    def append_to_entity(self, df, entity):
        """Append DataFrame to entity"""
        pass

    @abstractmethod
    def read_entity(self, entity):
        """Read entire entity as DataFrame"""
        pass

    @abstractmethod
    def read_entity_since_version(self, entity, since_version):
        """Read entity changes since specific version"""
        pass

    @abstractmethod
    def write_to_entity(self, df, entity):
        """Write DataFrame to entity (overwrite)"""
        pass

    @abstractmethod
    def get_entity_version(self, entity):
        """Get current version of entity"""
        pass
```

**Purpose**: Abstract interface defining all data operations for entities.

**Implementation Required**: Concrete implementation for your storage backend (typically Delta Lake).

**Key Operations**:
- **CRUD Operations**: Create, read, update, delete functionality
- **Merge Operations**: Upsert capabilities using defined merge columns
- **Version Management**: Support for time travel and incremental processing
- **Schema Management**: Automatic table creation and schema evolution

### 6. DataEntityRegistry (Abstract)

```python
class DataEntityRegistry(ABC):
    @abstractmethod
    def register_entity(self, entityid, **decorator_params):
        """Register an entity with given parameters"""
        pass

    @abstractmethod
    def get_entity_ids(self):
        """Get all registered entity IDs"""
        pass

    @abstractmethod
    def get_entity_definition(self, name):
        """Get entity definition by ID"""
        pass
```

**Purpose**: Abstract interface for entity registry operations.

**Default Implementation**: `DataEntityManager` provides the concrete implementation.

### 7. DataEntityManager

```python
@GlobalInjector.singleton_autobind()
class DataEntityManager(DataEntityRegistry):
    def get_entity_ids(self):
        """Returns all registered entity IDs"""

    def get_entity_definition(self, name):
        """Returns EntityMetadata for given entity ID"""
```

**Purpose**: Concrete implementation of entity registry with automatic dependency injection.

**Key Features**:
- Singleton pattern with automatic binding
- Thread-safe registry storage
- Runtime entity discovery

## Usage Examples

### Basic Entity Definition

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define schema
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_date", TimestampType(), True),
    StructField("region", StringType(), True)
])

# Register entity
@DataEntities.entity(
    entityid="bronze.customers",
    name="Bronze Layer Customers",
    partition_columns=["region"],
    merge_columns=["customer_id"],
    tags={"layer": "bronze", "domain": "customer", "pii": "true"},
    schema=customer_schema
)
```

### Complex Partitioned Entity

```python
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, DateType

# Sales transaction entity with multiple partitions
sales_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("amount", DecimalType(10, 2), False),
    StructField("transaction_date", DateType(), False),
    StructField("region", StringType(), False),
    StructField("channel", StringType(), False)
])

@DataEntities.entity(
    entityid="silver.sales_transactions",
    name="Silver Layer Sales Transactions",
    partition_columns=["transaction_date", "region"],
    merge_columns=["transaction_id"],
    tags={"layer": "silver", "domain": "sales", "frequency": "daily"},
    schema=sales_schema
)
```

### Working with Registered Entities

```python
# Get the registry from dependency injection
registry = GlobalInjector.get(DataEntityRegistry)

# List all registered entities
all_entities = registry.get_entity_ids()
print(f"Registered entities: {list(all_entities)}")

# Get specific entity definition
customer_entity = registry.get_entity_definition("bronze.customers")
print(f"Entity: {customer_entity.name}")
print(f"Partitions: {customer_entity.partition_columns}")
print(f"Merge keys: {customer_entity.merge_columns}")
print(f"Tags: {customer_entity.tags}")
```

### Entity Operations (with EntityProvider Implementation)

```python
# Assuming you have an EntityProvider implementation
entity_provider = GlobalInjector.get(EntityProvider)
entity_registry = GlobalInjector.get(DataEntityRegistry)

# Get entity definition
entity_def = entity_registry.get_entity_definition("bronze.customers")

# Ensure table exists
entity_provider.ensure_entity_table(entity_def)

# Check if entity exists
exists = entity_provider.check_entity_exists(entity_def)
print(f"Entity exists: {exists}")

# Read entity data
df = entity_provider.read_entity(entity_def)
df.show(10)

# Write new data (overwrite)
new_data = spark.createDataFrame([...], entity_def.schema)
entity_provider.write_to_entity(new_data, entity_def)

# Merge/upsert data
updated_data = spark.createDataFrame([...], entity_def.schema)
entity_provider.merge_to_entity(updated_data, entity_def)

# Append new data
additional_data = spark.createDataFrame([...], entity_def.schema)
entity_provider.append_to_entity(additional_data, entity_def)

# Get current version
version = entity_provider.get_entity_version(entity_def)
print(f"Current version: {version}")

# Read incremental changes
incremental_df = entity_provider.read_entity_since_version(entity_def, version - 5)
```

## Implementation Requirements

To use this framework, you must implement:

1. **EntityProvider**: Core data operations for your storage backend
2. **EntityPathLocator**: Path resolution for your storage system
3. **EntityNameMapper**: Table naming conventions
4. **Schema Definitions**: Spark StructType schemas for all entities

## Common Implementation Patterns

### Delta Lake EntityProvider Example

```python
@GlobalInjector.singleton_autobind()
class DeltaEntityProvider(EntityProvider):
    @inject
    def __init__(self, path_locator: EntityPathLocator, name_mapper: EntityNameMapper):
        self.path_locator = path_locator
        self.name_mapper = name_mapper

    def ensure_entity_table(self, entity):
        if not self.check_entity_exists(entity):
            path = self.path_locator.get_table_path(entity)
            empty_df = spark.createDataFrame([], entity.schema)
            empty_df.write.format("delta").save(path)

    def merge_to_entity(self, df, entity):
        path = self.path_locator.get_table_path(entity)
        delta_table = DeltaTable.forPath(spark, path)

        merge_condition = " AND ".join([
            f"target.{col} = source.{col}"
            for col in entity.merge_columns
        ])

        delta_table.alias("target") \
            .merge(df.alias("source"), merge_condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
```

### Path Locator Example

```python
@GlobalInjector.singleton_autobind()
class S3EntityPathLocator(EntityPathLocator):
    def __init__(self, base_path: str = "s3a://data-lake/"):
        self.base_path = base_path

    def get_table_path(self, entity):
        # Convert entity ID to path: bronze.customers -> bronze/customers
        path_parts = entity.entityid.split('.')
        return f"{self.base_path}/{'/' .join(path_parts)}"
```

## Integration with Data Pipes

The Data Entities Framework integrates seamlessly with the Data Pipes Framework:

```python
# Entity definition
@DataEntities.entity(
    entityid="bronze.raw_sales",
    name="Raw Sales Data",
    partition_columns=["date"],
    merge_columns=["id"],
    tags={"layer": "bronze"},
    schema=raw_sales_schema
)

@DataEntities.entity(
    entityid="silver.clean_sales",
    name="Clean Sales Data",
    partition_columns=["date", "region"],
    merge_columns=["id"],
    tags={"layer": "silver"},
    schema=clean_sales_schema
)

# Pipe using registered entities
@DataPipes.pipe(
    pipeid="clean_sales_data",
    name="Clean Sales Data",
    tags={"category": "cleaning"},
    input_entity_ids=["bronze.raw_sales"],
    output_entity_id="silver.clean_sales",
    output_type="table"
)
def clean_sales_data(bronze_raw_sales):
    return bronze_raw_sales.filter(col("amount") > 0) \
                          .dropDuplicates(["id"])
```

## Best Practices

1. **Entity Naming**: Use hierarchical naming with layer prefixes (bronze.*, silver.*, gold.*)
2. **Partitioning**: Choose partition columns based on query patterns and data distribution
3. **Merge Keys**: Use stable, unique identifiers for merge columns
4. **Schema Evolution**: Design schemas to support evolution (nullable fields, optional columns)
5. **Tagging**: Use consistent tagging for data governance and discovery
6. **Documentation**: Document entity purpose, data sources, and update frequency

## Error Handling

- **Missing Parameters**: Raises `ValueError` if required `EntityMetadata` fields are missing
- **Registry Access**: Automatic initialization of registry through dependency injection
- **Schema Validation**: Depends on EntityProvider implementation
- **Storage Errors**: Handled by concrete EntityProvider implementations

## Data Governance Features

The framework supports data governance through:
- **Metadata Tags**: Categorization and compliance tracking
- **Schema Registry**: Centralized schema management
- **Version Tracking**: Data lineage and change history
- **Access Patterns**: Standardized read/write operations
- **Audit Trail**: Integration with logging and monitoring systems

This framework provides a robust foundation for managing data entities in a lakehouse architecture with strong governance, discoverability, and operational capabilities.
