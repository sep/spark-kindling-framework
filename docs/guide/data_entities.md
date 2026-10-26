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
    merge_columns: List[str]
    tags: Dict[str, str]
    schema: Any
    partition_columns: List[str] = field(default_factory=list)
    cluster_columns: List[str] = field(default_factory=list)
    sql: Optional[str] = None
```

**Purpose**: Defines comprehensive metadata for a data entity.

**Fields**:
- `entityid`: Unique identifier for the entity
- `name`: Human-readable name for the entity
- `partition_columns`: Optional columns used for file partitioning the underlying table (Delta `partitionBy`)
- `cluster_columns`: Optional clustering columns (for engines that support liquid clustering / `CLUSTER BY`)
- `merge_columns`: Columns used as keys for merge operations (typically primary keys)
- `tags`: Key-value pairs for categorization and metadata
- `schema`: Spark schema definition for the entity
- `sql`: Resolved SQL body for SQL-defined entities; set by `DataEntities.sql_entity()`, `None` otherwise

### 2. DataEntities.entity() Declaration

```python
DataEntities.entity(
    entityid="domain.entity_name",
    name="Human Readable Entity Name",
    partition_columns=["date", "region"],
    merge_columns=["id"],
    tags={"domain": "sales", "tier": "bronze"},
    schema=schema_definition
)
```

**Purpose**: Registers an entity definition with the framework at call time.

**Parameters**: All parameters correspond to `EntityMetadata` fields.

**Usage Notes**:
- `entityid`, `name`, `merge_columns`, `tags`, and `schema` are required; a missing one raises `ValueError` (pass `merge_columns=[]` for an entity without keys)
- `partition_columns` and `cluster_columns` are optional (default: `[]`)
- Returns an identity decorator, so the call can also be written as `@DataEntities.entity(...)` above a function or class; registration happens either way
- Must be called at module level for proper registration
- Schema must be a Spark `StructType`, or `None` to let the first write define the table. String (DDL) schemas are not supported: SCD2 validation and schema augmentation, `add_columns`, and the `derived.replace_keys` check all need a `StructType`

### 3. @DataEntities.sql_entity() Decorator

```python
@DataEntities.sql_entity(
    entityid="reporting.recent_sales",
    name="recent_sales",
    tags={"layer": "reporting"},
    sql="SELECT * FROM sales.transactions WHERE event_date >= current_date() - 30",
)
```

Or via a bundled package resource:

```python
@DataEntities.sql_entity(
    entityid="reporting.recent_sales",
    name="recent_sales",
    sql_source=SqlSource(resource="my_app:sql/recent_sales.sql"),
)
```

**Purpose**: Registers a SQL-defined entity: a read-only dataset defined by its SQL, which `kindling migrate apply` publishes as a permanent catalog view (`CREATE OR REPLACE VIEW`).

**Parameters**:
- `entityid`: Unique identifier for the entity
- `name`: The catalog view identifier that `kindling migrate apply` creates (`CREATE OR REPLACE VIEW <name>`), unless the `provider.table_name` tag is set, which wins. Use a valid identifier such as `recent_sales` or `reporting.recent_sales`, not a display label.
- `tags`: Optional key-value metadata. `provider_type: "view"` is added automatically, and any other `provider_type` is an error: SQL entities are always read-only.
- `sql`: A literal SQL string (mutually exclusive with `sql_source`)
- `sql_source`: A `SqlSource` object pointing to an inline string, a package resource (`"package:path/to/file.sql"`), or a filesystem path (mutually exclusive with `sql`)

**`SqlSource` helper**:

```python
@dataclass
class SqlSource:
    inline:   Optional[str] = None   # literal SQL string
    resource: Optional[str] = None   # "package:path/to/file.sql" via importlib.resources
    file:     Optional[str] = None   # filesystem path (local dev only)
```

Exactly one of `inline`, `resource`, or `file` must be provided.

**Usage Notes**:
- SQL entities have no `merge_columns`, `partition_columns`, or `schema` — they are fully defined by their SQL body.
- **Reading**: a SQL entity can be a pipe input. The built-in `view` provider (`SqlEntityProvider`) reads it by evaluating the declared SQL (`spark.sql(sql)`), not by reading the catalog view. The read does not need `kindling migrate apply` to have run, always matches the SQL the running code declares, and works standalone (where the session catalog does not persist a view between runs) and over session temp views such as `memory` entities. Qualify table names in the SQL: it resolves against the session's current catalog and schema. Reads are batch only (the provider is not streamable), and a watermarked read falls back to a full read every time.
- **Publishing**: `kindling migrate apply` creates or replaces the catalog view and `kindling migrate plan` detects SQL changes by hash. Reading a SQL entity never creates or touches the view.
- **Writing**: SQL entities are read-only. A pipe whose output is a SQL entity fails at persist time with a `ValueError` naming the entity (`Entity '...' is read-only: provider 'view' does not support write operations ...`). The provider exposes no write or destination-ensuring capability, so no DDL is issued.
- **SDP / Lakeflow declaration**: SQL entities are not declarable as SDP inputs or outputs (`external_input_not_declarable` / `output_entity_not_table_backed`).
- Raises `KindlingNotInitializedError` if called before `initialize()`.
- Must be called at module level for proper registration.

### 4. EntityPathLocator (Abstract)

```python
class EntityPathLocator(ABC):
    @abstractmethod
    def get_table_path(self, entity):
        """Get the storage path for an entity"""
        pass
```

**Purpose**: Abstract interface for determining where entity data is stored.

**Default Implementation**: `ConfigDrivenEntityPathLocator` (in `kindling.entity_resolution`) is bound automatically. It uses the entity's `provider.path` tag when set, otherwise `kindling.storage.table_root` (default `Tables`) plus the entity ID with dots turned into slashes.

**Usage**: Typically implemented to support different storage backends or path conventions.

### 5. EntityNameMapper (Abstract)

```python
class EntityNameMapper(ABC):
    @abstractmethod
    def get_table_name(self, entity):
        """Get the table name for an entity"""
        pass
```

**Purpose**: Abstract interface for mapping entity IDs to table names.

**Default Implementation**: `ConfigDrivenEntityNameMapper` (in `kindling.entity_resolution`) is bound automatically. It honors the `provider.table_name`, `provider.table_catalog`, and `provider.table_schema` tags and the `kindling.storage.*` naming config.

**Usage**: Enables flexible naming strategies (e.g., environment prefixes, schema organization).

### 6. Entity Provider Interfaces (Composed)

The provider system uses interface composition. Providers implement only the capabilities they support; the runtime checks capability at call sites via `isinstance`.

#### 6a. BaseEntityProvider (required)

```python
class BaseEntityProvider(ABC):
    @abstractmethod
    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """Read entity as a batch DataFrame."""
        pass

    @abstractmethod
    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """Return True if the entity exists."""
        pass
```

**Purpose**: Core interface that every provider must implement. Covers batch read and existence checks.

#### 6b. WritableEntityProvider (optional)

```python
class WritableEntityProvider(ABC):
    @abstractmethod
    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Write DataFrame to entity (overwrites existing data)."""
        pass

    @abstractmethod
    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Append DataFrame to entity (preserves existing data)."""
        pass
```

**Purpose**: Opt-in batch write capability.

#### 6c. StreamableEntityProvider (optional)

```python
class StreamableEntityProvider(ABC):
    @abstractmethod
    def read_entity_as_stream(
        self,
        entity_metadata: EntityMetadata,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> DataFrame:
        """Read entity as a streaming DataFrame."""
        pass
```

**Purpose**: Opt-in streaming read capability.

#### 6d. StreamWritableEntityProvider (optional)

```python
class StreamWritableEntityProvider(ABC):
    @abstractmethod
    def append_as_stream(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        checkpoint_location: str,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> Union[DataStreamWriter, StreamingQuery]:
        """Append a streaming DataFrame to the entity.

        Returns an unstarted DataStreamWriter for the caller to start, or an
        already-started StreamingQuery when the provider resolves the
        destination itself.
        """
        pass
```

**Purpose**: Opt-in streaming write capability.

#### 6e. DestinationEnsuringProvider (optional)

```python
class DestinationEnsuringProvider(ABC):
    @abstractmethod
    def ensure_destination(self, entity_metadata: EntityMetadata) -> None:
        """Ensure the write destination exists (provider-specific semantics)."""
        pass
```

**Purpose**: Pre-creation of write destinations. Intentionally separated so providers that do not need DDL (e.g., most event-streaming sinks) are not forced to implement it.

#### 6f. DeclarableStreamingSource (optional)

```python
class DeclarableStreamingSource(ABC):
    @abstractmethod
    def streaming_source_spec(self, entity_metadata: EntityMetadata) -> StreamingSourceSpec:
        """Return secret-safe declaration metadata and validation issues."""
        pass
```

**Purpose**: Declaration-only capability for provider-owned streaming
sources. A provider that implements this must also implement
`StreamableEntityProvider`; the declaration engine uses
`streaming_source_spec()` while planning, and the provider's existing
`read_entity_as_stream()` when the declarative runtime evaluates the flow.
The spec must contain structural metadata, supported/applied option names,
and validation issues only. It must not contain connection strings, SAS keys,
passwords, JAAS values, resolved `@secret` values, or other secret-bearing
connector option values.

#### Capability check helpers

```python
from kindling.entity_provider import (
    can_ensure_destination,
    is_declarable_streaming_source,
    is_stream_writable,
    is_streamable,
    is_writable,
)

if is_writable(provider):
    provider.write_to_entity(df, entity)
```

#### Interface composition by provider type

| Provider | BaseEntityProvider | WritableEntityProvider | StreamableEntityProvider | StreamWritableEntityProvider | DestinationEnsuringProvider | DeclarableStreamingSource |
|----------|:-----------------:|:---------------------:|:------------------------:|:---------------------------:|:---------------------------:|:-------------------------:|
| Delta (full) | yes | yes | yes | yes | yes | |
| CSV | yes | yes | | | | |
| EventHub | yes | | yes | | | yes |
| Memory (testing) | yes | yes | yes | yes | | |

#### Legacy EntityProvider ABC

`EntityProvider` (in `data_entities.py`) is a legacy monolithic ABC kept for backward compatibility. New providers should implement the composed interfaces from `entity_provider.py` instead. `DeltaEntityProvider` implements both.

**Key Operations**:
- **Batch reads/writes**: via `BaseEntityProvider` + `WritableEntityProvider`
- **Streaming**: via `StreamableEntityProvider` + `StreamWritableEntityProvider`
- **Destination setup**: via `DestinationEnsuringProvider`
- **Version Management**: `read_entity_since_version` / `get_entity_version` on `EntityProvider`
- **Schema Management**: Automatic table creation and schema evolution via `ensure_entity_table`

### 7. DataEntityRegistry (Abstract)

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

### 8. DataEntityManager

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
- Config overlays from the `dataentities:` and `dataentities-bytag:` sections
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
DataEntities.entity(
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

DataEntities.entity(
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

Built-in providers cover `delta` (the default when an entity has no `provider_type`
tag), `csv`, `parquet`, `memory`, `eventhub`, and `adx-api`, and the path locator and
name mapper have config-driven defaults. To add a custom storage backend, implement:

1. **`BaseEntityProvider`** (required): batch read and existence check for your storage backend
2. **`WritableEntityProvider`** (if writable): batch write and append operations
3. **`StreamableEntityProvider`** (if streaming reads needed): streaming read operations
4. **`StreamWritableEntityProvider`** (if streaming writes needed): streaming append operations
5. **`DestinationEnsuringProvider`** (if DDL needed): pre-creation of write destinations
6. **`DeclarableStreamingSource`** (if a declarative engine may own the provider's stream): secret-safe source specs
7. **EntityPathLocator** (optional): replace the default path resolution
8. **EntityNameMapper** (optional): replace the default table naming
9. **Schema Definitions**: Spark StructType schemas for your entities

## Common Implementation Patterns

### Custom EntityProvider Example (composed interfaces)

```python
@GlobalInjector.singleton_autobind()
class MyDeltaEntityProvider(
    BaseEntityProvider,
    WritableEntityProvider,
    DestinationEnsuringProvider,
):
    @inject
    def __init__(self, path_locator: EntityPathLocator, name_mapper: EntityNameMapper):
        self.path_locator = path_locator
        self.name_mapper = name_mapper

    def check_entity_exists(self, entity):
        path = self.path_locator.get_table_path(entity)
        try:
            spark.read.format("delta").load(path)
            return True
        except Exception:
            return False

    def read_entity(self, entity):
        path = self.path_locator.get_table_path(entity)
        return spark.read.format("delta").load(path)

    def ensure_destination(self, entity):
        if not self.check_entity_exists(entity):
            path = self.path_locator.get_table_path(entity)
            empty_df = spark.createDataFrame([], entity.schema)
            empty_df.write.format("delta").save(path)

    def write_to_entity(self, df, entity):
        path = self.path_locator.get_table_path(entity)
        df.write.format("delta").mode("overwrite").save(path)

    def append_to_entity(self, df, entity):
        path = self.path_locator.get_table_path(entity)
        df.write.format("delta").mode("append").save(path)


# Make it available to entities tagged provider_type: "my_delta"
GlobalInjector.get(EntityProviderRegistry).register_provider("my_delta", MyDeltaEntityProvider)
```

The pipe runner resolves each entity's provider from its `provider_type` tag
through `EntityProviderRegistry`, so a custom provider is used only after it is
registered there.

### Path Locator Example

```python
@GlobalInjector.singleton_autobind()
class S3EntityPathLocator(EntityPathLocator):
    def __init__(self, base_path: str = "s3a://data-lake"):
        self.base_path = base_path

    def get_table_path(self, entity):
        # Convert entity ID to path: bronze.customers -> bronze/customers
        path_parts = entity.entityid.split('.')
        return f"{self.base_path}/{'/' .join(path_parts)}"
```

## Integration with Data Pipes

The Data Entities Framework integrates seamlessly with the Data Pipes Framework.

### Input parameter naming

The runtime binds each input entity to the pipe function as a keyword argument.
The parameter name is derived from the entity ID by replacing every `.` with `_`:

| `input_entity_ids` entry | Expected parameter name |
|--------------------------|------------------------|
| `"bronze.records"` | `bronze_records` |
| `"silver.clean_sales"` | `silver_clean_sales` |
| `"a.b.c"` | `a_b_c` |

The pipe function must declare exactly these names — no extra positional arguments
and no `spark` parameter (Spark is not injected into batch pipe functions).

```python
# Entity definition
DataEntities.entity(
    entityid="bronze.raw_sales",
    name="Raw Sales Data",
    partition_columns=["date"],
    merge_columns=["id"],
    tags={"layer": "bronze"},
    schema=raw_sales_schema
)

DataEntities.entity(
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

### Event Hub Ingestion for Databricks Lakeflow

Event Hub entities use `provider_type: "eventhub"` and are read-only. With
`engine="databricks_sdp"`, an Event Hub entity can be the pipe's single
driving input; Kindling declares a Lakeflow streaming table and append flow,
and Lakeflow owns checkpoints and query lifecycle.

```python
DataEntities.entity(
    entityid="stream.telemetry",
    name="Telemetry Event Hub",
    merge_columns=[],
    tags={
        "provider_type": "eventhub",
        "provider.eventhub.connectionString": "@secret:lakeflow:eh-conn",
        "provider.eventhub.name": "telemetry",
        "provider.transport": "kafka",
        "provider.kafka.includeHeaders": "true",
        "provider.preprocess": "kafka",
    },
    schema=None,
)

DataEntities.entity(
    entityid="bronze.telemetry",
    name="Bronze Telemetry",
    merge_columns=[],
    tags={"provider_type": "delta"},
    schema=bronze_telemetry_schema,
)

@DataPipes.pipe(
    pipeid="bronze.telemetry.ingest",
    name="Ingest Telemetry",
    tags={},
    input_entity_ids=["stream.telemetry"],
    output_entity_id="bronze.telemetry",
    output_type="delta",
    driving_entity_ids=["stream.telemetry"],
)
def ingest_telemetry(stream_telemetry):
    return stream_telemetry.selectExpr("cast(body as string) as payload")
```

The Event Hub provider validates declarable-source configuration during
planning without rendering secret-bearing tag values. Use Kafka transport for
Lakeflow; the legacy `eventhubs` transport is rejected by declaration
validation because Lakeflow cannot run that connector.

## SCD Type 2 Entities

Entities tagged `scd.type: "2"` automatically receive Slowly Changing Dimension Type 2 write semantics. The framework handles the staged-updates merge, temporal column management, and companion entity registration — no pipe-level boilerplate required.

### Declaration

```python
DataEntities.entity(
    entityid="silver.dim_customer",
    name="Customer Dimension",
    merge_columns=["customer_id"],          # business key — required for SCD2
    tags={
        "scd.type": "2",
        "scd.tracked": "name,email,region", # columns that trigger a new version when changed
                                             # omit to track all non-key, non-temporal columns
                                             # (or use scd.untracked to list the exceptions)
    },
    schema=customer_schema,
)
```

The schema you declare should contain only business columns. The framework auto-augments the Delta table with three temporal columns at `ensure_entity_table()` time:

| Column | Default name | Override tag |
|--------|-------------|--------------|
| Version start timestamp | `__effective_from` | `scd.effective_from_col` |
| Version end timestamp (null = current) | `__effective_to` | `scd.effective_to_col` |
| Current-row boolean flag | `__is_current` | `scd.current_col` |

### Tracked and untracked columns

A change to a tracked column closes the current version and opens a new one.
By default every column is tracked except the keys (`merge_columns`), the
temporal columns and `scd.sequence_by`. Narrow the tracked set with one of two
tags:

- `scd.tracked: "name,email,region"` lists the columns to track (an
  allow-list). Every other column is untracked.
- `scd.untracked: "ingested_at,load_id"` lists the columns not to track (an
  exclude-list). Every other column stays tracked. Use it for bookkeeping
  columns such as ingest timestamps and load ids.

Both tags take a comma-separated list of literal column names. An incoming row
for an existing key is handled like this:

| Incoming change | Result |
|---|---|
| A tracked column changed | Close the current version and insert a new one with every incoming value |
| Only untracked columns changed | Update the current version in place; `__effective_from` and `__is_current` don't change |
| Only `scd.sequence_by` changed | Update the current version in place |
| Nothing changed | No-op |

Closed versions keep the untracked values they had when they were closed.

A merge applies one row per business key. With `scd.sequence_by`, a batch with
several rows for one key keeps the latest by sequence (as streaming merges do
per micro-batch); the earlier rows in that batch open no versions. Without
`scd.sequence_by`, rows identical in every column collapse to one (full reloads
and retried extracts often contain them); a key whose rows differ is rejected,
since nothing orders them.

> **Behaviour change for `scd.tracked`.** Columns outside the `scd.tracked`
> allow-list used to be ignored when only they changed: the current version
> kept the values it had when it was opened. They are now updated in place on
> the current version, the same as `scd.untracked` and the same as Lakeflow
> AUTO CDC.

Column names starting with `__scd2_` are reserved for the merge's staging
columns, like `__merge_key`: registration rejects them in an SCD2 schema, and the
merge rejects them in an incoming batch.

A batch that brings a column the table doesn't have yet adds it, with
schema evolution, as keyed merges and appends already do. A new untracked
column is filled in on the current version in place; a new tracked column opens
a new version for each key that now has a value (the closed version keeps NULL).

Registration rejects:

- `scd.tracked` and `scd.untracked` on the same entity;
- `scd.untracked` on an entity without `merge_columns`, on a derived dataset,
  or with `write.mode: insert`;
- an untracked key column, temporal column or `__merge_key`;
- an untracked column missing from the entity's declared schema (when it
  declares one);
- an `scd.untracked` list that leaves no tracked column (`scd.sequence_by`
  counts as untracked).

### Lakeflow (AUTO CDC)

When the pipeline runs on Databricks Lakeflow (`kindling_ext_databricks`), the
tags map to AUTO CDC's history-tracking lists. Only one list is ever passed,
and the same list goes to the change-feed flow (`create_auto_cdc_flow`) and the
snapshot flow (`create_auto_cdc_from_snapshot_flow`):

| Kindling tags | AUTO CDC argument |
|---|---|
| `scd.tracked` | `track_history_column_list` = the tracked columns |
| `scd.untracked` | `track_history_except_column_list` = the untracked columns, plus `scd.sequence_by` when it is set |
| neither | `track_history_except_column_list` = `[scd.sequence_by]` when it is set, otherwise no list |

AUTO CDC updates columns outside the tracked set in place on the current
version, so the runner and Lakeflow give the same result.

### Companion entity

Registering an SCD2 entity also auto-registers a read-only companion at `{entityid}.current` that is pre-filtered to current rows (`is_current=true`). Downstream pipes reference it by ID:

```python
@DataPipes.pipe(
    pipeid="gold_active_customers",
    input_entity_ids=["silver.dim_customer.current"],   # always current rows
    output_entity_id="gold.active_customers",
    ...
)
def gold_active_customers(silver_dim_customer_current):
    return silver_dim_customer_current.select("customer_id", "name", "region")
```

Writing to a companion entity raises `NotImplementedError`.

In DAG mode (`run_datapipes_dag` / `use_dag=True`), a pipe that reads the companion runs after the pipe that writes the base entity, as if it read the base entity. A pipe can write the SCD2 entity and read its own companion (for example to detect changes against the current rows); it reads the rows from before its write and is not a cycle. SDP/Lakeflow declaration does not lower the companion: a declared pipe that reads it fails validation (`external_input_not_declarable`).

### Tag reference

| Tag | Default | Description |
|-----|---------|-------------|
| `scd.type` | — | Set to `"2"` to enable SCD2 |
| `scd.tracked` | all non-key, non-temporal columns | Comma-separated tracked columns. A change confined to other columns updates the current version in place. Can't be combined with `scd.untracked` |
| `scd.untracked` | none | Comma-separated columns that don't open a new version. A change confined to them updates the current version in place. Also applies to keyed merges without `scd.type` (see [Untracked columns on keyed merges](#untracked-columns-on-keyed-merges-scd1)). Can't be combined with `scd.tracked` |
| `scd.effective_from_col` | `__effective_from` | Temporal column name override |
| `scd.effective_to_col` | `__effective_to` | Temporal column name override |
| `scd.current_col` | `__is_current` | Temporal column name override |
| `scd.current_entity_id` | `{entityid}.current` | Override companion entity id |
| `scd.routing_key` | `hash` | `hash` (safe for all types) or `concat` (cheaper, readable) |
| `scd.close_on_missing` | `false` | Set to `"true"` to close (expire) rows whose business key is absent from the incoming batch — acts as a soft-delete for keys that disappear from the source |
| `scd.optimize_unchanged` | `false` | Set to `"true"` to skip re-inserting rows whose tracked columns are unchanged; uses a hash comparison to detect changes and avoids writing no-op history records |

## Untracked Columns on Keyed Merges (SCD1)

An entity with `merge_columns` and no `scd.type` is written with a keyed
merge: matched rows are updated, new keys are inserted. The same
`scd.untracked` tag applies here:

```python
DataEntities.entity(
    entityid="silver.customers",
    name="Customers",
    merge_columns=["customer_id"],
    tags={"scd.untracked": "ingested_at,load_id"},
    schema=customer_schema,
)
```

A matched row is updated only when a column other than the keys and the
untracked columns differs. A change confined to untracked columns doesn't
rewrite the row, so `ingested_at` and `load_id` keep the values from the last
load that made a real change. New keys are inserted as usual. Entities
without `scd.untracked` keep the unconditional update. The same registration
rules apply as for SCD2 (above), except the temporal-column rule. `scd.tracked`
has no meaning without `scd.type: "2"`.

This is runner-only behaviour. Keyed SCD1 merges are not lowered to Lakeflow
AUTO CDC today, so there is no Lakeflow mapping. AUTO CDC type 1 has no
untracked-column setting and rewrites the row on any change, so if keyed SCD1
is ever lowered to it, this becomes a documented divergence.

## Surrogate Keys

An entity can declare surrogate key columns and have the framework compute
them. Keys are declared with tags, built from one canonical encoding (`v1`)
that is safe to store, and set once when a row is inserted.

```python
# Hash key on a keyed (SCD1) entity: hashes merge_columns.
DataEntities.entity(
    entityid="silver.dim_product",
    name="Products",
    merge_columns=["product_id"],
    tags={"surrogate.column": "product_sk"},
    schema=product_schema,
)

# SCD2 dimension: a version key and a durable key.
DataEntities.entity(
    entityid="silver.dim_customer",
    name="Customers",
    merge_columns=["customer_id", "source_system"],
    tags={
        "scd.type": "2",
        "scd.sequence_by": "updated_at",
        "surrogate.column": "customer_version_sk",  # H(customer_id, source_system, effective_from)
        "surrogate.durable_column": "customer_dk",   # H(customer_id, source_system)
    },
    schema=customer_schema,
)
```

`DataEntities.entity(..., surrogate_key="customer_version_sk", durable_key="customer_dk")`
is sugar for the two column tags. Like `insert_only_entity`, it only fills in
the tags; explicit tags win.

### Tags

| Tag | Values | Default |
|-----|--------|---------|
| `surrogate.column` | column name (the version key on an SCD2 entity) | none |
| `surrogate.durable_column` | column name; SCD2 only | none |
| `surrogate.strategy` | `hash`, `uuid5`, `custom` | `hash` |
| `surrogate.from` | comma-separated input columns, in order | `merge_columns` |
| `surrogate.include_effective_from` | `true` / `false`; SCD2 version key only | `true` |
| `surrogate.namespace` | literal string encoded ahead of the inputs | none |
| `surrogate.hash.algo` | `sha256` | `sha256` |
| `surrogate.hash.format` | `hex` | `hex` |
| `surrogate.uuid.namespace` | UUID; required for `uuid5` | none |
| `surrogate.generator` | registered generator name; required for `custom` | none |
| `surrogate.generator.<option>` | string, passed to the generator as `ctx.options["<option>"]` | none |
| `surrogate.require_reproducible` | `true` / `false` | `false` |
| `surrogate.encoding` | `v1`; written by the framework | `v1` |

`uuid4` and `identity` strategies, `md5` / `xxhash64`, the `binary` format,
`surrogate.normalize` and `surrogate.unknown_member` are planned and rejected
for now with a "not yet supported" error. The durable key uses the version
key's strategy, namespace and inputs, without the effective date.

### The `v1` encoding

All strategies hash one canonical string, built with Spark's built-in
functions (no UDFs), so keys agree across Spark 3, Spark 4 and Spark Connect:

1. The inputs are the `surrogate.from` columns **in declared order**, then
   effective_from for an SCD2 version key. Column names are not part of the
   input: renaming a column keeps its keys.
2. `surrogate.namespace`, when set, comes first as `@<length>:<namespace>`.
   Its own marker keeps it apart from the inputs: namespace `a` over `("b")`
   and no namespace over `("a", "b")` give different keys.
3. Each value is `N` when NULL, otherwise `S<length>:<canonical value>`, and
   the values are concatenated with no separator. `("a|b", "c")` and
   `("a", "b|c")` can't collide, and NULL (`N`) differs from `""` (`S0:`).
4. Canonical values: integers as decimal strings (an int `42` and a long `42`
   agree); decimals in plain notation at the declared scale (a scale change is
   a key change; scale above 36 is rejected); strings as is; booleans `T` / `F`;
   dates `yyyy-MM-dd`; timestamps as UTC microseconds (`unix_micros`, the same in
   every session timezone); binary as lowercase hex. Float, double,
   `timestamp_ntz` and complex types are rejected.
5. `hash` is the lowercase sha256 hex of the string (64 characters).
   `uuid5` is the RFC 4122 name-based UUID of the string in
   `surrogate.uuid.namespace`, identical to Python's
   `uuid.uuid5(namespace, canonical)`.

The encoding is recorded in the `surrogate.encoding` tag and, on Delta tables,
in the table property `kindling.surrogate.encoding` (written at
`ensure_destination`). A table that records a different encoding fails, so one
table never mixes key encodings.

A key column missing from the declared schema is added to the table, like
the SCD2 temporal columns: `nullable=False`, with field metadata
`kindling.surrogate=<strategy>`. It is a STRING for `hash` and `uuid5`, or
the generator's `output_type` for `custom`. A declared key column with
another type fails validation.

### Where keys are computed

- **SCD2** version and durable keys are minted inside the merge (Delta and
  memory), on the inserted rows only, from the effective_from value that is
  stored with the row. Streaming merges use the same merge per micro-batch.
- **Other entities** get the key as a column just before the write: in batch
  runs the persist step adds it after every `persist.before_persist` handler
  has run, and in streaming pipes it is added after the pipe's transform,
  just before the sink starts. A handler that normalizes an input column (for
  example trims an id) therefore changes the key too: the key is always
  computed from the values that are stored.

`before_persist` and `read.after_read` receivers chain: each receiver gets the
DataFrame the previous one returned, so all your transforms apply, in
whatever order they run.

### Keys are insert-only

A key is set once, when its row is inserted, and no merge clause writes it
again. Merges never match on keys.

- Key columns are never tracked, so they never open an SCD2 version.
- Key columns are never untracked either. Under `scd.tracked` every column
  outside the list is updated in place, but key columns are left out of that
  update.
- Closing a version (effective_to, is_current) never touches its keys.
- On a keyed (SCD1) merge, the matched-row update is an explicit set without
  the key columns, so a tracked change doesn't rewrite the key.
- A key column in the incoming data is ignored: the framework always computes
  the key.

Registration rejects a key column in `merge_columns`, `surrogate.from`,
`scd.tracked` or `scd.untracked`, an SCD2 temporal or reserved column as a key,
effective_from, effective_to or is_current in `surrogate.from`, and
`surrogate.durable_column` on an entity without `scd.type: "2"`. Column names
are compared without regard to case, as Spark resolves them by default.

### SCD2 version and durable keys

The version key identifies one version, so facts can pin the version that
was current when they happened. The durable key is the same for every version
of a business key, for joins on current state. The `.current` companion
carries both, since they are ordinary columns.

With `scd.sequence_by`, effective_from is the sequence value, so a replay or
rebuild recreates the same version rows with the same keys. Stale rows
rejected by the strictly-later check are never inserted and never get a key.

Without `scd.sequence_by`, effective_from is processing time (one instant per
merge, captured once). Version keys are unique and fixed once stored, but a
rebuild produces different ones, so registration logs a warning.
`surrogate.require_reproducible: "true"` turns the warning into an error.
Durable keys are unaffected.

### Custom generators

A generator is a function that builds a Spark Column from a `KeyContext`. It
is not a row-level UDF, so the same generator runs before a write and inside
an SCD2 merge.

```python
from pyspark.sql import Column, functions as F
from kindling.surrogate_keys import KeyContext, SurrogateKeyGenerators

@SurrogateKeyGenerators.register("acme_customer_key", deterministic=True, output_type="string")
def acme_customer_key(ctx: KeyContext) -> Column:
    parts = [
        F.lit(ctx.options["prefix"]),
        ctx.col("source_system"),
        F.lpad(ctx.col("customer_id").cast("string"), 10, "0"),
    ]
    # Only an SCD2 version key has an effective date ("version"); it is None
    # for a durable key ("durable") and on non-SCD2 entities ("key").
    if ctx.kind == "version":
        parts.append(F.date_format(ctx.effective_from, "yyyyMMddHHmmss"))
    return F.concat_ws("-", *parts)
```

```python
tags={
    "surrogate.column": "acme_key",
    "surrogate.strategy": "custom",
    "surrogate.generator": "acme_customer_key",
    "surrogate.generator.prefix": "CUST",
}
```

`KeyContext` gives the generator:

| Member | What it is |
|--------|------------|
| `col(name)` | a column of the rows being keyed, bound by the framework |
| `columns` | every data column after transforms (no staging, temporal or key columns) |
| `key_columns` | the resolved `surrogate.from` |
| `effective_from` | the stored effective_from of the version being inserted; `None` for non-SCD2 entities and for durable keys |
| `effective_to`, `is_current` | their insert-time values (NULL, true) on SCD2 entities; `None` otherwise |
| `entity` | read-only view: `entityid`, `name`, `merge_columns`, `tags` |
| `kind` | `"version"` (SCD2 version key), `"durable"` (SCD2 durable key) or `"key"` (non-SCD2) |
| `options` | the entity's `surrogate.generator.<option>` tags, read-only |
| `canonical(*cols)`, `hash(*cols, algo=)` | the `v1` helpers; pass column names or columns from `col()` / `effective_from` for their typed forms |

There are no batch or run ids on purpose: they would make every key
non-reproducible.

Generator names resolve at `ensure_destination`, after bootstrap has
imported every module, so entity and generator modules can be imported in
any order. An unknown name fails there and lists the registered generators.
Streaming pipes and parallel batch generations call `ensure_destination`
before any data moves. A sequential batch run doesn't, so there a mistyped
name fails at the pipe's first persist, after its reads (the persist fails,
so watermarks don't advance).
A streaming merge resolves the generator on the driver and carries the
function itself into the micro-batch callback. Under Spark Connect that
callback runs in a process that never ran bootstrap and may not be able to
import your module, so the generator's module is pickled by value while the
callback is serialized. Generators must therefore be picklable: functions
with no SparkSession, DI service or other live object in their closure,
calling only helpers that are themselves importable or in the same module.

Determinism is declared, not inspected. Only `deterministic=True` generators
are accepted for now; non-deterministic ones arrive with `uuid4`. Check a
generator in your tests with `assert_generator_deterministic`, which
evaluates it twice with a repartition in between and compares:

```python
from kindling.surrogate_keys import assert_generator_deterministic

assert_generator_deterministic("acme_customer_key", sample_df, key_columns=["customer_id"],
                               options={"prefix": "CUST"})
```

### Lakeflow (SDP)

Surrogate keys are not lowered to SDP/Lakeflow yet. Declaring a pipe whose
output entity carries `surrogate.*` tags fails validation with
`surrogate_keys_not_lowered`, so a Lakeflow app can't silently write the
dataset without its keys. Run such pipes with the Kindling runner, or compute
the key in the pipe and drop the tags.

### Project-wide overrides

There is no global default generator. To use one generator across a group of
entities, set the tags with a `dataentities-bytag:` overlay, so each entity
still shows its own key declaration:

```yaml
dataentities-bytag:
  key_family:
    acme:
      tags:
        surrogate.strategy: custom
        surrogate.generator: acme_customer_key
        surrogate.generator.prefix: CUST
```

Every entity tagged `key_family: acme` (and declaring `surrogate.column`)
then uses the generator.

## Provenance Stamping

A stamped entity carries one nullable struct column, `__provenance` by
default, that records which pipe, in which run, from which inputs wrote each
row. The record is data: it lives in the row, is queried with SQL and is kept
as long as the row is. It is not telemetry (nothing is emitted, and turning
tracing off does not turn it off), so its settings live under
`kindling.provenance.*`. Design: `docs/proposals/provenance_stamping.md`.

### Turning it on

Project-wide, in any settings layer (base, platform, environment or the app's
own files):

```yaml
kindling:
  provenance:
    enabled: true             # default false
    column: __provenance      # default
    pipe_tags: [domain, owner, "layer.*"]   # allow-list; names and fnmatch globs
```

Per entity, with tags (`tags=`, `dataentities:`, `dataentities-bytag:` or
`entity_tags`):

```python
tags={
    "provenance.enabled": "false",          # opt out of the settings default
    "provenance.column": "_lineage",
    "provenance.pipe_tags": "domain,owner",
}
```

The settings block is the **lowest-precedence default**: it fills in each
`provenance.*` tag an entity does not set after the `dataentities-bytag:` and
`dataentities:` overlays, so every other layer wins, and an entity opts out in
its own declaration. The block reaches Delta and memory entities only; it
skips read-only, SQL, SCD2 companion and `system.*` entities. Any other
provider opts in with an explicit `provenance.enabled: "true"` tag.

Registration rejects the column in `merge_columns`, `partition_columns`,
`cluster_columns`, `scd.tracked`, `scd.untracked`, `scd.sequence_by`,
`surrogate.from`, the surrogate key columns or `derived.replace_keys`, as an
SCD2 temporal column, in the declared schema (the framework adds it), and a
name starting with `__scd2_`. `provenance.pipe_tags` must name tags: `*` is
rejected. Pipe tags that held a resolved secret are never copied.

### The column

| Field | Value |
|---|---|
| `pipe_id`, `pipe_name` | The pipe that wrote the row |
| `run_id` | The initialization's run id from the runtime manifest (`kindling.runtime_manifest`); null for SDP datasets, whose values are bound at declaration time |
| `execution_id` | The executor's run id (one per `execute` / `run_datapipes` call, or per streaming orchestrator start); null outside an executor |
| `persist_id` | The batch persist call; null for streaming and SDP |
| `processed_at` | Write time (per micro-batch on a stream; the declaration time on SDP) |
| `mode` | `batch`, `streaming` or `sdp` |
| `app_name`, `environment`, `platform`, `kindling_version` | Best effort; null when unknown |
| `input_entity_ids` | The pipe's inputs |
| `input_versions` | Reserved for the version of each input read; null for now |
| `tags` | The allow-listed pipe tags (`map<string,string>`) |
| `schema_version` | `1` |

Fields are only ever added. Existing rows of an existing table get a null
stamp ("written before provenance was enabled") until they are next written.

### Overwrite overwrites

The column is written with every row write, and it is **never a change**:

| Write | Provenance on the stored row |
|---|---|
| Append (batch or streaming), derived replace | The write that stored the row |
| `write.mode: insert` | The write that inserted it; an existing key keeps its stamp |
| SCD1 keyed merge | The last write that updated the row. With `scd.untracked`, a provenance-only (or untracked-only) difference is skipped, so the stamp names the last real change; without it every matched row is rewritten and restamped |
| SCD2 | Each version: the write that stored it. A close keeps the closed version's stamp; an in-place update (untracked-only change, sequence-only bump) overwrites it |

A difference only in the stamp never opens an SCD2 version and never
triggers an in-place update, so adding the column to an existing SCD2 table
re-versions nothing. On Lakeflow the column is in the AUTO CDC
`track_history_except_column_list` (unless the entity has an `scd.tracked`
allow-list, which already leaves it out). A custom surrogate key generator's
`ctx.columns` never includes it.

### Reads downstream

A downstream pipe's read drops an input's provenance column (batch, streaming
and SDP reads), so a table never inherits a stamp naming another pipe. A pipe
keeps it for named inputs with a pipe tag:

```python
tags={"provenance.keep_inputs": "silver.orders"}
```

Writing a kept stamp to a stamped output fails (the stamp would be replaced):
rename or drop it in the pipe.

Writes outside a pipe's persist step (file ingestion, the watermark entity)
are not stamped.

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
- **`KindlingNotInitializedError`**: Raised by `DataEntities.entity()`, `@DataEntities.sql_entity()`, and `@DataPipes.pipe()` decorators when they fire before `initialize()` has been called. This typically means an entity or pipe module was imported before `app.py`'s `register_all()` completed. Fix: ensure `initialize()` runs before any entity or pipe module is imported.

## Data Governance Features

The framework supports data governance through:
- **Metadata Tags**: Categorization and compliance tracking
- **Schema Registry**: Centralized schema management
- **Version Tracking**: Data lineage and change history
- **Access Patterns**: Standardized read/write operations
- **Audit Trail**: Integration with logging and monitoring systems

This framework provides a robust foundation for managing data entities in a lakehouse architecture with strong governance, discoverability, and operational capabilities.

## Cloning and extending declarations

A declaration that is *almost* another one is a derivation, not a copy.
`DataEntities.clone` declares a new entity from another's declaration used as
a template; `DataEntities.extend` adds to an entity in place. Both are
additive and stack last-in-wins, like config overlays, and both resolve when
the source is registered, in any import order:

```python
DataEntities.clone(
    "silver.orders_eu", from_entity="silver.orders",
    name="orders_eu",
    add_columns=[StructField("region", StringType())],
    tags={"region": "eu"},
)
DataEntities.extend("silver.orders", tags={"owner": "sales"})
```

A clone implies nothing about data: a cloned pipe, a new pipe, or nothing
yet may write to it. Adding a column with the same name but a different type
is an error, because that is a migration (`kindling migrate`), not an
extension. The same shape exists in settings YAML (`clone_of`,
`add_columns` on an exact id under `dataentities:`), so environment overlays
can declare variants without code. See
`docs/proposals/declaration_derivations.md` and, for pipes,
`docs/guide/data_pipes.md`.
