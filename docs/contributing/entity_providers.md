# Entity Providers

Entity Providers are a core component of the Kindling Framework, responsible for abstracting the storage and access mechanisms for data entities. They provide a consistent interface for performing CRUD operations, regardless of the underlying storage technology.

## Core Concepts

### Interface Composition

The framework uses **interface composition** rather than a single monolithic abstract base class. Providers declare only the capabilities they support by implementing one or more of the following interfaces defined in `entity_provider.py`:

| Interface | Required? | Capability |
|---|---|---|
| `BaseEntityProvider` | Yes (all providers) | Batch read + existence check |
| `WritableEntityProvider` | Optional | Batch write and append |
| `StreamableEntityProvider` | Optional | Streaming read |
| `StreamWritableEntityProvider` | Optional | Streaming write |
| `DestinationEnsuringProvider` | Optional | Pre-create the write destination |

This means a read-only CSV provider only implements `BaseEntityProvider`, while a full-featured Delta provider implements all five.

Capability helpers are provided for runtime checks:

```python
from kindling.entity_provider import is_streamable, is_writable, is_stream_writable, can_ensure_destination

if is_writable(provider):
    provider.write_to_entity(df, entity)
```

### Provider Abstraction

Each interface is an ABC. Consumers accept `BaseEntityProvider` and narrow to optional interfaces only when needed. The dependency injection system resolves the concrete implementation at runtime.

### Delta Lake Integration

The primary implementation, `DeltaEntityProvider`, implements all five interfaces, providing seamless integration with Delta Lake features:

- ACID transactions
- Schema evolution
- Time travel
- Merge operations (SCD1 and SCD2)
- Partitioning and liquid clustering
- Change Data Feed

## Key Components

### BaseEntityProvider

The required base interface all providers must implement.

```python
class BaseEntityProvider(ABC):
    @abstractmethod
    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """Read entity as a batch DataFrame."""

    @abstractmethod
    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """Check if the entity exists."""
```

`BaseEntityProvider` also provides concrete helper methods available to all subclasses:

- `_get_provider_config(entity_metadata)` — extracts and type-converts entity tags, stripping the `provider.` prefix from provider-specific keys
- `_convert_tag_type(value)` — converts string tag values to bool or int where appropriate

### WritableEntityProvider

Optional interface for providers that support batch writes.

```python
class WritableEntityProvider(ABC):
    @abstractmethod
    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Write DataFrame to entity (overwrites existing data)."""

    @abstractmethod
    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Append DataFrame to entity (preserves existing data)."""
```

### StreamableEntityProvider

Optional interface for providers that support streaming reads.

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
```

### StreamWritableEntityProvider

Optional interface for providers that support streaming writes.

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
    ) -> StreamingQuery:
        """Append streaming DataFrame to entity."""
```

### DestinationEnsuringProvider

Optional interface for providers that can pre-create a write destination.

```python
class DestinationEnsuringProvider(ABC):
    @abstractmethod
    def ensure_destination(self, entity_metadata: EntityMetadata) -> None:
        """Ensure the destination exists (provider-specific semantics)."""
```

### DeltaEntityProvider

The default implementation for Delta Lake storage. It implements all five interfaces.

```python
@GlobalInjector.singleton_autobind()
class DeltaEntityProvider(
    EntityProvider,
    BaseEntityProvider,
    DestinationEnsuringProvider,
    StreamableEntityProvider,
    WritableEntityProvider,
    StreamWritableEntityProvider,
    SignalEmitter,
):
    @inject
    def __init__(
        self,
        config: ConfigService,
        entity_name_mapper: EntityNameMapper,
        path_locator: EntityPathLocator,
        tp: PythonLoggerProvider,
        signal_provider: SignalProvider = None,
    ):
        ...
```

**Injected dependencies:**

| Parameter | Type | Purpose |
|---|---|---|
| `config` | `ConfigService` | Runtime config (access mode, feature flags) |
| `entity_name_mapper` | `EntityNameMapper` | Resolve catalog table names from entity metadata |
| `path_locator` | `EntityPathLocator` | Resolve physical storage paths from entity metadata |
| `tp` | `PythonLoggerProvider` | Logger factory |
| `signal_provider` | `SignalProvider` | Optional observability signal bus |

**Tag-based overrides** (set via entity tags with the `provider.` prefix) take precedence over the injected services:

- `provider.path` — override the physical table path
- `provider.table_name` — override the catalog table name
- `provider.access_mode` — override the access mode (`catalog` or `storage`)

**Methods implemented:**

- `read_entity(entity)` — batch read with signal emissions
- `check_entity_exists(entity)` — checks catalog and/or physical path
- `write_to_entity(df, entity)` — full overwrite with signal emissions
- `append_to_entity(df, entity)` — append with signal emissions
- `merge_to_entity(df, entity)` — SCD1 or SCD2 merge with signal emissions
- `read_entity_as_stream(entity, format=None, options=None)` — streaming read
- `append_as_stream(df, entity, checkpointLocation, format=None, options=None)` — streaming write
- `read_entity_since_version(entity, since_version)` — change feed read
- `read_entity_as_of(entity, point_in_time)` — point-in-time read
- `get_entity_version(entity)` — current Delta table version
- `ensure_entity_table(entity)` — create table if it does not exist
- `ensure_destination(entity_metadata)` — `DestinationEnsuringProvider` entry point (delegates to `ensure_entity_table`)

### DeltaTableReference

A utility class for handling different ways of accessing Delta tables.

```python
class DeltaTableReference:
    """Encapsulates how to reference a Delta table"""
    def __init__(self, table_name: str, table_path: Optional[str], access_mode: DeltaAccessMode):
        ...

    def get_delta_table(self) -> DeltaTable:
        """Get DeltaTable instance using the appropriate method."""

    def get_read_path(self) -> str:
        """Get path or name for spark.read operations."""

    def get_spark_read_stream(self, spark, options=None):
        """Build a readStream for this reference."""
```

### DeltaAccessMode

A class (not an `Enum`) with two string constants defining how Delta tables are accessed.

```python
class DeltaAccessMode:
    CATALOG = "catalog"   # Catalog or metastore-managed table names
    STORAGE = "storage"   # Direct Delta path access
```

## Usage Examples

### Basic CRUD Operations

```python
from kindling.injection import get_kindling_service
from kindling.entity_provider import BaseEntityProvider

entity_provider = get_kindling_service(BaseEntityProvider)

# Get entity definition
entity = data_entity_registry.get_entity_definition("sales.transactions")

# Read entity
df = entity_provider.read_entity(entity)
```

For write operations, narrow to `WritableEntityProvider`:

```python
from kindling.entity_provider import WritableEntityProvider, is_writable

if is_writable(entity_provider):
    # Write to entity (overwrite)
    entity_provider.write_to_entity(transformed_df, entity)

    # Append to entity
    entity_provider.append_to_entity(new_records_df, entity)
```

### Merge Operations

```python
from kindling.entity_provider import BaseEntityProvider
from kindling.injection import get_kindling_service

entity_provider = get_kindling_service(BaseEntityProvider)

# Define entity with merge columns
@DataEntities.entity(
    entityid="customers.profiles",
    name="Customer Profiles",
    partition_columns=["country"],
    merge_columns=["customer_id"],  # These columns define the merge key
    tags={"domain": "customer"},
    schema=customer_schema
)

# Later, merge new/updated records
entity_provider.merge_to_entity(
    updated_customers_df,
    entity_registry.get_entity_definition("customers.profiles")
)
```

### Versioning and Time Travel

```python
# Get entity version
current_version = entity_provider.get_entity_version(entity)

# Read changes since a specific version
changes_df = entity_provider.read_entity_since_version(entity, last_processed_version)
```

## Implementation Details

### Table Creation

`ensure_entity_table` (and `ensure_destination`) creates tables with appropriate configuration. The strategy differs by access mode:

- **catalog mode** — creates a managed table via `saveAsTable` or `DeltaTable.createIfNotExists(...).tableName(...)`
- **storage mode** — creates physical Delta files via `DeltaTable.createIfNotExists(...).location(...)` or an empty DataFrame write

The method applies schema, clustering, and partitioning based on entity metadata. It emits `entity.before_ensure_table` and `entity.after_ensure_table` signals.

### Merge Operations

`merge_to_entity` dispatches to a registered `DeltaMergeStrategy`:

- **`scd1`** (default) — update-all / insert-all merge
- **`scd2`** — staged-updates SCD Type 2 merge (activated when `tags={"scd.type": "2"}`)

```python
def merge_to_entity(self, df: DataFrame, entity):
    table_ref = self._get_table_reference(entity)
    if self._check_table_exists(table_ref):
        self._merge_to_delta_table(df, entity, table_ref)
    else:
        self.write_to_entity(df, entity)
```

The merge condition is built from `entity.merge_columns`.

## Advanced Features

### SCD Type 2 Merge

When an entity carries `tags={"scd.type": "2"}`, `merge_to_entity` automatically applies the **staged-updates** SCD2 pattern instead of a simple overwrite-in-place merge. For each incoming row:

- If the row matches a current target row **and** tracked columns have changed — the existing row is closed (`__effective_to = now`, `__is_current = false`) and a new version is inserted.
- If the row has no match in the target — it is inserted as a new current row.
- If the row matches but tracked columns are unchanged — no action (no false history entries).

This is handled entirely by `DeltaMergeStrategies` — no pipe-level changes are needed.

### Point-in-Time Reads (`read_entity_as_of`)

For SCD2 entities, `read_entity_as_of` returns the entity state as it appeared at a specific instant:

```python
from datetime import datetime

provider = get_kindling_service(BaseEntityProvider)

# Returns rows where effective_from <= point_in_time < effective_to (or is_current=true)
snapshot_df = provider.read_entity_as_of(entity, datetime(2024, 6, 1))

# Also accepts a string timestamp
snapshot_df = provider.read_entity_as_of(entity, "2024-06-01 00:00:00")
```

For non-SCD2 entities the method falls back to Delta's native `timestampAsOf` time travel.

### Schema Evolution

The DeltaEntityProvider supports automatic schema evolution:

```python
# Enable schema evolution in Spark config
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# During merge operations, new columns will be added automatically
```

### Liquid Clustering

Set `cluster_columns` on the entity definition instead of (or instead of `partition_columns`) to enable Delta liquid clustering. Use `cluster_columns=["auto"]` to request automatic clustering (requires the `kindling.features.delta.auto_clustering` feature flag).

When both `partition_columns` and `cluster_columns` are provided, `cluster_columns` takes precedence and a warning is logged.

## Best Practices

1. **Use Merge Operations**: Prefer merge operations over appends for upsert scenarios.

2. **Define Proper Merge Keys**: Carefully select merge columns that uniquely identify records.

3. **Partitioning Strategy**: Choose partitioning columns based on query patterns and data distribution.

4. **Schema Evolution**: Enable schema evolution for development, but manage schema carefully in production.

5. **Version Management**: Use versioning for incremental processing and auditing.

6. **Performance Optimization**: Consider liquid clustering (`cluster_columns`) for frequently filtered columns.

7. **Data Retention**: Delta's VACUUM and OPTIMIZE commands can be run directly via `spark.sql` or the Databricks UI; they are not exposed as provider methods.

## Custom Entity Providers

To implement a custom entity provider, extend `BaseEntityProvider` plus whichever optional interfaces match your provider's capabilities, then register it with the dependency injection system.

```python
from kindling.entity_provider import BaseEntityProvider, WritableEntityProvider
from kindling.injection import GlobalInjector, inject

@GlobalInjector.singleton_autobind()
class CustomEntityProvider(BaseEntityProvider, WritableEntityProvider):
    @inject
    def __init__(self, config: ConfigService):
        self.config = config

    def read_entity(self, entity_metadata):
        # Custom read implementation
        ...

    def check_entity_exists(self, entity_metadata) -> bool:
        # Custom existence check
        ...

    def write_to_entity(self, df, entity_metadata) -> None:
        # Custom write implementation
        ...

    def append_to_entity(self, df, entity_metadata) -> None:
        # Custom append implementation
        ...
```

A read-only provider (e.g. CSV, EventHub source) only needs to implement `BaseEntityProvider`.
