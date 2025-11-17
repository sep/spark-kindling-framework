# File Ingestion

The File Ingestion module provides a declarative way to define and process file-based data ingestion into the Kindling Framework's entity system.

## Core Concepts

### Ingestion Entries

File ingestion entries define a mapping between file patterns and destination entities. Each entry specifies:

- Source file patterns to match
- Destination entity to load data into
- Schema inference settings
- Metadata tags

### Processing Flow

1. Files matching specified patterns are discovered
2. Data is read according to file format
3. Schemas are validated or inferred
4. Data is written to the destination entity

## Key Components

### FileIngestionMetadata

Defines the configuration for a file ingestion entry.

```python
@dataclass
class FileIngestionMetadata:
    entry_id: str         # Unique identifier for this ingestion entry
    name: str             # Human-readable name
    patterns: List[str]   # List of file patterns to match
    dest_entity_id: str   # ID of destination entity
    tags: Dict[str, str]  # Metadata tags
    infer_schema: bool = True  # Whether to infer schema from files
```

### @FileIngestionEntries.entry Decorator

Decorator to register a file ingestion entry with the framework.

```python
@FileIngestionEntries.entry(
    entry_id="sales_ingestion",
    name="Sales Data Ingestion",
    patterns=["data/sales/*.csv"],
    dest_entity_id="bronze.sales",
    tags={"domain": "sales", "frequency": "daily"},
    infer_schema=True
)
```

### FileIngestionRegistry

Registry that stores and manages all file ingestion entries.

```python
class FileIngestionRegistry(ABC):
    @abstractmethod
    def register_entry(self, entryId, **decorator_params):
        pass

    @abstractmethod
    def get_entry_ids(self):
        pass

    @abstractmethod
    def get_entry_definition(self, entryId):
        pass
```

### FileIngestionProcessor

Interface for file ingestion processing logic.

```python
class FileIngestionProcessor(ABC):
    @abstractmethod
    def process_path(self, path: str):
        pass
```

## Usage Examples

### Defining a File Ingestion Entry

```python
# Register a file ingestion entry
@FileIngestionEntries.entry(
    entry_id="customer_ingestion",
    name="Customer Data Ingestion",
    patterns=["data/customers/*.csv", "data/customers/*.parquet"],
    dest_entity_id="bronze.customers",
    tags={"domain": "customer", "layer": "bronze"}
)
```

### Processing a File Ingestion Entry

```python
# Get the processor
processor = GlobalInjector.get(FileIngestionProcessor)

# Process a specific ingestion entry
processor.process_entry("customer_ingestion")

# Or process all entries
processor.process_all_entries()
```

## File Format Support

The File Ingestion module supports various file formats:

- CSV
- Parquet
- JSON
- Delta Lake
- ORC
- Avro (with appropriate Spark extensions)

## Advanced Configuration

### Custom Schema Definition

When `infer_schema=False`, you need to define the schema for your ingestion entry:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define schema
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Register with explicit schema
@FileIngestionEntries.entry(
    entry_id="customer_ingestion",
    name="Customer Data Ingestion",
    patterns=["data/customers/*.csv"],
    dest_entity_id="bronze.customers",
    tags={"domain": "customer"},
    infer_schema=False,
    schema=customer_schema
)
```

### Format-Specific Options

You can provide format-specific options for file reading:

```python
@FileIngestionEntries.entry(
    entry_id="csv_ingestion",
    name="CSV Ingestion with Options",
    patterns=["data/*.csv"],
    dest_entity_id="bronze.data",
    tags={},
    options={
        "header": "true",
        "delimiter": "|",
        "inferSchema": "true"
    }
)
```

## Best Practices

1. **Use specific file patterns**: Avoid overly broad patterns that might match unwanted files.

2. **Consider partitioning**: For large datasets, ensure your file organization aligns with your entity partitioning.

3. **Schema management**: For production workloads, explicitly define schemas rather than relying on inference.

4. **Error handling**: Configure appropriate error handling for malformed records.

5. **Metadata enrichment**: Consider adding ingestion metadata like source file and timestamp.
