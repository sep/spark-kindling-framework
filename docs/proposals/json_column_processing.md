# Generic JSON Column Processing for Kindling

**Status**: Draft
**Author**: System Analysis
**Created**: 2026-02-13
**Updated**: 2026-02-13

## Executive Summary

This proposal introduces **generic JSON processing capabilities** to the Kindling framework, enabling seamless ingestion, parsing, and transformation of JSON data from various sources (files, Event Hubs, APIs) into structured Delta Lake tables. The solution provides both **declarative schema-driven** and **flexible schema-less** approaches for handling JSON payloads.

## Problem Statement

### Current Limitations

JSON is ubiquitous in modern data architectures (APIs, Event Hubs, IoT devices, logs), but Kindling currently lacks first-class JSON support for the **common raw → staging pattern** where JSON payloads are stored as string columns and later parsed:

1. **No JSON Column Parsing Utilities**: Users must manually write PySpark JSON parsing code for every table
2. **No Schema Inference from Columns**: No framework support for discovering JSON schemas from existing data
3. **No Nested Structure Handling**: No utilities for flattening nested JSON stored in columns
4. **No JSON Validation**: No built-in validation against expected schemas
5. **Repetitive Transform Code**: Every staging pipe duplicates the same JSON parsing logic
6. **Poor Error Handling**: No standardized approach for handling malformed JSON in columns

### Common JSON Scenarios

**PRIMARY SCENARIO: JSON Stored in Column (Raw → Staging Pattern)**

This is the most common pattern - JSON payloads stored as string columns in raw tables:

```python
# RAW LAYER - JSON stored as string column
# Table: raw.api_events
# Columns: event_id (string), received_at (timestamp), payload (string)
# Sample payload value:
# '{"user_id": "123", "action": "login", "metadata": {"ip": "1.2.3.4", "device": "mobile"}}'

# STAGING LAYER - Parse JSON column into structured columns
# Desired output columns: event_id, received_at, user_id, action, metadata_ip, metadata_device
```

**Scenario 1: Event Hub → Raw (JSON as String)**
```python
# Event Hub stream writes raw JSON to Delta
# raw.telemetry table:
# - event_time: timestamp (from Event Hub)
# - offset: string (from Event Hub)
# - json_payload: string (entire JSON message as string)

# staging.telemetry must parse json_payload into columns
```

**Scenario 2: API Ingestion → Raw (JSON as Column)**
```python
# API responses stored as JSON strings
# raw.api_responses table:
# - request_id: string
# - response_time: timestamp
# - response_body: string  # <-- JSON stored here

# staging.parsed_responses must parse response_body
```

**Scenario 3: Nested JSON in Column**
```python
# Complex nested JSON in a column
# raw.transactions table has column 'transaction_data' containing:
# '{"transaction_id": "tx-001", "customer": {"id": "123", "address": {"city": "Seattle"}}, "items": [...]}'

# staging.flat_transactions must:
# - Parse the JSON string
# - Flatten nested structures
# - Explode arrays
```

### Current Workaround (Manual)

Users must write custom transforms for every JSON use case:

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define schema manually
json_schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType())
])

@DataPipes.pipe(
    pipeid="parse_events",
    input_entity_ids=["stream.raw_events"],
    output_entity_id="stream.parsed_events",
    output_type="append",
    tags={}
)
def parse_json(df):
    # Manual JSON parsing
    return df \
        .selectExpr("cast(body as string) as json_string") \
        .select(from_json(col("json_string"), json_schema).alias("data")) \
        .select("data.*")
```

**Issues**:
- ❌ Verbose and repetitive
- ❌ Schema definition duplicated across pipes
- ❌ No reusability
- ❌ Error-prone
- ❌ No schema evolution support

## Proposed Solution

### 1. JSON Entity Provider

A dedicated provider for reading JSON files:

```python
@GlobalInjector.singleton_autobind()
class JSONEntityProvider(BaseEntityProvider, WritableEntityProvider):
    """
    JSON file entity provider supporting both batch reads and writes.

    Provider configuration options (via entity tags with 'provider.' prefix):
    - provider.path: JSON file path (required)
    - provider.multiLine: Support multi-line JSON (default: "false" for JSONL)
    - provider.mode: Parsing mode - PERMISSIVE, DROPMALFORMED, FAILFAST
    - provider.compression: Compression codec - gzip, bzip2, etc.

    Example:
    @DataEntities.entity(
        entityid="raw.api_events",
        tags={
            "provider_type": "json",
            "provider.path": "Files/raw/events/*.jsonl",
            "provider.multiLine": "false"
        },
        schema=None  # Auto-infer
    )
    """
```

### 2. JSON Parsing Utilities

Core utilities in `packages/kindling/json_transforms.py`:

#### **parse_json_column()**
Parse JSON string column into structured columns with schema inference:

```python
def parse_json_column(
    df: DataFrame,
    json_column: str,
    schema: Optional[StructType] = None,
    infer_from_sample: bool = True,
    flatten: bool = True,
    drop_source: bool = True
) -> DataFrame:
    """
    Parse JSON string column into structured columns.

    Example:
        >>> df = spark.createDataFrame([
        ...     ('{"name": "Alice", "age": 30}',),
        ... ], ["json_data"])
        >>> parse_json_column(df, "json_data")
        DataFrame[name: string, age: bigint]
    """
```

#### **parse_eventhub_json()**
Parse JSON from Event Hub body column:

```python
def parse_eventhub_json(
    df: DataFrame,
    schema: Optional[StructType] = None,
    keep_metadata: bool = True,
    metadata_columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Parse JSON from Event Hub body + keep metadata.

    Example:
        >>> stream = eventhub_provider.read_entity_as_stream(entity)
        >>> parsed = parse_eventhub_json(stream, keep_metadata=True)
        # Result includes parsed JSON fields + enqueuedTime, offset, etc.
    """
```

#### **flatten_json_struct()**
Flatten nested struct into top-level columns:

```python
def flatten_json_struct(
    df: DataFrame,
    struct_column: str,
    separator: str = "_",
    drop_source: bool = True
) -> DataFrame:
    """
    Flatten nested struct column.

    Example:
        # Input: address: {city: "Seattle", state: "WA"}
        >>> flatten_json_struct(df, "address")
        # Output: address_city: "Seattle", address_state: "WA"
    """
```

#### **explode_json_array()**
Explode JSON array into multiple rows:

```python
def explode_json_array(
    df: DataFrame,
    array_column: str,
    flatten_elements: bool = True
) -> DataFrame:
    """
    Explode array column into multiple rows.

    Example:
        # Input: items: [{"sku": "A", "qty": 2}, {"sku": "B", "qty": 1}]
        >>> explode_json_array(df, "items")
        # Output: 2 rows with sku and qty columns
    """
```

#### **extract_json_path()**
Extract specific fields using JSONPath (lightweight):

```python
def extract_json_path(
    df: DataFrame,
    json_column: str,
    paths: Dict[str, str],
    drop_source: bool = False
) -> DataFrame:
    """
    Extract fields via JSONPath without full parsing.

    Example:
        >>> paths = {
        ...     "user_id": "$.user.id",
        ...     "event_type": "$.event",
        ...     "device_ip": "$.metadata.ip"
        ... }
        >>> extract_json_path(df, "json_data", paths)
    """
```

#### **validate_json_schema()**
Validate JSON against expected schema:

```python
def validate_json_schema(
    df: DataFrame,
    json_column: str,
    expected_schema: StructType,
    error_column: str = "json_parse_error"
) -> DataFrame:
    """
    Validate JSON and add error column.

    Returns DataFrame with error column (null if valid).
    """
```

#### **infer_schema_from_json_column()**
Infer schema from JSON stored in existing table column:

```python
def infer_schema_from_json_column(
    df: DataFrame,
    json_column: str,
    sample_size: int = 1000
) -> StructType:
    """
    Infer JSON schema from existing table column.

    Useful when creating staging tables from raw tables with JSON columns.

    Example:
        >>> # Infer schema from raw table
        >>> raw_df = entity_provider.read_entity(raw_entity)
        >>> schema = infer_schema_from_json_column(raw_df, "json_payload")
        >>>
        >>> # Use inferred schema to create staging table
        >>> @DataEntities.entity(
        ...     entityid="staging.events",
        ...     schema=schema  # Use inferred schema
        ... )
    """
    from pyspark.sql.functions import schema_of_json

    # Sample JSON values from column
    sample_df = df.select(json_column).limit(sample_size)
    json_values = [row[0] for row in sample_df.collect() if row[0]]

    if not json_values:
        raise ValueError(f"No non-null JSON found in column {json_column}")

    # Infer schema from first sample
    return schema_of_json(json_values[0])
```

### 3. Integration with Existing Components

#### **File Ingestion**
```python
@FileIngestionEntries.entry(
    entry_id="sensor_events",
    patterns=[r"(?P<date>\d{4}-\d{2}-\d{2})_sensors\.json"],
    dest_entity_id="raw.sensor_events_{date}",
    filetype="json",  # JSON format support
    infer_schema=True
)
```

#### **Streaming Pipes**
```python
@DataPipes.pipe(
    pipeid="parse_sensors",
    input_entity_ids=["stream.raw_sensors"],
    output_entity_id="stream.parsed_sensors",
    output_type="append",
    tags={
        "processing_mode": "streaming",
        "json.auto_parse": "true"  # Auto-parse via framework
    }
)
def transform_sensors(df):
    # JSON pre-parsed by framework based on tags
    return df.filter(col("sensor_type") == "temperature")
```

## Use Case Examples

### Use Case 1: Raw → Staging (JSON Column Parsing)

**THE PRIMARY PATTERN** - Parse JSON stored in a column:

```python
from kindling.json_transforms import parse_json_column

# RAW table already exists with JSON in 'payload' column
@DataEntities.entity(
    entityid="raw.api_events",
    name="api_events",
    partition_columns=["date"],
    merge_columns=["event_id"],
    tags={"provider_type": "delta"},
    schema=StructType([
        StructField("event_id", StringType()),
        StructField("received_at", TimestampType()),
        StructField("payload", StringType()),  # <-- JSON stored here
        StructField("date", DateType())
    ])
)

# STAGING table with parsed/structured columns
@DataEntities.entity(
    entityid="staging.parsed_events",
    name="parsed_events",
    partition_columns=["date"],
    merge_columns=["event_id"],
    tags={"provider_type": "delta"},
    schema=parsed_schema  # Structured schema
)

# Pipe to parse JSON column from raw → staging
@DataPipes.pipe(
    pipeid="parse_api_events",
    name="parse_api_events",
    input_entity_ids=["raw.api_events"],
    output_entity_id="staging.parsed_events",
    output_type="merge",
    tags={}
)
def parse_events(df):
    # Parse JSON column into structured columns
    return parse_json_column(
        df,
        json_column="payload",  # Column containing JSON string
        schema=None,  # Auto-infer from data
        flatten=True,  # Expand into top-level columns
        drop_source=True  # Drop the 'payload' column after parsing
    )

# Result: staging.parsed_events has structured columns instead of JSON string
```

### Use Case 2: Event Hub → Raw → Staging (Two-Stage Pattern)

**Best Practice**: Store raw JSON in raw layer, parse in staging layer:

```python
from kindling.json_transforms import parse_json_column

# STEP 1: Event Hub → Raw (store JSON as string)
@DataEntities.entity(
    entityid="raw.telemetry",
    name="raw_telemetry",
    partition_columns=["date"],
    merge_columns=[],
    tags={"provider_type": "delta"},
    schema=StructType([
        StructField("event_time", TimestampType()),
        StructField("offset", StringType()),
        StructField("json_payload", StringType()),  # <-- Store entire JSON here
        StructField("date", DateType())
    ])
)

@DataPipes.pipe(
    pipeid="eventhub_to_raw",
    input_entity_ids=["stream.eventhub_telemetry"],
    output_entity_id="raw.telemetry",
    output_type="append",
    tags={"processing_mode": "streaming"}
)
def store_raw(df):
    # Just cast body to string and store
    return df.select(
        col("enqueuedTime").alias("event_time"),
        col("offset"),
        col("body").cast(StringType()).alias("json_payload"),
        to_date(col("enqueuedTime")).alias("date")
    )

# STEP 2: Raw → Staging (parse JSON column)
@DataEntities.entity(
    entityid="staging.telemetry",
    name="staging_telemetry",
    partition_columns=["device_type", "date"],
    merge_columns=["device_id", "event_time"],
    tags={"provider_type": "delta"},
    schema=telemetry_schema
)

@DataPipes.pipe(
    pipeid="raw_to_staging",
    input_entity_ids=["raw.telemetry"],
    output_entity_id="staging.telemetry",
    output_type="merge",
    tags={}
)
def parse_telemetry(df):
    # Parse the json_payload column from raw
    return parse_json_column(
        df,
        json_column="json_payload",
        schema=telemetry_schema,
        flatten=True,
        drop_source=True
    )

# Benefits:
# - Raw layer has complete audit trail (original JSON)
# - Can reprocess staging if parsing logic changes
# - Separates ingestion from transformation concerns
```

### Use Case 3: Complex Nested JSON Column

Parse and flatten complex nested JSON stored in a column:

```python
from kindling.json_transforms import parse_json_column, flatten_json_struct, explode_json_array

# Raw table has 'transaction_json' column with nested JSON
@DataEntities.entity(
    entityid="raw.api_transactions",
    tags={"provider_type": "delta"},
    schema=StructType([
        StructField("transaction_id", StringType()),
        StructField("received_at", TimestampType()),
        StructField("transaction_json", StringType())  # <-- Complex nested JSON here
    ])
)

# Sample transaction_json value:
# '{"customer": {"id": "123", "address": {"city": "Seattle"}}, "items": [{"sku": "A", "qty": 2}]}'

@DataPipes.pipe(
    pipeid="flatten_transactions",
    input_entity_ids=["raw.api_transactions"],
    output_entity_id="staging.flat_transactions",
    output_type="merge",
    tags={}
)
def flatten_transactions(df):
    # Step 1: Parse JSON column into struct
    df = parse_json_column(
        df,
        json_column="transaction_json",
        flatten=False,  # Keep as struct initially
        drop_source=True
    )
    # Now we have: transaction_json_parsed: {customer: {id, address: {city}}, items: [...]}

    # Step 2: Flatten customer struct
    df = flatten_json_struct(df, "transaction_json_parsed_customer")
    # Result: customer_id, customer_address: {city}

    # Step 3: Flatten nested address
    df = flatten_json_struct(df, "customer_address")
    # Result: customer_address_city

    # Step 4: Explode items array (creates multiple rows)
    df = explode_json_array(df, "transaction_json_parsed_items")
    # Result: One row per item with item_sku, item_qty

    return df

# Output: Fully flattened table with structured columns
```

### Use Case 4: Schema Evolution in JSON Columns

Handle schema changes in JSON columns over time:

```python
from kindling.json_transforms import parse_json_column
from pyspark.sql.functions import when, col, lit

# Raw table with JSON that has evolved over time
# Old format: {"user_id": "123", "action": "login"}
# New format: {"user_id": "123", "action": "login", "session_id": "abc", "app_version": "2.0"}

@DataPipes.pipe(
    pipeid="parse_with_evolution",
    input_entity_ids=["raw.user_events"],
    output_entity_id="staging.user_events",
    output_type="merge",
    tags={}
)
def parse_with_schema_evolution(df):
    # Define schemas for both versions
    old_schema = StructType([
        StructField("user_id", StringType()),
        StructField("action", StringType())
    ])

    new_schema = StructType([
        StructField("user_id", StringType()),
        StructField("action", StringType()),
        StructField("session_id", StringType()),
        StructField("app_version", StringType())
    ])

    # Try parsing with new schema (PERMISSIVE mode handles missing fields)
    df = parse_json_column(
        df,
        json_column="json_payload",
        schema=new_schema,  # Use latest schema
        flatten=True,
        drop_source=True
    )

    # Fill in defaults for old records (where new fields are null)
    df = df \
        .withColumn(
            "session_id",
            when(col("session_id").isNull(), lit("legacy")).otherwise(col("session_id"))
        ) \
        .withColumn(
            "app_version",
            when(col("app_version").isNull(), lit("1.0")).otherwise(col("app_version"))
        )

    return df

# Benefit: Single pipe handles both old and new JSON formats
# Raw data never needs to be re-ingested
```

### Use Case 5: Lightweight JSONPath Extraction from Column

When you only need a few fields from large JSON payloads stored in columns:

```python
from kindling.json_transforms import extract_json_path

# Raw table with large JSON payloads in 'log_data' column
@DataEntities.entity(
    entityid="raw.api_logs",
    tags={"provider_type": "delta"},
    schema=StructType([
        StructField("log_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("log_data", StringType())  # <-- Large JSON blob
    ])
)

# Sample log_data (100+ fields, we only need 4):
# '{"request": {"user": {"id": "123"}, "path": "/api/data"}, "response": {"status": 200}, "metrics": {"latency": 45}}'

@DataPipes.pipe(
    pipeid="extract_metrics",
    input_entity_ids=["raw.api_logs"],
    output_entity_id="analytics.user_activity",
    output_type="append",
    tags={}
)
def extract_metrics(df):
    # Extract only needed fields using JSONPath (faster than full parse)
    paths = {
        "user_id": "$.request.user.id",
        "endpoint": "$.request.path",
        "status_code": "$.response.status",
        "latency_ms": "$.metrics.latency"
    }

    return extract_json_path(
        df,
        json_column="log_data",  # Column containing JSON
        paths=paths,
        drop_source=True  # Drop large JSON column after extraction
    )

# Benefit: Much faster than parsing entire JSON when you only need a few fields
```

## Implementation Phases

### Phase 1: Core Utilities (1-2 weeks)
- `json_transforms.py` module
- `parse_json_column()`, `parse_eventhub_json()`, `flatten_json_struct()`
- Unit tests with 100% coverage
- Documentation

### Phase 2: JSON Entity Provider (1-2 weeks)
- `JSONEntityProvider` class
- JSONL and multi-line JSON support
- Schema inference
- Registry integration

### Phase 3: Advanced Utilities (1 week)
- `explode_json_array()`, `extract_json_path()`, `validate_json_schema()`
- Complex nested JSON handling
- Performance optimization

### Phase 4: File Ingestion Integration (1 week)
- JSON support in FileIngestionProcessor
- Auto-detection
- Tag-based configuration

### Phase 5: Streaming Integration (1-2 weeks)
- Auto-parse in streaming pipes
- Event Hub examples
- Performance tuning

## Architectural Pattern: Raw → Staging with JSON Columns

### The Core Pattern

This proposal primarily addresses the **layered architecture pattern** where JSON is stored as strings in raw tables and parsed into structured columns in staging:

```
┌──────────────────────────────────────────────────────────────┐
│  BRONZE/RAW LAYER                                            │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Table: raw.events                                      │  │
│  │ Columns:                                               │  │
│  │   - event_id: string                                   │  │
│  │   - ingested_at: timestamp                             │  │
│  │   - json_payload: string  ◄── JSON stored as string   │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
                            │
                            │ parse_json_column()
                            ↓
┌──────────────────────────────────────────────────────────────┐
│  SILVER/STAGING LAYER                                        │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Table: staging.events                                  │  │
│  │ Columns:                                               │  │
│  │   - event_id: string                                   │  │
│  │   - user_id: string       ◄── Parsed from JSON        │  │
│  │   - action: string        ◄── Parsed from JSON        │  │
│  │   - timestamp: timestamp  ◄── Parsed from JSON        │  │
│  │   - device: string        ◄── Parsed from JSON        │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

### Why This Pattern?

**✅ Audit Trail**: Raw layer preserves original JSON for compliance and debugging
**✅ Reprocessability**: Can reparse staging if schema or logic changes
**✅ Schema Evolution**: Handle schema changes without re-ingesting raw data
**✅ Separation of Concerns**: Ingestion logic separate from transformation logic
**✅ Error Isolation**: Parsing errors don't affect raw ingestion
**✅ Cost Efficiency**: Store once in raw, parse multiple ways in staging

### Implementation Pattern

```python
# Pattern: Raw → Staging transformation
@DataPipes.pipe(
    pipeid="raw_to_staging",
    input_entity_ids=["raw.events"],     # Source: JSON in column
    output_entity_id="staging.events",   # Target: Structured columns
    output_type="merge",
    tags={}
)
def parse_raw_to_staging(df):
    return parse_json_column(
        df,
        json_column="json_payload",  # Parse this column
        flatten=True,                # Create top-level columns
        drop_source=True             # Remove JSON column
    )
```

## Architecture Benefits

✅ **JSON Column First**: Optimized for parsing JSON stored in Delta columns
✅ **Layered Architecture**: Supports standard raw → staging → analytics pattern
✅ **Reusable Utilities**: Common operations extracted into framework functions
✅ **Declarative**: Minimal code for common JSON transformations
✅ **Framework-Integrated**: Works seamlessly with existing Kindling patterns
✅ **Performant**: Uses native Spark JSON functions (no external dependencies)
✅ **Flexible**: Supports schema-driven and schema-less approaches
✅ **Production-Ready**: Error handling, validation, monitoring signals

## Key Design Decisions

**1. JSON Column Parsing First**
- **Primary focus**: Parse JSON from existing table columns (raw → staging pattern)
- **Secondary**: Direct JSON file ingestion via entity provider
- Aligns with standard data lake architecture (bronze → silver → gold)

**2. Use Native Spark Functions**
- Leverage PySpark's `from_json()`, `get_json_object()`, `schema_of_json()`
- No external dependencies (pandas, external JSON libs)
- Optimal performance and compatibility

**3. Utility-First Approach**
- Composable functions for common operations
- Can be used in any pipe transform
- Not tied to specific providers or data sources
- Mix and match utilities as needed

**4. Schema Inference from Data**
- Auto-detect schema from JSON stored in columns
- Sample existing tables to infer structure
- Users can override with explicit schema
- Balances convenience (auto-infer) and control (explicit)

**5. Raw Data Preservation**
- Store original JSON in raw layer
- Parse into structured columns in staging
- Enables reprocessing without re-ingestion
- Audit trail for compliance

**6. Handle Schema Evolution**
- Support changing JSON structures over time
- PERMISSIVE mode allows missing fields
- Fill nulls with defaults for old records
- Single pipe handles multiple versions

## Success Criteria

- **Adoption**: >20% of new entities use JSON capabilities within 3 months
- **Performance**: <10% overhead vs manual JSON parsing
- **Quality**: >85% test coverage, <2 bugs/month after GA
- **Developer Experience**: >80% positive feedback

## Future Enhancements

- **JSON Schema Registry**: Central schema storage with versioning
- **JSON Merge**: Merge JSON fields with existing columns
- **Validation Rules**: Declarative validation (required, type, range)
- **Schema Evolution**: Auto-migration for schema changes

## Best Practices for Raw → Staging Pattern

### 1. Always Store Raw JSON

**❌ Don't**: Parse JSON immediately upon ingestion
```python
# BAD: Parse during ingestion - no audit trail
@DataPipes.pipe(
    pipeid="eventhub_direct_to_staging",
    input_entity_ids=["stream.eventhub"],
    output_entity_id="staging.events",
    output_type="append",
    tags={}
)
def parse_immediately(df):
    # If this parsing logic changes, you must re-ingest from Event Hub
    return parse_eventhub_json(df)
```

**✅ Do**: Store raw JSON first, then parse in separate pipe
```python
# GOOD: Two-stage approach
# Pipe 1: Store raw
@DataPipes.pipe(
    pipeid="eventhub_to_raw",
    input_entity_ids=["stream.eventhub"],
    output_entity_id="raw.events",
    output_type="append",
    tags={}
)
def store_raw(df):
    return df.select(
        col("body").cast(StringType()).alias("json_payload")
    )

# Pipe 2: Parse raw to staging
@DataPipes.pipe(
    pipeid="raw_to_staging",
    input_entity_ids=["raw.events"],
    output_entity_id="staging.events",
    output_type="merge",
    tags={}
)
def parse_staging(df):
    # Can reprocess staging anytime by re-running this pipe
    return parse_json_column(df, "json_payload")
```

### 2. Use Schema Inference Initially, Explicit Schema in Production

**Development/Discovery**:
```python
# Infer schema during development
df = parse_json_column(df, "json_payload", schema=None)
```

**Production**:
```python
# Use explicit schema in production for stability
EVENTS_SCHEMA = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("action", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False)
])

df = parse_json_column(df, "json_payload", schema=EVENTS_SCHEMA)
```

### 3. Keep JSON Column Initially, Drop Later

**Initial Staging Table**:
```python
# Keep json_payload for debugging/validation
df = parse_json_column(df, "json_payload", drop_source=False)
```

**After Validation**:
```python
# Drop json_payload to save storage
df = parse_json_column(df, "json_payload", drop_source=True)
```

### 4. Add Validation Column for Production

```python
from kindling.json_transforms import parse_json_column

def parse_with_validation(df):
    # Parse JSON
    df = parse_json_column(
        df,
        json_column="json_payload",
        schema=EXPECTED_SCHEMA,
        drop_source=False  # Keep for error analysis
    )

    # Add validation column
    df = df.withColumn(
        "parse_error",
        when(col("user_id").isNull(), "Missing user_id")
        .when(col("timestamp").isNull(), "Missing timestamp")
        .otherwise(None)
    )

    return df

# Monitor parse_error column for data quality issues
```

### 5. Partition Raw by Ingestion Date, Staging by Business Date

```python
# Raw layer: Partition by when data arrived
@DataEntities.entity(
    entityid="raw.events",
    partition_columns=["ingestion_date"],  # When ingested
    tags={"provider_type": "delta"}
)

# Staging layer: Partition by business date
@DataEntities.entity(
    entityid="staging.events",
    partition_columns=["event_date"],  # From parsed JSON
    tags={"provider_type": "delta"}
)

@DataPipes.pipe(...)
def parse_staging(df):
    df = parse_json_column(df, "json_payload")
    # Extract business date from parsed timestamp
    df = df.withColumn("event_date", to_date(col("timestamp")))
    return df
```

## Open Questions

1. **Schema Storage**: Where should inferred schemas be cached? Config? Delta table?
2. **Error Handling**: Should malformed JSON fail the entire batch or just log errors?
3. **Performance Budget**: What's acceptable overhead for JSON parsing in streaming?
4. **Schema Evolution**: Should framework auto-upgrade schemas or require explicit migration?
5. **Compression**: Should JSON provider support compressed files by default?

## References

- [PySpark JSON Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Event Hub Spark Connector](https://github.com/Azure/azure-event-hubs-spark)
- [Delta Lake Schema Evolution](https://docs.delta.io/latest/delta-update.html#automatic-schema-evolution)
- [JSONL Format](https://jsonlines.org/)
- [JSONPath Specification](https://goessner.net/articles/JsonPath/)
- [Entity Provider Architecture](../entity_providers.md)
- [Data Entities Documentation](../data_entities.md)

## Appendix

### A. Complete End-to-End Example: Raw → Staging Pattern

**Three-layer architecture with JSON column parsing:**

```python
from kindling.json_transforms import parse_json_column, flatten_json_struct

# ===== LAYER 1: RAW - Store JSON as string =====

@DataEntities.entity(
    entityid="raw.iot_events",
    name="raw_iot_events",
    partition_columns=["ingestion_date"],
    merge_columns=[],
    tags={"provider_type": "delta"},
    schema=StructType([
        StructField("event_id", StringType()),
        StructField("ingested_at", TimestampType()),
        StructField("source_system", StringType()),
        StructField("json_payload", StringType()),  # <-- Original JSON stored here
        StructField("ingestion_date", DateType())
    ])
)

# Raw ingestion pipe (Event Hub → Raw)
@DataPipes.pipe(
    pipeid="ingest_iot_raw",
    input_entity_ids=["stream.eventhub_iot"],
    output_entity_id="raw.iot_events",
    output_type="append",
    tags={"processing_mode": "streaming"}
)
def ingest_raw(df):
    # Store entire JSON as string - no parsing yet
    return df.select(
        col("properties.messageId").alias("event_id"),
        current_timestamp().alias("ingested_at"),
        lit("iot-hub").alias("source_system"),
        col("body").cast(StringType()).alias("json_payload"),
        to_date(current_timestamp()).alias("ingestion_date")
    )

# ===== LAYER 2: STAGING - Parse JSON column =====

@DataEntities.entity(
    entityid="staging.iot_events",
    name="staging_iot_events",
    partition_columns=["event_date"],
    merge_columns=["device_id", "event_timestamp"],
    tags={"provider_type": "delta"},
    schema=StructType([
        StructField("event_id", StringType()),
        StructField("device_id", StringType()),
        StructField("event_timestamp", TimestampType()),
        StructField("sensor_type", StringType()),
        StructField("value", DoubleType()),
        StructField("device_location", StringType()),  # Flattened from nested JSON
        StructField("device_version", StringType()),   # Flattened from nested JSON
        StructField("event_date", DateType())
    ])
)

# Staging pipe (Raw → Staging) - Parse JSON column
@DataPipes.pipe(
    pipeid="parse_iot_staging",
    input_entity_ids=["raw.iot_events"],
    output_entity_id="staging.iot_events",
    output_type="merge",
    tags={}
)
def parse_staging(df):
    # Parse JSON column into structured columns
    df = parse_json_column(
        df,
        json_column="json_payload",  # Column with JSON string
        schema=None,  # Auto-infer
        flatten=False,  # Keep as struct first
        drop_source=False  # Keep for debugging initially
    )

    # Flatten nested device_info struct
    df = flatten_json_struct(df, "json_payload_parsed_device_info")

    # Select and rename columns
    df = df.select(
        col("event_id"),
        col("json_payload_parsed_device_id").alias("device_id"),
        col("json_payload_parsed_timestamp").alias("event_timestamp"),
        col("json_payload_parsed_sensor_type").alias("sensor_type"),
        col("json_payload_parsed_value").alias("value"),
        col("device_info_location").alias("device_location"),
        col("device_info_version").alias("device_version"),
        to_date(col("json_payload_parsed_timestamp")).alias("event_date")
    )

    return df

# ===== LAYER 3: ANALYTICS - Business logic =====

@DataPipes.pipe(
    pipeid="analytics_iot",
    input_entity_ids=["staging.iot_events"],
    output_entity_id="analytics.iot_metrics",
    output_type="merge",
    tags={}
)
def calculate_analytics(df):
    # Now work with clean, structured data
    return df \
        .filter(col("sensor_type") == "temperature") \
        .filter(col("value").between(-50, 100)) \
        .groupBy("device_id", "event_date") \
        .agg(
            avg("value").alias("avg_temperature"),
            max("value").alias("max_temperature")
        )

# Benefits of this pattern:
# 1. Raw layer preserves original JSON (audit trail, reprocessing)
# 2. Staging layer handles all JSON parsing complexity
# 3. Analytics layer works with clean, typed data
# 4. Can reprocess staging if schema or parsing logic changes
# 5. Separation of concerns (ingestion vs parsing vs analytics)
```

### B. Performance Considerations

**Schema Inference Cost**:
- Reading sample: ~1-3 seconds per file
- **Mitigation**: Cache inferred schemas in config

**JSON Parsing Overhead**:
- Simple JSON: ~5-10% overhead vs pre-parsed
- Nested JSON: ~20-30% overhead
- **Mitigation**: Use JSONPath extraction for large payloads

**Memory Usage**:
- Deeply nested JSON can cause OOM
- **Mitigation**: Process in smaller batches, use columnar pruning

### C. Error Handling Strategies

**PERMISSIVE Mode** (Default):
- Invalid JSON → null values in parsed columns
- Partial parsing succeeds
- Good for production resilience

**DROPMALFORMED Mode**:
- Invalid JSON → entire row dropped
- Silent data loss risk
- Use with monitoring

**FAILFAST Mode**:
- Invalid JSON → job fails immediately
- Safe for critical data
- Requires retry logic

**Recommendation**: Use PERMISSIVE + validation column for observability

### D. JSON Schema Definition Helper

```python
from pyspark.sql.types import *

def json_schema_from_dict(schema_dict):
    """
    Create StructType from simple dictionary.

    Example:
        schema = json_schema_from_dict({
            "event_id": "string",
            "timestamp": "timestamp",
            "value": "double",
            "metadata": {
                "device": "string",
                "location": "string"
            }
        })
    """
    type_map = {
        "string": StringType(),
        "int": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "timestamp": TimestampType(),
        "date": DateType(),
        "boolean": BooleanType()
    }

    fields = []
    for name, dtype in schema_dict.items():
        if isinstance(dtype, dict):
            # Nested struct
            fields.append(StructField(name, json_schema_from_dict(dtype)))
        else:
            fields.append(StructField(name, type_map[dtype]))

    return StructType(fields)
```

---

This proposal provides comprehensive JSON support while maintaining Kindling's design principles of simplicity, composability, and framework integration. The utilities can be adopted incrementally, starting with simple use cases and scaling to complex nested JSON processing.
