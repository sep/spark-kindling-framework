# File Ingestion

The File Ingestion module provides a declarative way to map file patterns to destination entities. When a file matches an entry's pattern, the processor reads it, enriches it with metadata columns, and appends it to the target entity table.

## Registering an ingestion entry

Use `FileIngestionEntries.entry()` to declare a mapping. All parameters must be provided except `infer_schema` (defaults to `True`) and `static_values` (defaults to `None`).

```python
FileIngestionEntries.entry(
    entry_id="sales_daily",
    name="Daily Sales Files",
    patterns=[r"sales_(?P<region>\w+)_(?P<date>\d{8})\.csv"],
    dest_entity_id="bronze.sales",
    tags={"domain": "sales", "layer": "bronze"},
    filetype="csv",
)
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `entry_id` | `str` | Yes | Unique identifier for this entry |
| `name` | `str` | Yes | Human-readable description |
| `patterns` | `List[str]` | Yes | Regex patterns to match filenames |
| `dest_entity_id` | `str` | Yes | Entity ID to write matched files into |
| `tags` | `Dict[str, str]` | Yes | Metadata tags (may be empty) |
| `filetype` | `str` | Yes | Accepted by the API but **not currently used** — see note below |
| `infer_schema` | `bool` | No | Accepted by the API but **not currently effective** — see note below |
| `static_values` | `Dict[str, Any]` | No | Literal column values added to every matched row |

> **`filetype` is not read by the processor.** The Spark format is driven entirely by the `filetype` named regex group in the matched pattern (e.g. `(?P<filetype>csv)`), falling back to `"csv"` when that group is absent. The `filetype` parameter stored on the entry is never consulted.

> **`infer_schema` is not effective.** Schema inference is always disabled (`inferSchema=false` is hardcoded in `_build_df_plan`). The parameter is stored on the entry but never passed to the Spark reader.

## Controlling the read format

Because the format comes from the regex match, embed a `filetype` named group in the pattern when you need to read non-CSV files:

```python
FileIngestionEntries.entry(
    entry_id="sales_daily",
    name="Daily Sales Files",
    patterns=[r"sales_(?P<region>\w+)_(?P<date>\d{8})\.(?P<filetype>parquet)"],
    dest_entity_id="bronze.sales",
    tags={"domain": "sales", "layer": "bronze"},
    filetype="parquet",  # stored but ignored at runtime; document for human readers only
)
```

If no `filetype` named group is present in the pattern, the reader defaults to `"csv"`.

## Processing files

```python
from kindling.file_ingestion import ParallelizingFileIngestionProcessor
from kindling.injection import get_kindling_service

processor = get_kindling_service(ParallelizingFileIngestionProcessor)

# Ingest all matching files from a path
processor.process_path("abfss://landing@account.dfs.core.windows.net/sales/")

# Optionally move processed files after a successful write
processor.process_path(
    "abfss://landing@account.dfs.core.windows.net/sales/",
    movepath="abfss://archive@account.dfs.core.windows.net/processed/",
)

# Apply a transformation before writing
processor.process_path(path, transform=lambda df: df.withColumn("amount", df.amount.cast("double")))
```

`process_path` discovers all files in `path`, matches each against registered entry patterns, groups matches by destination entity, and writes each group in a single batched append. Tables for multiple destinations can be written in parallel (controlled by `ingestion.max_parallel_tables` config, default `3`).

## Columns added automatically

For every matched file the processor appends extra columns before writing:

| Column | Source |
|--------|--------|
| One column per named regex group | The group **name** becomes the column name; the captured **value** becomes the column value |
| `ingestion_timestamp` | `current_timestamp()` at the time of processing |

For example, a pattern `r"sales_(?P<region>\w+)_(?P<date>\d{8})\.csv"` matched against `sales_west_20240601.csv` adds two columns to every row: `region = "west"` and `date = "20240601"`.

Named groups are also available for interpolation in `dest_entity_id`:

```python
FileIngestionEntries.entry(
    entry_id="regional_sales",
    patterns=[r"sales_(?P<region>\w+)_(?P<filetype>csv)\.csv"],
    dest_entity_id="bronze.sales_{region}",   # resolves to e.g. "bronze.sales_west"
    ...
)
```

## Static values

`static_values` adds literal columns to every row ingested by a matching entry. Use it to tag rows with context that isn't in the file itself — source system, environment, load type, etc.

```python
FileIngestionEntries.entry(
    entry_id="erp_orders",
    name="ERP Order Files",
    patterns=[r"orders_(?P<date>\d{8})\.csv"],
    dest_entity_id="bronze.orders",
    tags={"source": "erp"},
    filetype="csv",
    static_values={
        "source_system": "erp_prod",
        "load_type": "full",
        "environment": "production",
    },
)
```

The static columns are added after regex named-group columns and before `ingestion_timestamp`. Values are coerced to strings by Spark's `lit()` function.

## Signals emitted

`ParallelizingFileIngestionProcessor` emits these signals for monitoring and orchestration:

| Signal | When |
|--------|------|
| `file_ingestion.before_process` | Before batch processing starts |
| `file_ingestion.after_process` | After the batch completes |
| `file_ingestion.process_failed` | Batch processing fails |
| `file_ingestion.before_file` | Before each individual file |
| `file_ingestion.after_file` | After each file is processed |
| `file_ingestion.file_failed` | A file fails to process |
| `file_ingestion.file_moved` | A file is moved to `movepath` |
| `file_ingestion.batch_written` | A destination table group is written |

## Best practices

- **Specific patterns over broad ones** — `orders_\d{8}\.csv` is better than `.*\.csv`.
- **Use named groups** to capture useful metadata from filenames (date, region, feed type) and have them land as columns automatically.
- **Use a `filetype` named group** when ingesting non-CSV files, since the entry-level `filetype` parameter is not currently read by the processor.
- **Use `static_values`** for context that isn't in the filename or file content — source system, environment, ETL run ID.
- **Cast types explicitly** with a `transform` function — schema inference is always disabled; every column arrives as a string.
- **Test patterns locally** with `re.match(pattern, filename)` before deploying.
