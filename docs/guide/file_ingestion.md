# File Ingestion

The File Ingestion module provides a declarative way to map file patterns to destination entities. When a file matches an entry's pattern, the processor reads it, enriches it with metadata columns, and appends it to the target entity table.

Each entry discovers files one of two ways: a one-shot directory listing on every `process_path()` call (`discovery="batch"`, the default), or a per-entry Databricks Auto Loader stream (`discovery="autoloader"`) — see [Auto Loader discovery](#auto-loader-discovery-databricks).

## Registering an ingestion entry

Use `FileIngestionEntries.entry()` to declare a mapping. All parameters must be provided except `infer_schema` (defaults to `True`), `static_values` (defaults to `None`), `discovery` (defaults to `"batch"`), and `source_glob` (defaults to `None`; required when `discovery="autoloader"`).

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
| `filetype` | `str` | Yes | Ignored for `discovery="batch"` — see note below. For `discovery="autoloader"`, passed through as `cloudFiles.format` |
| `infer_schema` | `bool` | No | Accepted by the API but **not currently effective** — see note below |
| `static_values` | `Dict[str, Any]` | No | Literal column values added to every matched row |
| `discovery` | `str` | No | `"batch"` (default) or `"autoloader"` — see [Auto Loader discovery](#auto-loader-discovery-databricks) |
| `source_glob` | `str` | Only for `discovery="autoloader"` | Glob passed to Auto Loader's `pathGlobFilter`; scopes that entry's own stream |

> **For `discovery="batch"` entries, `filetype` is not read by the processor.** The Spark format is driven entirely by the `filetype` named regex group in the matched pattern (e.g. `(?P<filetype>csv)`), falling back to `"csv"` when that group is absent. The `filetype` parameter stored on the entry is never consulted. **`discovery="autoloader"` entries are the exception** — see [Auto Loader discovery](#auto-loader-discovery-databricks).

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

This section describes `discovery="batch"` entries (the default). For `discovery="autoloader"` entries, set `filetype` directly instead — see [Auto Loader discovery](#auto-loader-discovery-databricks).

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

This same `process_path()` call also drives any `discovery="autoloader"` entries registered for the same path — each runs its own Auto Loader stream to completion before `process_path()` returns. See [Auto Loader discovery](#auto-loader-discovery-databricks).

## Auto Loader discovery (Databricks)

Set `discovery="autoloader"` on an entry to give it its own Databricks Auto Loader (`cloudFiles`) stream instead of the default directory listing. Auto Loader tracks which files it has already seen via a checkpoint, so repeated `process_path()` calls discover only new files instead of re-listing the whole path.

```python
import kindling_ext_databricks_autoloader  # noqa: F401 -- registers the Auto Loader runner

FileIngestionEntries.entry(
    entry_id="orders",
    name="Orders feed",
    patterns=[r"(?P<filetype>csv)_orders_(?P<region>\w+)\.csv"],
    dest_entity_id="orders_{region}",
    tags={},
    discovery="autoloader",
    source_glob="*_orders_*.csv",
)
```

### `batch` vs. `autoloader`

| | `discovery="batch"` (default) | `discovery="autoloader"` |
|---|---|---|
| File discovery | Lists the whole path on every `process_path()` call | Databricks Auto Loader (`cloudFiles`); checkpointed, incremental |
| Prevents reprocessing via | `movepath` only — there is no checkpoint, so without `movepath` the same files are re-ingested on every call | The Auto Loader checkpoint, regardless of `movepath`. If `movepath` is also set, it still copies-then-deletes each source file exactly as it does on the batch path — that's just landing-zone hygiene here, not what keeps files from being reprocessed |
| Requires | Nothing extra | `kindling_ext_databricks_autoloader` installed and imported; a Databricks runtime (`cloudFiles` is Databricks-only) |
| `source_glob` | Not used | Required |
| `filetype` | Ignored (see [Controlling the read format](#controlling-the-read-format)) | Read — passed as `cloudFiles.format` |

Prefer `autoloader` on Databricks once a landing path accumulates enough files that listing it on every run gets expensive, or when you want checkpointed discovery instead of relying on `movepath` to avoid duplicate rows. Otherwise, `batch` (the default) needs no extra dependency and works on any engine.

### Config surface

- `source_glob` (required) scopes the entry's own `cloudFiles` stream via `pathGlobFilter`, so multiple entries can watch the same landing path without each one discovering files meant for another entry. `patterns[0]` keeps its normal job on top of that — it's still matched per file for named-group extraction, `dest_entity_id` templating, and (for `discovery="batch"` entries) `filetype` fallback. Glob and regex are different languages: a file can pass an entry's `source_glob` and still miss its own `patterns[0]`, in which case it's skipped like any other non-matching file.
- `filetype` is passed straight through as `cloudFiles.format` for `autoloader` entries — unlike `discovery="batch"`, where it's ignored in favor of a `filetype` named regex group (see [Controlling the read format](#controlling-the-read-format)).

### Checkpoint and schema locations

Auto Loader needs a `checkpointLocation` and a `cloudFiles.schemaLocation` per entry. Kindling derives both from the `kindling.storage.checkpoint_root` config key — the same root Delta streaming pipes already use (`packages/kindling/pipe_streaming.py`) — namespaced under `file_ingestion/`:

```text
{checkpoint_root}/file_ingestion/{entry_id}/checkpoint
{checkpoint_root}/file_ingestion/{entry_id}/schema
```

There is no separate config key for file ingestion — set `kindling.storage.checkpoint_root` once and every `autoloader` entry gets its own subpath, keyed by `entry_id`. If `kindling.storage.checkpoint_root` is unset, `process_path()` raises before starting any Auto Loader stream.

### Requirements and failure behavior

- `process_path()` only touches Auto Loader at all if at least one registered entry has `discovery="autoloader"`; batch-only registries never resolve or require the extension.
- Import `kindling_ext_databricks_autoloader` (package `spark-kindling-ext-databricks-autoloader`) so it can bind its runner. This is resolved lazily, only once a `discovery="autoloader"` entry is actually encountered.
- If such an entry exists but the extension was never imported, `process_path()` raises a `RuntimeError` naming the missing extension, instead of silently falling back to batch or failing with a raw DI stack trace.
- On a non-Databricks Spark runtime, `cloudFiles` isn't a registered source; Spark itself raises at stream start.

> **Signal timing shifts for `autoloader` entries.** `file_ingestion.before_file`/`after_file` still fire once per file, but by the time either fires, Auto Loader has already read that file into the microbatch — the two signals fire back-to-back around enrichment rather than bracketing the physical read the way they do on the batch path. `before_process`/`after_process` are unaffected: one pair still wraps each `process_path()` call end-to-end, whatever mix of batch listing and Auto Loader microbatches it triggers.

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

For `discovery="autoloader"` entries, `before_file`/`after_file` timing shifts slightly relative to the physical file read — see [Auto Loader discovery](#auto-loader-discovery-databricks).

## Best practices

- **Specific patterns over broad ones** — `orders_\d{8}\.csv` is better than `.*\.csv`.
- **Use named groups** to capture useful metadata from filenames (date, region, feed type) and have them land as columns automatically.
- **Use a `filetype` named group** when ingesting non-CSV files, since the entry-level `filetype` parameter is not currently read by the processor.
- **Use `static_values`** for context that isn't in the filename or file content — source system, environment, ETL run ID.
- **Cast types explicitly** with a `transform` function — schema inference is always disabled; every column arrives as a string.
- **Test patterns locally** with `re.match(pattern, filename)` before deploying.
- **Use `discovery="autoloader"`** (Databricks only) for landing paths where a full directory listing on every run is expensive, or where you want checkpointed discovery instead of relying on `movepath` to avoid reprocessing. Keep `discovery="batch"` (the default) everywhere else — it needs no extra dependency and works on any engine.
