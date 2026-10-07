# File Ingestion

The File Ingestion module provides a declarative way to map file patterns to destination entities. When a file matches an entry's pattern, the processor reads it, enriches it with metadata columns, and appends it to the target entity through that entity's own provider.

Each entry discovers files one of two ways: a one-shot directory listing on every `process_path()` call (`discovery="batch"`, the default), or a per-entry Databricks Auto Loader stream (`discovery="autoloader"`) — see [Auto Loader discovery](#auto-loader-discovery-databricks).

## Registering an ingestion entry

Use `FileIngestionEntries.entry()` to declare a mapping. All parameters must be provided except `infer_schema` (defaults to `False`), `static_values` (defaults to `None`), `discovery` (defaults to `"batch"`), `source_glob` (defaults to `None`; required when `discovery="autoloader"`), and `schema_evolution_mode` (defaults to `None`).

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
| `patterns` | `List[str]` | Yes | Non-empty list of regexes matched against the file name; tried in order, first match wins |
| `dest_entity_id` | `str` | Yes | Entity ID to write matched files into |
| `tags` | `Dict[str, str]` | Yes | Metadata tags (may be empty) |
| `filetype` | `str` | Yes | Spark read format (`"csv"`, `"parquet"`, `"json"`, ...). For `discovery="batch"`, a `filetype` named group in the matching pattern overrides it per file — see [Controlling the read format](#controlling-the-read-format). For `discovery="autoloader"`, passed through as `cloudFiles.format` |
| `infer_schema` | `bool` | No | `discovery="batch"` only: passed to the reader as `inferSchema`. Default `False` — every column arrives as a string |
| `static_values` | `Dict[str, Any]` | No | Literal column values added to every matched row |
| `discovery` | `str` | No | `"batch"` (default) or `"autoloader"` — see [Auto Loader discovery](#auto-loader-discovery-databricks) |
| `source_glob` | `str` | Only for `discovery="autoloader"` | Glob passed to Auto Loader's `pathGlobFilter`; scopes that entry's own stream |
| `schema_evolution_mode` | `str` | No | Only applied for `discovery="autoloader"` entries — passed through as `cloudFiles.schemaEvolutionMode`. Unset (`None`) leaves Auto Loader's own default in effect |

Each file is matched with `re.match` (anchored at the start) against its **file name**, not the full path. Every pattern in `patterns` is tried in order and the first one that matches decides the named groups and the destination. `FileIngestionEntries.entry()` rejects an empty list, a bare string, or a pattern that does not compile.

> **Schema inference is off by default.** With `infer_schema=False` (the default) the reader runs with `inferSchema=false`, so CSV columns arrive as strings; cast them in a `transform`. Pass `infer_schema=True` to let Spark infer column types instead. Formats that carry their own schema (Parquet, Delta) are unaffected. Auto Loader entries do not read `infer_schema`.

## Controlling the read format

For `discovery="batch"` entries (the default), the Spark format for each matched file is chosen in this order:

1. A non-empty `filetype` named group in the pattern that matched the file, e.g. `(?P<filetype>json|csv)`. This lets one entry ingest several formats.
2. The entry's `filetype` argument.
3. `"csv"`.

```python
FileIngestionEntries.entry(
    entry_id="sales_daily",
    name="Daily Sales Files",
    patterns=[r"sales_(?P<region>\w+)_(?P<date>\d{8})\.parquet"],
    dest_entity_id="bronze.sales",
    tags={"domain": "sales", "layer": "bronze"},
    filetype="parquet",
)

FileIngestionEntries.entry(
    entry_id="events_mixed",
    name="Event files in JSON or CSV",
    patterns=[r"events_(?P<date>\d{8})\.(?P<filetype>json|csv)"],
    dest_entity_id="bronze.events",
    tags={},
    filetype="csv",  # used only when the pattern has no filetype group
)
```

A `filetype` group also becomes a column like any other named group. Every file is read with `header=true`, which non-CSV formats ignore.

For `discovery="autoloader"` entries, `filetype` is passed to `cloudFiles.format` as is — see [Auto Loader discovery](#auto-loader-discovery-databricks).

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

### Where the data lands

Each group is appended through the destination entity's own provider, resolved from its `provider_type` tag the same way pipes resolve their output provider. No `provider_type` tag means the default Delta provider. The write is always an append: ingestion never merges, even when the entity declares `merge_columns`. Declare the destination entity before running ingestion. A provider that cannot append (for example `sql`) raises a `ValueError` naming the entity and provider.

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
| `filetype` | Read format, unless the matching pattern has a `filetype` group (see [Controlling the read format](#controlling-the-read-format)) | Passed as `cloudFiles.format` |
| `infer_schema` | Passed as the reader's `inferSchema` | Not used |
| `schema_evolution_mode` | Not used | Optional — passed as `cloudFiles.schemaEvolutionMode` when set |

Prefer `autoloader` on Databricks once a landing path accumulates enough files that listing it on every run gets expensive, or when you want checkpointed discovery instead of relying on `movepath` to avoid duplicate rows. Otherwise, `batch` (the default) needs no extra dependency and works on any engine.

### Config surface

- `source_glob` (required) scopes the entry's own `cloudFiles` stream via `pathGlobFilter`, so multiple entries can watch the same landing path without each one discovering files meant for another entry. `patterns` keeps its normal job on top of that: each delivered file is still matched against the patterns in order for named-group extraction and `dest_entity_id` templating. Glob and regex are different languages: a file can pass an entry's `source_glob` and still miss every one of its `patterns`, in which case it's skipped like any other non-matching file.
- `filetype` is passed straight through as `cloudFiles.format` for `autoloader` entries. A `filetype` named group in the pattern does not change the format here; the microbatch has already been read.
- `schema_evolution_mode` is optional and only consulted for `autoloader` entries. When set, it's passed straight through as `cloudFiles.schemaEvolutionMode` — Databricks' own values (`"addNewColumns"`, `"rescue"`, `"failOnNewColumns"`, `"none"`) are accepted verbatim rather than remapped to a kindling-specific vocabulary. Left unset (the default), `cloudFiles` applies its own default evolution behavior. Not read at all for `discovery="batch"` entries.

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
- **Set `filetype`** for non-CSV files. Use a `filetype` named group only when one entry must read several formats.
- **Use `static_values`** for context that isn't in the filename or file content — source system, environment, ETL run ID.
- **Cast types explicitly** with a `transform` function, or pass `infer_schema=True`. By default every CSV column arrives as a string.
- **Test patterns locally** with `re.match(pattern, filename)` before deploying.
- **Use `discovery="autoloader"`** (Databricks only) for landing paths where a full directory listing on every run is expensive, or where you want checkpointed discovery instead of relying on `movepath` to avoid reprocessing. Keep `discovery="batch"` (the default) everywhere else — it needs no extra dependency and works on any engine.
