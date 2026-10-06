# spark-kindling-ext-databricks-autoloader

Databricks Auto Loader (`cloudFiles`) discovery extension for Kindling file
ingestion. GitHub issue #228.

Importing this package registers an `AutoLoaderFileIngestionRunner` that
`ParallelizingFileIngestionProcessor` (`packages/kindling/file_ingestion.py`)
calls into for any `FileIngestionEntry` registered with `discovery="autoloader"`.
Entries left at the default `discovery="batch"` are unaffected and never touch
this package.

## Installation

This extension is not on PyPI. Install its wheel from a
[GitHub Release](https://github.com/sep/spark-kindling-framework/releases): the
extension keeps its own version, so take the `spark_kindling_ext_databricks_autoloader-*.whl` asset
attached to the release that matches your Kindling version.

```bash
pip install "https://github.com/sep/spark-kindling-framework/releases/download/v0.14.0/spark_kindling_ext_databricks_autoloader-0.1.0-py3-none-any.whl"
```

In a uv project, `kindling env add spark-kindling-ext-databricks-autoloader` resolves it from
the release and pins that wheel URL for you.

## Usage

```python
from kindling.file_ingestion import FileIngestionEntries
import kindling_ext_databricks_autoloader  # noqa: F401  -- registers the runner

FileIngestionEntries.entry(
    entry_id="orders",
    name="Orders feed",
    patterns=[r"(?P<filetype>csv)_orders_(?P<region>\w+)\.csv"],
    source_glob="*_orders_*.csv",
    dest_entity_id="orders_{region}",
    tags={},
    discovery="autoloader",
)
```

- `source_glob` scopes the entry's own `cloudFiles` stream (`pathGlobFilter`)
  so it only discovers files meant for it, even when other entries watch the
  same landing path -- one stream per entry, not one shared stream per path.
- `patterns[0]` keeps its existing batch-path meaning: matched per file
  inside `foreachBatch` for named-group extraction, `dest_entity_id`
  resolution, and `filetype` fallback -- regex and glob are different
  languages and are not interchangeable here.
- Checkpoint and schema locations are derived from `entry_id` under
  `kindling.storage.checkpoint_root` (`.../file_ingestion/<entry_id>/checkpoint`
  and `.../schema`) -- the same checkpoint root Delta streaming pipes already
  read (`packages/kindling/pipe_streaming.py`). No separate config key.
- Each `process_path(path)` call runs every opted-in entry's stream with
  `Trigger.AvailableNow` and blocks until it drains, matching the existing
  batch-path's synchronous run-now-drain-stop contract.
- On a non-Databricks Spark runtime, `cloudFiles` is not a registered data
  source -- Spark itself raises a clear error at stream start. If this
  package is not installed/imported at all, `ParallelizingFileIngestionProcessor`
  raises its own clear `RuntimeError` instead of a DI stack trace, and only
  when a `discovery="autoloader"` entry is actually encountered -- batch-only
  pipelines are never affected.
