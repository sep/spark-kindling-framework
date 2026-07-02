# Configuration Reference

This page enumerates the configuration keys Kindling reads and what they do.

Kindling has three main configuration surfaces:

- **Bootstrap config**: a flat `dict` passed to `kindling.initialize(...)` / `initialize_framework(...)` or injected as `config:<key>=<value>` arguments in system test jobs.
- **YAML config**: layered YAML files (see `docs/platform_workspace_config.md`) loaded into `kindling.*` keys.
- **Entity tag overrides**: per-entity tag overrides provided via the YAML `entity_tags` map, merged when entities are retrieved from `DataEntityRegistry`.

Kindling also supports SparkConf injection via `spark.kindling.*` keys; Kindling reads these and merges them into bootstrap/YAML config at startup.

## Bootstrap Config Keys

These are read from the bootstrap config dict and/or job parameters passed to the Kindling bootstrap script.

- `app_name`: Data app name to auto-run after framework initialization.
- `artifacts_storage_path`: Artifacts root used to download config and wheels (enables hot reload and lake package loading).
- `platform`: Platform selector (`fabric`, `synapse`, `databricks`, `standalone`); used to choose the platform module.
- `platform_environment`: Alias for `platform` used by platform runner internals.
- `environment`: Environment name used for config layering (for example `development`, `prod`).
- `workspace_id`: Workspace identifier used for workspace-specific config selection.
- `use_lake_packages`: If true, load Kindling and extensions from artifacts storage (instead of local environment). Defaults to `false` when `platform` is `standalone`, `true` otherwise.
- `load_workspace_packages`: If true, load workspace packages (notebooks) after platform init.
  (`load_local_packages` still accepted as a deprecated alias.)
- `temp_path`: Temporary file path root used during wheel/extension install; `kindling.temp_path` is the YAML equivalent.
- `config_files`: Explicit list of local YAML config file paths (or a single path string) to load in addition to any files downloaded from artifacts storage. Useful for standalone/local runs where no artifacts storage is configured. Files are loaded after downloaded files so they take precedence.
- `install_bootstrap_dependencies`: Boolean; controls whether `install_bootstrap_dependencies()` runs to install `kindling.required_packages` and `kindling.extensions` at startup. Defaults to `true` for cloud platforms and `false` for `standalone`.
- `allow_standalone_fallback`: Boolean; when `true`, platform detection falls back to `standalone` if no cloud platform (Databricks, Fabric, Synapse) is detected. Default `false`.
- `job_name`: Optional job name (mostly used by system test harness).
- `spark_app_name`: Optional Spark application name.

Job-deployment (system-test / deployment API) keys:

- `entry_point`: Python entry point filename for the deployed app (default depends on platform deployer).
- `parameters`: Parameter list/values passed to the platform runner (platform-specific).
- `libraries`: Additional libraries for the job (platform-specific).
- `additional_files`: Extra `.py` files packaged into the job (Synapse uses this for `py_files`).
- `command_line_arguments`: Fabric Spark job definition argument string.
- `additional_lakehouse_ids`: Fabric multi-lakehouse attachment list (when needed).
- `retry_policy`: Fabric run retry policy.
- `lakehouse_id`: Fabric Lakehouse artifact id (required for Fabric job deployment).
- `environment_id`: Fabric environment artifact id (optional).
- `spark_pool_name`: Synapse Spark pool name (required for Synapse job deployment).
- `executors`: Synapse executor count (dynamic allocation bounds and job sizing).
- `executor_cores`, `executor_memory`, `driver_cores`, `driver_memory`: Resource sizing hints used by platform deployers.

## Kindling YAML Keys (`kindling.*`)

### Core

- `kindling.platform.name`: Platform name used by platform config files.
- `kindling.platform.environment`: Environment label (used by platform code and diagnostics).
- `kindling.apps.directory`: Subdirectory under artifacts used to discover data apps (default `data-apps`).
- `kindling.temp_path`: Temp path root (preferred over `temp_path` when using YAML).

### Bootstrap Behavior

- `kindling.bootstrap.load_lake`: Same intent as bootstrap `use_lake_packages`.
- `kindling.bootstrap.load_workspace_packages`: Same intent as bootstrap `load_workspace_packages`.
- `kindling.bootstrap.ignored_folders`: Folder names ignored when loading workspace packages/notebooks.
- `kindling.required_packages`: List of PyPI packages to `pip install` at startup.
- `kindling.extensions`: List of Kindling extension wheels to load from artifacts storage.

Legacy/compat keys (still accepted by config translation, logged as deprecated):

- `kindling.bootstrap.load_local` / `kindling.BOOTSTRAP.load_local` (use
  `kindling.bootstrap.load_workspace_packages`)
- `kindling.BOOTSTRAP.load_lake`
- `kindling.REQUIRED_PACKAGES`
- `kindling.EXTENSIONS` or `kindling.extensions`
- `kindling.IGNORED_FOLDERS`

### Delta

- `kindling.delta.access_mode`: Default Delta access mode.
  - `catalog`: Use Spark table names (`saveAsTable`, `spark.read.table`, metastore/catalog lookup). This supports Unity Catalog and workspace-local Hive metastore databases.
  - `storage`: Use direct Delta storage paths (`DeltaTable.forPath`, `spark.read.format("delta").load(...)`). This is the safest default for Databricks workspaces without Unity Catalog.
- `kindling.delta.tablerefmode`: Legacy compatibility key. Prefer `kindling.delta.access_mode`.
- `kindling.features.delta.auto_clustering`: Static feature flag override to allow `cluster_columns: auto` (Databricks only; Kindling also computes a default under `kindling.runtime.features.delta.auto_clustering` during startup).

### Runtime Feature Flags

These are primarily runtime-discovered keys under `kindling.runtime.features.*`. Static overrides may be supplied under `kindling.features.*` where appropriate.

- `kindling.runtime.features.databricks.runtime_version`: Best-effort Databricks runtime version string (for example `15.4.x-scala2.12`).
- `kindling.runtime.features.databricks.runtime_major`: Parsed Databricks runtime major version when available.
- `kindling.runtime.features.databricks.runtime_minor`: Parsed Databricks runtime minor version when available.
- `kindling.runtime.features.databricks.uc_enabled`: Best-effort detection that Databricks Unity Catalog is active.
- `kindling.runtime.features.databricks.volumes_enabled`: Best-effort detection that Databricks `/Volumes/...` paths are available.
- `kindling.runtime.features.databricks.any_file_required_for_bootstrap`: Indicates Databricks bootstrap staging will need URI/DBFS fallback rather than governed volume staging.
- `kindling.runtime.features.databricks.name_mode_catalog_qualified`: Indicates Databricks name-based defaults should assume catalog-qualified names.
- `kindling.runtime.features.delta.cluster_by`: Best-effort detection that the runtime parser supports `ALTER TABLE ... CLUSTER BY`.
- `kindling.runtime.features.delta.auto_clustering`: Best-effort detection that `CLUSTER BY AUTO` is supported.

### Storage (Delta Table Naming/Paths)

These keys are used by the config-driven `EntityNameMapper`/`EntityPathLocator` defaults.

- `kindling.storage.table_catalog`: Default catalog for `catalog` access mode tables (when the engine supports catalogs). Leave unset for Hive metastore-only Databricks workspaces.
- `kindling.storage.table_schema`: Default schema/database for `catalog` access mode tables.
- `kindling.storage.table_schema_location`: Optional schema/database LOCATION for engines that require it for managed table creation (notably Synapse). When set, Kindling may `CREATE SCHEMA IF NOT EXISTS ... LOCATION ...` as a best-effort convenience before name-based table writes.
- `kindling.storage.table_name_prefix`: Optional prefix added to the generated leaf table name.
- `kindling.storage.table_root`: Default path root for `storage` access mode entities (default `Tables`).
- `kindling.storage.checkpoint_root`: Default checkpoint root used by system test apps (common default `Files/checkpoints`).
- `kindling.databricks.volume_staging_root`: Optional Databricks-specific governed staging root for bootstrap wheel/config temp files. When omitted, Databricks bootstrap will try to derive a volume-backed staging root from `kindling.storage.checkpoint_root` or `kindling.storage.table_root` before falling back to DBFS.

### Databricks Without Unity Catalog

Azure Government Databricks regions should be treated as non-UC environments unless the target workspace proves otherwise. The recommended baseline is direct cloud storage:

```yaml
kindling:
  features:
    databricks:
      uc_enabled: false
  delta:
    access_mode: storage
  storage:
    table_root: abfss://artifacts@<account>.dfs.core.usgovcloudapi.net/kindling/tables
    checkpoint_root: abfss://artifacts@<account>.dfs.core.usgovcloudapi.net/kindling/checkpoints
```

If the workspace has a usable Hive metastore/database and you want table-name semantics, use `catalog` mode without a catalog:

```yaml
kindling:
  features:
    databricks:
      uc_enabled: false
  delta:
    access_mode: catalog
  storage:
    table_schema: kindling
    table_schema_location: abfss://artifacts@<account>.dfs.core.usgovcloudapi.net/kindling/hive/kindling
    table_root: abfss://artifacts@<account>.dfs.core.usgovcloudapi.net/kindling/tables
    checkpoint_root: abfss://artifacts@<account>.dfs.core.usgovcloudapi.net/kindling/checkpoints
```

DBFS mounts can still be used for legacy workspaces, but prefer direct `abfss://...` paths for new Gov deployments because DBFS root and mounts are deprecated by Databricks.

Backward-compatible fallbacks that are still read if the generic keys are not set:

- `kindling.databricks.catalog`, `kindling.databricks.schema`, `kindling.databricks.table_root`
- `kindling.synapse.schema`, `kindling.synapse.table_root`
- `kindling.fabric.catalog`, `kindling.fabric.schema`, `kindling.fabric.table_root`

### Telemetry

YAML keys:

- `kindling.telemetry.logging.level`: Logging level (INFO/WARN/DEBUG).
- `kindling.telemetry.logging.print`: If true, print logs to stdout.
- `kindling.telemetry.tracing.print`: If true, print trace spans to stdout.

Flat keys (supported for bootstrap/back-compat):

- `log_level`: Logging level.
- `print_logging`: Print logs to stdout.
- `print_trace`: Print traces to stdout.

Legacy/compat keys (translated into flat keys):

- `kindling.TELEMETRY.logging.level`
- `kindling.TELEMETRY.logging.print`
- `kindling.TELEMETRY.tracing.print`

### Secrets

- `kindling.secrets.key_vault_url`: Azure Key Vault URL used by Fabric/Synapse secret resolution.
- `kindling.secrets.linked_service`: Fabric/Synapse linked service name (alternative to `key_vault_url`).
- `kindling.secrets.secret_scope`: Databricks secret scope name used with `dbutils.secrets`.

### Standalone Platform

These keys are only meaningful when `platform` is `standalone` (local/OSS Spark deployments).

- `kindling.standalone.abfss_az_cli_auth`: Boolean (default `true`). When `true` and `az` is present on PATH, configures the Spark session to acquire Azure Storage tokens via `az account get-access-token` so that `abfss://` paths work without a service principal or storage key. Set to `false` to opt out of this injection (for example when using a different credential mechanism or when `az` is on PATH but you do not want automatic auth configuration).

## Entity Tag Overrides (`entity_tags`)

YAML can specify a top-level `entity_tags` mapping where the key is an `entityid` and the value is a dict of tags to merge over the entity’s base tags at lookup time.

Example:

```yaml
entity_tags:
  stream.my_entity:
    provider.access_mode: catalog
    provider.table_name: main.analytics.my_entity
```

## Provider Configuration via Entity Tags (`EntityMetadata.tags`)

Provider configuration is driven by entity tags. Tags with the `provider.` prefix are treated as provider config.

Common tags:

- `provider_type`: Provider selector (for example `delta`, `csv`, `eventhub`, `memory`).

### Delta Provider (`provider_type: delta`)

- `provider.access_mode`: `catalog` | `storage` (per-entity override).
- `provider.table_name`: Fully qualified table name override.
- `provider.path`: Storage path override (`storage` access mode).

### CSV Provider (`provider_type: csv`)

- `provider.path`: CSV file path.
- `provider.header`: boolean (default true).
- `provider.inferSchema`: boolean (default true).
- `provider.delimiter`: string (default `,`).
- `provider.encoding`: string (default `UTF-8`).
- `provider.quote`: string (default `"`).
- `provider.escape`: string (default `\\`).
- `provider.multiLine`: boolean (default false).
- `provider.compression`: write codec — one of `none`, `gzip`, `bzip2`, `lz4`, `snappy`,
  `deflate` (default `none`, write only).

CSV supports both `write_to_entity` (overwrite) and `append_to_entity` (append) via Spark's
CSV writer. Like any Spark file write, output is a directory of part-files at `provider.path`,
not a single `.csv` file — merge them downstream if a single file is required.

### EventHub Provider (`provider_type: eventhub`)

- `provider.eventhub.connectionString`: Event Hubs connection string.
- `provider.eventhub.name`: Event Hub name.
- `provider.eventhub.consumerGroup`: Consumer group (default `$Default`).
- `provider.startingPosition`: `earliest` | `latest` or JSON offset specification (default `latest`).
- `provider.maxEventsPerTrigger`: Max events per micro-batch (streaming only).
- `provider.receiverTimeout`: Receiver timeout (ms).
- `provider.operationTimeout`: Operation timeout (ms).

### Memory Provider (`provider_type: memory`)

Batch:

- `provider.table_name`: In-memory table name.
- `provider.seed.rows`: Optional list of inline row mappings used to seed a memory entity on first read. Requires a schema on the entity. Existing table/store data takes precedence; seed rows are not written if data already exists. Use for small starter data in local standalone runs; prefer `tests/entities/<entity>.csv` for larger samples and test fixtures.

Streaming reads:

- `provider.stream_type`: `rate` | `memory` (default `rate`).
- `provider.rowsPerSecond`: Rate source rows per second (default 10).
- `provider.numPartitions`: Rate source partitions (default 1).

Streaming writes:

- `provider.output_mode`: `append` | `complete` | `update` (default `append`).
- `provider.query_name`: Streaming query name (default entity name).

## Testing-Only Settings

These are not framework defaults; they are typically injected by system tests or test apps.

- `test_id`: Unique suffix used by system tests to correlate logs and avoid collisions.
- `kindling.eventhub_test.connection_string`: System test EventHub connection string.
- `kindling.eventhub_test.eventhub_name`: System test EventHub name.
- `kindling.eventhub_test.marker`: Marker string the test searches for in events.
