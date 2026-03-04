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
- `platform`: Platform selector (`fabric`, `synapse`, `databricks`); used to choose the platform module.
- `platform_environment`: Alias for `platform` used by some job runners.
- `environment`: Environment name used for config layering (for example `development`, `prod`).
- `workspace_id`: Workspace identifier used for workspace-specific config selection.
- `use_lake_packages`: If true, load Kindling and extensions from artifacts storage (instead of local environment).
- `load_local_packages`: If true, load local workspace packages (notebooks) after platform init.
- `temp_path`: Temporary file path root used during wheel/extension install; `kindling.temp_path` is the YAML equivalent.
- `job_name`: Optional job name (mostly used by system test harness).
- `spark_app_name`: Optional Spark application name.

Job-deployment (system-test / deployment API) keys:

- `entry_point`: Python entry point filename for the deployed app (default depends on platform deployer).
- `parameters`: Parameter list/values passed to the job runner (platform-specific).
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
- `kindling.bootstrap.load_local`: Same intent as bootstrap `load_local_packages`.
- `kindling.bootstrap.ignored_folders`: Folder names ignored when loading workspace packages/notebooks.
- `kindling.required_packages`: List of PyPI packages to `pip install` at startup.
- `kindling.extensions`: List of Kindling extension wheels to load from artifacts storage.

Legacy/compat keys (still accepted by config translation):

- `kindling.BOOTSTRAP.load_lake`, `kindling.BOOTSTRAP.load_local`
- `kindling.REQUIRED_PACKAGES`
- `kindling.EXTENSIONS` or `kindling.extensions`
- `kindling.IGNORED_FOLDERS`

### Delta

- `kindling.delta.tablerefmode`: Default Delta access mode (`forName`, `forPath`, `auto`).

### Storage (Delta Table Naming/Paths)

These keys are used by the config-driven `EntityNameMapper`/`EntityPathLocator` defaults.

- `kindling.storage.table_catalog`: Default catalog for `forName` tables (when the engine supports catalogs).
- `kindling.storage.table_schema`: Default schema/database for `forName` tables.
- `kindling.storage.table_schema_location`: Optional schema/database LOCATION for engines that require it for managed table creation (notably Synapse). When set, Kindling may `CREATE SCHEMA IF NOT EXISTS ... LOCATION ...` as a best-effort convenience before name-based table writes.
- `kindling.storage.table_name_prefix`: Optional prefix added to the generated leaf table name.
- `kindling.storage.table_root`: Default path root for `forPath` entities (default `Tables`).
- `kindling.storage.checkpoint_root`: Default checkpoint root used by system test apps (common default `Files/checkpoints`).

Backward-compatible fallbacks that are still read if the generic keys are not set:

- `kindling.databricks.catalog`, `kindling.databricks.schema`, `kindling.databricks.table_root`
- `kindling.synapse.schema`, `kindling.synapse.table_root`
- `kindling.fabric.schema`, `kindling.fabric.table_root`

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

## Entity Tag Overrides (`entity_tags`)

YAML can specify a top-level `entity_tags` mapping where the key is an `entityid` and the value is a dict of tags to merge over the entity’s base tags at lookup time.

Example:

```yaml
entity_tags:
  stream.my_entity:
    provider.access_mode: forName
    provider.table_name: main.analytics.my_entity
```

## Provider Configuration via Entity Tags (`EntityMetadata.tags`)

Provider configuration is driven by entity tags. Tags with the `provider.` prefix are treated as provider config.

Common tags:

- `provider_type`: Provider selector (for example `delta`, `csv`, `eventhub`, `memory`).

### Delta Provider (`provider_type: delta`)

- `provider.access_mode`: `forName` | `forPath` | `auto` (per-entity override).
- `provider.table_name`: Fully qualified table name override.
- `provider.path`: Storage path override (forPath).

### CSV Provider (`provider_type: csv`)

- `provider.path`: CSV file path.
- `provider.header`: boolean (default true).
- `provider.inferSchema`: boolean (default true).
- `provider.delimiter`: string (default `,`).
- `provider.encoding`: string (default `UTF-8`).
- `provider.quote`: string (default `"`).
- `provider.escape`: string (default `\\`).
- `provider.multiLine`: boolean (default false).

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
