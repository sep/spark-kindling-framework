# kindling_ext_databricks

Databricks Lakeflow adapter for Kindling's SDP declaration engine.

Lakeflow "extends and is interoperable with" the OSS `pyspark.pipelines`
API, so this adapter **augments** the `kindling_ext_sdp` core rather than
translating it: the OSS emission (materialized views with comments, table
properties, partitioning, clustering, schema) runs unchanged, and
Databricks-only capabilities are layered on top.

Selected with:

```python
kindling.initialize(engine="databricks_sdp")
...
kindling.declare_pipeline()
```

— resolved through kindling core's engine-extension mapping
(`kindling_<name>.engine_extension()`); adding this engine required zero
core changes.

## Phase 3 (this package today): expectations

```yaml
datapipes:
  silver.orders:
    engine:
      databricks_sdp:
        expectations:            # violations counted, rows kept (warn)
          valid_order_id: "order_id IS NOT NULL"
        expectations_drop:       # violating rows dropped
          positive_qty: "quantity > 0"
        expectations_fail:       # violation fails the update
          no_future_dates: "order_date <= current_date()"
```

Blocks map to `expect_all` / `expect_all_or_drop` / `expect_all_or_fail`
decorators. Declaring them while targeting OSS fails fast at validation
(capability gating in the core); declaring them here against a runtime
without the Lakeflow decorators fails at declaration with an actionable
error.

`refresh_policy: incremental` is accepted (adapter-tier gated) but emits
nothing yet — incremental MV refresh on Databricks (Enzyme) is engine
behavior rather than a declaration keyword; treated as a documented hint
pending verification against a live workspace.

## Phase 5: SCD declared flows → AUTO CDC

An SCD-tagged output entity (`scd.type` plus the declared-flow tags from
kindling core) is declared as the Lakeflow CDC pattern — a pipeline-scoped
source view (the pipe's DataFrame), a streaming table target, and an AUTO
CDC flow — instead of a materialized view:

| Kindling declaration | AUTO CDC |
|---|---|
| `scd.type: "2"` (the only type core registration accepts) | `stored_as_scd_type` |
| entity `merge_columns` | `keys` |
| `scd.sequence_by` | `sequence_by` (change feed) |
| `scd.source_kind: change_feed` (default) | `create_auto_cdc_flow` |
| `scd.source_kind: snapshot` / `scd.close_on_missing: true` | `create_auto_cdc_from_snapshot_flow` |
| `scd.delete_when` | `apply_as_deletes=expr(...)` |
| `scd.tracked` | `track_history_column_list` |
| (default) sequence column ≠ content | `track_history_except_column_list=[sequence_by]` |

Fail-fast mapping requirements (validated with everything else, all
errors at once): change-feed targets need `scd.sequence_by`
(`scd_sequence_by_required`); `scd.type` must be `2`, matching core registration
(`scd_type_unsupported` — bitemporal is deliberately excluded while in
Beta); keys are required (`scd_keys_required`).

Documented divergences from the runner engine (dual-engine parity
criterion): history columns are `__START_AT`/`__END_AT` (the entity's
runner-shape schema is not passed to the streaming table); snapshot
ordering is ingestion order, not `scd.sequence_by`; out-of-order
change-feed rows are reconciled into history rather than ignored under
the runner's strictly-later rule; the `scd.current_entity_id`
current-view companion stays runner-only (an ordinary declared view is
the SDP-native replacement, tracked separately). Expectations, when
configured, attach to the source view — quality is checked on the
incoming feed.

Change-feed sources are consumed as a **stream**: every input selected by
`driving_entity_ids` is read with `spark.readStream.table()` so the AUTO CDC
flow processes it incrementally — the declarative replacement for
hand-rolled foreachBatch+MERGE. Remaining inputs stay batch reads
(stream-static joins). Snapshot sources keep batch reads: the snapshot API
diffs whole snapshots per update.

## Provider-owned streaming sources

An external provider stream, initially Event Hubs over Kafka, can drive a
normal Kindling ingestion pipe when targeting `engine="databricks_sdp"`:

```python
DataEntities.entity(
    entityid="stream.telemetry",
    name="Telemetry Event Hub",
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

@DataPipes.pipe(
    pipeid="bronze.telemetry.ingest",
    input_entity_ids=["stream.telemetry", "ref.devices"],
    output_entity_id="bronze.telemetry",
    driving_entity_ids=["stream.telemetry"],
)
def ingest_telemetry(stream_telemetry, ref_devices):
    ...
```

The shared declaration planner asks the provider for a secret-safe
`StreamingSourceSpec`. Invalid source configuration is reported before
emission without printing connection strings, SAS keys, JAAS values, or
resolved `@secret` values. The Databricks adapter then emits exactly one
`dp.create_streaming_table(...)` target and one `dp.append_flow(...)`. When
Lakeflow evaluates the append-flow function, Kindling resolves the provider,
calls `read_entity_as_stream()` with the declarative-source option, passes
later inputs as static DataFrames, invokes the registered pipe transform, and
returns the streaming DataFrame to Lakeflow.

Lakeflow owns query startup, checkpoint placement, retries, update scheduling,
and target persistence for this path. Kindling does not call `writeStream`,
does not pass `checkpointLocation`, and does not start a streaming query.

## Deferred

- Multiple provider-owned streaming inputs and stream-stream joins.
- Bitemporal AUTO CDC (`stored_as_scd_type="bitemporal"`) — Beta;
  tracked, not adopted (proposal decision).
- Current-view companion as a declared view.


## Output dataset naming

Lakeflow uses the shared [SDP dataset naming configuration](../kindling_ext_sdp/README.md#output-dataset-naming):

```yaml
kindling:
  sdp:
    dataset_naming: leaf
```

With a pipeline destination of catalog `dev_silver`, schema `cwmdp`,
`silver.device_telemetry` is declared as `device_telemetry`, producing
`dev_silver.cwmdp.device_telemetry`. Set separate destinations and select
the appropriate pipes for each medallion resource. Same-leaf outputs in
one plan are rejected.

This also controls AUTO CDC targets and temporary sources, temporal
events/episodes, and their generated strata, snapshots, and internal
references. Cross-pipeline external table resolution remains independent,
including temporal reads through `EntityNameMapper` and entity
`provider.table_catalog` overrides. Omitting the setting preserves the
existing `silver_device_telemetry` convention.


When consuming these outputs, explicitly align external entity metadata with
the leaf table, for example
`provider.table_name: dev_silver.cwmdp.device_telemetry`. Otherwise a consumer
configured with catalog and schema still resolves the historical flattened
name. See [external reads and generated-name reservations](../kindling_ext_sdp/README.md#reading-leaf-named-outputs-from-elsewhere)
for the complete example and the temporal source limitation.

## Canonical configuration from a Bundle

Lakeflow uses the same Kindling configuration hierarchy as jobs and notebooks.
Publish ordinary Kindling settings files and pass them through the canonical
bootstrap key `spark.kindling.bootstrap.config_files`, or publish them through
`spark.kindling.bootstrap.artifacts_storage_path` and let bootstrap discover
base, platform, workspace, environment, and app overlays.

The selector does not parse YAML, inspect files, or validate structured
sections. It bridges Lakeflow pipeline configuration into
`kindling.initialize(..., app_name=<selected>, engine="databricks_sdp")` and
sets `declaration_only`; the shared bootstrap/Dynaconf path owns transport,
parsing, validation, precedence, and overlays. Flat bridged pipeline keys still
act as bootstrap overrides and win over settings files.

Use Bundle sync to deploy config to a stable workspace or volume path. This
matches the transport pattern in the
[DAB config promotion guide](../../../docs/guide/dab_config_promotion.md):

```yaml
# databricks.yml
bundle:
  name: telemetry-lakeflow

targets:
  dev:
    workspace:
      host: https://adb-<dev>.azuredatabricks.net
      file_path: /Workspace/Shared/kindling/dev

sync:
  include:
    - config/**
    - data-apps/**

resources:
  pipelines:
    telemetry_bronze:
      name: telemetry-bronze
      catalog: dev_bronze
      target: cwmdp
      configuration:
        "kindling.data_app": telemetry
        "kindling.lakeflow.pipes": bronze.ingest_telemetry
        "spark.kindling.bootstrap.environment": dev
        "spark.kindling.bootstrap.workspace_id": adb-dev
        "spark.kindling.bootstrap.config_files": '["/Workspace/Shared/kindling/dev/config/settings.yaml", "/Workspace/Shared/kindling/dev/config/settings.databricks.yaml", "/Workspace/Shared/kindling/dev/data-apps/telemetry/settings.yaml"]'
    telemetry_silver:
      name: telemetry-silver
      catalog: dev_silver
      target: cwmdp
      configuration:
        "kindling.data_app": telemetry
        "kindling.lakeflow.pipes": silver.build_telemetry,silver.derive_events,silver.derive_episodes
        "spark.kindling.bootstrap.environment": dev
        "spark.kindling.bootstrap.workspace_id": adb-dev
        "spark.kindling.bootstrap.config_files": '["/Workspace/Shared/kindling/dev/config/settings.yaml", "/Workspace/Shared/kindling/dev/config/settings.databricks.yaml", "/Workspace/Shared/kindling/dev/data-apps/telemetry/settings.yaml"]'
```

```yaml
# data-apps/telemetry/settings.yaml
kindling:
  sdp:
    dataset_naming: leaf

dataentities:
  bronze.device_telemetry:
    tags:
      provider.table_name: dev_bronze.cwmdp.device_telemetry
  silver.device_telemetry:
    tags:
      provider.table_name: dev_silver.cwmdp.device_telemetry
  silver.events:
    tags:
      provider.table_name: dev_silver.cwmdp.events
  silver.episodes:
    tags:
      provider.table_name: dev_silver.cwmdp.episodes
```

With `dataset_naming: leaf`, dotted logical IDs stay unchanged in the Kindling
registry while Lakeflow emits single-part output names inside each pipeline
destination:

| Logical ID | Pipeline destination | Physical table |
|---|---|---|
| `bronze.device_telemetry` | `dev_bronze.cwmdp` | `dev_bronze.cwmdp.device_telemetry` |
| `silver.device_telemetry` | `dev_silver.cwmdp` | `dev_silver.cwmdp.device_telemetry` |
| `silver.events` | `dev_silver.cwmdp` | `dev_silver.cwmdp.events` |
| `silver.episodes` | `dev_silver.cwmdp` | `dev_silver.cwmdp.episodes` |

Pipeline-local outputs get those names from `dataset_naming: leaf` plus the
pipeline catalog/schema. External reads of the same tables still use the
ordinary entity resolver, so they need the fully qualified
`provider.table_name` overrides shown in the structured YAML. A catalog/schema
override alone is not enough: it would still resolve the default flattened
table name. Temporal chain base-event sources always resolve externally, so
keep their external table overrides in the same structured YAML.

Workspace files under `/Workspace/...` are documented as driver-readable for
clusters and jobs. Readability from a Lakeflow serverless pipeline has not yet
been confirmed; if that is required, deploy the same YAML to a Unity Catalog
volume path and reference that path in
`spark.kindling.bootstrap.config_files`.

`kindling.lakeflow.config_files` remains as a deprecated comma-separated alias
for one release cycle. It logs a warning, appends its paths to bootstrap
`config_files`, and is eligible for removal at 0.13.0. New Bundle
configuration should use `spark.kindling.bootstrap.config_files`.
