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

The output entity's declared `schema` is not forwarded to
`create_streaming_table` pending Lakeflow platform evidence; the target schema
is inferred from the append-flow DataFrame on this path.

Lakeflow owns query startup, checkpoint placement, retries, update scheduling,
and target persistence for this path. Kindling does not call `writeStream`,
does not pass `checkpointLocation`, and does not start a streaming query.

## Temporal chain execution mode

A temporal chain lowers its event strata `<events>__g0..gK` as streaming
tables fed by append flows, so each base-event transform runs over a
Structured Streaming DataFrame. That rejects ordered analytic windows —
`row_number`, `lag`, unbounded forward fill — even though the same transform
runs correctly under the runner engine. Set the mode to `batch` to lower
those strata as materialized views with batch reads instead:

```yaml
kindling:
  lakeflow:
    temporal_mode: batch
```

From a Databricks Asset Bundle, use the canonical SparkConf spelling in the
pipeline's `configuration:` mapping — it is point-looked-up by the selector,
so it also works on serverless and shared-access runtimes that cannot
enumerate Spark configuration:

```yaml
configuration:
  spark.kindling.lakeflow.temporal_mode: "batch"
```

| Component | `streaming` (default, omitted) | `batch` |
| --- | --- | --- |
| `<events>__g0` | Streaming table, one append flow per base declaration | One materialized view, union of the transformed base inputs |
| Base reads | `spark.readStream.table(...)` | `spark.table(...)` |
| `<events>__g1..gK` | Streaming tables and append flows | Materialized view per generation, batch reads of lower strata |
| Empty generation | Empty streaming projection of `__g0` | Empty batch projection of `__g0`, same schema and dependency |
| Episode snapshot, episodes, determinations, higher-order strata, public events | unchanged | unchanged |

### Unpersisted generations

In `batch` mode the numbered strata can be declared as pipeline-scoped
temporary views instead of materialized views, so no `__g*` tables are
created and nothing intermediate is written:

```yaml
kindling:
  lakeflow:
    temporal_mode: batch
    temporal_strata_materialization: view    # default: table
```

Only the numbered strata change. The determinations view, higher-order
boundaries, the episodes Auto CDC target and the public `events` union are
declared identically, so query results are unchanged.

The cost is recomputation. Generation `k` reads every stratum below it, so a
temporary view is re-expanded once per reference rather than read back from
storage: the plan behind `events` grows exponentially in
`kindling.temporal.max_generations`, on the order of `2^K` rescans of the
base stratum at ceiling `K`. That is fine at a ceiling of 1–3 and a serious
problem at the default of 10. You also give up per-generation observability —
there is no `__g*` table to query when working out which generation produced
an event, and no per-stratum metrics in the event log.

`view` requires `temporal_mode: batch`. A streaming stratum is an append-flow
target and a temporary view cannot be one, so the combination fails the
declaration instead of quietly downgrading to batch reads.

Multi-source fan-in is unchanged: every base declaration lands in the one
stratum-0 dataset, each input keeping its own transform. Base sources still
resolve to external physical names through `EntityNameMapper` in both modes,
so a producer selected in the same pipeline establishes no local dependency
edge — keep those sources in an upstream resource. A batch chain declares no
`readStream` and no append flow at all; a chain *with* episodes still calls
`create_streaming_table` for the snapshot-CDC target only, because that is
what the AUTO CDC FROM SNAPSHOT API requires.

Semantics to plan for:

- Batch strata carry batch-query semantics over the rows available at each
  refresh. A refresh can revise or remove previously produced events, and
  late arrivals can change window results across a whole subject partition.
  They are not append-only event archives and they do not use runner
  watermarks.
- Retain the source history the computation needs. Removing input rows can
  remove derived events and change the episode snapshot; snapshot CDC keeps
  its SCD2 version history but cannot reconstruct source history that was
  never retained.
- Use stable event identity and deterministic window ordering, including a
  tie breaker for equal timestamps. Never use a mutable row rank as event
  identity.
- Changing the mode changes `__g0..gK` dataset types. It is not a hot toggle
  with portable checkpoint state: select the mode for newly provisioned
  pipeline outputs, and treat conversion of an existing pipeline as a
  deployment operation. Nothing is dropped or reset automatically.
- The value ignores surrounding whitespace and case. Anything other than
  `streaming` or `batch` fails the declaration before any Lakeflow object is
  created. The setting is inert when no temporal chain-events pipe is
  selected, and never changes general SDP pipe execution, Lakeflow triggered
  versus continuous scheduling, or the runner engine's watermark behavior.

Batch mode enables batch analysis; it does not guarantee that arbitrary user
code is declarable, or that a query refreshes incrementally.


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
