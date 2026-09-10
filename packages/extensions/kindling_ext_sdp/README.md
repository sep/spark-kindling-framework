# kindling_ext_sdp

Spark Declarative Pipelines (SDP) declaration engine for Kindling.

This package turns existing `DataEntities`/`DataPipes` registrations into a
validated, pure-metadata `DeclarationPlan` that a concrete engine can emit as
`pyspark.pipelines` (OSS Spark 4.1+) or Databricks Lakeflow declarations.

Phase 1 contains:

- The abstract `DeclarationEngine` interface and the shared plan builder.
- Internal/external input classification derived from the registries.
- Capability gating (`OSS_SDP` vs `DATABRICKS_SDP` feature sets) with
  fail-fast, all-errors-at-once validation.
- Dataset-type selection (entity tag, then engine config, then default
  `materialized_view`).

Phase 2 adds:

- `OssSdpEngine` — emits the plan through the OSS `pyspark.pipelines`
  decorator API (materialized views only). Provider-owned streaming sources
  are recognized in the shared plan but capability-gated off for OSS SDP
  until Spark pipeline parity is accepted separately. The
  `dp` module is an injected dependency resolved lazily at declaration
  time, so importing this package still never imports `pyspark.pipelines`
  and Kindling keeps supporting Spark runtimes older than 4.1.
- `SdpWriteGuardProvider` — the write-inert provider personality installed
  by SDP-mode bootstrap ("SDP owns persistence"): reads delegate, every
  write path raises `SdpModeWriteError`.
- The bootstrap surface: `kindling.initialize(engine="sdp")` activates the
  guard and skips the watermark aspect (SDP owns incrementality);
  `kindling.declare_pipeline()` — the mandatory LAST step of the entry
  point — builds, validates, and declares the plan.
- The local validation harness (`write_pipeline_spec` + `dry_run`):
  ephemeral `spark-pipeline.yml` + `spark-pipelines dry-run`, a
  unit-test-tier gate for the declared graph with no cluster in the loop
  (requires the Spark 4.1+ CLI: `pip install 'pyspark[pipelines]>=4.1'`).

Phase 3 adds:

- Full entity-metadata emission on the OSS engine: `comment`,
  `table_properties` (entity tags `sdp.table_properties.<key>` over an
  engine-config `table_properties` block, tag winning per key),
  `partition_cols`, `cluster_by`, and `schema` — the complete Spark 4.1
  `dp.materialized_view` keyword surface. Deliberate divergence from the
  runner engine, per the dual-engine parity criterion: SDP does NOT force
  `delta.enableChangeDataFeed` (that feeds the runner's watermark
  machinery); declare it as a tag if external consumers need CDF.
- An `engine_factory` seam on `declare_pipeline()` so adapter packages
  reuse the whole bootstrap path.
- The `kindling_ext_databricks` adapter package (separate README):
  Lakeflow expectations, selected via `engine="databricks_sdp"`.

Provider-owned streaming sources:

- The shared planner can classify an external input as
  `EXTERNAL_STREAMING_SOURCE` when its provider implements
  `DeclarableStreamingSource` and `StreamableEntityProvider`.
- The provider returns an inert, secret-safe `StreamingSourceSpec`; plans and
  diagnostics include tag/option names and validation constraints, never
  connection strings, SAS keys, passwords, JAAS values, or resolved secrets.
- A streaming source must be a driving input selected by
  `driving_entity_ids`; only one declarable streaming source is supported in
  a pipe, and temporal/AUTO CDC compositions reject the ambiguous shape.
- The output dataset type is inferred as `streaming_table`. Explicit
  materialized-view requests fail with `streaming_dataset_type_conflict`
  instead of being silently overridden.
- `engine="sdp"` reports `streaming_source_lowering_not_supported` for this
  shape today. `engine="databricks_sdp"` lowers it through Lakeflow; see the
  Databricks extension README.

Entry point shape (fixed bootstrap surface — never generated code):

```python
import kindling

kindling.initialize(engine="sdp")

from my_app import register_all
register_all()

kindling.declare_pipeline()
```

See:

- `docs/proposals/declarative_pipelines_engine.md` — the full proposal.
- `docs/proposals/obsolete/sdp_engine_phase1_notes.md` — Phase-1 architecture
  decisions (bootstrap ordering, provider write-inertness, deferrals).


## Output dataset naming

Configure names within the pipeline's target catalog/schema independently
of logical entity IDs and external table resolution:

```yaml
kindling:
  sdp:
    dataset_naming: leaf
```

| Mode | Logical entity ID | Emitted dataset name |
|---|---|---|
| `normalized` (default) | `silver.device_telemetry` | `silver_device_telemetry` |
| `leaf` | `silver.device_telemetry` | `device_telemetry` |

Both modes replace hyphens with underscores. Config values ignore surrounding
whitespace and case; an omitted or null value uses `normalized`. Unknown modes
are reported alongside other declaration validation errors.
The setting is resolved by `declare_pipeline()` after configuration overlays
and applies to both `sdp` and `databricks_sdp`. Direct engine constructors
accept `dataset_naming="leaf"`; `DatasetNameMapper` implements the shared
pipeline-local naming strategy.

Names must be unique (case-insensitively) among selected outputs in one
declaration plan. Use `kindling.declare_pipeline(pipe_ids=[...])` to select
the pipes belonging to each resource. Separate bronze and silver resources
can each emit `device_telemetry`; selecting both same-leaf outputs in one
resource fails before emission with the conflicting entity and pipe IDs.

Internal batch/stream reads use these single-part names. External Delta batch
reads resolve the registered entity metadata through the runtime's
`EntityNameMapper` before calling `spark.table`. This honors
`provider.table_catalog`, `provider.table_schema`, `provider.table_name`, and
`provider.table_name_strategy`, independently of the pipeline's current catalog
and its output naming mode. Logical IDs and pipe function argument names stay
unchanged; an explicit `external_read_resolver` still receives the logical ID.
Provider-owned streaming sources retain their provider streaming read path.


### Reading leaf-named outputs from elsewhere

Enabling `leaf` does not change the consumer's `EntityNameMapper`. With
catalog `dev_silver` and schema `cwmdp` configured, that mapper still resolves
`silver.device_telemetry` to `dev_silver.cwmdp.silver_device_telemetry`,
while the leaf-mode pipeline writes `dev_silver.cwmdp.device_telemetry`.
Align the consumer's entity metadata with the mapper's `leaf` name strategy:

```yaml
dataentities-bytag:
  ldp_output:
    "true":
      tags:
        provider.table_name_strategy: leaf
```

The strategy retains the entity's resolved catalog and schema, but uses only the
logical entity ID's final segment for its table name. Apply the `ldp_output`
tag to the matching entities; `dataentities-bytag` adds the mapper strategy to
the whole family. The pipeline continues to declare the single-part name
`device_telemetry`.

`provider.table_name` remains a complete override and takes precedence over
the name strategy, `provider.table_catalog`, and `provider.table_schema`.

Temporal chain base-event sources currently always use external table
resolution, even if a producing pipe is selected in the same pipeline.
Keep those sources in an upstream resource and align their external names as
above; producing a source in the same resource does not establish a local
temporal dependency.

On `engine="databricks_sdp"`, `kindling.lakeflow.temporal_mode: batch` lowers
`<events>__g0..gN` as materialized views with batch reads instead of streaming
tables with append flows; the reserved names below are unchanged in both
modes. See the
[Databricks extension documentation](../kindling_ext_databricks/README.md#temporal-chain-execution-mode).

### Generated dataset names

Lakeflow reserves helper names as well as selected outputs. Validation rejects
collisions with AUTO CDC `<target>__scd_source`, temporal `<events>__g0..N`
(where N is `kindling.temporal.max_generations`), `<events>__ghi`,
`<events>__determinations`, and `<episodes>__episode_snapshot`.
Conditional temporal helper names are reserved even when the current rules
do not use them, so later rule changes cannot introduce name collisions.

Selecting a temporal chain-events pipe also reserves and emits its registered
chain-episodes sibling, even when the episodes pipe is not explicitly selected.
These reservations apply only to the resource containing that chain or CDC
target; other pipeline resources can use the same names.
