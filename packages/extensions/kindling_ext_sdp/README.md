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
  decorator API (materialized views; streaming tables are Phase 4). The
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

Internal batch/stream reads use these single-part names. External reads
retain their existing resolver behavior; this setting does not change
`EntityNameMapper`, `provider.table_catalog`, logical IDs, or pipe function
argument names. It does not choose the pipeline's catalog/schema.


### Reading leaf-named outputs from elsewhere

Enabling `leaf` does not change the consumer's `EntityNameMapper`. With
catalog `dev_silver` and schema `cwmdp` configured, that mapper still resolves
`silver.device_telemetry` to `dev_silver.cwmdp.silver_device_telemetry`,
while the leaf-mode pipeline writes `dev_silver.cwmdp.device_telemetry`.
Align the consumer's entity metadata explicitly:

```yaml
dataentities:
  silver.device_telemetry:
    tags:
      provider.table_name: dev_silver.cwmdp.device_telemetry
```

`provider.table_name` is a complete override: supply the fully qualified name;
it takes precedence over `provider.table_catalog` and `provider.table_schema`.
The pipeline continues to declare the single-part name `device_telemetry`.
Use corresponding overrides for other entities and resource destinations.

Temporal chain base-event sources currently always use external table
resolution, even if a producing pipe is selected in the same pipeline.
Keep those sources in an upstream resource and align their external names as
above; producing a source in the same resource does not establish a local
temporal dependency.

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
