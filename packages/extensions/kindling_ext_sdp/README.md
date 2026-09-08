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

Pipeline-local dataset names are single-part identifiers within the pipeline's
target catalog/schema. By default they now project the shared table naming
policy used by the runtime resolver:

```yaml
kindling:
  storage:
    table_naming: leaf
    table_schema: cwmdp
```

An explicit SDP-only setting is still supported:

```yaml
kindling:
  sdp:
    dataset_naming: leaf
```

| Mode | Logical entity ID | Emitted dataset name |
|---|---|---|
| `normalized` / `legacy` | `silver.device_telemetry` | `silver_device_telemetry` |
| `leaf` | `silver.device_telemetry` | `device_telemetry` |

Both modes replace hyphens with underscores. Config values ignore surrounding
whitespace and case. When `kindling.sdp.dataset_naming` is omitted or null,
`declare_pipeline()` projects `kindling.storage.table_naming`: `leaf` stays
`leaf`, while absent policy, `legacy`, and `normalized` emit normalized names.
Unknown modes are reported alongside other declaration validation errors.
The setting is resolved by `declare_pipeline()` after configuration overlays
and applies to both `sdp` and `databricks_sdp`. Direct engine constructors
accept `dataset_naming="leaf"`; `DatasetNameMapper` implements the shared
pipeline-local naming strategy.

Per-entity `provider.table_naming` tags can override the shared global policy.
If an explicit `kindling.sdp.dataset_naming` disagrees with the shared policy
or a per-entity tag, validation raises `naming_policy_conflict`. Set
`kindling.sdp.dataset_naming_divergence: intentional` only when the pipeline
local name is deliberately different from the external table naming policy.

Names must be unique (case-insensitively) among selected outputs in one
declaration plan. Use `kindling.declare_pipeline(pipe_ids=[...])` to select
the pipes belonging to each resource. Separate bronze and silver resources
can each emit `device_telemetry`; selecting both same-leaf outputs in one
resource fails before emission with the conflicting entity and pipe IDs.

Internal batch/stream reads use these single-part names. With an explicit
shared naming policy configured, external reads are resolved through the
injected `EntityNameMapper`, so a consumer reads the same external name that
the producer writes:

```yaml
kindling:
  storage:
    table_naming: leaf
    table_schema: cwmdp

dataentities:
  "silver.**":
    tags:
      provider.table_catalog: dev_silver
```

Here `silver.device_telemetry` declares the pipeline-local dataset
`device_telemetry` and resolves external reads/writes as
`dev_silver.cwmdp.device_telemetry`. With no shared table-naming policy
configured, external reads keep the historical bare logical-ID behavior.

`provider.table_name` remains a complete external override: supply the fully
qualified name when one entity is an exception. It takes precedence over
`provider.table_catalog`, `provider.table_schema`, and table naming policy, but
the pipeline continues to declare a single-part dataset name.

Temporal chain base-event sources currently always use external table
resolution, even if a producing pipe is selected in the same pipeline.
Keep those sources in an upstream resource; producing a source in the same
resource does not establish a local temporal dependency.

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
