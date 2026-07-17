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
- `docs/proposals/sdp_engine_phase1_notes.md` — Phase-1 architecture
  decisions (bootstrap ordering, provider write-inertness, deferrals).
