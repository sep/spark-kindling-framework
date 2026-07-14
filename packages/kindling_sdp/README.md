# kindling_sdp

Spark Declarative Pipelines (SDP) declaration engine for Kindling.

This package turns existing `DataEntities`/`DataPipes` registrations into a
validated, pure-metadata `DeclarationPlan` that a concrete engine can emit as
`pyspark.pipelines` (OSS Spark 4.1+) or Databricks Lakeflow declarations.

Phase 1 (this package today) contains:

- The abstract `DeclarationEngine` interface and the shared plan builder.
- Internal/external input classification derived from the registries.
- Capability gating (`OSS_SDP` vs `DATABRICKS_SDP` feature sets) with
  fail-fast, all-errors-at-once validation.
- Dataset-type selection (entity tag, then engine config, then default
  `materialized_view`).

Phase 1 deliberately has **no dependency on `pyspark.pipelines`** — emission
of actual SDP declarations is Phase 2.

See:

- `docs/proposals/declarative_pipelines_engine.md` — the full proposal.
- `docs/proposals/sdp_engine_phase1_notes.md` — Phase-1 architecture
  decisions (bootstrap ordering, provider write-inertness, deferrals).
