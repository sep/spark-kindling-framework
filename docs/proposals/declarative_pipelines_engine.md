# Spark Declarative Pipelines Execution Engine for Kindling

**Status:** Draft — direction reviewed in design discussion; prerequisite
watermark refactor implemented (PR #158); not yet scheduled.
**Created:** 2026-07-13
**Related:** `package_config_architecture.md` (post-registration config
overlay this engine's bootstrap ordering depends on),
`temporal_event_segmentation.md` (extension explicitly scoped to the runner
engine — see "What Stays Runner-Only"), PR #158 (watermark aspect — the
implemented precedent for engine-owned incrementality).

## Recommendation

Proceed with a **Spark Declarative Pipelines (SDP) execution mode** for
Kindling, implemented as a **declaration engine** that consumes existing
`DataEntities`/`DataPipes` registrations and declares SDP datasets and
flows at pipeline evaluation time.

Explicitly rejected alternatives:

- **Source generation.** No per-app generated pipeline code. The entry
  point is a fixed bootstrap surface that evaluates Kindling declarations
  and asks the configured engine to declare them.
- **Running the imperative persist path inside SDP.** SDP owns
  orchestration, refresh, retries, dependency planning, and persistence.
  Kindling's imperative merge/watermark machinery is not ported into
  dataset functions.

```text
Kindling app declarations
  -> Kindling registry and IoC bootstrap
  -> engine = sdp (or databricks_sdp)
  -> engine declares dp tables, materialized views, and flows
  -> SDP executes and maintains the pipeline
```

This keeps Kindling's core design intact: user code declares entities and
transformations; execution policy is an injected engine component. SDP
becomes one execution backend among others — not a separate application
model and not a generated-code target.

## Packaging: OSS Core, Databricks Adapter

Declarative pipelines are **Apache Spark**, not Databricks: Spark 4.1
shipped SDP as `pyspark.pipelines` (aliased `dp`), with a `spark-pipelines`
CLI (`init`/`dry-run`/`run`) and `pip install pyspark[pipelines]`. (Spark
4.0 did *not* include it — an earlier draft of this direction said
otherwise.) Databricks Lakeflow SDP "extends and is interoperable with"
the OSS API on the Databricks Runtime.

That fact inverts the natural packaging. The engine core must not be
Databricks-named or Databricks-dependent:

- **`kindling_sdp`** — the declaration engine, targeting only the OSS
  `pyspark.pipelines` API. Runs on vanilla Spark 4.1+ and on Databricks
  unchanged.
- **`kindling_databricks_sdp`** — a thin adapter layering on the
  Databricks-only features (see gap analysis below).

"DLT" naming is legacy everywhere; prose and code target `pyspark.
pipelines as dp`.

The OSS core also buys the biggest practical win: **local pipeline
validation with no Databricks in the loop** — `spark-pipelines dry-run`
against declarations built from local registrations is a unit-test-tier
gate for the declared graph.

Spark 4.1+ is a floor for the SDP extra only. Kindling core keeps
supporting older runtimes; `kindling_sdp` is strictly optional with no
core dependency on `pyspark.pipelines`.

## The Gap Between OSS SDP and Databricks Lakeflow, and the Bridge

The two share one declaration API; the gap is three layers deep.

**Layer 1 — API surface (code you can write).** OSS Spark 4.1 SDP has the
full structural vocabulary: `@dp.table`, `@dp.materialized_view`,
`@dp.temporary_view`, `@dp.append_flow`, `create_streaming_table()`,
`create_sink()`, and dependency inference. Databricks adds several
capabilities on top — per its own comparison table: AUTO CDC,
expectations, update flows, `foreachBatch` sinks, a queryable event log,
and continuous mode. This is not an exhaustive capability analysis; the
two **relevant to this proposal's adapter** are:

| Databricks-only API | What it does |
|---|---|
| Expectations (`@dp.expect*`) | Declarative data-quality constraints with warn/drop/fail actions |
| AUTO CDC (`create_auto_cdc_flow`, `..._from_snapshot_flow`) | CDC ingestion handling out-of-order events via `sequence_by`, with built-in SCD Type 1/2 |

**Layer 2 — engine behavior (same code, different execution).**
Materialized views on OSS fully recompute per refresh; Databricks has
incremental MV refresh (Enzyme). A `refresh_policy: incremental` config
key is therefore a Databricks *hint*, not a portable declaration.

**Layer 3 — platform (how a pipeline runs).** OSS: `spark-pipelines` CLI
driven by a spec file, triggered "available now" execution only; you own
scheduling, retries, monitoring. Databricks: managed workspace object,
triggered or continuous, built-in scheduler, event log, serverless, Unity
Catalog governance.

**The bridge is augmentation, not translation.** Because Lakeflow is an
interoperable superset, nothing the core emits needs rewriting per
platform:

1. `kindling_sdp` emits only Layer-1 common surface. The same output runs
   unmodified on vanilla Spark and Databricks.
2. `kindling_databricks_sdp` decorates those declarations with
   expectations (from config/tags) and maps `scd.*` entity tags to AUTO
   CDC flows (see "SCD2 as a Declared Flow").
3. **Capability gating in the core:** config naming an adapter feature
   (`expectations:`, `refresh_policy:`, an SCD tag routed to AUTO CDC)
   while declaring against OSS fails at declaration time with a clear
   diagnostic — the fail-fast principle extended from "unsupported pipe
   shape" to "unsupported platform feature."
4. Deployment glue (spec YAML + CLI vs. workspace object/bundles/REST)
   stays out of both packages — docs and tooling, not engine — with one
   deliberate exception: **`kindling_sdp` owns a minimal local validation
   harness** that generates an ephemeral pipeline spec and invokes
   `spark-pipelines dry-run` against the declared graph. The dry-run
   acceptance criterion below needs an owner, and dev-time validation is
   engine responsibility even though production deployment is not.

## Model Mapping

- `input_entity_ids` → SDP reads, after **internal/external
  classification**: an input produced by another registered pipe in the
  same pipeline is an internal dependency (read by dataset name so SDP
  infers the graph edge); anything else is an external table read. This
  classification is derivable from the registry (does any registered pipe
  output that entity?) and is the engine's core piece of Phase-1 logic.
- `output_entity_id` → the declared dataset target.
- The pipe function → the DataFrame-returning query body, unchanged.
- Entity metadata → table properties, schema, partitioning/clustering,
  comments, governance hints.
- Entity IDs (`silver.orders`) → catalog/schema/table names: needs an
  explicit mapping story under Unity Catalog and OSS catalogs (open
  question).
- SDP owns persistence. No manual merge/write/save/start inside dataset
  functions.

### Entry Point Shape

```python
import kindling

kindling.initialize(engine="sdp")          # or "databricks_sdp"

from my_app import register_all
register_all()

kindling.declare_pipeline()
```

A fixed, generic bootstrap surface — not generated application code.
Ordering constraint: `declare_pipeline()` must run **after** the
post-registration config overlay (`package_config_architecture.md`), or
engine config like `dataset_type` won't be resolved when datasets are
declared.

### Provider Behavior in SDP Mode

"SDP owns persistence" means the Delta provider's write path must be
inert or forbidden while the read path stays live in this mode — a
provider-mode question the implementation must answer explicitly rather
than leave to convention.

## Prerequisite Refactors

Two core-Kindling refactors make this engine honest rather than a
compatibility layer over imperative internals. Both were identified in
design discussion; one is done.

### Watermarking as a Signal Aspect — IMPLEMENTED (PR #158)

Incremental reads and watermark advancement now attach to pipe execution
via signals (`read.resolve_read`, `persist.after_persist`,
`persist.persist_failed`) through `WatermarkAspect`, instead of being
woven into the read/persist strategy. An execution engine that owns its
own incrementality — SDP with streaming checkpoints and MV refresh —
**simply never registers the aspect**. Nothing in a pipe knows the
difference.

Consequence for this proposal: the SDP unsupported-list entry narrows
from "watermark-dependent pipes" (broad, ambient) to "pipes that
*explicitly call* the watermark API" (rare, detectable).

The refactor also fixed two incremental-read correctness bugs (skipped
intermediate CDF versions; watermark over-advance between read and
persist), which is evidence for the design: logic smeared across three
files had no single owner to notice either bug.

### SCD2 as a Declared Flow — PROPOSED

Today SCD2 is a persist-path side effect: tags describe intent, the Delta
provider executes the merge, `effective_from` is stamped with
`current_timestamp()` at merge time. Three changes reduce the friction
with AUTO CDC to a mechanical mapping:

1. **Explicit, declared `sequence_by`.** AUTO CDC's correctness rests on
   ordering authority coming from the data, so out-of-order arrivals
   reconcile; merge-time system timestamps are arrival-order — the thing
   AUTO CDC exists to avoid. This also shrinks the system-time vs.
   business-time divergence that `read_entity_as_of` replay tripped over
   in `temporal_event_segmentation.md`.
2. **Split the input contract:** `source_kind: snapshot | change_feed`
   plus a `delete_when` predicate. `scd.close_on_missing: true` is
   snapshot semantics (= `create_auto_cdc_from_snapshot_flow`);
   change-event semantics with explicit deletes is `create_auto_cdc_flow`
   + `apply_as_deletes`. One boolean currently conflates them.
3. **SCD2 becomes a flow declaration** — "entity X = scd2(source, keys,
   sequence_by, source_kind)". The runner engine compiles it to the
   existing merge machinery as an ordinary step; the Databricks adapter
   compiles it to an AUTO CDC flow. The current-view companion stops
   being special machinery — it's a declared view (`WHERE effective_to
   IS NULL`), which is *more* natural in SDP than what exists now.
   Existing `scd.*` tags survive as sugar; migration is additive.

Deliberately out of the MVP mapping: bitemporality. Databricks now has
bitemporal AUTO CDC (`stored_as_scd_type="bitemporal"` +
`system_sequence_by`, adding `__SYSTEM_START_AT`/`__SYSTEM_END_AT`
alongside the SCD2 columns) — but it is in Beta and Databricks-only, so
the declared-flow contract must not depend on it. Worth tracking, not
adopting: it maps directly onto the business-time vs. system-time split
(`valid_from`/`valid_to` vs. `effective_from`/`effective_to`) that
`temporal_event_segmentation.md`'s Condition-replay open question needs,
so a future adapter tier could resolve that question natively once the
feature GAs.

## MVP Scope

- Delta/table-backed entities only.
- Pure DataFrame-returning pipes only.
- Batch materialized views; streaming tables only where the input
  provider can produce a valid streaming DataFrame.
- Explicit dataset type selection through entity tags or config.
- No custom persistence; no actions or manual writes inside dataset
  functions.
- Expectations and AUTO CDC: adapter tier only, capability-gated.

Example config (note `refresh_policy` and `expectations` are
adapter-tier keys):

```yaml
datapipes:
  silver.orders:
    engine:
      sdp:
        dataset_type: materialized_view
      databricks_sdp:
        refresh_policy: incremental
        expectations:
          valid_order_id: "order_id IS NOT NULL"
```

### Unsupported — Fail Fast at Declaration Time

- Side-effecting pipe logic; actions (`collect`, `count`, `write`,
  `saveAsTable`, `start`) inside dataset functions.
- Custom persist strategies; manual Delta merges.
- Pipes that explicitly call the watermark API (`get_watermark`) — the
  ambient case is gone per the aspect refactor.
- Local-only entity providers; provider behavior that cannot be
  represented as an SDP read.
- Adapter-tier features requested against the OSS runtime.

### What Stays Runner-Only

*Revised 2026-07-15 (design review; see `kindling_core_runner_split.md`,
Friction #9) — the original text overstated the runner requirement.*

`kindling-temporal`'s **current executor** is runner-hosted, but most of
the temporal model has no inherent runner affinity. The
Event/Condition/Episode ontology is engine-agnostic by design;
condition evaluation is df→df transformation over a dependency graph the
ontology requires to be acyclic and statically known, which is exactly
the shape a declarative engine executes natively (generation ordering is
native DAG ordering — SDP infers it; the runner implements it via
`generation_executor`). The driver-side `collect()` loop is a property
of the first executor implementation, not the model.

What is genuinely engine-constrained is **episode lifecycle revision** —
stable identity across revision, open episodes closed later, provisional
closes superseded — which needs keyed merge/upsert semantics: the
runner's merge machinery today; plausibly AUTO CDC in the Databricks
adapter tier (the same keyed-sequenced-upsert shape); weakest fit on
plain OSS SDP, where materialized-view recompute makes
processing-time-dependent expiration awkward. Temporal therefore scopes
its *executor* to the runner engine for now, while its declarations
remain portable.

## Phases

1. **Architecture note + declaration-engine interface.** Includes the
   internal/external read classification, capability gating, bootstrap
   ordering, and the provider write-inertness decision.
2. **Prototype:** one bronze source and one silver materialized view from
   existing registrations, validated with `spark-pipelines dry-run`
   locally.
3. Expectations (adapter), table properties, partitioning/clustering,
   schema mapping.
4. Streaming table support and append flows.
5. SCD mapping to AUTO CDC (requires "SCD2 as a Declared Flow"), rather
   than porting the imperative merge path.

## Acceptance Criteria

- [ ] An existing Kindling app registers entities and pipes normally; the
      SDP engine declares corresponding pipeline objects.
- [ ] No per-app source generation.
- [ ] The imperative persist path is not invoked in SDP mode.
- [ ] SDP infers and executes the pipeline graph from the declared reads.
- [ ] The declared pipeline dry-runs on OSS Spark locally via
      `kindling_sdp`'s validation harness (ephemeral spec +
      `spark-pipelines dry-run` — see the bridge section), with no
      Databricks dependency.
- [ ] Unsupported pipes fail during declaration with actionable errors,
      including adapter-tier features requested on the OSS runtime.
- [ ] **Dual-engine parity:** the same app under the runner engine and
      the SDP engine either produces semantically equivalent outputs or
      the divergence is explicitly documented per feature (persist
      strategies, SCD2 semantics, current-view companions).

## Open Questions

- **`dataset_type` placement.** Materialized-view-vs-streaming-table is a
  property of the output *dataset* — entity-level metadata — but engine
  config naturally hangs off the pipe. Which owns it, and what happens
  when they disagree?
- **Multiple flows into one target.** SDP's `append_flow` allows several
  flows per target; Kindling's model assumes one pipe per output entity
  (the driving-source convention's write side). Deliberate mismatch to
  resolve — the temporal proposal's mirror-pipe pattern would want append
  flows eventually.
- **Catalog naming.** `silver.orders` → catalog/schema/table mapping
  under Unity Catalog and OSS Spark catalogs.
- **Databricks runtime floor.** Which DBR versions support the unified
  `from pyspark import pipelines as dp` import vs. legacy `import dlt`,
  and does the adapter need to paper over that?
- **Provider write-inertness mechanism.** Tag, mode flag on the
  registry, or a distinct provider binding in SDP bootstrap?

## References

- Spark Release 4.1.0 — https://spark.apache.org/releases/spark-release-4.1.0.html
- Spark Declarative Pipelines Programming Guide —
  https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html
- What is Lakeflow Spark Declarative Pipelines (Databricks) —
  https://docs.databricks.com/aws/en/ldp/concepts
- Lakeflow vs. Apache Spark Declarative Pipelines feature comparison —
  https://learn.microsoft.com/azure/databricks/ldp/concepts/spark-declarative-pipelines
- The AUTO CDC APIs (incl. bitemporal, Beta) —
  https://learn.microsoft.com/azure/databricks/ldp/cdc
