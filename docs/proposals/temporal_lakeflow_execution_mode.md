# Configurable Temporal-Chain Execution Mode for Lakeflow

**Status:** Proposed — not implemented.
**Created:** 2026-09-10
**Reviewed against:** release v0.12.39 (`dfea418`)
**Related:** `temporal_event_segmentation.md`,
`declarable_streaming_sources.md`,
`lakeflow_structured_config.md`,
`docs/guide/temporal_streaming_contract.md`,
`docs/reference/config_reference.md`.

## Problem

The Databricks temporal-chain lowering emits every event stratum as a
streaming table fed by append flows
(`packages/extensions/kindling_ext_databricks/kindling_ext_databricks/temporal_lowering.py`).
Base declarations read `spark.readStream.table(...)` before their transform
runs, and pre-determination strata `__g1..gK` read the lower strata the same
way.

Structured Streaming therefore constrains what a base-event transform may do.
Ordinary ordered analytic windows — `row_number`, `lag`, unbounded
forward fill — cannot be applied to those streaming DataFrames. This is not a
claim that all window operations are unsupported in Structured Streaming:
time-window aggregations have a separate supported contract. It does mean that
a domain whose events are derived by ranking or by carrying a prior value
forward across a subject's history cannot express its transform on the
Lakeflow path today, even though the same transform runs fine under the
runner engine's batch execution.

Episodes are already computed from a batch snapshot over the strata, so the
streaming requirement buys nothing downstream of the base transforms; it only
restricts them.

## Decision

Add `kindling.lakeflow.temporal_mode` with values `streaming` (default) and
`batch`, owned by the Databricks extension. Apply it to temporal-chain
lowering only. Keep domain event, condition, and episode declarations
unchanged.

Three constraints shape the design:

1. Batch mode must convert **all pre-determination event strata**, not just
   base events, to batch materialized views. Keeping append flows downstream
   of a recomputed base view would not propagate revisions and removals
   correctly.
2. Keep episodes as the existing AUTO CDC FROM SNAPSHOT SCD2 target. This
   requires a streaming-table declaration even in batch mode, but does not
   place base-event transforms in Structured Streaming.
3. Batch mode means batch-query result semantics over available inputs on each
   refresh, not a promise to physically scan every row on every update.
   Lakeflow can choose incremental materialized-view maintenance or full
   recomputation. [Databricks refresh semantics](https://docs.databricks.com/aws/en/ldp/incremental-refresh)

This is execution configuration, so a `kindling.*` key read through
ConfigService fits existing conventions (`AGENTS.md`: execution options belong
in `kindling.*` config; parameters are just-in-time overrides). It is distinct
from deployment configuration such as workspace, pipeline identity,
permissions, or compute. Those remain separately maintained, for example in
GitHub variables and bundle configuration. There is no new deployment section
in domain settings.

### Why `kindling.lakeflow.temporal_mode`

The key names a Lakeflow lowering choice, not a temporal-domain behavior, so
it belongs in the engine namespace alongside `kindling.lakeflow.allowed_apps`,
`kindling.lakeflow.pipes`, and `kindling.sdp.dataset_naming`, with its
constant defined in `kindling_ext_databricks`. The alternative —
`kindling.temporal.lakeflow.mode` — would put an engine-specific key inside
the engine-agnostic temporal namespace, where every existing constant
(`kindling.temporal.max_generations`, `kindling.temporal.autocollapse`,
`kindling.temporal.evaluation_time`, `kindling.temporal.revise_persisted`,
`kindling.temporal.conditions.quarantine_entity_id`) is defined in
`kindling_ext_temporal` and consumed by any engine. Splitting that namespace
across two packages would leave no rule for where the next key goes.

## Existing implementation and constraints

The Databricks adapter's
[`_temporal_chain_settings` and `_declare_temporal_chain`](../../packages/extensions/kindling_ext_databricks/kindling_ext_databricks/engine.py)
resolve sibling pipes and the generation ceiling, then delegate to
[`declare_stratified_temporal`](../../packages/extensions/kindling_ext_databricks/kindling_ext_databricks/temporal_lowering.py).
The temporal extension remains engine-independent and a soft dependency, and
the SDP extension contains no temporal lowering of its own.

Current lowering emits one append flow per base-event declaration into
`events__g0`. Every flow calls `spark.readStream.table(...)` before invoking
the registered transform and selecting the canonical event envelope.
Pre-determination strata `__g1` through `__gK` also use streaming reads and
append flows.

Episodes already consume a batch snapshot over these strata. Snapshot CDC
maintains SCD2 episode versions; determinations project all episode versions;
higher-order boundaries and the public events union are materialized views.
The fixed generation ceiling and rejection of episodes over higher-order
conditions are existing constraints, and remain unchanged.

Batch lowering makes base and boundary transforms ordinary batch Spark
queries, subject to Lakeflow's normal query restrictions.

`lakeflow_structured_config.md` is implemented (shipped 0.12.35, with the
transport since moved to the shared bootstrap layer); the current selector and
bootstrap ingestion code are authoritative for supported keys.

## Configuration contract

```yaml
kindling:
  lakeflow:
    temporal_mode: batch
```

The scope is all selected temporal chains in one initialized Databricks
pipeline. No per-entity tags, per-base-event overrides, or mixed modes within
a chain are introduced. This setting does not select Lakeflow triggered versus
continuous scheduling, change general SDP pipe execution, or change the runner
engine's watermark/incremental behavior.

Resolve the mode once for a declaration invocation through
`GlobalInjector.get(ConfigService).get("kindling.lakeflow.temporal_mode", "streaming")`,
in the engine's shared `_temporal_chain_settings` path so declaration
validation and emission see the same value. Do not read configuration inside a
dataset query function.

- An absent key defaults to `streaming`.
- Accept strings after trimming whitespace and converting to lowercase.
- Reject null, empty strings, booleans, numbers, collections, and other values.
- Raise `ValueError` naming the key and allowed values, before any Lakeflow
  decorators or target-creation calls run. For example:
  `Invalid kindling.lakeflow.temporal_mode: expected 'streaming' or 'batch'.`
- Do not copy the broad `except Exception` currently used for generation-ceiling
  lookup: it could hide configuration failures and silently select streaming.
  Missing or broken ConfigService in initialized engine use must surface as a
  configuration/initialization error. Bare engine tests should bind a minimal
  config service. The low-level lowering helper can retain an explicit default
  `mode="streaming"` for existing direct callers, with the same validation.
- With no temporal chain-events pipe selected, the mode is inert. It does not
  affect the per-declaration temporal lowering,
  `kindling.temporal.autocollapse`, or any non-temporal pipe. Setting it in
  such a pipeline is not an error.

Use the existing settings/initialization and SparkConf ingestion path; do not
add another configuration reader or precedence system. A deployment may supply
this scalar without maintaining a settings file. The canonical Lakeflow
transport is the shared `spark.kindling.*` route mapped by
`map_spark_kindling_items` in `packages/kindling/bootstrap.py`:

```yaml
configuration:
  spark.kindling.lakeflow.temporal_mode: "batch"
```

Bare `kindling.*` pipeline-configuration keys are still bridged by the
selector when Spark configuration can be enumerated. Restricted runtimes
(serverless, shared access) block every enumeration surface and allow only
point lookups, so a bare key there must be named explicitly:

```yaml
configuration:
  kindling.lakeflow.temporal_mode: "batch"
  kindling.lakeflow.config_keys: "kindling.lakeflow.temporal_mode"
```

Append the mode key to any existing `config_keys` list instead of replacing
it. Note that `kindling.lakeflow.config_files` is deprecated in favor of
`spark.kindling.bootstrap.config_files` (removal eligible at 0.13.0) and is
unrelated to this key. Test the current ingestion path rather than inventing a
mode-specific bridge.

## Proposed topology

Names below use `events` and `episodes` for readability. Preserve the adapter's
actual dataset-name mapping and collision checks.

| Component | Streaming, including omitted mode | Batch |
| --- | --- | --- |
| Base stratum `events__g0` | Streaming table, one append flow per base declaration | One materialized view, union of transformed base inputs |
| Base reads | `spark.readStream.table(resolved_source)` | `spark.table(resolved_source)` |
| Pre-determination `events__g1..gK` | Streaming tables and append flows over lower strata | Materialized view per generation, batch reads of lower strata |
| Empty generation | Empty streaming projection of `__g0` | Empty batch projection of `__g0`, preserving envelope schema and dependency |
| Episode snapshot | Temporary view, batch reads of all pre-determination strata | Same |
| Episodes | Streaming-table target with AUTO CDC FROM SNAPSHOT, SCD2 | Same |
| Determinations | Materialized view reading episode versions | Same |
| Higher-order boundaries | Existing materialized-view lowering | Same |
| Public events | Materialized-view union with existing event-ID deduplication | Same |

For batch `__g0`, resolve each registered input, invoke its own transform,
apply `TemporalPipeTranslator.select_event_envelope`, and `unionByName` the
resulting envelopes inside a single query function. Do not union heterogeneous
raw inputs before their transforms. This preserves native multi-source fan-in
without extra per-entity application readers or generated base-view names.

For each batch generation, use the same precomputed rules and lower-stratum list
as streaming mode. Only the dataset kind and read operation change. Capture
loop variables explicitly in closures. Preserve the fixed-K topology, empty
strata, existing schemas, names, and public event deduplication.

Base-source name resolution is unchanged in both modes: sources resolve to
external physical names through the registered EntityNameMapper, including
catalog/schema/explicit/leaf metadata, even when a producing pipe is selected
in the same pipeline. Batch mode inherits that documented limitation
(`packages/extensions/kindling_ext_sdp/README.md`, temporal source
resolution): a same-pipeline producer establishes no local temporal dependency
edge, so those sources belong in an upstream resource with aligned external
names. Making internal producers resolve to their emitted pipeline-local
dataset names is a separate change affecting both modes — see
[Deliberately out of scope](#deliberately-out-of-scope).

All batch data reads and transforms belong inside dataset query functions.
There must be no `readStream` or append flow anywhere in the batch event strata.
A pipeline with episodes may still call `create_streaming_table` **only for the
episode snapshot-CDC target**. A chain without episodes needs none of these
streaming APIs. Databricks documents a streaming-table target for snapshot CDC.
[Snapshot CDC API](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes-from-snapshot)

Conditions remain external configuration data ingested through Kindling's
existing validated path. `_read_rules` currently collects them while declaring
the graph; they are not a newly introduced pipeline-local condition dataset.
Preserve that behavior and require a new declaration/update to pick up changed
rules. Do not claim a new declarative condition-table dependency or automatic
rule-triggered refresh. Existing higher-order limitations are not fixed here.

## Semantics, constraints, and mode changes

Batch base and condition strata reflect the query over the Delta rows currently
available for each refresh. They can revise or remove previously produced
results. They are not append-only event archives, and they do not use runner
watermarks to restrict a transform to new rows. Late arrivals can change window
results across an entire subject partition.

Retain enough source data for the intended computation. Removing historical
input rows can remove derived events and change the episode snapshot. Snapshot
CDC retains version history according to its existing SCD2 semantics, but it
cannot reconstruct source history that was never retained. Source removal,
correction, and episode disappearance require multi-update platform tests;
do not advertise batch mode as an immutable temporal audit log.

Use stable event identity and deterministic window ordering, including a tie
breaker for equal timestamps. Do not use a mutable row rank as event identity.
Batch mode enables batch analysis; it does not guarantee that arbitrary user
code is declarable or that a query will refresh incrementally.

Event Hub ingestion remains a separate producer of retained Bronze Delta data.
Since 0.12.39 an Event Hub entity can itself be lowered as a provider-owned
declarable streaming source, but that shape is only valid for a normal pipe
whose driving input carries the capability — temporal and AUTO CDC
compositions reject it (`declarable_streaming_sources.md`,
`packages/extensions/kindling_ext_sdp/README.md`). A temporal chain therefore
reads the retained Delta surface, never the Event Hub stream itself. If the
Bronze producer is selected in the same pipeline, retain its normal streaming
ingestion and, subject to the base-source resolution limitation above, keep
that producer in an upstream resource.

Preserve existing episode evaluation-time and determination-history behavior.
Do not promise expiration without an update or change how higher-order rules
are evaluated as part of this option.

Changing mode changes `__g0..gK` dataset types. It is not a hot toggle with
portable checkpoint state. Initially support mode selection for newly provisioned
pipeline outputs; document existing-pipeline conversion only after testing the
platform's supported transition procedure. Never automatically drop targets,
reset checkpoints, or discard episode history. Keep public names stable and
leave lifecycle operations under the existing deployment process.

This is a deliberate, narrow exception to Kindling's desired-state convergence
rule (`AGENTS.md`: declare the target and let the framework converge). Schema
convergence can rewrite a table in place; a Lakeflow dataset-type flip cannot
converge without dropping a target and its checkpoint or episode history.
Declining to automate that is the safe reading of the convention, not a
departure from it, and no versioned migration script is introduced.

## Deliberately out of scope

**Internal base-source dependency edges.** `declare_stratified_temporal`
resolves base sources only as external physical names, falling back to the raw
entity ID when no entity is registered. Passing the selected plan's emitted
pipeline-local dataset names into temporal lowering would make same-pipeline
producers real dependency edges. That is desirable, but it changes streaming
mode too, contradicts this proposal's requirement that omitted and explicit
`streaming` remain byte-identical, and invalidates a documented SDP behavior.
It needs its own acceptance coverage and doc correction, so it is tracked
separately rather than smuggled in behind a mode flag.

**Everything else unchanged.** No new core provider capability, shared SDP
execution mode, CLI flag, application reader, or temporal domain API is
required. Higher-order episode support, rule-triggered refresh, and streaming
window support are not addressed.

## Implementation scope and acceptance

The implementation belongs entirely in `kindling_ext_databricks`:

1. Add a mode constant/parser and resolve it through the engine's shared
   `_temporal_chain_settings` path before declaration. Pass it explicitly into
   the lowerer.
2. Preserve the default streaming branch's emitted objects, flow names, and
   read behavior exactly. Add the MV branch for `__g0..gK`, reusing the
   existing transformations, rules, and name resolution.
3. Reuse collision validation without introducing helper datasets merely to
   implement fan-in. Generated-name reservations in `_emitted_dataset_names`
   are mode-independent and stay as they are.
4. Documentation: add the key to `docs/reference/config_reference.md` under
   "SDP and Databricks Lakeflow" beside `kindling.sdp.dataset_naming`; document
   configuration, topology, retained-input semantics, and transition
   constraints in `packages/extensions/kindling_ext_databricks/README.md` and
   `docs/guide/temporal_streaming_contract.md`; note the per-mode strata
   dataset kinds where `packages/extensions/kindling_ext_sdp/README.md`
   documents the temporal topology and generated names. Add an Unreleased
   changelog entry when the feature ships.

Version the Databricks extension per `docs/contributing/release_process.md`
(new features are a minor bump; the repository's pre-1.0 practice has been to
ship features in patch releases). This proposal assigns no release version or
support date.

Focused acceptance coverage:

- Omitted mode and explicit `streaming` produce identical current topology and
  reads, including fan-in, empty strata, episodes, and normalized/leaf naming.
- `batch` declares the complete MV event topology. Invoke recorded query functions
  to verify batch reads, transform invocation, schema-preserving empty strata,
  union membership, and absence of streaming event operations. Test with and
  without episodes, and with pre/post-determination rules.
- Reject each invalid type/value before any declarations; exercise whitespace
  normalization, missing key, and ConfigService failures separately.
- A pipeline with `batch` set and no temporal chain selected declares exactly
  what it declares today.
- Test configuration through the actual ingestion path in enumeration-capable
  and get-only Spark configurations, covering both the canonical
  `spark.kindling.lakeflow.temporal_mode` key and the bare key named through
  `kindling.lakeflow.config_keys`. Existing permissive test stubs that return
  `2` for every config key (`tests/unit/test_temporal_sdp_lowering.py`) must
  become key-aware.
- Verify an external logical input still resolves to a different physical
  catalog with leaf naming in both modes.
- Use a real local Spark batch DataFrame with a transform containing
  `row_number().over(Window.partitionBy("machine_id").orderBy("reading_ts", "reading_id"))`.
  Evaluate the batch declaration function, assert `isStreaming` is false, force
  execution, and verify ranking and canonical envelope results. A mocked
  decorator test alone does not establish window support.
- Extend the existing Lakeflow temporal system test for batch mode with that
  transform, multiple updates and a late correction. Verify graph analysis,
  event/condition outputs, SCD2 episodes, determination history, and downstream
  consumption. Keep default-streaming coverage. Treat this as the platform
  acceptance gate before advertising supported Lakeflow execution.

Run focused suites through Poe, including `test_temporal_sdp_lowering`, Databricks
engine and selector tests, and the temporal integration test containing the real
window execution. Use the existing Databricks extension system-test task for
platform acceptance. No cloud tests or implementation are part of this proposal.
