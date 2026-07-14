# SDP Engine — Phase 1 Architecture Notes

**Status:** Implemented (Phase 1 of `declarative_pipelines_engine.md`)
**Created:** 2026-07-14
**Related:** `declarative_pipelines_engine.md` (parent proposal),
`package_config_architecture.md` (post-registration config overlay),
PR #158 (watermark aspect).

Phase 1 delivers the declaration-engine *interface* and the registry-derived
logic behind it — internal/external read classification, capability gating,
dataset-type selection, and fail-fast validation — as the `kindling_sdp`
package (`packages/kindling_sdp/`). It emits no `pyspark.pipelines`
declarations and has no dependency on them; that is Phase 2. This note
records the decisions Phase 1 was chartered to make.

## What Phase 1 Contains

- `kindling_sdp.declaration_plan` — the pure-metadata plan:
  `DeclarationPlan` → `DatasetDeclaration` (name, dataset type, the pipe's
  execute callable passed through unchanged, classified inputs, and entity
  metadata passthrough: partition/cluster columns, tags, comment).
- `kindling_sdp.capabilities` — `CapabilitySet` (`OSS_SDP` vs
  `DATABRICKS_SDP`) plus `SdpFeature` (expectations, AUTO CDC, incremental
  MV refresh), in the small-ABC/`is_*`-helper style of
  `kindling.entity_provider`.
- `kindling_sdp.declaration_engine` — the abstract `DeclarationEngine`.
  Plan building and `validate()` (all errors at once, never
  first-error-only) are concrete; only `declare_pipeline(plan)` — actual
  emission — is abstract, for Phase-2 subclasses.

### Input classification

For each pipe input entity id: **INTERNAL** if some registered pipe in the
same declaration scope produces it (an in-pipeline dependency — SDP infers
the graph edge from a read by dataset name), otherwise **EXTERNAL** (a read
from storage). Derived purely from the registries, exactly as the parent
proposal's Model Mapping section specifies.

### Fail-fast validation (statically checkable subset)

`validate()` encodes what the proposal's "MVP Scope" / "Unsupported — Fail
Fast at Declaration Time" sections make checkable from registry metadata:

| Check | Issue code |
|---|---|
| Pipe has no/empty `output_entity_id` | `missing_output_entity` |
| Output entity not registered | `output_entity_not_registered` |
| Output entity not Delta/table-backed (SQL entities, `memory`, `current_view`, other providers) | `output_entity_not_table_backed` |
| SQL-view pipes (`DataPipes.view`, memory-backed) | `sql_view_pipe` |
| External input not registered anywhere | `input_entity_not_registered` |
| External input not representable as an SDP storage read (local-only/non-table providers) | `external_input_not_declarable` |
| Pipe reads its own output entity | `self_referencing_pipe` |
| Two pipes producing one target (append-flow open question) | `duplicate_output_entity` |
| Adapter-tier feature requested on a target without it | `capability_not_supported` |
| Invalid `dataset_type` value | `invalid_dataset_type` |

Deliberately **not** checked in Phase 1, because it requires pipe-body
introspection or runtime interception rather than registry metadata:
side-effecting pipe logic (actions like `collect`/`count`/`write`/
`saveAsTable`/`start` inside dataset functions) and explicit watermark-API
calls (`get_watermark`). These are runtime/Phase-2 concerns — the dry-run
harness and provider write guards (below) are the enforcement points.
`use_watermark=True` pipes are *not* errors: watermarking is a signal
aspect (PR #158) that SDP mode simply never registers.

### Capability gating

Gating compares the pipe's **active engine config block**
(`datapipes.<pipeid>.engine.<engine_name>`) and the output entity's `scd.*`
tags against the target `CapabilitySet`:

- `expectations:` or `refresh_policy:` in the active block on `OSS_SDP`
  fails with an actionable diagnostic (move under `databricks_sdp` or
  drop).
- `scd.*` tags route to AUTO CDC (proposal Phase 5); on a target without
  AUTO CDC they fail rather than silently dropping SCD semantics.
- Config blocks addressed to a *different* engine are declared intent for
  another target and are ignored (the same app config can carry both `sdp`
  and `databricks_sdp` blocks).

### Dataset-type selection

Precedence: output-entity tag `sdp.dataset_type` first, engine config
(`engine.<name>.dataset_type`) second, default `materialized_view`. The
parent proposal's open question — the dataset kind is a property of the
output *dataset* (entity metadata) but engine config hangs off the pipe —
is resolved here only as precedence (entity tag wins); ownership stays open
until Phase 4 (streaming tables) forces it.

## Decision: Bootstrap Ordering

`declare_pipeline()` (and `build_plan()`) must run **after** the
post-registration config overlay defined in
`package_config_architecture.md`. That proposal's model is eager decorator
registration with bootstrap layering config overrides on top afterward
(`apply_config_overrides()`); until that overlay has run, engine keys like
`dataset_type` and tag overrides are not final, and a plan built earlier
would bake in stale metadata.

Concretely:

1. `kindling.initialize(engine="sdp")` — IoC + config + platform services
   (`bootstrap.py::initialize_framework`). In SDP mode the watermark-aspect
   registration step is skipped entirely (SDP owns incrementality — PR
   #158's design makes this a non-event for pipes).
2. App registrations (`register_all()` / package imports) — eager, as
   today.
3. Post-registration config overlay — `package_config_architecture.md`.
4. `kindling.declare_pipeline()` — the engine reads the registries and
   builds/declares the plan.

Two mitigations keep this ordering robust rather than merely documented:
`DataEntityManager.get_entity_definition()` already merges config tag
overrides at *retrieval* time, so entity-tag inputs (including
`sdp.dataset_type`) pick up the overlay automatically whenever the plan is
built; and the engine takes its `engine_config` mapping as an explicit
constructor input, so the bootstrap surface — not the engine — owns
resolving it from the fully-overlaid config. The remaining requirement on
the entry point is simply that step 4 is the last step.

## Decision: Provider Write-Inertness in SDP Mode

The parent proposal requires that "SDP owns persistence": the Delta
provider's write path must be inert or forbidden in SDP mode while reads
stay live, and lists the mechanism as an open question (tag, registry mode
flag, or a distinct provider binding).

**Recommendation: a distinct engine-mode binding.** SDP bootstrap
(`initialize(engine="sdp")`) binds a guard provider in place of the normal
write-capable provider: reads delegate unchanged, and every write-path
method (`write_to_entity`, `append_to_entity`, `merge_to_entity`,
`append_as_stream`, `ensure_entity_table`) raises immediately with a
diagnostic naming the entity and the reason ("SDP owns persistence — a
pipe or extension attempted an imperative write in SDP mode").

Why a binding rather than the alternatives:

- **Per-entity tag** (`read_only: true`) — the precedent is
  `entity_provider_delta.py`'s existing guard (`_is_read_only()` →
  `ReadOnlyEntityError` raised from the write paths), and the guard
  provider should raise in exactly that style. But the tag is per-entity
  data intent ("this entity is externally owned"); SDP-mode inertness is a
  *global execution-mode* property. Overloading the tag would make mode
  state ambient in entity metadata, mutate registrations the user wrote,
  and be forgettable entity-by-entity.
- **Mode flag on the registry/provider** — adds `if sdp_mode:` branches
  inside every write method of every provider; the read_only experience
  shows guard logic already tends to smear. A swap-in guard keeps providers
  mode-unaware.
- **Binding swap** matches how Kindling already composes execution policy:
  the engine is an injected component (`GlobalInjector` bindings decide
  strategy classes), so "which provider personality is bound" is exactly
  the kind of decision bootstrap already makes. It is also fail-fast by
  construction — nothing can write imperatively, including code paths
  Phase-1 static validation cannot see (side-effecting pipe bodies), which
  is what makes deferring body introspection safe.

Implementation lands in Phase 2 alongside the first concrete engine (there
is no SDP bootstrap path to bind against until then).

## Deliberately Deferred to Phase 2+

- **Emission**: any import of `pyspark.pipelines`; translating
  `DatasetDeclaration` → `@dp.materialized_view` / `create_streaming_table`
  + flows (Phase 2 prototype: one bronze source, one silver MV).
- **The local validation harness**: ephemeral spec + `spark-pipelines
  dry-run` (Phase 2, per the parent proposal's bridge section).
- **The guard provider itself** (decision above; mechanism lands with the
  first bootstrap path that can bind it).
- **Pipe-body side-effect detection** (actions, `get_watermark` calls) —
  runtime guard + dry-run territory, not registry metadata.
- **Entity-id → catalog/schema/table mapping** (open question in the
  parent proposal; `DatasetDeclaration.name` is a logical id).
- Expectations/AUTO CDC emission (`kindling_databricks_sdp` adapter,
  Phases 3/5), streaming tables and append flows (Phase 4), and the
  multiple-flows-per-target question (fails fast for now as
  `duplicate_output_entity`).
