# Kindling Core / Runner Split: Declaration Framework vs Execution Engine

**Status:** Proposed — direction agreed in design discussion (2026-07-15);
deliberately NOT scheduled (see Platform Context).
**Created:** 2026-07-15
**Related:** `declarative_pipelines_engine.md` (the SDP engine whose
existence exposed this split), `sdp_engine_phase1_notes.md` (bootstrap
ordering, provider write-inertness), PR #158 (watermark aspect — the first
extraction of runner machinery from ambient core), PRs #160/#162/#165/#166
(the SDP engine phases).

## Motivation: Two Paradigms, One Package

Kindling today is one package with two identities fused together:

1. **A declaration framework** — entities and pipes as decorated
   declarations, registries, tags, config overlay, DI, platform services,
   packaging. The application model.
2. **An execution engine** — the generation executor, read/persist
   strategies, watermarking, SCD merge machinery, the streaming
   orchestration stack. The thing that *runs* the application model.

The SDP work made the fusion visible by building a second execution
engine. Under the runner, kindling's lifecycle is *start → bootstrap →
prep → kindling executes*. Under a declarative engine, the lifecycle
inverts completely: *the engine evaluates kindling*. The definitions file
is evaluated inside the pipeline runner's process; `initialize()`,
registration, and `declare_pipeline()` all run as a guest of the engine's
graph-construction phase. Kindling stops being a program that calls Spark
and becomes a library that Spark calls.

Once there are two engines, the imperative machinery is revealed as what
it always was: **one pluggable execution backend among several**, not
framework core. The watermark-aspect refactor (#158) was the first
concrete admission (an engine that owns incrementality "simply never
registers the aspect"); the SDP-mode write guard and the
`engine_owns_incrementality` gate are the rest of it. This proposal
finishes the thought structurally:

- **`kindling` (core)** — declaration, validation, translation seams,
  config, DI, platform services, packaging. Engine-agnostic.
- **`kindling_runner`** — the imperative engine, packaged as an ordinary
  engine extension (the same `kindling_<name>.engine_extension()`
  convention `kindling_ext_sdp` and `kindling_ext_databricks` already use),
  and bound as the **default** engine when `initialize()` is called with
  no `engine=`.

The coexistence is deliberate and permanent, not transitional. The runner
is the imperative escape hatch the declarative model refuses to provide —
driver-side loops, revision-in-place, custom persistence. Extensions like
`kindling-ext-temporal` illustrate the layering rather than break it: the
temporal *model* (the Event/Condition/Episode ontology and the entity/
process declaration patterns it prescribes) is engine-agnostic and sits
on the core, and even condition evaluation is engine-neutral df->df
transformation over a statically-known DAG; the one genuinely
engine-constrained piece is episode lifecycle revision (keyed
merge/upsert semantics). See Friction #9.

## Platform Context (Why This Is a Plan, Not a Sprint)

The split's *architecture* is settled by the SDP work; its *urgency* is
governed by where kindling actually runs:

- **Synapse**: Spark 3.4-era runtimes, no Spark 4.x path. Runner-only,
  indefinitely.
- **Fabric**: Runtime 2.0 (Public Preview) is Spark 4.1 — but the preview
  documentation **explicitly lists "Lakeflow Spark Declarative Pipelines"
  as an unsupported feature**. Shipping Spark 4.1 does not ship a way to
  run SDP there, and Microsoft's declarative investment on Fabric is
  visibly **Materialized Lake Views** (GA 2026: declared SQL/PySpark
  transformations, inferred dependency DAG, CDF-based incremental
  refresh, constraint-based data quality — roughly the materialized-view
  half of SDP, platform-managed, with no streaming tables and no CDC/SCD
  semantics). Runtime 1.3 (Spark 3.5) enters LTS 2026-10 through 2027-03.
- **Databricks**: Lakeflow SDP is real today; the
  `kindling_ext_databricks` adapter (expectations, AUTO CDC) targets it.
- **Self-managed Spark 4.1+**: OSS SDP works today; the dry-run harness
  validates against it in CI-adjacent tooling.

Conclusion: the runner engine remains the production engine on the
primary platforms for the foreseeable future. The split should proceed
**incrementally and additively** — its near-term value is architectural
hygiene and honest dependency direction, not platform enablement. The
most likely *third* engine extension is a future `kindling_fabric_mlv`
targeting Materialized Lake Views' PySpark authoring surface (currently
preview, full-refresh-only), not OSS SDP on Fabric.

## Target Architecture

```text
kindling (core)                      # the application model
  declarations: DataEntities / DataPipes decorators, registries, metadata
  config: ConfigService, loaders, overlay        DI: GlobalInjector
  signals infra, logging/tracing/session, platform services (4x)
  packaging/deploy: data_apps, job_deployment, notebook_framework
  provider CONTRACTS (ABCs) + provider registry (+ decorator seam)
  graph ANALYSIS: pipe_graph, cache_optimizer
  engine-extension loader: initialize(engine=...), declare_pipeline()

kindling_runner (engine extension: "runner", the default)
  DataPipesExecuter, execution strategies/orchestrator, generation executor
  read/persist strategies, stage processor, watermarking
  persistence machinery: merge/SCD compilers, table DDL/ensure
  streaming stack: orchestrator, query manager, recovery, health, listener
  file-ingestion processor, migration
  concrete provider WRITE personalities (see Friction #3)

kindling_ext_sdp (engine extension: "sdp")            # exists — OSS SDP
kindling_ext_databricks ("databricks_sdp")        # exists — Lakeflow
kindling_fabric_mlv ("fabric_mlv")                # future — Fabric MLVs
```

The runner riding the existing engine-extension seam is the load-bearing
idea: `initialize()` with no `engine=` resolves the `runner` extension,
whose `activate()` does what bootstrap's tail does today (watermark
aspect registration, executor bindings). Core never imports it by name —
the same rule the SDP engines already obey, applied to the incumbent.

## Module Disposition

From a full sweep of `packages/kindling/` (~50 modules; verdicts verified
against module-level import direction):

**Core (27 modules):** `injection`, `signaling`, `spark_session`,
`spark_config`, `config_loaders`, `spark_log`, `spark_log_provider`,
`spark_trace`, `spark_jdbc`, `features`, `common_transforms`,
`platform_provider`, `platform_databricks`, `platform_fabric`,
`platform_synapse`, `platform_standalone`, `notebook_framework`,
`app_files`, `data_apps`, `job_deployment`, `pip_manager`,
`entity_resolution`, `entity_provider` (ABCs + capability predicates),
`entity_provider_registry`, `pipe_graph` (pure DAG analysis — build,
cycles, topo-sort, generations; never executes), `cache_optimizer`,
`test_framework`.

**Runner (17 modules):** `generation_executor`, `execution_strategy`,
`execution_orchestrator`, `simple_read_persist_strategy`,
`simple_stage_processor`, `pipe_streaming`, `watermarking`,
`streaming_orchestrator`, `streaming_query_manager`,
`streaming_recovery_manager`, `streaming_health_monitor`,
`streaming_listener`, `file_ingestion` (registry portion could stay
core), `migration`, plus the concrete providers' write/persistence sides:
`entity_provider_memory`, `entity_provider_sql`,
`entity_provider_eventhub`, `entity_provider_current_view` (SCD2-read
companion — runner semantics).

**Mixed — the four files that need surgery:** `data_pipes.py`,
`data_entities.py`, `entity_provider_delta.py`, `bootstrap.py`, plus
`__init__.py`'s eager import list. Detailed below.

Dependency direction is already remarkably clean: **no core module
imports a runner module at module scope** except through the two
data_pipes-mediated cases below. Runner modules import core freely
(correct direction). The graph is separable.

## Semantic Friction

The split is not a file move; each item below is a place where the two
paradigms share vocabulary or code and a decision is required.

### 1. `data_pipes.py`: registry and executer share a file

`PipeMetadata`, the `DataPipes` decorators, and `DataPipesRegistry`/
`DataPipesManager` are core; `DataPipesExecuter` (~lines 288–513) is the
runner. `pipe_graph` (core) imports this file for the registry, dragging
the executer along. **Resolution:** extract `DataPipesExecuter` to
`kindling_runner`; the `DataPipesExecution` ABC (the contract) stays
core. Straightforward, but it is the seam every downstream import
crosses — shims required (see #7).

### 2. `PipeMetadata.use_watermark`: engine vocabulary in the declaration

The one field on `PipeMetadata` that is a runner concept, not a
declaration (SDP mode's answer is "watermarking is an aspect that never
registers"). A declaration should state *what* ("this input is consumed
incrementally"), not *how* ("via the runner's watermark table").
**Resolution options:** (a) leave it, documented as a runner-engine hint
other engines ignore — cheap, honest, slightly impure; (b) generalize to
a declaration (`incremental: true` or a tag) that each engine compiles
its own way — the runner maps it to watermarking, SDP already gets
incrementality from checkpoints/refresh. (b) is the better end state; (a)
is an acceptable Phase-1 posture. Runner-only *run options*
(`no_watermark`, `use_dag`, parallelism) already live on executer method
parameters, correctly outside the declaration.

### 3. Entity providers straddle the paradigm boundary (the deep one)

Provider *contracts* (ABCs, capability predicates) are core. But concrete
providers fuse three roles per class:

- **Read paths** — needed by *every* engine (SDP-mode reads delegate
  through the guard to these; external reads, fixtures, current-view
  queries). Engine-neutral.
- **Write/append/merge paths** — "the engine owns persistence." Under
  SDP these are guarded into inertness; under the runner they ARE the
  persistence implementation.
- **The SCD merge compiler** — `_execute_scd2_merge` and the merge
  strategies inside `entity_provider_delta.py` are precisely "the runner
  engine's compilation of the SCD2 declared flow" (the AUTO CDC mapping
  in `kindling_ext_databricks` is the same declaration compiled by a
  different engine). It is engine code that happens to live in a
  provider.

**Resolution:** keep concrete providers in core but extract the
persistence machinery: merge strategies + SCD compiler + table-DDL/ensure
logic move to `kindling_runner` as persistence strategies the provider
delegates to via a binding the runner extension installs. A core-only
install then has providers whose write paths fail fast with "no
persistence engine installed" — which is the write guard's semantics,
derived structurally instead of by wrapper. This is the largest single
piece of surgery in the split; it is also the most honest, because it
makes "SDP owns persistence" and "the runner owns persistence" the same
kind of statement.

*(Rejected alternative: move whole concrete providers to the runner.
Breaks SDP mode, which reads through them today, and would force every
future engine to depend on the runner package for I/O.)*

### 4. SCD2 knowledge baked into declaration-time code

Two leaks in `data_entities.py` (otherwise core): the
`_register_scd2_current_companion` side effect auto-registers the
current-view companion entity at registration time — runner machinery
expressed as a declaration-time behavior (SDP's answer is "the current
view is an ordinary declared view," tracked separately as a follow-up) —
and `scd_config_from_tags`/validation, which is genuinely a declaration
concern (both engines consume it: the runner's merge compiler and the
AUTO CDC mapping) and stays core. **Resolution:** SCD *declaration*
(tags, config, validation) is core vocabulary; the companion
registration becomes engine behavior (runner activation) or is replaced
by the declared-view design when that lands.

### 5. `bootstrap.py`: mostly core, with a runner tail

The 1,700-line file is overwhelmingly core init (config load/merge,
platform detection/services, dependency install, package imports,
staging paths). The runner wiring is small and already lazy and gated:
watermark-aspect registration (function-local import, skipped for
standalone and when `engine_owns_incrementality`). **Resolution:** the
tail moves into `kindling_runner`'s `engine_extension().activate()`; the
gate condition dissolves — engines that own incrementality simply have a
different `activate()`. No decomposition of the rest of bootstrap is
required for the split (its internal untidiness is a separate concern).

### 6. Signal vocabulary

`signaling.py` is generic infrastructure (verified: no runner signal
names in the module). But the signal *taxonomy* — `read.resolve_read`,
`persist.after_persist`, `persist.persist_failed`, streaming lifecycle
signals — is runner-published contract that extensions (watermarking
itself, kindling-ext-temporal, monitoring) subscribe to. **Resolution:**
infra stays core; signal namespaces get documented ownership (runner
signals are the runner's public API, versioned with it). Cross-engine
consumers must not assume runner signals exist in SDP mode — already
true today, now stated.

### 7. `kindling/__init__.py` eager imports + backward compatibility

The single biggest mechanical blocker: `import kindling` eagerly imports
every module including the entire runner and all concrete providers, so
a core-only install is impossible regardless of file layout. And every
existing app/notebook imports runner classes via `kindling.*` paths.
**Resolution:** (Phase 1) prune the eager list behind module-level
`__getattr__` (PEP 562) lazy imports — behavior-preserving, breaks
nothing, makes the coupling opt-in; (Phase 2) when files move, keep
`kindling.generation_executor` et al. as thin re-export shims delegating
to `kindling_runner.*` with a deprecation note, for at least one minor
release cycle. KDA packaging and platform wheels follow the same shim
rule.

### 8. Distribution and versioning

Today one wheel (`spark-kindling`) ships everything, and cloud runtimes
pin against it. **Resolution:** `spark-kindling` (core) +
`spark-kindling-runner`, with core declaring the runner as a default
extra (`spark-kindling[runner]`, included by the platform wheels) so
existing installs are unchanged. The engine-extension loader gives a
precise, actionable error when `engine="runner"` (implicit default) is
requested but the package is absent. Version coupling: runner pins a
compatible core range, exactly as `kindling_ext_sdp` does now. CI: the
coverage flags, test tree split (the sweep produced the full
test-to-package mapping), and KDA packaging tests all follow the
packages.

### 9. What does NOT move

`test_framework`, `notebook_framework`, `data_apps`, `job_deployment`,
platform services, `pipe_graph`/`cache_optimizer` (analysis, not
scheduling), and the provider registry (with its execution-mode
decorator seam) are core, verified by import direction.

**`kindling-ext-temporal` is NOT runner-only — it is split-shaped itself.**
The temporal extension's purpose is a consistent, ontology-driven pattern
(`event_condition_episode_ontology.md`, deliberately
implementation-agnostic) for *what* to declare (Event/Condition/Episode
entities) and *how* (the processes supporting temporal segmentation and
analysis). That declaration layer depends only on the core — and so, in
principle, does condition *evaluation*: Conditions are df->df
transformations over a dependency graph the ontology requires to be
acyclic and statically known (its boundedness argument), which is
precisely the shape any engine executes; generation ordering is native
DAG ordering, which declarative engines infer for free and the runner
implements via `generation_executor`. The genuinely engine-constrained
piece is narrower: **episode lifecycle revision** — stable identity
across revision, open episodes closed later, provisional closes
superseded — which needs keyed merge/upsert semantics (the runner's
merge machinery today; plausibly AUTO CDC on the Databricks adapter,
which is the same keyed-sequenced-upsert shape; weakest fit on plain OSS
SDP, where MV recompute makes processing-time-dependent expiration
awkward). The driver-side evaluation loop the SDP proposal's "What Stays
Runner-Only" section cites is a property of temporal's *first executor
implementation*, not of the model. Post-split, the extension depends on
core for its model and on `kindling_runner` for its current executor; an
engine that can host the episode-revision semantics could take the
declarations unchanged. Conflating the temporal model
with its executor would repeat, at extension scale, the exact fusion
this proposal unwinds in kindling itself.

## Migration Phases (each additive, each shippable alone)

1. **Phase 0 — this document**, plus the paradigm note in the SDP
   proposal. No code.
2. **Phase 1 — intra-package extraction, no moves:** `DataPipesExecuter`
   out of `data_pipes.py`; persistence machinery (merge strategies, SCD
   compiler, DDL/ensure) out of `entity_provider_delta.py` into runner
   modules; `__init__.py` eager imports → PEP 562 lazy; introduce the
   `runner` engine extension (`activate()` = watermark aspect + executor
   bindings) and make it the `initialize()` default. Everything still
   ships in one wheel; imports unchanged.
3. **Phase 2 — package split:** move runner modules to
   `packages/kindling_runner/`, leave `kindling.*` shims, move tests per
   the mapping, split CI coverage targets.
4. **Phase 3 — distribution split:** separate wheels, `[runner]` extra
   default in platform wheels, deprecation window on shims; the temporal
   extension pins core for its ontology/declaration layer and adds
   `kindling_runner` only for its current execution strategy (see
   Friction #9).

## Acceptance Criteria

- [ ] `import kindling` in a runner-less environment succeeds and
      supports declare/validate/translate (SDP mode fully functional).
- [ ] `initialize()` with no `engine=` behaves byte-for-byte as today,
      resolved through the `runner` extension.
- [ ] No core module imports a runner module at module scope (enforced
      by a test, like the SDP no-`pyspark.pipelines`-import invariant).
- [ ] Every pre-split `kindling.*` import path still works through
      Phase 2 shims, with deprecation warnings.
- [ ] Dual-engine parity criterion (from the SDP proposal) unchanged.

## Open Questions

- **`use_watermark` end state** — hint field vs generalized incremental
  declaration (Friction #2, option b): decide before Phase 2 freezes the
  metadata surface.
- **Provider persistence-strategy seam shape** — per-provider binding vs
  a single persistence service the providers consult; prototype during
  Phase 1's delta extraction.
- **`file_ingestion` registry** — peel the small declaration part into
  core, or accept the whole module as runner?
- **Current-view companion** — this split's Friction #4 vs the standing
  "current-view-as-declared-view" follow-up; one design should resolve
  both.
- **`kindling_fabric_mlv` timing** — MLV PySpark authoring is preview and
  full-refresh-only; revisit when it GAs or when incremental refresh
  reaches the PySpark surface.
