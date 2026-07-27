# Kindling — architectural context for Genie Code

Kindling is not a library of Spark helper functions. It is a declarative data
application framework: the application declares the data it owns, the
transformations that relate that data, and the environment binding required to
run it. The framework registers those declarations into a graph and an execution
engine runs the graph.

This file is the authoritative high-level context for Kindling. Detailed files
in child folders refine the rules for a particular concern; they do not replace
the model below.

## The one-sentence model

**Entities are durable data contracts; pipes are the declared transformations
between those contracts; tags express semantics; configuration binds the same
model to an environment; signals extend the lifecycle without becoming the
model.**

~~~text
                         logical application
 ┌──────────────────────────────────────────────────────────────────┐
 │  entities  ──>  pipes  ──>  entities                              │
 │     │              │             │                                │
 │   contracts      transforms     contracts                          │
 │   schema/keys    DataFrame      schema/keys                        │
 │   semantics      returned       persistence semantics              │
 └──────────────────────────────────────────────────────────────────┘
                │                              │
                └──── tags + configuration ────┘
                               │
                               v
                    runner or declarative engine
                               │
                               v
                  Delta tables, views, streams, checkpoints
~~~

The important design separation is:

| Logical model — stable across environments | Environment binding — differs by deployment |
| --- | --- |
| entity ID, schema, business keys, pipe graph, write mode, SCD and derived semantics | catalog, schema, paths, checkpoints, provider access mode, secret scope, runtime engine |
| declaration tags | YAML/bootstrap configuration and environment tag overrides |

A correct Kindling change begins by deciding which side of that boundary it
belongs to. Do not encode environmental choices in a pipe, and do not use
configuration to conceal a change in business meaning.

## The graph is the application

A Kindling application is a directed graph, not a sequence of hand-written
Spark writes.

- An **entity** is a named dataset contract. It states what the dataset means,
  its schema and keys, how its rows persist, and which provider supplies it.
  The physical table/path is a deployment concern unless it is intrinsic to an
  external source.
- A **pipe** is a graph edge. It declares input entity IDs and one output entity
  ID, receives DataFrames for the inputs, and returns one DataFrame for the
  framework to persist to the output.
- The dependency graph allows a runner to plan ordering, caching,
  incremental reads, retries, and failure handling. A pipe must not re-create
  this orchestration with nested reads, writes, or driver loops.
- Entity IDs are stable logical names, normally dotted by domain/layer such as
  "bronze.orders", "silver.orders", and "reporting.daily_sales". They are not
  merely aliases for physical Delta tables.
- Layers such as bronze, silver, gold, source, or reporting describe a useful
  organization, but the actual contract is the entity's schema, keys, tags,
  provider, and producer pipe.

Read a new Kindling application vertically:

~~~text
business source/fact
        ↓
source entity contract
        ↓
pipe transformation and its output contract
        ↓
persistent table/view/stream in this environment
        ↓
downstream entity contracts and pipes
~~~

Read it horizontally when answering an operational question:

~~~text
one entity's declared tags
        + environment entity_tags override
        + optional run-scoped override
        = active entity definition used by the provider/executor
~~~

## Registration and bootstrap lifecycle

Decorators make Kindling declarations. They register metadata into framework
registries backed by dependency injection; they do not execute data work.

The required lifecycle is:

~~~text
1. initialize Kindling
   - load bootstrap/YAML configuration
   - select platform services/providers/engine
   - create the dependency-injection container and registries

2. import entity, pipe, and signal modules
   - decorators register metadata and handlers

3. register/declare the application graph
   - normal runner: invoke the selected run path later
   - Lakeflow: declare the pipeline as the final entry-point action

4. execute
   - engine reads entities, calls pipes, persists outputs, and emits signals
~~~

This is why declaration modules must only declare. Never import an entity or
pipe module before initialization, and never trigger Spark actions, streaming
queries, table writes, backfills, package installation, or network side effects
at import time. The conventional "register_all()" function exists to make
registration explicit and deterministic.

## Entity contracts and persistence semantics

An entity must answer five questions before a pipe is written:

1. **Identity** — what is its stable entity ID, and what business key identifies
   a row?
2. **Shape** — what explicit Spark schema is promised to producers and
   consumers?
3. **Ownership** — is the data source-owned/read-only, managed by Kindling, a
   derived result, or a SQL view?
4. **Provider** — is it Delta, memory, CSV, Event Hub, Parquet, or another
   registered provider?
5. **Lifecycle** — are rows appended, merged as current state, inserted only,
   replaced as a derived result, or versioned as SCD Type 2?

"merge_columns" are business keys. They define the match identity for merge,
insert-only, and SCD2 behavior. They are not a performance hint and must not
be chosen only because a field happens to be available in a source.

The core write choices are:

| Contract | Meaning |
| --- | --- |
| append | Add new immutable facts/log rows. Preserve event ID and event time to make replay meaningful. |
| merge | Maintain a keyed current-state dataset. The declared key controls upsert identity. |
| insert | Insert only records whose business key is absent; replayed keys remain untouched. |
| derived | Treat the dataset as a function of declared inputs; it has replacement, not accumulating, semantics. |
| SCD Type 2 | Preserve historical versions of a keyed state when tracked attributes change. |
| read-only | A source-owned/external entity that Kindling may read but must not persist. |
| SQL entity | A permanent read-only catalog view managed as view DDL, not as a pipe output. |

SCD2 is a contract with explicit business keys, tracked attributes, source
semantics, sequence, and deletion policy. The framework owns temporal columns
and the current-row companion. Do not hand-code opening/closing history rows in
an ordinary transform when the entity declares SCD2.

A derived entity and an SCD entity are intentionally different: a derived
dataset is recomputable from inputs; SCD history is state whose lifecycle
preserves change history.

## Tags, helpers, configuration, and overrides

Tags are the canonical semantic surface. Engines, validators, and tooling read
tags, not the spelling of a decorator/helper used to create them.

~~~text
helper defaults
      ↓
explicit tags on the entity or pipe declaration
      ↓
top-level YAML entity_tags for this environment
      ↓
run-scoped entity_tags passed to one execution
      ↓
active metadata seen by the provider/executor
~~~

Helpers such as a derived-entity, insert-only-entity, view-pipe, or temporal
declaration are useful names for common shapes. Their job is to supply default
tags; explicit declaration tags win over helper defaults. If a new capability
cannot be stated through a validated tag vocabulary, it is not yet a complete
declarative capability.

Use tags for durable meaning: provider type, write mode, SCD contract, derived
kind, or pipe type. Use configuration for environmental execution choices:
catalog/schema/path, checkpoints, secret scope, platform feature settings,
engine-specific options, and per-environment provider overrides.

Top-level YAML "entity_tags" is the preferred way to map a logical entity to an
environment-specific table name, path, or provider setting. It is not a reason
to make separate source declarations for development and production.

## What happens when a normal pipe runs

A normal batch execution follows this conceptual lifecycle:

~~~text
resolve pipe inputs
   ↓
read driving input (optionally incrementally) + read reference inputs
   ↓
read.after_read signal handlers
   ↓
call pipe function with named input DataFrames
   ↓
persist.before_persist signal handlers
   ↓
provider persists the output according to the output entity contract
   ↓
persist.after_persist / watermark bookkeeping
~~~

The first pipe input is the **driving input**. When a pipe uses watermarks, only
that input is read incrementally; later inputs are full reference reads. Put the
incremental/event source first. Define event time, deterministic ordering,
lateness, replay behavior, and idempotent write semantics before enabling
incrementality.

The pipe function receives keyword DataFrame arguments named from input entity
IDs with dots changed to underscores. It returns a DataFrame. Do not put
"save", "writeStream", direct Delta merge, uncontrolled "collect", or other
execution orchestration into a normal pipe; that bypasses provider semantics,
signals, watermark bookkeeping, and engine portability.

A pipe has one output entity. Split unrelated outputs or use an explicit
orchestration design; do not hide multiple dataset writes inside one transform.

## Signals and quality are lifecycle extensions

Signals are deliberately outside the entity/pipe graph. They can enforce or
observe behavior around an already-declared lifecycle stage.

- A synchronous "read.after_read" handler can inspect or replace an input
  DataFrame before a pipe receives it.
- A synchronous "persist.before_persist" handler can inspect or replace a pipe
  output, or raise to block the normal write. This is the correct execution
  point for a validation gate.
- "persist.after_persist" and asynchronous handlers are for telemetry,
  notification, and bookkeeping. They cannot reliably veto an already-completed
  write.
- Ordering, error policy, scope, expectation suites, quarantine policy, and
  metrics must be explicit and tested.

Great Expectations or any other quality system should be treated as a
declarative, versioned quality contract executed through an appropriate
synchronous signal stage. It is not currently a first-class Kindling feature to
assume exists, and it must not become an opaque after-the-fact receiver.

## Schema evolution, migrations, and backfills

Entity schema is a contract. A field-list change may be a storage migration and
a data-correction problem, not merely a code edit.

Additive nullable fields and some safe type widening can be automated under the
chosen drift policy. Renames, drops, arbitrary type changes, key changes,
partition/cluster changes, non-null additions, semantic defaults, and historical
backfills require an explicit rollout.

The migration capability is not a general business-data migration system: it
does not infer renames, invent a default for old rows, or automatically backfill
history. Use a controlled, idempotent pipe or reviewed SQL/data operation for
that work. State its input scope, derivation, rerun behavior, validation,
containment, and completion criterion. Test in representative non-production
data before applying it to production.

## Temporal model: domain semantics on the graph

Temporal processing does not change the Kindling primitives. It supplies a
domain model that entities and pipes carry:

- An **event** is an immutable time-stamped fact.
- A **condition** is an evaluated state/predicate over a declared business
  scope.
- An **episode** is a derived open or bounded interval, with stable identity,
  scope, boundaries, status, and provenance.

Keep those concepts separate from the processor that produces them.

There are two distinct episode models:

| Model | The episode exists because |
| --- | --- |
| event-delimited | Start and end events are first-class business delimiters. A start opens an episode and a matching end closes it. |
| condition-duration | A condition is true continuously for a specified period, subject to a continuity/gap/end rule. |

"Condition true for 20 minutes" is condition-duration logic; it is not the same
as an event-delimited episode. For event-delimited episodes, declare matching,
correlation, unmatched-event, duplicate, ordering, and late-event policy.

Scope belongs in the condition/episode contract: a condition is evaluated
independently per machine, asset, tenant, patient, account, or another business
key. Every relevant event carries that scope key and every output retains it.
Do not create one entity per machine merely because a threshold differs. Model
parameterized business values as effective-dated data keyed by scope, then join
the applicable parameters into condition evaluation. Separate entities only
when their contract, ownership, security, retention, or lifecycle differs.

Episode lifecycle often needs keyed merge semantics because an open episode may
close later or be revised by late data. Decide whether corrections recompute,
supersede, or preserve prior episode versions before implementing it.

## Orchestration, lowering, and Databricks platform concepts

Kindling has three distinct orchestration concerns. Keep them separate rather
than building a single controller that knows business logic, Spark writes, and
Databricks APIs.

| Concern | Owner | Responsibility |
| --- | --- | --- |
| Graph orchestration | Kindling runner | Derive dependency order from pipes, read inputs, run transforms, persist outputs, manage watermarks, and report results. |
| Declarative lowering | Engine extension | Convert the registered, engine-neutral graph into a validated target-specific declaration plan and emit native pipeline objects. |
| Deployment orchestration | Databricks Asset Bundles/jobs/pipeline config | Deliver code and config, select a target/environment, grant identity, and start/operate the platform workload. |

### Declarative lowering: Kindling graph to SDP/Lakeflow graph

An SDP engine is not a different application model and it is not a runner that
calls ordinary pipe persistence. It is a **lowerer**:

~~~text
Kindling entity + pipe registrations
              ↓
validated DeclarationPlan
  - classify inputs as internal or external
  - resolve output dataset type and active engine configuration
  - reject unsupported providers, duplicate outputs, cycles, or features
              ↓
engine-native declarations
  - OSS Spark pyspark.pipelines objects
  - Databricks Lakeflow objects and flows
              ↓
the platform builds and runs the dataflow graph
~~~

The plan is built from registries, not from a second pipeline definition.
Within the selected pipeline scope, an input produced by another selected pipe
is **internal**; an input with no in-pipeline producer is an **external**
storage read. This is how the lowerer preserves the Kindling graph while
allowing Lakeflow to infer dependencies.

Select the Databricks adapter with "engine='databricks_sdp'". The entry point
initializes Kindling, registers declarations, applies any post-registration
configuration overlay, and calls "kindling.declare_pipeline()" as its last
action. The adapter reuses the shared SDP declaration plan and augments it with
Lakeflow-only capabilities. Application pipes remain ordinary DataFrame
transforms; they must not directly decorate themselves as old DLT tables or call
native pipeline APIs. Native API names belong inside the adapter lowering layer,
where they can be capability-gated and tested once.

### What the Databricks adapter lowers to

The default SDP dataset type is a materialized view, so a normal table-backed
pipe lowers to a native materialized-view declaration plus its applicable
schema, comment, table properties, partitioning, clustering, and transform.

Adapter-only features stay explicit and capability-gated: a per-pipe
"datapipes.<pipe-id>.engine.databricks_sdp" block can lower expectations to
Lakeflow policies; an SCD2 flow can lower to a source view, streaming-table
target, and AUTO CDC flow; and a complex semantic feature such as a temporal
chain can lower to several native datasets/flows. In every case, Kindling tags
and config are the source of intent and the adapter owns native wiring.

This is where to build materialized-view, streaming-table, expectation, AUTO
CDC, or successor-to-DLT integrations, keeping application code independent of
the Databricks API spelling.

### Important engine-boundary consequences

In SDP/Lakeflow mode, the platform owns persistence and incrementality. The
extension makes providers write-inert and omits the runner watermark aspect.
Use adapter-supported native features, such as engine expectations, for
guarantees inside a Lakeflow update; a regular provider write or runner-side
persist signal is not a portable control point. Runner SCD2 and Lakeflow AUTO
CDC can also have different native history semantics, which the adapter must
document and validate rather than conceal.

### How to build a new platform integration

Build from the core outward: define the engine-neutral semantic tag/config
contract and its registration validation; declare capability support; validate
the graph and required metadata in the declaration plan; then lower the plan in
an engine extension/adapter. Keep DAB/pipeline deployment separate: it supplies
libraries, identity, target catalog/schema, pipeline ID, and environment
settings, never a second logical graph. Test the plan, emitted declarations,
and a target pipeline behavior.

## How to make a safe Kindling change

Start from the business contract and work outward:

~~~text
1. Define the source/fact and the scope/ownership boundary.
2. Define the entity ID, explicit schema, business key, and persistence model.
3. Define the pipe inputs, deterministic transformation, and one output.
4. Put durable semantics in tags.
5. Bind catalogs, paths, checkpoints, and secrets through configuration.
6. Add quality, replay, late-data, migration, and operational behavior.
7. Test the declaration contract and the relevant execution path.
~~~

Ask for a decision only when it changes correctness, data ownership, security,
retention, or an irreversible behavior. Otherwise make the smallest
implementation consistent with the existing logical graph.

## Scoped guidance

The nearest instruction file carries exact rules for that folder:

- "config/": YAML keys, environment overlays, Unity Catalog, storage paths,
  checkpoints, and configuration precedence.
- "entities/": decorator arguments, provider tags, write modes, SCD2 fields,
  read-only and SQL entities, and migration details.
- "pipes/": pipe signatures, multi-input behavior, watermarks, views, and
  validation placement.
- "signals/": handler signatures, lifecycle stages, priority/error policy, and
  quality-engine integration.
- "temporal/": event/condition/episode contracts, scope, parameter values,
  event matching, duration rules, late data, and temporal testing.

Those files deepen this model. They must not contradict it.
