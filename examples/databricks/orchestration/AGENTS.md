# Kindling orchestration and Databricks-lowering instructions

Use this file when building or changing the machinery that turns a Kindling
application into a runnable graph or Databricks Lakeflow pipeline. This folder
contains **framework/platform orchestration**, not business transformations.
Application code declares entities and pipes; orchestration derives, validates,
schedules, or lowers that declared graph.

## The orchestration layers

~~~text
Kindling declarations
entities + pipes + tags + resolved config
             |
             +--> normal runner
             |      graph plan -> read -> transform -> persist -> signals/watermarks
             |
             +--> declaration engine
                    validated DeclarationPlan -> native platform declarations
                                                  |
                                                  v
                                    Databricks Lakeflow executes the graph
~~~

Keep three concerns distinct:

| Layer | Owns | Must not own |
| --- | --- | --- |
| Core graph orchestration | DAG derivation, execution order, runner reads/writes, retries, signals, watermarks | Databricks-native declaration syntax or business transforms |
| Engine/adapter lowering | Capability validation and translation of a declared graph to an engine-native graph | A second user-authored DAG, application-level writes, or hidden business semantics |
| Deployment orchestration | Bundles, pipeline/job configuration, artifacts, identity, target selection, lifecycle operations | Logical entity/pipe dependencies or persistence semantics |

## Core contract

The entity and pipe registries are the sole source of graph truth. A pipe's
input entity IDs and output entity ID define dependencies; do not introduce a
parallel dependency list, orchestration notebook, or generated DAG that can
drift from the registrations.

A normal runner derives a topological execution plan. Independent pipes may run
in parallel; dependent pipes run after their producers. Concurrency, timeout,
retry/error strategy, streaming options, and checkpoints are execution
configuration. Application pipes return DataFrames; the runner owns provider
persistence and lifecycle signals.

## Declarative engine contract

An engine extension converts registered metadata into a pure, validated
DeclarationPlan before it emits anything native:

~~~text
registries + post-registration config
             ↓
validate selected pipe scope
  - every input/output entity exists
  - every output is representable by the target
  - no duplicate output producer, self-reference, or unsupported graph shape
  - requested capabilities are supported
             ↓
classify inputs
  internal = produced by another selected pipe
  external = read from target storage
             ↓
resolve dataset type + engine configuration
             ↓
emit native target declarations
~~~

Validation must be fail-fast before native emission, but return all actionable
issues in one attempt. Do not defer a predictable unsupported declaration to a
cluster failure.

The lowering is a compilation boundary. It must preserve the Kindling semantic
contract, make unavoidable platform divergence explicit, and never silently
drop a tag or engine option.

## Databricks SDP/Lakeflow adapter

"kindling.initialize(engine='databricks_sdp')" selects the Databricks engine
extension. It reuses the shared SDP plan builder, then emits Lakeflow-compatible
pipeline declarations. The required entry-point order is:

~~~python
kindling.initialize(engine="databricks_sdp")
register_all()                 # declarations only
kindling.declare_pipeline()    # final entry-point action
~~~

Do not call the normal runner from this entry point. In declarative mode,
Lakeflow owns persistence and incrementality; providers are write-inert and the
runner watermark aspect is not active.

Application pipes must not directly use old DLT decorators, Lakeflow decorators,
or native pipeline control APIs. The adapter owns that mapping so it is
capability-gated, reusable, and testable.

## Native mapping rules

| Kindling intent | Databricks lowering |
| --- | --- |
| Ordinary table-backed pipe | Materialized-view declaration, normally the default dataset type |
| Entity schema/comment/properties/partitioning/clustering | Corresponding native dataset metadata, where the target supports it |
| Per-pipe "datapipes.<pipe-id>.engine.databricks_sdp" expectations | Native warn/drop/fail expectation decorators |
| SCD2 change-feed or snapshot declaration | Source view + streaming-table target + AUTO CDC flow |
| Non-trivial semantic graph, for example temporal chain | Adapter-specific lowerer may emit several native datasets/flows |
| Unsupported provider, dataset type, or feature | Declaration validation error; never silent fallback |

For SCD AUTO CDC, map the declared business keys, sequence column, source kind,
delete predicate, and tracked attributes explicitly. Document the differences
from runner SCD2, including native history columns and any snapshot or
out-of-order behavior; shared declaration intent does not guarantee byte-for-
byte physical parity.

A runner-side "persist.before_persist" signal is not the portable Lakeflow
quality gate. Lower declared engine expectations or another native capability
when the guarantee must be enforced during a Lakeflow update.

## Building a new lowering capability

1. Define an engine-neutral tag/configuration contract and registration-time
   validation before writing native Databricks API calls.
2. Add a capability declaration so unsupported engines reject the feature.
3. Extend DeclarationPlan validation for the needed graph shape and metadata.
4. Implement native emission in the relevant adapter; resolve platform APIs
   lazily so merely importing Kindling remains portable.
5. Keep deployment inputs separate: bundle/pipeline config supplies wheel,
   source, identity, target catalog/schema, pipeline ID, and environment—not a
   second logical graph.
6. Test the pure plan, all validation failures, emitted native calls, and a
   target pipeline behavior.

Never bake a Databricks-only concept into the core entity/pipe model unless it
has an engine-neutral semantic contract. If the concept is only a platform
execution option, keep it in the adapter engine configuration.

## Operational and test rules

- Declaration-time code must not read data, write data, start a stream, install
  packages, or call remote control-plane APIs.
- Resolve configuration after registrations and their overlays, before building
  the plan; engine settings must reflect active metadata.
- Keep native names deterministic and obey the adapter's name-normalization
  rules. Do not use logical dotted entity IDs directly where a pipeline target
  requires a normalized name.
- Test internal/external input classification, duplicate outputs, unsupported
  providers, feature gating, and emitted metadata with unit tests.
- Test native integrations against a real target pipeline before claiming
  parity. Record known runner-versus-platform divergences in the adapter
  contract.
