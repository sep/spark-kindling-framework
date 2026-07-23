# First-Class Data Validation with a Great Expectations Adapter

**Status:** Proposed
**Created:** 2026-07-22
**Related:** [Data Pipes](../guide/data_pipes.md), [Data Entities](../guide/data_entities.md), [Signal Quick Reference](../contributing/signal_quick_reference.md), [obsolete pre/post-transform analysis](obsolete/pre_post_transform_analysis.md)

## Summary

Kindling should add a first-class, provider-neutral data validation capability
and implement Great Expectations (GX) as an optional adapter.

The proposed separation is:

```text
Kindling validation declaration
        │
        ▼
Kindling validation runner
        │
        ├── native Spark/schema validators
        ├── Great Expectations adapter
        └── future validators
        │
        ▼
Validation result entity + signals
```

Validation declarations should be explicit and discoverable. The existing
signal system should provide the execution seam for batch DataFrame validation,
but arbitrary signal handlers should not be the user-facing validation model.

## Motivation

Kindling already provides the lifecycle needed to validate data as it moves
through a pipeline:

```text
read input → transform DataFrame → validate output → persist output
```

The current framework has basic entity validation through the CLI and has
DataFrame-aware signal interception in the batch read/persist strategy, but it
does not yet provide a declarative, runtime data-quality contract for pipes.

Great Expectations is a useful implementation partner because it provides
expectation suites, validation results, runtime parameters, checkpoints, and
human-readable Data Docs. GX Core currently supports Spark DataFrames through
Data Sources, Data Assets, Batch Definitions, Validation Definitions, and
Checkpoints. See the [GX Core overview](https://docs.greatexpectations.io/docs/core/introduction/gx_overview/)
and [Spark DataFrame documentation](https://docs.greatexpectations.io/docs/core/connect_to_data/dataframes/).

Kindling should provide the lifecycle and platform integration rather than
making the core framework depend directly on GX.

## Current Kindling capabilities

The current batch path already supports two relevant DataFrame interception
points:

- `read.after_read` can replace an input DataFrame before the pipe function
  receives it.
- `persist.before_persist` can replace the output DataFrame before it is
  written.
- Synchronous `DataSignals` handlers can raise to abort execution.
- Synchronous handlers are ordered by priority and may be scoped to a pipe.
- Asynchronous handlers are observational; they cannot gate execution.

The behavior is implemented by [`DataSignals`](/workspaces/kindling/packages/kindling/signaling.py:397)
and [`SimpleReadPersistStrategy`](/workspaces/kindling/packages/kindling/simple_read_persist_strategy.py:45).

This means a validation handler can already act as a write gate:

```text
persist.before_persist(df)
        │
        ├── validation passes → return df → provider writes
        └── validation fails  → raise     → provider is not called
```

The existing `datapipes.before_pipe` and `datapipes.after_pipe` signals are
execution-lifecycle signals and do not carry DataFrames. They are useful for
metrics and orchestration, but are not the primary DataFrame validation seam.

## Goals

- Declare validation contracts explicitly on entities, pipes, or both.
- Support input, output, and post-persist validation stages.
- Allow validation to fail, warn, quarantine, or continue according to policy.
- Keep the core validation contract independent of Great Expectations.
- Integrate GX without adding it to the core Kindling dependency set.
- Persist normalized validation results in a Kindling-managed entity.
- Include pipe, entity, run, watermark, and batch identity in results.
- Emit validation lifecycle signals for observability and external actions.
- Preserve Kindling's batch, provider, watermark, tracing, and deployment model.

## Non-goals

- Reimplementing the full Great Expectations expectation library.
- Making every validation a Spark action before a transform begins.
- Treating signals as an unstructured replacement for validation declarations.
- Making GX the only validation provider.
- Solving streaming validation in the first implementation phase.
- Automatically joining arbitrary business parameter tables into expectations.

## Proposed validation model

Kindling should introduce a provider-neutral validation declaration. The exact
public API is intentionally left open, but it should express at least:

```text
validation_id
target                  input, output, or entity
pipe_id / entity_id      where the validation applies
stage                   pre_transform, post_transform, post_persist
provider                native, great_expectations, or another adapter
suite_or_contract       provider-specific validation reference
failure_policy          fail, warn, quarantine, or continue
severity                critical, warning, or info
batch_identity          run, watermark, version, or time window
```

An illustrative declaration could look like:

```python
@DataValidations.definition(
    validationid="orders.silver.output_quality",
    pipe_id="orders.bronze_to_silver",
    target="output",
    provider="great_expectations",
    suite="orders.silver",
    stage="post_transform",
    failure_policy="fail",
    severity="critical",
)
def orders_output_quality():
    ...
```

This is illustrative rather than a committed API. The important design point
is that the declaration describes *where, when, and what policy applies*;
Great Expectations describes *how the assertions are evaluated*.

A validation may also be associated with an entity independently of a pipe for
scheduled or post-hoc audits. A validation attached to a pipe can gate that
pipe's execution. A standalone validation pipe can produce audit results but
cannot prevent an upstream pipe from writing after the fact.

## Great Expectations adapter

The adapter should be a separate package, for example:

```text
kindling-ext-great-expectations
```

The adapter should:

1. Resolve the declared GX suite and validation definition.
2. Receive the already-read Kindling Spark DataFrame.
3. Pass the DataFrame to GX as a runtime batch parameter.
4. Supply Kindling batch identity as GX batch metadata or parameters.
5. Run the validation and normalize the result.
6. Return success/failure to the Kindling validation runner.
7. Optionally publish GX Data Docs or other GX actions.

Kindling should continue to own entity reads. GX should not independently
re-read the same table merely because it is the validation provider. This keeps
watermark behavior, provider abstraction, and batch identity under one owner.

GX's current production abstraction is a Checkpoint, which runs one or more
Validation Definitions and can execute Actions based on results. That maps
naturally to the adapter, but GX configuration should not replace Kindling's
pipe/entity declarations. See the [GX Checkpoint documentation](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions/).

The older repository analysis references `SparkDFDataset`, which belongs to the
pre-1.x GX API and should not be used as the basis for a new integration.

## Execution integration

### Batch validation

The initial batch lifecycle should be:

```text
1. Read input entities
2. Run configured input/schema validations
3. Execute the pipe transform
4. Run configured output validations
5. Persist only if the failure policy allows it
6. Persist validation results and emit lifecycle signals
```

The output gate should use the existing synchronous
`persist.before_persist` interception point. A successful validator returns
the original DataFrame. A failing validator raises for `failure_policy=fail`.

The validation implementation must not use asynchronous signal handlers for a
gate. Asynchronous handlers are appropriate for notifications and metrics
only.

### Post-persist validation

Post-persist validation is useful for informational checks and audits, but a
generic provider cannot necessarily roll back a write after a failure. It
should therefore default to `warn` or `continue`, unless the provider supports
an explicit staging and promotion workflow.

### Streaming validation

The current streaming path constructs an unbounded DataFrame and starts the
sink directly. It does not use the batch `persist.before_persist` DataFrame
interception path. Streaming validation therefore needs a separate design,
most likely validation inside `foreachBatch` or an equivalent provider-owned
microbatch hook.

Streaming validation should be a later phase, with explicit decisions about:

- whether validation runs per microbatch or on a periodic aggregate;
- how failures affect the streaming query;
- how failed batches are retried or quarantined;
- how validation interacts with checkpoints and exactly-once claims.

## Validation results

Kindling should persist a normalized result entity rather than relying only on
GX's local stores or Data Docs. A result schema should include fields such as:

```text
validation_id
provider
suite_or_contract
pipe_id
source_entity_id
target_entity_id
stage
run_id
batch_id / entity_version / watermark
success
status
severity
failed_expectation_count
observed_metrics
result_summary
result_artifact_uri
started_at
completed_at
```

Large or complete GX results should be stored as an artifact or compact JSON,
not emitted through signals or placed unboundedly in a table row. GX Data Docs
can remain an optional presentation layer; GX stores and Data Docs are designed
to retain expectation and validation metadata, but Kindling needs a result
record aligned with its own run, pipe, and entity lineage.

## Signals

The validation capability should emit signals such as:

```text
validation.before_validate
validation.after_validate
validation.failed
validation.gate_blocked
validation.quarantined
```

The signals should contain metadata, not the full DataFrame:

```text
validation_id
pipe_id
entity_id
stage
run_id
batch identity
provider
success
severity
failure count
result reference
duration
```

The existing `persist.before_persist` signal can remain the execution seam for
the first batch implementation. A dedicated validation aspect should register
handlers from explicit validation declarations rather than requiring users to
write arbitrary signal handlers.

## Failure and watermark semantics

A failed validation gate must behave like a failed pipe:

- the output provider must not be called;
- the pipe must emit its normal failure lifecycle;
- a captured input watermark must not advance;
- retry behavior must re-read the same input slice;
- validation results must record the failure.

**Resolved (2026-07-23):** `SimpleReadPersistStrategy` now emits
`persist.before_persist` inside the same failure-handling boundary as the
write. A raising handler prevents the write, propagates, and pairs with
`persist.persist_failed`, so `WatermarkAspect` discards the pending watermark
and a retry re-reads the same input slice. The gate contract above is
therefore already supported by the framework; the validation runner can rely
on it.

## Performance and evaluation semantics

Spark DataFrames are lazy, while most content expectations require Spark work
to calculate metrics. Validating an output and then persisting it can therefore
cause recomputation.

The runner should support explicit validation execution modes:

```text
metadata_only       schema and plan checks without a data action
gate                validate before persistence; cache or stage as needed
post_persist        validate after persistence; normally non-blocking
audit               scheduled or on-demand validation with no pipe gating
```

For `gate`, the adapter should use a bounded result format by default and allow
the runner to cache the output when the same DataFrame will subsequently be
written. Complete failed-row results should be opt-in because they can be
expensive and large.

## Native validation and Delta constraints

Great Expectations should not be used for every check. Kindling can perform
cheap structural checks directly:

- required entity columns;
- Spark schema compatibility;
- declared key columns;
- empty-input policy;
- provider capabilities;
- basic SQL predicates.

Provider-native constraints, such as Delta write constraints where available,
may be preferable for invariants that must remain enforced independently of a
particular pipeline run. The validation runner should allow these mechanisms to
coexist with GX rather than hiding them behind one implementation.

## Business parameters and expectation parameters

GX supports runtime expectation parameters, but that does not by itself solve
per-subject business variables such as a different threshold for each machine.
That requires Kindling to resolve a parameter value using the Event or entity
scope and effective time, then pass the resolved value to the validator or
join it into the DataFrame.

This should remain a separate capability. The initial validation contract may
support resolved runtime parameters, but should not make arbitrary parameter
table joins part of the first GX integration.

## Implementation phases

### Phase 1: Provider-neutral validation foundation

- Define validation metadata and registry interfaces.
- Define normalized validation result schema.
- Add validation lifecycle signals.
- Add native schema and basic predicate validators.
- Add batch gate integration through `persist.before_persist`.
- Correct failure/watermark semantics for a rejected gate.

### Phase 2: Great Expectations extension

- Add `kindling-ext-great-expectations` with an optional GX dependency.
- Support Spark DataFrame runtime batches.
- Resolve suites and Validation Definitions from package/configuration.
- Normalize GX results into the Kindling result entity.
- Support fail, warn, and continue policies.
- Add unit and Spark integration coverage.

### Phase 3: Operational quality workflows

- Add quarantine support where the output provider supports it.
- Add result retention and artifact references.
- Integrate Data Docs as an optional action.
- Add CLI commands for validation status and on-demand audits.
- Add validation metrics to tracing and run summaries.

### Phase 4: Streaming validation

- Define per-microbatch validation semantics.
- Add `foreachBatch` or provider-native validation hooks.
- Define checkpoint, retry, quarantine, and failure behavior.
- Test on supported streaming providers and platforms.

## Open questions

- Should validation declarations attach to `PipeMetadata`, live in a separate
  registry, or support both forms?
- Where should GX suites be stored: package resources, a versioned entity, or a
  configured external store?
- Should a suite be version-pinned in the validation result?
- Is `quarantine` a core failure policy or an extension/provider capability?
- Should output gating default to caching, staging, or an explicit opt-in?
- Which checks belong in Kindling core versus provider adapters?
- How should validation declarations interact with the declarative SDP/Lakeflow
  engine, where persistence is owned by the downstream engine?
- What is the supported policy for per-subject runtime parameters?

## Recommendation

Proceed with the provider-neutral validation foundation and a separate Great
Expectations adapter. Use explicit validation declarations to define contracts,
use synchronous DataFrame-aware signals for the initial batch enforcement seam,
and use validation signals for observability and external actions.

Do not make raw GE objects part of core pipe metadata, do not make arbitrary
signal handlers the only validation API, and do not treat streaming validation
as equivalent to batch validation.
