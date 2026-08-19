# Declarable Streaming Sources for Lakeflow

**Status:** Proposed
**Created:** 2026-08-19
**Initial source:** Azure Event Hubs over Kafka
**Initial target:** Databricks Lakeflow Spark Declarative Pipelines
**Related:** `declarative_pipelines_engine.md`,
`event_hub_kafka_transport.md`, and
`kindling_core_runner_split.md`

## Recommendation

Add a provider capability for streaming sources that a declaration engine can
own. Begin with the existing Event Hub provider and lower an ingestion pipe
whose driving input has that capability to a Lakeflow streaming table and
append flow.

The application model stays unchanged: an application declares an Event Hub
entity and a normal Kindling pipe. The provider constructs the source
DataFrame, the pipe performs the existing transformation, and Lakeflow owns
query lifecycle, checkpoints, retries, and persistence.

```text
Event Hub entity declaration
  -> secret-safe streaming source specification
  -> Kindling declaration plan
  -> Lakeflow create_streaming_table
  -> Lakeflow append_flow
       -> provider streaming read
       -> existing pipe transform
       -> returned streaming DataFrame
  -> Lakeflow-managed persistence and checkpointing
```

This is a generic provider capability, not Event Hub logic embedded in the
declaration planner. Future Kafka, Auto Loader, or other streaming providers
can implement the same contract.

## Motivation

Kindling already separates data declarations, provider-backed reads, pipe
transforms, and execution engines. It also already has a runtime streaming-read
capability: `StreamableEntityProvider.read_entity_as_stream()`.

The declarative engine cannot currently use that capability:

- SDP plan inputs are classified only as internal pipeline dependencies or
  external storage reads.
- External storage reads are restricted to Delta entities.
- An Event Hub input is therefore rejected as
  `external_input_not_declarable`.
- Ordinary Databricks pipes are lowered as materialized views.
- Generic streaming-table and append-flow lowering is deliberately left as
  the unimplemented Phase 4 of the SDP engine.

The result is an artificial split: the runner can read Event Hubs as a stream,
and Lakeflow can natively manage a Kafka stream, but a Kindling declaration
cannot connect the two.

## Why This Was Not Declarative Already

Kindling's public application model is declarative, but its original execution
model was the runner. Under that model, provider interfaces were designed as
execution operations:

```text
entity metadata -> provider.read_*() -> DataFrame -> pipe -> provider.write_*()
```

`BaseEntityProvider.read_entity()` and
`StreamableEntityProvider.read_entity_as_stream()` therefore answer the
question "how do I construct this DataFrame now?" They do not answer the
separate declaration questions:

- what kind of source is this;
- can the selected engine represent it;
- what non-secret configuration does it require;
- what validation can run before Spark evaluates the graph; and
- who owns progress, retries, checkpoints, and persistence?

That distinction did not matter while the runner was the only execution
engine. Registration was declarative at the application boundary, but the
framework immediately interpreted each registration through imperative
provider operations.

The SDP engine exposed the missing intermediate representation. It needs to
inspect a complete, inert graph before any query function runs. Phase 1 modeled
the source forms needed for the initial implementation: internal dataset reads
and external Delta table reads. Event Hub was correctly rejected because it is
neither one; treating it as another external table would have hidden its
streaming lifecycle semantics.

Generic streaming-table and append-flow support was also explicitly deferred
as SDP Phase 4. Consequently the existing Event Hub runtime capability was not
promoted into a declaration capability when the initial SDP plan was built.

The architectural lesson is broader than Event Hubs:

> A Kindling provider should declare source or destination intent independently
> of performing the corresponding Spark operation. An execution engine lowers
> that intent according to its ownership model.

For the runner, lowering a streaming-source declaration means calling the
provider streaming read and letting the runner manage the query. For Lakeflow,
it means placing the same provider read inside an append-flow function and
letting Lakeflow manage the query. The entity and pipe declarations remain the
same; only the engine lowering changes.

This proposal does not attempt a wholesale redesign of every provider
interface. It introduces the smallest declaration-side capability needed to
remove the Event Hub mismatch and establish the pattern for later sources.

## Declaration Versus Evaluation

The implementation must preserve a strict two-phase boundary.

### Declaration phase

The framework:

- reads registered entity and pipe metadata;
- asks providers for inert, secret-safe capability specifications;
- validates source shape and engine support;
- classifies graph inputs and output datasets; and
- emits Lakeflow declarations.

It does not create a streaming DataFrame or contact the source.

### Evaluation phase

When Lakeflow evaluates the registered append flow, the flow:

- resolves the provider for the declared source entity;
- calls the provider's existing streaming-read operation;
- receives provider-normalized and preprocessed data;
- obtains any static inputs;
- invokes the existing pipe transform; and
- returns the transformed streaming DataFrame to Lakeflow.

This split is consistent with the rest of Kindling: registrations express
intent, the declaration plan validates and translates intent, and the selected
engine decides when and how operations execute.

## Goals

- Accept an Event Hub entity as the driving input of a Databricks declarative
  ingestion pipe.
- Reuse the Event Hub provider's Kafka transport, configuration mapping,
  preprocessing, AMQP header handling, and secret-resolution behavior.
- Reuse the registered Kindling pipe transform without generating application
  code or introducing a second transform API.
- Keep later pipe inputs batch/static, preserving the runner's driving-input
  convention and enabling stream-static joins.
- Make source configuration validation part of declaration planning without
  resolving, retaining, displaying, or logging secret values in the plan.
- Leave checkpointing, retries, query startup, and output persistence entirely
  to Lakeflow.
- Preserve existing Event Hub batch and runner streaming behavior.
- Keep the capability small and provider-neutral.

## Non-goals

- Adding `writeStream`, `checkpointLocation`, `start()`, or imperative provider
  writes to application or pipe code.
- Making Event Hub a table-backed external input.
- Implementing arbitrary multi-stream joins. The first implementation permits
  one declarable streaming source in the driving-input position.
- Changing the cross-platform `provider.transport: auto` policy. That decision
  remains in `event_hub_kafka_transport.md`.
- Adding Event Hub write support.
- Automatically enabling this feature on OSS SDP before its runtime behavior
  and parity requirements are separately accepted.

## Existing Architecture and Reusable Seams

### Provider interfaces

`EventHubEntityProvider` already implements `BaseEntityProvider` and
`StreamableEntityProvider`. Its streaming path:

1. reads resolved entity tags;
2. selects the Event Hubs or Kafka transport;
3. maps the Event Hub settings to connector options;
4. calls `spark.readStream.format(...).options(...).load()`;
5. normalizes the connector schema; and
6. applies the configured `kafka` or `avro` preprocessing mode.

On Databricks, `provider.transport: auto` currently selects Kafka. This is the
required Lakeflow transport because the legacy Event Hubs Spark connector is
not part of the managed pipeline runtime.

### Declaration plan

`kindling_ext_sdp` currently has two input classifications:

- `INTERNAL`: another selected pipe produces the entity; and
- `EXTERNAL`: the entity is read from storage.

The planner allows only Delta for external reads. That rule is correct for
storage inputs but is too narrow to describe provider-owned streams.

### Databricks lowering

The Databricks extension already emits `create_streaming_table` and
`append_flow` for temporal lowering and emits streaming tables for AUTO CDC.
The generic dataset path nevertheless accepts only materialized views.

The shared SDP query-function builder also already implements the useful
driving-input convention for AUTO CDC: stream the first input and read later
inputs as static DataFrames. Declarable source lowering should generalize this
behavior rather than duplicate pipe argument binding.

### Persistence guard

SDP mode installs `SdpWriteGuardProvider`, which rejects imperative write,
append, merge, streaming append, and destination-creation methods. Provider
streaming reads pass through the guard. This is already the correct ownership
model and must remain in force.

## Proposed Provider Capability

Retain `StreamableEntityProvider` as the runtime streaming-read interface and
add a separate declaration capability:

```python
class DeclarableStreamingSource(ABC):
    @abstractmethod
    def streaming_source_spec(
        self, entity_metadata: EntityMetadata
    ) -> StreamingSourceSpec:
        """Return secret-safe declaration metadata and validation results."""
```

An implementation must also implement `StreamableEntityProvider`; the existing
`read_entity_as_stream()` method remains the single runtime read operation.
Adding a synonymous `read_stream()` method would create two ways to perform the
same provider operation and is not recommended.

This capability means:

- the provider can describe the source without constructing a DataFrame;
- the declaration engine may call the provider's streaming read inside a
  declared flow; and
- the returned DataFrame is unstarted and has no sink or checkpoint settings.

It does not mean that the provider can write, start a query, or manage
declarative pipeline state.

## Secret-safe Source Specification

`StreamingSourceSpec` is immutable plan metadata. A representative shape is:

```python
@dataclass(frozen=True)
class StreamingSourceSpec:
    provider_type: str
    source_format: str
    source_identity: str
    supported_option_names: tuple[str, ...]
    preprocessing: Optional[PreprocessingSpec]
    validation_issues: tuple[SourceValidationIssue, ...]
```

The exact field types can be refined during implementation, but the following
rules are normative:

1. The specification contains option names and structural metadata, never
   connector option values.
2. It contains no connection string, SAS key, password, JAAS configuration, or
   resolved secret value.
3. Validation issues name the invalid tag and constraint but do not echo its
   value.
4. `repr`, serialization, logging, and exception rendering are safe by
   construction rather than dependent on caller-side redaction.
5. Building the specification performs no Spark read, network access, JVM
   connector call, or secret lookup.

Kindling resolves entity-tag secret references during the post-registration
configuration overlay, before declaration. Therefore merely avoiding a new
secret lookup is insufficient: the provider must deliberately exclude the
already-resolved value from its specification and diagnostics.

`source_identity` should be useful for diagnostics without containing
credentials. For Event Hubs it can contain the Event Hub name and, if needed,
a sanitized namespace host. It must not contain the original connection
string.

## Plan Model and Classification

Extend input classification to distinguish three concepts:

```text
INTERNAL
EXTERNAL_STORAGE
EXTERNAL_STREAMING_SOURCE
```

The naming may preserve `EXTERNAL` as an alias for storage compatibility, but
the plan must represent streaming sources explicitly. A classified streaming
input carries its `StreamingSourceSpec`.

The planner must use capability checks, not provider-name checks:

1. Resolve the external input entity and its provider.
2. If it implements `DeclarableStreamingSource`, obtain its source spec.
3. Convert source-spec validation issues into actionable declaration issues.
4. Classify a valid source as `EXTERNAL_STREAMING_SOURCE`.
5. Apply the existing external-storage rules to providers without the
   capability.

The generic planner must not contain branches such as
`provider_type == "eventhub"`.

The provider registry (or a narrow provider-capability resolver) must therefore
be available to the declaration engine. It should be injectable so plan tests
can remain hermetic and avoid constructing Spark-backed providers.

## Driving-input and Dataset Rules

For the initial capability:

- exactly one declarable streaming source is allowed per pipe;
- it must be the first input;
- later inputs remain normal internal or external static reads;
- the output must be a Delta/table-backed declarable output; and
- the output dataset type is `streaming_table`.

A streaming driving input determines the physical dataset type. An explicit
conflicting `sdp.dataset_type: materialized_view`, engine setting, or
`dataset.kind: derived` must produce a declaration error rather than be
silently overridden.

An explicit `streaming_table` setting without a streaming driving input remains
unsupported until a separate source or internal-streaming semantic is defined.

Specialized lowerings retain precedence:

- temporal chain pipes continue through temporal lowering; and
- SCD-tagged targets continue through AUTO CDC lowering.

If either specialized pipe shape also declares an external provider stream,
validation must reject the ambiguous combination until an explicit composition
rule exists.

## Event Hub Implementation

`EventHubEntityProvider` becomes the first implementation of
`DeclarableStreamingSource`.

### Supported configuration

The declaration capability reuses the existing entity tags:

- `provider.eventhub.connectionString` — required secret-bearing connection
  setting;
- `provider.eventhub.name` — Event Hub/topic name;
- `provider.eventhub.consumerGroup` — optional consumer group;
- `provider.transport` — `auto`, `kafka`, or legacy `eventhubs`;
- `provider.startingPosition` — `earliest` or `latest` for Kafka;
- `provider.maxEventsPerTrigger` — maps to the Kafka trigger-limit option;
- `provider.operationTimeout` — maps to Kafka request/session timeouts;
- `provider.kafka.*` — generic Kafka connector option passthrough;
- `provider.preprocess` — unset, `kafka`, or `avro`; and
- `provider.amqp_headers` — optional AMQP primitive header decoding.

The plan records only which supported tags apply, never their secret-bearing
values.

### Validation

At plan time, Event Hub validation must report:

- missing connection-string configuration;
- missing Event Hub name when it cannot be obtained from the supported entity
  identity form;
- invalid transport;
- a transport that cannot run in Lakeflow (`eventhubs`);
- invalid Kafka starting-position values;
- invalid preprocessing modes; and
- malformed typed options where validation does not require exposing their
  value.

Validation must not contact Event Hubs. Authentication, authorization, topic
existence, and connector availability remain runtime concerns surfaced by
Lakeflow.

### Kafka record schema

The current Kafka normalization renames `value` to `body` and `timestamp` to
`enqueuedTime`. Existing transforms depend on the normalized Event Hub shape,
while native streaming consumers may need Kafka metadata.

For declarative Kafka streaming reads, the provider should retain the native
fields when available:

- `topic`;
- `partition`;
- `offset`;
- `value`;
- `headers`; and
- `timestamp`.

It should also retain the compatibility aliases used by existing Event Hub
transforms:

- `body` as an alias of `value`; and
- `enqueuedTime` as an alias of `timestamp`.

Any implementation must preserve the current batch schema. If adding aliases
to all Kafka reads would change batch behavior, native-field retention should
be limited to the declarable streaming path or introduced behind an explicit
compatibility policy.

Kafka headers are present only when the connector is configured to include
them. Documentation and validation must state when
`provider.kafka.includeHeaders: true` is required by the selected preprocessing
mode.

## Databricks Lakeflow Lowering

For a planned dataset with an external streaming driving input, the Databricks
adapter emits two declarations.

First, create the target streaming table using the existing dataset metadata
mapping:

```python
dp.create_streaming_table(
    name=target_name,
    comment=comment,
    table_properties=table_properties,
    partition_cols=partition_columns,
    cluster_by=cluster_columns,
    schema=schema,
)
```

Empty or unsupported arguments continue to be omitted according to the current
metadata-emission rules.

Second, register an append flow:

```python
@dp.append_flow(target=target_name, name=flow_name)
def flow():
    source_df = source_provider.read_entity_as_stream(source_entity)
    static_inputs = read_remaining_inputs()
    return pipe.execute(source_arg=source_df, **static_inputs)
```

The actual implementation should reuse the existing query-function builder so
entity IDs are converted to pipe keyword arguments exactly as they are in the
runner and current SDP engine.

The provider is resolved and invoked when Lakeflow evaluates the flow function,
not while the declaration plan is built. This preserves late Spark-session
availability and keeps the plan free of DataFrames and connector options.

No Kindling layer calls `writeStream`, supplies `checkpointLocation`, starts a
query, retries a batch, or writes the target. Returning the transformed
streaming DataFrame is the entire flow contract.

## Static Additional Inputs

Inputs after the driving source retain existing semantics:

- internal inputs use the emitted pipeline dataset name so Lakeflow infers the
  graph edge;
- external Delta inputs use the existing external table-read behavior; and
- unsupported providers continue to fail declaration validation.

They are read as batch/static DataFrames. This supports stream-static joins
without turning every dependency into a streaming source.

Multiple streaming inputs and stream-stream joins require separate semantics
for watermarking, state bounds, and restart behavior and are deferred.

## Persistence and Checkpoint Ownership

Lakeflow owns:

- streaming query construction and startup;
- checkpoint location and checkpoint lifecycle;
- offset progress;
- retries and recovery;
- streaming-table creation and persistence;
- update scheduling or continuous execution; and
- operational monitoring.

Kindling owns:

- source and output declarations;
- secret-safe validation;
- provider-specific DataFrame construction and preprocessing;
- pipe transformation; and
- declaration lowering.

`SdpWriteGuardProvider` remains active. The streaming-source capability adds no
write surface, and an Event Hub provider remains read-only in every engine.

## Error Model

New declaration issues should be specific and actionable. Suggested codes are:

- `streaming_source_invalid` — the provider returned one or more source-spec
  validation issues;
- `streaming_source_not_driving_input` — a declarable stream is not first;
- `multiple_streaming_sources_not_supported` — more than one is present;
- `streaming_dataset_type_conflict` — metadata explicitly requests an
  incompatible dataset type; and
- `streaming_source_lowering_not_supported` — the selected engine cannot lower
  the otherwise valid capability.

Existing unsupported providers continue to use
`external_input_not_declarable`. Diagnostics should identify the entity,
provider type, unsupported constraint, and remediation without echoing option
values.

## Compatibility and Migration

Existing applications require no changes unless they opt into the Databricks
declaration engine.

### Runner applications

- `read_entity()` remains the existing batch snapshot read.
- `read_entity_as_stream()` remains the existing runner streaming read.
- Provider transport selection and preprocessing remain provider-owned.
- Existing pipe functions retain their current arguments.

### Databricks declarative applications

An existing runner ingestion application can migrate by:

1. keeping the Event Hub entity declaration;
2. ensuring `provider.transport` resolves to `kafka`;
3. keeping the existing ingestion pipe and Delta output entity;
4. removing any application-owned streaming query startup, checkpoint, or
   sink code; and
5. selecting the `databricks_sdp` engine.

The output becomes a Lakeflow streaming table. Operational checkpoint paths
are no longer application configuration and should not be migrated into entity
tags.

Applications using the legacy `eventhubs` transport must select Kafka before
using this declaration path. Cross-platform changes to the default transport
remain governed by `event_hub_kafka_transport.md`.

## Testing Strategy

### Provider unit tests

- Event Hub implements the declarable streaming-source capability.
- Valid Kafka configuration produces a valid secret-safe spec.
- Missing required tags, invalid transport, invalid offsets, and invalid
  preprocessing produce useful validation issues.
- Specs, issue strings, logs, and exception messages do not contain connection
  strings, SAS keys, JAAS fragments, or resolved secret values.
- The declarable read uses `readStream.format("kafka")` and the existing option
  mapping and preprocessing functions.
- Native Kafka metadata and compatibility aliases have the documented schema.
- Existing batch reads and existing runner streaming reads remain unchanged.

### Plan tests

- An Event Hub input is classified as a declarable streaming source.
- Its output is planned as a streaming table.
- A Delta external input retains external-storage behavior.
- Memory, CSV, Parquet, view, and other non-capable providers remain rejected
  where they are rejected today.
- A later static Delta input remains a batch input.
- A non-driving or second streaming source is rejected.
- Dataset-type conflicts are rejected.
- The plan's representation and serialization contain no secret values.

### Databricks lowering tests

- The adapter emits one `create_streaming_table` and one `append_flow` for the
  ingestion dataset.
- The flow targets the normalized pipeline dataset name.
- Invoking the recorded flow calls the provider streaming read exactly once.
- The returned source DataFrame is passed under the existing normalized input
  keyword.
- Additional inputs use static reads.
- The existing pipe transform is invoked and its DataFrame is returned.
- Expectations, table metadata, and naming continue to compose where
  supported.
- Temporal and AUTO CDC lowerings retain their existing behavior.

### Ownership regression tests

- Every provider write method remains blocked in SDP mode.
- No generated declaration contains `writeStream`, `checkpointLocation`, or
  query startup.
- Batch Event Hub provider tests remain unchanged and passing.

## Documentation Changes

Implementation should update:

- `docs/contributing/entity_providers.md` with the new capability contract and
  rules for secret-safe specs;
- `docs/reference/config_reference.md` with all supported Event Hub tags,
  including transport and Kafka header requirements;
- the Databricks extension README with streaming-source lowering and
  Lakeflow-owned checkpoints;
- the SDP extension README with capability gating and current OSS behavior;
- `docs/guide/data_entities.md` with an Event Hub ingestion example; and
- `CHANGELOG.md` under `Unreleased`.

## Implementation Sequence

1. Add the provider-neutral capability and secret-safe specification types.
2. Implement Event Hub specification validation without changing runtime
   reads.
3. Inject provider capability resolution into declaration planning.
4. Add streaming-source classification, dataset inference, and validation.
5. Generalize the dataset-function builder for a provider-resolved streaming
   driving input and static additional inputs.
6. Add Databricks streaming-table and append-flow lowering.
7. Address native Kafka field retention while locking down batch compatibility.
8. Add focused tests, documentation, and changelog entries.
9. Run unit tests and targeted Databricks platform tests before broadening the
   capability to another provider or engine.

## Acceptance Criteria

The proposal is implemented when:

- a normal Kindling ingestion pipe with a driving Event Hub entity builds a
  valid Databricks declaration plan;
- invalid source configuration fails before emission with secret-safe,
  actionable diagnostics;
- the plan explicitly identifies the streaming source and streaming-table
  output;
- the Databricks adapter emits a streaming table and append flow;
- evaluating the flow obtains the provider stream, applies existing provider
  preprocessing, invokes the registered pipe transform, and returns its
  streaming DataFrame;
- later inputs remain static reads;
- Lakeflow owns all checkpoint and persistence behavior;
- provider writes remain unavailable in SDP mode;
- existing Delta planning and Event Hub batch/runner behavior remain
  unchanged; and
- all source specs, plan output, logs, and errors are demonstrably free of
  secret values.

## Open Questions

1. Should generic declarable streaming sources be enabled on OSS SDP in the
   same change, or capability-gated to Databricks until parity tests exist?
2. Should native Kafka fields be retained only for declarative streams or
   introduced consistently across Event Hub Kafka batch and runner streaming
   reads through a compatibility release?
3. Should the plan hold only a provider type and entity ID, resolving the
   provider at flow evaluation, or hold a narrow provider resolver callable?
   The plan must remain serializable and secret-safe either way.
4. Should an explicit `sdp.dataset_type: streaming_table` be required for
   migration visibility, or should a streaming driving source infer it? This
   proposal recommends inference with explicit conflicts rejected.
5. Which Event Hub/Kafka options are safe and meaningful for Lakeflow-managed
   queries, particularly explicit consumer group IDs and trigger-rate limits?
