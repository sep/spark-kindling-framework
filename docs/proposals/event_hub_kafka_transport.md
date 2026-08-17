# Event Hub Provider: Kafka-Compatible Transport

**Status:** Partially implemented. `provider.transport` supports
`auto`/`eventhubs`/`kafka`, and `auto` already selects Kafka on Databricks.
The remaining decision is the cross-platform default-to-Kafka policy and its
compatibility rollout for Fabric and Synapse.
**Scope:** Make the Kafka Spark connector the default transport for the Event
Hub entity provider across Synapse, Fabric, and Databricks.
**Target provider:** `eventhub`

## Summary

Azure Event Hubs exposes an Apache Kafka-compatible endpoint. Current Spark
runtimes increasingly treat that endpoint as the supported integration path:

- Synapse Spark 3.5 deprecates `EventHubConnector` and recommends the Kafka
  Spark connector.
- Fabric Runtime 1.3, based on Spark 3.5, deprecates `EventHubConnector` and
  recommends the Kafka Spark connector.
- Databricks Runtime includes the Kafka connector, while the Structured
  Streaming Event Hubs connector is not available. Lakeflow pipelines also do
  not allow the third-party JVM library that the Event Hubs connector requires.

The provider should therefore use Kafka when `provider.transport` is `auto`,
regardless of platform. The existing Event Hubs transport should remain as an
explicit, legacy compatibility mode while older runtimes are retired.

This is primarily a transport and compatibility change, not a new provider.
The existing provider already supports both `eventhubs` and `kafka`; the Kafka
path needs to become the portable implementation and its configuration and
schema semantics need to be tightened.

## Platform findings

| Platform/runtime | Kafka Spark connector | Event Hubs Spark connector | Provider policy |
|---|---|---|---|
| Synapse Spark 3.5 | Supported and recommended | Deprecated; planned removal | Kafka by default |
| Fabric Runtime 1.3 / Spark 3.5 | Supported and recommended | Deprecated; planned removal | Kafka by default |
| Current Databricks Runtime | Included in the runtime | Not included; unavailable to Lakeflow pipelines | Kafka required |

These findings are based on the current platform documentation:

- [Synapse Spark 3.5 runtime](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-35-runtime)
- [Fabric Runtime 1.3](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-1-3)
- [Databricks: Use Azure Event Hubs as a pipeline data source](https://docs.databricks.com/gcp/en/ldp/event-hubs)

Kafka compatibility is a property of the Event Hubs service as well as the
Spark runtime. It maps an Event Hub to a Kafka topic, partitions to partitions,
consumer groups to consumer groups, and offsets to offsets. Kafka access is
available only for Event Hubs Standard, Premium, and Dedicated tiers; Basic is
not supported. The endpoint uses TLS and normally port `9093`.

See the [Azure Event Hubs Kafka overview](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview)
and the [Azure Spark Kafka tutorial](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-kafka-spark-tutorial).

## Problem in the current provider

The current implementation resolves `auto` as follows:

```python
if self.platform == "databricks":
    return "kafka"

return "eventhubs"
```

That behavior reflects an earlier platform split. It is now contrary to the
current Synapse and Fabric runtime guidance and makes those environments
depend on the connector that is being deprecated.

The current Kafka implementation also has several assumptions that need to be
addressed before making it the universal default:

1. It appends `EntityPath` to the SAS connection string. The Kafka connector
   does not require `EntityPath`; the Event Hub name is supplied as the Kafka
   topic. Databricks explicitly documents that `EntityPath` is required only by
   the Event Hubs connector.
2. It maps the default `$Default` Event Hubs consumer group directly to
   `kafka.group.id`. Structured Streaming normally manages source progress via
   checkpoints, and Databricks warns that queries sharing a Kafka group ID can
   interfere with one another.
3. It hardcodes a Databricks-shaded Kafka login-module class. The Azure Spark
   example uses the unshaded Kafka class, so authentication needs to be
   runtime-aware or configurable.
4. Its normalization maps `value` to `body` and `timestamp` to
   `enqueuedTime`, but does not fully preserve or type the Kafka metadata
   (`key`, `topic`, `partition`, `offset`, and `timestampType`).
5. Its batch path can emit `startingOffsets=latest`. Kafka batch reads require
   a bounded offset range, and the Databricks API documentation states that
   `latest` is not valid as a batch starting offset.
6. It forces `failOnDataLoss=false`, which can silently skip data. The Kafka
   connector and the Azure example default to failing when data may have been
   lost.

## Proposal

### 1. Make Kafka the `auto` transport

Change transport resolution to:

```python
if configured_transport != "auto":
    return configured_transport

return TRANSPORT_KAFKA
```

The explicit values remain:

- `auto`: Kafka on all supported platforms;
- `kafka`: force the Kafka connector; and
- `eventhubs`: use the legacy Event Hubs connector explicitly.

Using `eventhubs` should emit a deprecation warning containing the runtime
reason and the migration setting:

```yaml
provider.transport: kafka
```

The provider should not silently fall back from Kafka to `eventhubs`. A
missing Kafka source, an unsupported Event Hubs tier, or a blocked network path
should fail with an actionable error instead of selecting a connector that is
being removed from the target runtime.

### 2. Separate Event Hubs identity from Kafka options

Keep the existing `provider.eventhub.*` tags for compatibility:

```yaml
provider.eventhub.connectionString: <namespace SAS connection string>
provider.eventhub.name: <event hub name>
```

For Kafka transport, build:

```text
kafka.bootstrap.servers = <namespace>.servicebus.windows.net:9093
subscribe               = <event hub name>
kafka.security.protocol = SASL_SSL
kafka.sasl.mechanism    = PLAIN
```

The SAS password should be the namespace connection string exactly as supplied
by the secret resolver. The provider should not add `EntityPath` for Kafka.

Future configuration should support platform-native authentication without
changing entity declarations:

```yaml
provider.kafka.auth.mode: sas
provider.kafka.bootstrapServers: <optional override>
provider.kafka.loginModule: <optional runtime-specific override>
```

SAS is sufficient for the first migration. OAuth/Entra ID and Databricks Unity
Catalog service credentials should be added as a follow-up, particularly for
deployments where shared access keys are prohibited. Secrets must be resolved
through the platform secret mechanism and must never be logged or embedded in
example configuration.

### 3. Make consumer-group behavior explicit

For Kafka transport, do not emit `kafka.group.id` for the implicit `$Default`
value. Let Spark generate a source-specific group identity and use the
Structured Streaming checkpoint as the durable progress record.

If an application explicitly supplies `provider.eventhub.consumerGroup`, the
provider may pass it through as `kafka.group.id`, but the documentation must
explain that the same group ID must not be shared by concurrent queries unless
partial consumption is intended.

The provider must also document that changing transport, subscription, or
checkpoint locations can change restart behavior. A transport migration should
use a new checkpoint unless offset compatibility has been verified.

### 4. Define a transport-neutral event schema

The provider should retain its canonical event columns while preserving Kafka
metadata where possible:

| Canonical column | Kafka source | Policy |
|---|---|---|
| `body` | `value` | Rename; retain as binary |
| `partition` | `partition` | Cast to the provider’s documented type |
| `offset` | `offset` | Cast to the provider’s documented type |
| `enqueuedTime` | `timestamp` | Rename; retain as timestamp |
| `key` | `key` | Preserve as an additional binary column or documented extension |
| `topic` | `topic` | Preserve as metadata or omit because entity identity supplies it |
| `timestampType` | `timestampType` | Preserve as metadata where available |
| `sequenceNumber` | unavailable | Null for Kafka transport |
| `publisher` | unavailable | Null for Kafka transport |
| `partitionKey` | not equivalent to Kafka key | Do not silently map |
| `properties` | Kafka headers, if requested | Map only with an explicit conversion policy |
| `systemProperties` | unavailable | Null for Kafka transport |

The provider should document which fields are transport-dependent. It should
not claim Event Hubs-specific metadata that the Kafka source cannot supply.

### 5. Redesign batch and incremental semantics

Streaming reads should use Spark checkpoints as the source of progress. The
provider can continue exposing `read_entity_as_stream()` with:

```python
spark.readStream.format("kafka")
```

For bounded batch reads, the provider must always construct a valid offset
range. Possible policies are:

- require `earliest` or explicit JSON offsets for batch reads;
- use a previously persisted provider cursor to construct the starting offset;
- use a short-lived `AvailableNow` streaming query for incremental ingestion;
  or
- reserve `read_entity()` for an explicitly bounded extraction configuration.

The provider should not interpret `latest` as both the starting and ending
position for a Kafka batch read.

`startingPosition` may remain as a compatibility setting for `earliest` and
`latest` streaming reads. Event Hubs-specific JSON position specifications
should remain supported only by the explicit legacy `eventhubs` transport.

### 6. Make loss and retry behavior visible

Add provider settings for:

```yaml
provider.kafka.failOnDataLoss: true
provider.kafka.allowNonConsecutiveOffsets: false
provider.kafka.groupIdPrefix: <optional prefix>
```

The default should favor detection of data loss. If Event Hubs offset behavior
requires non-consecutive-offset handling for a particular runtime, that should
be enabled deliberately and covered by an integration test rather than hidden
by `failOnDataLoss=false`.

Connection failures should include the namespace host, Event Hub name, and
transport, but never the connection string, SAS key, or rendered JAAS value.

## Compatibility and migration

### Existing entities

Entities without `provider.transport` will change from the Event Hubs connector
to Kafka on Synapse and Fabric. This is the desired direction, but it changes
the raw source schema and potentially the restart/checkpoint behavior. The
release notes must call out the change.

Entities that require the legacy connector can temporarily opt in:

```yaml
provider.transport: eventhubs
```

That override should be treated as a migration escape hatch, not a long-term
portable configuration.

### Service and network prerequisites

Before migration, deployments must verify:

- the Event Hubs namespace is Standard, Premium, or Dedicated;
- outbound TLS access to the Event Hubs Kafka endpoint on port `9093` is
  allowed;
- the configured SAS policy has listen permission, preferably through a
  listen-only policy; and
- the secret is available through the platform’s secret manager.

## Testing strategy

### Unit tests

Update the existing Event Hub provider tests to cover:

- `auto` resolving to Kafka on Fabric, Synapse, and Databricks;
- explicit `eventhubs` continuing to select the legacy connector;
- Kafka configuration without an appended `EntityPath`;
- runtime-specific or overridden JAAS login-module selection;
- omission of the default Kafka group ID;
- explicit consumer-group pass-through;
- correct Kafka metadata normalization and types;
- invalid batch `latest` configuration;
- configurable `failOnDataLoss`; and
- redaction of credentials from errors and logs.

### Platform integration tests

Run a small read and streaming-read matrix against one Event Hub in each
supported platform. Validate:

1. a known event can be read through `format("kafka")`;
2. `body`, partition, offset, and timestamp are populated correctly;
3. restart from a checkpoint does not duplicate beyond the documented
   at-least-once behavior;
4. bounded reads stop at the requested offset range; and
5. an unsupported tier or blocked port produces a clear diagnostic.

The integration tests should use a test-only secret and Event Hub. No
connection strings, tenant names, workspace names, catalog names, or client
identifiers belong in repository examples.

## Rollout plan

1. Add transport-neutral Kafka configuration and schema tests.
2. Change `auto` to Kafka and add the explicit legacy warning.

   Step 1 before step 2 is a correctness requirement, not a stylistic
   ordering. The current Kafka path hardcodes the Databricks-shaded
   `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule`
   JAAS class, which does not exist on Synapse or Fabric runtimes.
   Flipping `auto` to Kafka before the runtime-aware login-module
   selection (and the `EntityPath` removal) lands would break every
   Synapse and Fabric deployment that relies on `auto` in a single
   release. Reviewers of the implementation PRs should reject any
   sequencing that inverts these two steps.
3. Update the provider reference documentation and example entities.
4. Run platform integration tests on current Synapse, Fabric, and Databricks
   runtimes.
5. Release with a migration note and checkpoint guidance.
6. Remove the Event Hubs connector implementation after the supported-runtime
   compatibility window ends.

## Decision requested

Approve Kafka as the default and strategic transport for the Event Hub entity
provider, with `eventhubs` retained temporarily as an explicit legacy mode.
Approve the associated schema, consumer-group, batch-offset, authentication,
and secret-handling changes before implementation begins.
