# spark-kindling-ext-cosmos

Azure Cosmos DB (NoSQL API) entity provider extension for Kindling.

Entities tagged `provider_type: "cosmos"` read and write through the
[Cosmos DB Spark connector](https://learn.microsoft.com/azure/cosmos-db/nosql/quickstart-spark).
The provider is a batch source (`read_entity`), a **change-feed streaming
source** (`read_entity_as_stream`), a batch sink (`write_to_entity`,
`append_to_entity`, `merge_to_entity`) and a streaming sink (`append_as_stream`),
so a Cosmos entity can be the driving input or the output of a streaming pipe.

## Spark runtime package

The connector is a JVM artifact built **per Spark line**: one connector
release, one artifact per Spark minor / Scala binary. The Python wheel cannot
install it; put the coordinate for your pool's Spark version on the cluster
(`spark.jars.packages`, or the platform's library UI):

| Spark line | Runtimes | Maven coordinate |
|---|---|---|
| 3.4 (Scala 2.12) | Synapse Spark 3.4, Databricks 13.x-14.x | `com.azure.cosmos.spark:azure-cosmos-spark_3-4_2-12:4.49.2` |
| 3.5 (Scala 2.12) | Fabric Runtime 1.3, Synapse Spark 3.5, Databricks 15.x-16.x, local PySpark 3.5 | `com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.49.2` |
| 4.0 (Scala 2.13) | Databricks Runtime 17.x | `com.azure.cosmos.spark:azure-cosmos-spark_4-0_2-13:4.49.2` |
| 4.1 (Scala 2.13) | Spark 4.1 standalone / `standalone-4x`, newer Databricks runtimes | `com.azure.cosmos.spark:azure-cosmos-spark_4-1_2-13:4.49.2` |

`kindling_ext_cosmos.resolve_cosmos_spark_connector_coordinate()` returns the
row for the active session (or a given Spark version string), and
`COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES` is the whole table. The old
`COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE` constant still names the Spark 3.5
artifact.

### Install profiles

The wheel itself is Spark-version neutral. For local and CI environments the
extras pin `pyspark` to the Spark family you are targeting — named by Spark
line, not by platform, because a Databricks cluster on Spark 4.0 and a
standalone Spark 4.0 need the same connector artifact:

```bash
pip install 'spark-kindling-ext-cosmos[spark_3_x]'   # Spark 3.4-3.5 (Fabric, Synapse, standalone)
pip install 'spark-kindling-ext-cosmos[spark_4_x]'   # Spark 4.0-4.1 (Databricks 17+, standalone-4x)
```

Never request both. Managed runtimes install the bare wheel (Kindling's
extension bootstrap passes no extras) and keep their own `pyspark`.

## Upsert semantics

Writes use the connector's `ItemOverwrite` strategy by default: documents are
**upserted by `(id, partition key)`**. Writes are therefore idempotent — a
retried persist converges instead of duplicating rows. Map your entity's
logical key onto the document `id` column (Cosmos ids are strings) to get
merge-like behavior. Set `provider.write_strategy: ItemAppend` for
insert-only, or `ItemDelete` to delete by id.

`merge_to_entity()` is the same upsert, declared formally so the persist
path treats a Cosmos entity with `merge_columns` as merge-capable instead of
falling back to append. The entity's `merge_columns` are not used as a match
condition — the document `id` is the key, so the DataFrame must carry an `id`
column — and the call is rejected when `provider.write_strategy` is
`ItemAppend` or `ItemDelete`, since neither is a merge.

## Configuration

```python
tags={
    "provider_type": "cosmos",
    "provider.auth": "service_principal",          # or master_key
    "provider.account_endpoint": "https://myaccount.documents.azure.com:443/",
    "provider.database": "MyDatabase",
    "provider.container": "MyContainer",
    "provider.client_id": "<app id>",              # service_principal auth
    "provider.client_secret": "<secret>",
    "provider.tenant_id": "<tenant id>",
    "provider.subscription_id": "<subscription>",  # required for service_principal:
    "provider.resource_group": "<resource group>", # the connector resolves account
                                                   # metadata through ARM
    # "provider.account_key": "<key>",             # master_key auth instead
}
```

The service principal needs a Cosmos DB **data-plane RBAC** role assignment
(e.g. *Cosmos DB Built-in Data Contributor*) scoped to the account or
database — control-plane roles are not sufficient.

Any connector option can be passed through verbatim with the `provider.option.`
prefix, e.g. `provider.option.spark.cosmos.write.bulk.enabled: "false"`.
`provider.option.*` always has the last word: it overrides the run-level
`kindling.cosmos.*` defaults below and the change-feed defaults.

## Reads

`read_entity()` reads the whole container, or the result of a Cosmos SQL
query when `provider.query` is set:

```python
"provider.query": "SELECT c.id, c.amount FROM c WHERE c.amount > 15",
```

Schema inference is enabled by default (`provider.infer_schema: false` to
disable). Heterogeneous containers usually want a `provider.query` that
projects the relevant fields, so inference sees a consistent shape.

## Streaming reads (change feed)

`read_entity_as_stream()` reads the container's change feed through the
connector's `cosmos.oltp.changeFeed` source, which makes a Cosmos entity a
valid driving input for a streaming pipe (alongside Delta, Parquet and Event
Hubs). Nothing in `pipe_streaming` needed to change: the streaming starter
gates on the `StreamableEntityProvider` interface. Continuation tokens are
Spark Structured Streaming's responsibility and live in the sink's checkpoint.

```python
tags={
    ...,
    "provider.changefeed.mode": "latest_version",      # default; or full_fidelity
    "provider.changefeed.start_from": "Beginning",     # default; Now | ISO-8601 UTC timestamp
    "provider.changefeed.items_per_trigger": "5000",   # optional micro-batch size hint
}
```

- `latest_version` (connector `LatestVersion`) works on any container and
  delivers the latest version of each changed document. **Deletes are not
  visible** in this mode.
- `full_fidelity` (connector `AllVersionsAndDeletes`) delivers every
  intermediate version and deletes, but the container must be provisioned
  for it (all-versions-and-deletes change feed / continuous backup). Kindling
  cannot retrofit eligibility; the connector rejects an ineligible container
  at stream start.

The provider also implements `DeclarableStreamingSource`, so the declarative
(SDP / Lakeflow) engine can lower a Cosmos driving input as an external
streaming source. `streaming_source_spec()` is inert and secret-safe: it
validates the tags above and reports option *names* only.

## Streaming writes

Kindling calls `append_as_stream()` for streaming pipe outputs; the provider
starts the Cosmos sink with the configured options and checkpoint location.
Streaming defaults: `provider.output_mode: append`.

## Throughput controls (`kindling.cosmos.*`)

A whole-container read with the connector's default partitioning can draw a
large share of a container's provisioned or autoscale RU/s and starve other
consumers. The connector has the knobs for this; Kindling surfaces them as
run-level configuration, applied to every Cosmos read and write before
per-entity `provider.option.*` overrides ("config dictates, tags override"):

```yaml
kindling:
  cosmos:
    read:
      partitioning_strategy: Default   # Default | Custom | Restrictive | Aggressive
      max_item_count: 1000             # page size per request; connector default, now explicit
    throughput_control:
      enabled: false                   # off by default
      group_name: kindling-etl         # spark.cosmos.throughputControl.name
      target_threshold: 0.9            # fraction (0, 1] of provisioned/autoscale RU/s ...
      # target_throughput: 4000        # ... OR an absolute RU/s; never both
      global_control:                  # optional: coordinate across jobs through a
        database: ThroughputControl    # shared control container
        container: groups
```

`partitioning_strategy` and `max_item_count` always get an explicit value
(the connector's own defaults unless configured). Throughput control stays
**off** unless enabled — Kindling makes no ARM call to learn a container's
RU/s, so it cannot guess a safe default; when you enable it you must supply
exactly one of `target_threshold` or `target_throughput`. Setting both, a
threshold outside `(0, 1]`, or only one half of `global_control` is a
configuration error.

## Existence checks

`check_entity_exists()` returns `provider.assume_exists` (default `true`):
Cosmos writes are upserts to a pre-provisioned container, so append and write
behave identically and no query permission is needed for write-only targets.

## Testing

Unit tests: `poe test-unit` (option-building against a stubbed Spark).
Live round trip against a Cosmos account (`tests/system/extensions/cosmos`):
`poe test-extension --extension cosmos`; the test skips without credentials
and resolves the connector coordinate from the installed `pyspark`.
