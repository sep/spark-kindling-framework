# kindling-ext-cosmos

Azure Cosmos DB (NoSQL API) entity provider extension for Kindling.

Entities tagged `provider_type: "cosmos"` read and write through the
[Cosmos DB Spark connector](https://learn.microsoft.com/azure/cosmos-db/nosql/quickstart-spark)
(`com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.37.2` — a JVM package
that must be available on the Spark pool, e.g. via `spark.jars.packages`).

## Upsert semantics

Writes use the connector's `ItemOverwrite` strategy by default: documents are
**upserted by `(id, partition key)`**. Writes are therefore idempotent — a
retried persist converges instead of duplicating rows. Map your entity's
logical key onto the document `id` column (Cosmos ids are strings) to get
merge-like behavior. Set `provider.write_strategy: ItemAppend` for
insert-only, or `ItemDelete` to delete by id.

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

## Reads

`read_entity()` reads the whole container, or the result of a Cosmos SQL
query when `provider.query` is set:

```python
"provider.query": "SELECT c.id, c.amount FROM c WHERE c.amount > 15",
```

Schema inference is enabled by default (`provider.infer_schema: false` to
disable). Heterogeneous containers usually want a `provider.query` that
projects the relevant fields, so inference sees a consistent shape.

## Streaming writes

Kindling calls `append_as_stream()` for streaming pipe outputs; the provider
starts the Cosmos sink with the configured options and checkpoint location.
Streaming defaults: `provider.output_mode: append`.

## Existence checks

`check_entity_exists()` returns `provider.assume_exists` (default `true`):
Cosmos writes are upserts to a pre-provisioned container, so append and write
behave identically and no query permission is needed for write-only targets.
