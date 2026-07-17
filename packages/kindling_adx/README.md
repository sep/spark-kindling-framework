# kindling-adx

Azure Data Explorer entity provider extension for Kindling.

This extension registers `provider_type: adx` as an append-oriented materialization
target backed by the Azure Data Explorer Spark connector.

## Spark Runtime Package

Install the Kusto Spark connector on the Spark pool or cluster:

```text
com.microsoft.azure.kusto:kusto-spark_3.0_2.12:7.0.6
```

The Python wheel does not install this Maven dependency. Managed Spark runtimes
need the connector available before a Kindling app writes to ADX.

## Entity Tags

Synapse linked service:

```python
tags={
    "provider_type": "adx",
    "provider.auth": "synapse_linked_service",
    "provider.linked_service": "fawkes_adx",
    "provider.database": "MyDatabase",
    "provider.table": "MyTable",
}
```

Managed identity:

```python
tags={
    "provider_type": "adx",
    "provider.auth": "managed_identity",
    "provider.cluster": "https://mycluster.region.kusto.windows.net",
    "provider.database": "MyDatabase",
    "provider.table": "MyTable",
}
```

User-assigned managed identity:

```python
tags={
    "provider_type": "adx",
    "provider.auth": "managed_identity",
    "provider.cluster": "https://mycluster.region.kusto.windows.net",
    "provider.database": "MyDatabase",
    "provider.table": "MyTable",
    "provider.managed_identity_client_id": "<client-id>",
}
```

Service principal:

```python
tags={
    "provider_type": "adx",
    "provider.auth": "service_principal",
    "provider.cluster": "https://mycluster.region.kusto.windows.net",
    "provider.database": "MyDatabase",
    "provider.table": "MyTable",
    "provider.client_id": "<app-id>",
    "provider.client_secret": "<secret>",
    "provider.tenant_id": "<tenant-id>",
}
```

Access token:

```python
tags={
    "provider_type": "adx",
    "provider.auth": "access_token",
    "provider.cluster": "https://mycluster.region.kusto.windows.net",
    "provider.database": "MyDatabase",
    "provider.table": "MyTable",
    "provider.access_token": "<aad-token>",
}
```

## Optional Connector Options

Any tag under `provider.option.*` is passed through to the Spark connector with
the `provider.option.` prefix removed:

```python
tags={
    "provider_type": "adx",
    "provider.auth": "managed_identity",
    "provider.cluster": "https://mycluster.region.kusto.windows.net",
    "provider.database": "MyDatabase",
    "provider.table": "MyTable",
    "provider.option.adjustSchema": "GenerateDynamicCsvMapping",
}
```

Defaults:

- `provider.write_mode`: `Queued`
- `provider.table_create_options`: `FailIfNotExist`
- `provider.assume_exists`: `true`

`provider.assume_exists` defaults to `true` because this first extension version
is a write target. It avoids requiring read/query permissions just to decide
whether the Kindling persist path should call append or write.

## Streaming Writes

The provider also supports Spark Structured Streaming outputs. Kindling calls
`append_as_stream()` for streaming pipe outputs and the ADX provider starts the
Kusto sink with the configured connector options and checkpoint location.

```python
tags={
    "provider_type": "adx",
    "provider.auth": "managed_identity",
    "provider.cluster": "https://mycluster.region.kusto.windows.net",
    "provider.database": "MyDatabase",
    "provider.table": "MyTable",
    "provider.query_name": "orders_to_adx",
}
```

Streaming defaults:

- `provider.output_mode`: `append`
- `provider.write_mode`: `Queued`

`Queued` mode is the recommended default for most continuous Spark writes. Use
`provider.write_mode: KustoStreaming` only when the destination table/database is
configured for ADX streaming ingestion and the lower-latency tradeoff is
intentional.

## Reads

ADX entities can also be used as pipe *inputs*. `read_entity()` reads either a
whole table or the result of a KQL query through the same connector and auth
configuration used for writes:

```python
tags={
    "provider_type": "adx",
    "provider.auth": "managed_identity",
    "provider.cluster": "https://mycluster.region.kusto.windows.net",
    "provider.database": "MyDatabase",
    "provider.table": "MyTable",
    # Optional — takes precedence over provider.table when set:
    "provider.query": "MyTable | where Amount > 0 | project-away _etl_internal",
}
```

Notes:

- Reads require database **Viewer** permissions for the configured identity.
  Write-only entities keep working without them — the persist path never reads,
  and `check_entity_exists()` still honors `provider.assume_exists`.
- Large scans use the connector's distributed read mode. To force small results
  through the driver (reference-data lookups), pass
  `provider.option.readMode: ForceSingleMode`.
