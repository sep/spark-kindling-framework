# kindling-ext-adx

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
