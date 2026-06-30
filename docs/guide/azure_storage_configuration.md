# Azure Storage (ABFSS) Configuration

Guide for connecting Spark to Azure Data Lake Storage Gen2 (ABFSS) paths in the Kindling framework.

## Zero-Config Local Development (Recommended)

For local development, Kindling automatically configures Azure CLI token authentication when
`az login` has been run and the required JARs are present. No manual Spark configuration is needed.

### Step 1: Log in to Azure

```bash
az login
```

For Azure Government Cloud, set the cloud target first:

```bash
az cloud set --name AzureUSGovernment
az login
```

### Step 2: Download ABFSS JARs

```bash
kindling env ensure
```

This downloads `hadoop-azure`, `hadoop-azure-datalake`, and `kindling-abfss-local-auth.jar` into
`/tmp/hadoop-jars/`. Safe to re-run — already-present JARs are skipped.

### Step 3: Verify prerequisites

```bash
kindling env check --local
```

### Step 4: Run your app

```bash
kindling pipeline run bronze.ingest_myproject
```

Kindling's `StandaloneService` detects `az` on the PATH and automatically sets:

```
spark.hadoop.fs.azure.account.auth.type = Custom
spark.hadoop.fs.azure.account.oauth.provider.type = io.kindling.abfss.AzureCliTokenProvider
```

Tokens are acquired via `az account get-access-token` at runtime — no secrets in code, no
manual token refresh.

---

## Opting Out of Automatic Azure CLI Auth

The automatic injection can be disabled per-app by setting `kindling.standalone.abfss_az_cli_auth`
to `false` in your settings:

```yaml
# settings.yaml
kindling:
  standalone:
    abfss_az_cli_auth: false
```

Or via the `kindling config set` command:

```bash
kindling config set kindling.standalone.abfss_az_cli_auth false
```

With this flag disabled, the `AzureCliTokenProvider` is not injected and you must supply
authentication configuration manually (see below).

---

## ABFSS Path Format

```
abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
```

Example: `abfss://data@mystorageacct.dfs.core.windows.net/bronze/sales/`

**Azure Government Cloud**:

```
abfss://<container>@<storage-account>.dfs.core.usgovcloudapi.net/<path>
```

---

## Manual Authentication Configuration

When you need to supply credentials explicitly (production deployments, CI/CD, or when the
zero-config path is opted out), set Spark Hadoop properties using the
`spark.hadoop.fs.azure.*` prefix.

> The correct prefix is `spark.hadoop.fs.azure.*` — not `fs.azure.*`.
> Kindling passes these through to Hadoop via Spark's configuration bridge.

### Service Principal (OAuth 2.0)

Recommended for production and CI/CD pipelines.

Place credentials in environment variables or a secrets manager — never hardcode them.

```python
import os
import kindling

storage_account = os.environ["AZURE_STORAGE_ACCOUNT"]
tenant_id       = os.environ["AZURE_TENANT_ID"]
client_id       = os.environ["AZURE_CLIENT_ID"]
client_secret   = os.environ["AZURE_CLIENT_SECRET"]
dfs_endpoint    = f"{storage_account}.dfs.core.windows.net"

kindling.initialize({
    "platform_environment": "standalone",
    "spark_configs": {
        f"spark.hadoop.fs.azure.account.auth.type.{dfs_endpoint}": "OAuth",
        f"spark.hadoop.fs.azure.account.oauth.provider.type.{dfs_endpoint}":
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"spark.hadoop.fs.azure.account.oauth2.client.id.{dfs_endpoint}":
            client_id,
        f"spark.hadoop.fs.azure.account.oauth2.client.secret.{dfs_endpoint}":
            client_secret,
        f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{dfs_endpoint}":
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }
})
```

For Azure Government Cloud, replace the DFS endpoint suffix and the token endpoint:

```python
dfs_endpoint    = f"{storage_account}.dfs.core.usgovcloudapi.net"
token_endpoint  = f"https://login.microsoftonline.us/{tenant_id}/oauth2/token"
```

### Storage Account Key

Simpler but less secure. Use only for development or testing.

```python
import kindling

storage_account = "mystorageacct"
account_key     = "your-storage-account-key"

kindling.initialize({
    "platform_environment": "standalone",
    "spark_configs": {
        f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net":
            account_key,
    }
})
```

### Shared Access Signature (SAS)

```python
import kindling

storage_account = "mystorageacct"
sas_token       = "your-sas-token"  # without leading '?'

kindling.initialize({
    "platform_environment": "standalone",
    "spark_configs": {
        f"spark.hadoop.fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net":
            "SAS",
        f"spark.hadoop.fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net":
            "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
        f"spark.hadoop.fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net":
            sas_token,
    }
})
```

### Managed Identity (Azure-hosted environments only)

```python
import kindling

storage_account = "mystorageacct"
dfs_endpoint    = f"{storage_account}.dfs.core.windows.net"

kindling.initialize({
    "spark_configs": {
        f"spark.hadoop.fs.azure.account.auth.type.{dfs_endpoint}": "OAuth",
        f"spark.hadoop.fs.azure.account.oauth.provider.type.{dfs_endpoint}":
            "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",
        f"spark.hadoop.fs.azure.account.oauth2.msi.tenant.{dfs_endpoint}":
            "your-tenant-id",
    }
})
```

---

## Framework Initialization API

The correct initialization API is `kindling.initialize(config)`:

```python
import kindling

kindling.initialize({
    "platform_environment": "standalone",
    "spark_configs": {
        # ... spark.hadoop.fs.azure.* keys here
    }
})
```

`kindling.initialize` is the public entrypoint exposed from `kindling/__init__.py`. It calls
`initialize_framework` internally. The legacy `bootstrap.initialize` name does not exist — do
not use it.

For app-based initialization (the standard pattern), define an `initialize(env)` function in
`app.py` and invoke it via `kindling pipeline run` or `kindling app run`:

```python
# app.py
from kindling.spark_config import configure_injector_with_config

def initialize(env="local"):
    configure_injector_with_config(
        config_files=["settings.yaml"],
        environment=env,
    )
```

Spark configs can also be placed in `settings.yaml` under `kindling.spark_configs`:

```yaml
kindling:
  spark_configs:
    spark.hadoop.fs.azure.account.auth.type.mystorageacct.dfs.core.windows.net: OAuth
    spark.hadoop.fs.azure.account.oauth.provider.type.mystorageacct.dfs.core.windows.net:
      org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
    spark.hadoop.fs.azure.account.oauth2.client.id.mystorageacct.dfs.core.windows.net:
      ${AZURE_CLIENT_ID}
    spark.hadoop.fs.azure.account.oauth2.client.secret.mystorageacct.dfs.core.windows.net:
      ${AZURE_CLIENT_SECRET}
    spark.hadoop.fs.azure.account.oauth2.client.endpoint.mystorageacct.dfs.core.windows.net:
      https://login.microsoftonline.com/${AZURE_TENANT_ID}/oauth2/token
```

---

## Entity Provider with ABFSS

Configure DeltaEntityProvider to use ABFSS paths via entity tags:

```python
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

from kindling.data_entities import DataEntities, DataEntityRegistry
from kindling.entity_provider import EntityProvider
from kindling.injection import get_kindling_service

DataEntities.entity(
    entityid="bronze.sales",
    name="Bronze Sales",
    partition_columns=[],
    merge_columns=["id"],
    tags={
        "provider_type": "delta",
        "provider.path": "abfss://data@mystorageacct.dfs.core.windows.net/bronze/sales",
        "provider.access_mode": "storage",
    },
    schema=[
        StructField("id", IntegerType(), False),
        StructField("value", StringType(), True),
    ],
)

entity_registry   = get_kindling_service(DataEntityRegistry)
entity_provider   = get_kindling_service(EntityProvider)
entity            = entity_registry.get_entity_definition("bronze.sales")

df = entity_provider.read_entity(entity)
entity_provider.write_to_entity(df, entity)
```

---

## Common Issues

### Authentication failed (Forbidden)

```
java.io.IOException: GET https://mystorageacct.dfs.core.windows.net/: Operation failed: "Forbidden"
```

Ensure the identity (service principal, managed identity, or CLI user) has been granted:
- **Storage Blob Data Contributor** for read/write
- **Storage Blob Data Reader** for read-only

### ClassNotFoundException for token provider

```
java.lang.ClassNotFoundException: org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
```

The hadoop-azure JARs are missing from the Spark classpath. Run:

```bash
kindling env ensure
```

For the `AzureCliTokenProvider` class, the `kindling-abfss-local-auth.jar` must also be present
in `/tmp/hadoop-jars/` before the Spark session is created. `kindling env ensure` downloads it.

### Container not found

```
Status 404 (The specified container does not exist)
```

Verify the container name in the ABFSS URI and that it exists in the storage account.

---

## Security Best Practices

1. Never commit credentials to source control.
2. Use environment variables or Azure Key Vault for secrets.
3. Prefer Managed Identity when running in Azure services.
4. Use Service Principal with least-privilege roles for external access.
5. Rotate secrets regularly.
6. Use SAS tokens with expiration dates for temporary access.

---

## Resources

- [Azure Data Lake Storage Gen2 Documentation](https://docs.microsoft.com/azure/storage/blobs/data-lake-storage-introduction)
- [Apache Hadoop ABFS connector](https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html)
- [Azure Service Principal Setup](https://docs.microsoft.com/azure/active-directory/develop/howto-create-service-principal-portal)
