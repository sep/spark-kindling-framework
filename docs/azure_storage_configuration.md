# Azure Storage (ABFSS) Configuration

Guide for connecting Spark to Azure Data Lake Storage Gen2 (ABFSS) paths in the Kindling framework.

## Quick Start (Local Development) ⚡

The easiest way to connect to Azure Storage locally is using Azure CLI:

### Azure Commercial Cloud (Default)

```bash
# 1. Install Azure CLI (if not already installed)
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# 2. Login with your Azure account
az login

# 3. Install required Python package
pip install azure-identity
```

```python
# 4. Use in your code
from tests.spark_test_helper import get_local_spark_session_with_azure

# Create Spark session with Azure auth
spark = get_local_spark_session_with_azure(
    storage_account="mystorageacct",
    auth_type="azure_cli",
    azure_cloud="public"  # default
)

# 5. Access your data!
df = spark.read.parquet("abfss://data@mystorageacct.dfs.core.windows.net/bronze/sales/")
```

### Azure Government Cloud

```bash
# 1. Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# 2. Set cloud to Government
az cloud set --name AzureUSGovernment

# 3. Login with Government account
az login

# 4. Install required Python package
pip install azure-identity
```

```python
# Use in your code
spark = get_local_spark_session_with_azure(
    storage_account="mygovacct",
    auth_type="azure_cli",
    azure_cloud="government"  # or "gov"
)

# Access your data with .usgovcloudapi.net endpoint
df = spark.read.parquet("abfss://data@mygovacct.dfs.core.usgovcloudapi.net/bronze/sales/")
```

**Benefits**:
- ✅ No secrets or credentials in code
- ✅ Uses your existing Azure permissions
- ✅ Works across local dev, CI/CD, and Azure environments
- ✅ Tokens automatically managed

---

## Overview

ABFSS (Azure Blob File System Secure) is the URI scheme for accessing Azure Data Lake Storage Gen2 from Spark.

### ABFSS Path Formats by Cloud

**Azure Commercial Cloud** (default):
```
abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
```
Example: `abfss://data@mystorageacct.dfs.core.windows.net/bronze/sales/`

**Azure Government Cloud**:
```
abfss://<container>@<storage-account>.dfs.core.usgovcloudapi.net/<path>
```
Example: `abfss://data@mygovacct.dfs.core.usgovcloudapi.net/bronze/sales/`

**Azure China Cloud**:
```
abfss://<container>@<storage-account>.dfs.core.chinacloudapi.cn/<path>
```
Example: `abfss://data@mychinaacct.dfs.core.chinacloudapi.cn/bronze/sales/`

## Authentication Methods

### 1. Azure CLI Authentication (Recommended for Local Development) ⭐

Use your Azure CLI login for seamless local development. No secrets needed!

**Prerequisites**:
```bash
# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Login
az login

# Verify you're logged in
az account show
```

**Python Implementation**:
```python
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient

def get_azure_cli_token():
    """Get access token using Azure CLI credentials"""
    credential = AzureCliCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    return token.token

# Configuration
storage_account = "mystorageacct"

# Get token from Azure CLI
access_token = get_azure_cli_token()

# Configure Spark to use the token
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.access.token.{storage_account}.dfs.core.windows.net",
               access_token)

# Test connection
df = spark.read.parquet(f"abfss://data@{storage_account}.dfs.core.windows.net/test/")
```

**Note**: Tokens expire after ~1 hour. For long-running sessions, you'll need to refresh.

### 2. DefaultAzureCredential (Best for All Environments) ⭐⭐

Works locally with Azure CLI, in Azure with Managed Identity, and with environment variables.

```python
from azure.identity import DefaultAzureCredential

# Cloud-specific token scopes
CLOUD_SCOPES = {
    'public': 'https://storage.azure.com/.default',
    'government': 'https://storage.azure.us/.default',
    'china': 'https://storage.azure.cn/.default'
}

# Cloud-specific DFS suffixes
CLOUD_SUFFIXES = {
    'public': 'dfs.core.windows.net',
    'government': 'dfs.core.usgovcloudapi.net',
    'china': 'dfs.core.chinacloudapi.cn'
}

def get_azure_token(azure_cloud='public', credential=None):
    """
    Get Azure storage token using DefaultAzureCredential.

    Tries in order:
    1. Environment variables (AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET)
    2. Managed Identity (if running in Azure)
    3. Azure CLI (if logged in locally)
    4. Visual Studio Code
    5. Azure PowerShell

    Args:
        azure_cloud: 'public', 'government', or 'china'
    """
    if credential is None:
        credential = DefaultAzureCredential()

    token_scope = CLOUD_SCOPES.get(azure_cloud, CLOUD_SCOPES['public'])
    token = credential.get_token(token_scope)
    return token.token

# Configuration for Azure Commercial Cloud
storage_account = "mystorageacct"
azure_cloud = "public"
access_token = get_azure_token(azure_cloud)
dfs_suffix = CLOUD_SUFFIXES[azure_cloud]

# Configure Spark
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.{dfs_suffix}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.{dfs_suffix}",
               "org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.access.token.{storage_account}.{dfs_suffix}",
               access_token)

# Test connection
df = spark.read.parquet(f"abfss://data@{storage_account}.{dfs_suffix}/test/")
```

**For Azure Government Cloud**:
```python
# Use government cloud
storage_account = "mygovacct"
azure_cloud = "government"
access_token = get_azure_token(azure_cloud)
dfs_suffix = CLOUD_SUFFIXES[azure_cloud]  # dfs.core.usgovcloudapi.net

# Configure and use
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.{dfs_suffix}", "OAuth")
# ... rest of config
df = spark.read.parquet(f"abfss://data@{storage_account}.{dfs_suffix}/test/")
```

### 3. Service Principal (OAuth 2.0)

Best for production environments and CI/CD pipelines.

```python
# Configuration
storage_account = "mystorageacct"
container = "data"
tenant_id = "your-tenant-id"
client_id = "your-client-id"
client_secret = "your-client-secret"

# Spark configuration
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
               client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
               client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Test connection
df = spark.read.parquet(f"abfss://{container}@{storage_account}.dfs.core.windows.net/test/")
```

### 4. Storage Account Key

Simpler but less secure. Only use for development/testing.

```python
# Configuration
storage_account = "mystorageacct"
account_key = "your-storage-account-key"

# Spark configuration
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
               account_key)

# Test connection
df = spark.read.parquet(f"abfss://data@{storage_account}.dfs.core.windows.net/test/")
```

### 5. Shared Access Signature (SAS)

Good for time-limited access to specific containers.

```python
# Configuration
storage_account = "mystorageacct"
container = "data"
sas_token = "your-sas-token"  # Without leading '?'

# Spark configuration
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net",
               sas_token)

# Test connection
df = spark.read.parquet(f"abfss://{container}@{storage_account}.dfs.core.windows.net/test/")
```

### 6. Managed Identity (Azure Services Only)

Best for Azure Databricks, Synapse, or Fabric environments.

```python
# Configuration
storage_account = "mystorageacct"

# Spark configuration
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.msi.tenant.{storage_account}.dfs.core.windows.net",
               "your-tenant-id")

# Test connection
df = spark.read.parquet(f"abfss://data@{storage_account}.dfs.core.windows.net/test/")
```

## Azure Cloud Environments

The framework supports all Azure cloud environments with automatic endpoint configuration.

### Supported Clouds

| Cloud | Parameter Value | DFS Suffix | Token Scope | Login Endpoint |
|-------|----------------|------------|-------------|----------------|
| **Commercial** | `public` | `dfs.core.windows.net` | `https://storage.azure.com/.default` | `https://login.microsoftonline.com` |
| **Government** | `government` or `gov` | `dfs.core.usgovcloudapi.net` | `https://storage.azure.us/.default` | `https://login.microsoftonline.us` |
| **China** | `china` | `dfs.core.chinacloudapi.cn` | `https://storage.azure.cn/.default` | `https://login.chinacloudapi.cn` |

### Using Different Clouds

```python
from tests.spark_test_helper import get_local_spark_session_with_azure

# Azure Commercial Cloud (default)
spark = get_local_spark_session_with_azure(
    storage_account="mystorageacct",
    azure_cloud="public"
)

# Azure Government Cloud
spark = get_local_spark_session_with_azure(
    storage_account="mygovacct",
    azure_cloud="government"  # or "gov"
)

# Azure China Cloud
spark = get_local_spark_session_with_azure(
    storage_account="mychinaacct",
    azure_cloud="china"
)
```

### Azure CLI Setup by Cloud

**Commercial Cloud:**
```bash
az cloud set --name AzureCloud  # default
az login
```

**Government Cloud:**
```bash
az cloud set --name AzureUSGovernment
az login
```

**China Cloud:**
```bash
az cloud set --name AzureChinaCloud
az login
```

### Important Notes

1. **Tenant IDs differ** between clouds - ensure you use the correct tenant ID
2. **Service principals** must be created in the target cloud
3. **Storage accounts** must exist in the target cloud
4. **Azure CLI** must be configured for the correct cloud before login

---

## Token Refresh Helper

For long-running sessions, automatically refresh Azure tokens:

```python
from azure.identity import DefaultAzureCredential
from datetime import datetime, timedelta
import threading
import time

class AzureTokenRefresher:
    """Automatically refresh Azure storage tokens for Spark"""

    def __init__(self, spark, storage_account, refresh_interval_minutes=45):
        self.spark = spark
        self.storage_account = storage_account
        self.refresh_interval = refresh_interval_minutes * 60
        self.credential = DefaultAzureCredential()
        self.running = False
        self.thread = None

    def get_token(self):
        """Get fresh token from Azure"""
        token = self.credential.get_token("https://storage.azure.com/.default")
        return token.token

    def refresh_token(self):
        """Refresh token in Spark configuration"""
        try:
            access_token = self.get_token()
            dfs_endpoint = f"{self.storage_account}.dfs.core.windows.net"

            self.spark.conf.set(
                f"fs.azure.account.auth.type.{dfs_endpoint}",
                "OAuth"
            )
            self.spark.conf.set(
                f"fs.azure.account.oauth.provider.type.{dfs_endpoint}",
                "org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider"
            )
            self.spark.conf.set(
                f"fs.azure.account.oauth2.access.token.{dfs_endpoint}",
                access_token
            )
            print(f"✓ Refreshed Azure token for {self.storage_account} at {datetime.now()}")
        except Exception as e:
            print(f"✗ Failed to refresh token: {e}")

    def _refresh_loop(self):
        """Background refresh loop"""
        while self.running:
            time.sleep(self.refresh_interval)
            if self.running:
                self.refresh_token()

    def start(self):
        """Start automatic token refresh"""
        if not self.running:
            # Initial refresh
            self.refresh_token()

            # Start background thread
            self.running = True
            self.thread = threading.Thread(target=self._refresh_loop, daemon=True)
            self.thread.start()
            print(f"Started token refresh (every {self.refresh_interval//60} minutes)")

    def stop(self):
        """Stop automatic token refresh"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        print("Stopped token refresh")

# Usage
refresher = AzureTokenRefresher(spark, "mystorageacct", refresh_interval_minutes=45)
refresher.start()

# Your long-running code here...
# Tokens will refresh automatically

# When done
refresher.stop()
```

## Kindling Framework Integration

### Option 1: Bootstrap Configuration with Azure CLI (Local Dev) ⭐

Best for local development - uses your Azure CLI login:

```python
from azure.identity import DefaultAzureCredential

def get_azure_token():
    """Get token using Azure CLI or other available credentials"""
    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    return token.token

# Get token
storage_account = "mystorageacct"
access_token = get_azure_token()

BOOTSTRAP_CONFIG = {
    'platform_environment': 'local',
    'spark_configs': {
        # Azure CLI authentication
        f'fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net': 'OAuth',
        f'fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net':
            'org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider',
        f'fs.azure.account.oauth2.access.token.{storage_account}.dfs.core.windows.net':
            access_token,

        # Delta Lake settings
        'spark.databricks.delta.schema.autoMerge.enabled': 'true'
    }
}

# Initialize framework
from kindling import bootstrap
bootstrap.initialize(BOOTSTRAP_CONFIG)
```

### Option 1b: Bootstrap with Service Principal (Production)

For production environments with service principal:

```python
BOOTSTRAP_CONFIG = {
    'platform_environment': 'production',
    'spark_configs': {
        # Service principal authentication
        'fs.azure.account.auth.type.mystorageacct.dfs.core.windows.net': 'OAuth',
        'fs.azure.account.oauth.provider.type.mystorageacct.dfs.core.windows.net':
            'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
        'fs.azure.account.oauth2.client.id.mystorageacct.dfs.core.windows.net':
            'your-client-id',
        'fs.azure.account.oauth2.client.secret.mystorageacct.dfs.core.windows.net':
            'your-client-secret',
        'fs.azure.account.oauth2.client.endpoint.mystorageacct.dfs.core.windows.net':
            'https://login.microsoftonline.com/your-tenant-id/oauth2/token',

        # Delta Lake settings
        'spark.databricks.delta.schema.autoMerge.enabled': 'true'
    }
}

# Initialize framework
from kindling import bootstrap
bootstrap.initialize(BOOTSTRAP_CONFIG)
```

### Option 2: Environment Variables

Use environment variables for sensitive values:

```bash
# .env file
AZURE_STORAGE_ACCOUNT=mystorageacct
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
```

```python
import os

storage_account = os.getenv('AZURE_STORAGE_ACCOUNT')
tenant_id = os.getenv('AZURE_TENANT_ID')
client_id = os.getenv('AZURE_CLIENT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')

BOOTSTRAP_CONFIG = {
    'spark_configs': {
        f'fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net': 'OAuth',
        f'fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net':
            'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
        f'fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net':
            client_id,
        f'fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net':
            client_secret,
        f'fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net':
            f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
    }
}
```

### Option 3: Configuration File (Dynaconf)

```yaml
# settings.yaml
AZURE:
  storage_account: mystorageacct
  tenant_id: ${AZURE_TENANT_ID}
  client_id: ${AZURE_CLIENT_ID}
  client_secret: ${AZURE_CLIENT_SECRET}

SPARK_CONFIGS:
  fs.azure.account.auth.type.mystorageacct.dfs.core.windows.net: OAuth
  fs.azure.account.oauth.provider.type.mystorageacct.dfs.core.windows.net:
    org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
```

```yaml
# development.yaml (environment-specific overrides)
AZURE:
  storage_account: devstorageacct
```

```yaml
# production.yaml (environment-specific overrides)
AZURE:
  storage_account: prodstorageacct
```

```python
from kindling.spark_config import configure_injector_with_config

# Load configuration
configure_injector_with_config(
    config_files=['settings.yaml'],
    environment='development'
)
```

## Testing with ABFSS

### Update spark_test_helper.py

Add ABFSS configuration to test helper with Azure CLI support:

```python
# tests/spark_test_helper.py

def get_local_spark_session_with_azure(
    app_name="KindlingTest",
    storage_account=None,
    auth_type="azure_cli",  # Default to Azure CLI for local dev
    **auth_params
):
    """
    Create Spark session with Azure storage authentication.

    Args:
        app_name: Application name
        storage_account: Azure storage account name
        auth_type: 'azure_cli' (default), 'key', 'oauth', 'sas', or 'msi'
        **auth_params: Authentication parameters:
            - For 'azure_cli': None needed (uses az login)
            - For 'key': account_key
            - For 'oauth': client_id, client_secret, tenant_id
            - For 'sas': sas_token
            - For 'msi': tenant_id
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    # Add Azure storage authentication
    if storage_account:
        dfs_endpoint = f"{storage_account}.dfs.core.windows.net"

        if auth_type == "azure_cli":
            # Use Azure CLI or DefaultAzureCredential
            try:
                from azure.identity import DefaultAzureCredential
                credential = DefaultAzureCredential()
                token = credential.get_token("https://storage.azure.com/.default")
                access_token = token.token

                builder = (
                    builder
                    .config(f"fs.azure.account.auth.type.{dfs_endpoint}", "OAuth")
                    .config(f"fs.azure.account.oauth.provider.type.{dfs_endpoint}",
                           "org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider")
                    .config(f"fs.azure.account.oauth2.access.token.{dfs_endpoint}",
                           access_token)
                )
                print(f"✓ Using Azure CLI authentication for {storage_account}")
            except ImportError:
                print("⚠ azure-identity not installed. Run: pip install azure-identity")
                raise
            except Exception as e:
                print(f"⚠ Azure CLI auth failed: {e}")
                print("  Make sure you're logged in: az login")
                raise

        elif auth_type == "key":
            account_key = auth_params.get('account_key')
            builder = builder.config(
                f"fs.azure.account.key.{dfs_endpoint}",
                account_key
            )

        elif auth_type == "oauth":
            tenant_id = auth_params.get('tenant_id')
            client_id = auth_params.get('client_id')
            client_secret = auth_params.get('client_secret')

            builder = (
                builder
                .config(f"fs.azure.account.auth.type.{dfs_endpoint}", "OAuth")
                .config(f"fs.azure.account.oauth.provider.type.{dfs_endpoint}",
                       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
                .config(f"fs.azure.account.oauth2.client.id.{dfs_endpoint}",
                       client_id)
                .config(f"fs.azure.account.oauth2.client.secret.{dfs_endpoint}",
                       client_secret)
                .config(f"fs.azure.account.oauth2.client.endpoint.{dfs_endpoint}",
                       f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
            )

        elif auth_type == "sas":
            sas_token = auth_params.get('sas_token')
            builder = (
                builder
                .config(f"fs.azure.account.auth.type.{dfs_endpoint}", "SAS")
                .config(f"fs.azure.sas.token.provider.type.{dfs_endpoint}",
                       "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
                .config(f"fs.azure.sas.fixed.token.{dfs_endpoint}", sas_token)
            )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark
```

### Integration Test Example

**Local Development** (using Azure CLI):
```python
# tests/integration/test_azure_storage.py
import pytest
import os
from tests.spark_test_helper import get_local_spark_session_with_azure

@pytest.fixture(scope="module")
def spark_with_azure():
    """Spark session configured for Azure storage using Azure CLI"""
    spark = get_local_spark_session_with_azure(
        app_name="AzureStorageTest",
        storage_account=os.getenv("AZURE_STORAGE_ACCOUNT", "mystorageacct"),
        auth_type="azure_cli"  # Uses 'az login' credentials
    )
    yield spark
    spark.stop()
```

**CI/CD Pipeline** (using service principal):
```python
@pytest.fixture(scope="module")
def spark_with_azure():
    """Spark session configured for Azure storage using service principal"""
    spark = get_local_spark_session_with_azure(
        app_name="AzureStorageTest",
        storage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
        auth_type="oauth",
        tenant_id=os.getenv("AZURE_TENANT_ID"),
        client_id=os.getenv("AZURE_CLIENT_ID"),
        client_secret=os.getenv("AZURE_CLIENT_SECRET")
    )
    yield spark
    spark.stop()

def test_read_from_abfss(spark_with_azure):
    """Test reading from ABFSS path"""
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container = "data"
    path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/test/"

    # Read test data
    df = spark_with_azure.read.parquet(path)
    assert df.count() > 0

def test_write_to_abfss(spark_with_azure):
    """Test writing to ABFSS path"""
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container = "data"
    path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/test_output/"

    # Create test data
    test_df = spark_with_azure.createDataFrame(
        [(1, "test"), (2, "data")],
        ["id", "value"]
    )

    # Write to ABFSS
    test_df.write.mode("overwrite").parquet(path)

    # Verify
    result = spark_with_azure.read.parquet(path)
    assert result.count() == 2
```

## Entity Provider with ABFSS

Configure DeltaEntityProvider to use ABFSS paths:

```python
from pyspark.sql.types import StructField, StructType, IntegerType, StringType

from kindling.data_entities import DataEntities, DataEntityRegistry
from kindling.entity_provider import EntityProvider
from kindling.injection import get_kindling_service

# Register entity with ABFSS overrides in tags
DataEntities.entity(
    entityid="bronze.sales",
    name="Bronze Sales",
    partition_columns=[],
    merge_columns=["id"],
    tags={
        "provider_type": "delta",
        "provider.path": "abfss://data@mystorageacct.dfs.core.windows.net/bronze/sales",
        "provider.access_mode": "forPath",
    },
    schema=[
        StructField("id", IntegerType(), False),
        StructField("value", StringType(), True),
    ],
)

# Resolve registered entity + provider from DI
entity_registry = get_kindling_service(DataEntityRegistry)
entity_provider = get_kindling_service(EntityProvider)
entity = entity_registry.get_entity_definition("bronze.sales")

# Read/write using entity
df = entity_provider.read_entity(entity)
entity_provider.write_to_entity(df, entity)
```

## Common Issues & Solutions

### Issue: Authentication Failed
```
java.io.IOException: GET https://mystorageacct.dfs.core.windows.net/: Operation failed: "Forbidden"
```

**Solution**: Verify credentials and ensure service principal has proper permissions:
- Storage Blob Data Contributor (for read/write)
- Storage Blob Data Reader (for read-only)

### Issue: Container Not Found
```
Status 404 (The specified container does not exist)
```

**Solution**: Create container or verify container name in URI

### Issue: Missing Azure Dependencies
```
java.lang.ClassNotFoundException: org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
```

**Solution**: Ensure Hadoop Azure libraries are available:
```bash
# PySpark should include these by default, but if needed:
pip install azure-storage-blob azure-identity
```

### Issue: Token Expired
```
InvalidAuthenticationInfo: The specified authentication credentials are not valid
```

**Solution**: For OAuth, ensure token endpoint is correct and service principal is not expired

## Security Best Practices

1. **Never commit credentials** to source control
2. **Use environment variables** or Azure Key Vault for secrets
3. **Use Managed Identity** when running in Azure services
4. **Use Service Principal** with least privilege for external access
5. **Rotate secrets regularly**
6. **Use SAS tokens** with expiration for temporary access
7. **Enable firewall rules** on storage accounts where possible

## Resources

- [Azure Data Lake Storage Gen2 Documentation](https://docs.microsoft.com/azure/storage/blobs/data-lake-storage-introduction)
- [Spark Azure Storage Configuration](https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html)
- [Azure Service Principal Setup](https://docs.microsoft.com/azure/active-directory/develop/howto-create-service-principal-portal)
