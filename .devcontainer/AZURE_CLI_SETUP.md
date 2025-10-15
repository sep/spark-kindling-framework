# Installing Azure CLI in Dev Container

## Quick Install (Current Session)

If you don't want to rebuild the container, install Azure CLI now:

```bash
# Install Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Verify installation
az --version

# Login
az login
```

## For Government Cloud

```bash
# Set cloud
az cloud set --name AzureUSGovernment

# Login
az login
```

## Permanent Installation (Requires Rebuild)

The Dockerfile has been updated to include Azure CLI and required Python packages.

**To apply the changes:**

1. Rebuild the dev container:
   - Press `F1` or `Ctrl+Shift+P`
   - Select: `Dev Containers: Rebuild Container`
   
2. Or from command line:
   ```bash
   # Exit container and rebuild
   docker-compose down
   docker-compose build
   docker-compose up -d
   ```

## What's Included After Rebuild

The updated Dockerfile now includes:
- ✅ Azure CLI
- ✅ `azure-identity` Python package
- ✅ `azure-storage-blob` Python package
- ✅ All required dependencies for Azure storage authentication

## Verify Installation

After rebuild or manual install:

```bash
# Check Azure CLI
az --version

# Check Python packages
pip list | grep azure
```

You should see:
```
azure-cli                 X.X.X
azure-identity            X.X.X
azure-storage-blob        X.X.X
...
```

## Next Steps

Once installed, you can use Azure storage:

```bash
# Login to Azure
az login

# Test with Spark
python tests/integration/test_azure_storage_example.py
```

Or use in your code:

```python
from tests.spark_test_helper import get_local_spark_session_with_azure

spark = get_local_spark_session_with_azure(
    storage_account="mystorageacct",
    auth_type="azure_cli",
    azure_cloud="public"
)

df = spark.read.parquet("abfss://data@mystorageacct.dfs.core.windows.net/path/")
```
