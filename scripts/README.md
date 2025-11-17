# Development Scripts

This directory contains helper scripts for development workflows.

## Platform Wheel Building

### `build_platform_wheels.sh`

Builds platform-specific wheels using Poetry for each supported platform (Synapse, Databricks, Fabric).

**Features:**
- ✅ Creates single wheel per platform containing core + platform-specific code
- ✅ Uses standard Poetry build system (replaces custom 400+ line script)
- ✅ Maintains platform tag naming for app_framework.py compatibility
- ✅ Excludes other platform files to reduce wheel size
- ✅ Clean, maintainable 89-line shell script

**Usage:**

```bash
# Build all platform wheels
./scripts/build_platform_wheels.sh
```

**Output:**
```
output/wheels/
├── kindling-0.1.0-py3-none-synapse.whl      # Core + Synapse platform
├── kindling-0.1.0-py3-none-databricks.whl   # Core + Databricks platform
└── kindling-0.1.0-py3-none-fabric.whl       # Core + Fabric platform
```

**Each wheel contains:**
- Core kindling framework (data_apps.py, bootstrap.py, etc.)
- Single platform implementation (platform_synapse.py OR platform_databricks.py OR platform_fabric.py)
- Platform-specific dependencies (Azure SDKs, Databricks SDK, etc.)

**Requirements:**
- Poetry installed (`curl -sSL https://install.python-poetry.org | python3 -`)
- Platform-specific pyproject.toml files (pyproject-synapse.toml, etc.)

**Installation:**
```bash
# Install platform-specific wheel
pip install output/wheels/kindling-0.1.0-py3-none-synapse.whl
```

## Azure Development Environment Setup

### `init_azure_dev.sh`

Securely initializes your Azure development environment for any Azure cloud.

**Features:**
- ✅ Checks Azure login status
- ✅ Performs device code authentication if needed
- ✅ Auto-detects Azure cloud environment (Commercial, Government, China)
- ✅ Securely retrieves storage account key
- ✅ Sets environment variables without exposing secrets
- ✅ Validates storage account access
- ✅ Works with all Azure clouds automatically

**Usage:**

```bash
# Auto-detect cloud and use default storage account (recommended)
source scripts/init_azure_dev.sh

# With account verification (prompts to confirm/change account)
source scripts/init_azure_dev.sh --verify-account

# Explicitly set cloud environment
source scripts/init_azure_dev.sh --cloud AzureCloud              # Commercial
source scripts/init_azure_dev.sh --cloud AzureUSGovernment       # Government
source scripts/init_azure_dev.sh --cloud AzureChinaCloud         # China

# Override storage account or container
source scripts/init_azure_dev.sh --storage-account myaccount --container mycontainer

# Combine options
source scripts/init_azure_dev.sh --cloud AzureCloud --verify-account
```

**Security Features:**
- Storage key never displayed on screen
- Command history disabled during key retrieval
- Key variable cleared from memory after export
- All sensitive operations redirect output to /dev/null

**Environment Variables Set:**
- `AZURE_STORAGE_KEY` - Storage account access key (securely set)
- `AZURE_STORAGE_ACCOUNT` - Storage account name
- `AZURE_STORAGE_CONTAINER` - Default container name
- `AZURE_CLOUD_ENV` - Azure cloud environment name (AzureCloud, AzureUSGovernment, etc.)
- `AZURE_CLOUD_SIMPLE` - Simplified cloud name (public, government, china)
- `AZURE_STORAGE_ENDPOINT_SUFFIX` - Cloud-specific endpoint suffix (e.g., dfs.core.windows.net)

**Requirements:**
- Azure CLI (`az`) installed and configured
- `jq` for JSON parsing
- Access permissions to the storage account

**After Running:**

Test your setup:
```bash
python3 tests/integration/test_azure_gov_cloud_with_key.py
```

Access storage in your code:
```python
from tests.spark_test_helper import get_local_spark_session_with_azure
import os

spark = get_local_spark_session_with_azure(
    storage_account=os.getenv('AZURE_STORAGE_ACCOUNT'),
    auth_type='key',
    azure_cloud='government',
    account_key=os.getenv('AZURE_STORAGE_KEY')
)

# Read data
path = f"abfss://{os.getenv('AZURE_STORAGE_CONTAINER')}@{os.getenv('AZURE_STORAGE_ACCOUNT')}.dfs.core.usgovcloudapi.net/your/path"
df = spark.read.parquet(path)
```

**Troubleshooting:**

If you see "Not logged in":
- Follow the device code authentication prompts
- Open the provided URL in your browser
- Enter the code displayed in the terminal

If you see "Cannot access storage account":
- Verify you have permissions in Azure Portal
- Check that you're using the correct subscription
- Ensure the storage account name is correct

**Note:** Environment variables are set for the current shell session only. You'll need to source this script in each new terminal window.
