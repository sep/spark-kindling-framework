# Azure Cloud Support Summary

## What Changed

Added support for **Azure Government Cloud** and **Azure China Cloud** in addition to Azure Commercial Cloud.

## Changes Made

### 1. Updated `spark_test_helper.py`

**New function**: `_get_azure_cloud_config(cloud)`
- Returns cloud-specific endpoints and configuration
- Supports: `public`, `government`/`gov`, `china`

**Updated functions**:
- `get_local_spark_session()` - Added `azure_cloud` parameter
- `get_local_spark_session_with_azure()` - Added `azure_cloud` parameter

### 2. Updated Documentation

**`azure_storage_configuration.md`**:
- Added cloud-specific ABFSS path formats
- Added cloud comparison table
- Added Azure CLI setup for each cloud
- Updated all examples to show cloud parameter

**`azure_storage_quickref.md`**:
- Added Government Cloud examples
- Updated path formats section
- Updated authentication methods
- Updated troubleshooting

## Supported Clouds

| Cloud | Parameter | DFS Endpoint Suffix | Token Scope |
|-------|-----------|---------------------|-------------|
| **Commercial** | `public` | `.dfs.core.windows.net` | `https://storage.azure.com/.default` |
| **Government** | `government` or `gov` | `.dfs.core.usgovcloudapi.net` | `https://storage.azure.us/.default` |
| **China** | `china` | `.dfs.core.chinacloudapi.cn` | `https://storage.azure.cn/.default` |

## Usage Examples

### Azure Commercial Cloud (Default)
```python
spark = get_local_spark_session_with_azure(
    storage_account="mystorageacct",
    auth_type="azure_cli",
    azure_cloud="public"  # or omit, defaults to public
)

df = spark.read.parquet("abfss://data@mystorageacct.dfs.core.windows.net/path/")
```

### Azure Government Cloud
```python
spark = get_local_spark_session_with_azure(
    storage_account="mygovacct",
    auth_type="azure_cli",
    azure_cloud="government"  # or "gov"
)

df = spark.read.parquet("abfss://data@mygovacct.dfs.core.usgovcloudapi.net/path/")
```

### Azure China Cloud
```python
spark = get_local_spark_session_with_azure(
    storage_account="mychinaacct",
    auth_type="azure_cli",
    azure_cloud="china"
)

df = spark.read.parquet("abfss://data@mychinaacct.dfs.core.chinacloudapi.cn/path/")
```

## Azure CLI Setup

### Commercial Cloud
```bash
az cloud set --name AzureCloud
az login
```

### Government Cloud
```bash
az cloud set --name AzureUSGovernment
az login
```

### China Cloud
```bash
az cloud set --name AzureChinaCloud
az login
```

## Benefits

✅ **No hardcoded endpoints** - Cloud-specific suffixes automatically applied
✅ **Correct token scopes** - Uses cloud-appropriate authentication endpoints
✅ **Government Cloud support** - Full support for .usgovcloudapi.net
✅ **China Cloud support** - Full support for .chinacloudapi.cn
✅ **Backward compatible** - Defaults to public cloud if not specified
✅ **Clear error messages** - Helpful guidance if cloud parameter is invalid

## Breaking Changes

**None** - The `azure_cloud` parameter defaults to `"public"`, maintaining backward compatibility with existing code.

## Testing

To test with Government Cloud:

```python
# Set environment
export AZURE_STORAGE_ACCOUNT=mygovacct
export AZURE_CONTAINER=data

# Login to Gov Cloud
az cloud set --name AzureUSGovernment
az login

# Run tests
pytest tests/integration/test_azure_storage_example.py -v \
    --azure-cloud=government
```

Or programmatically:

```python
from tests.spark_test_helper import get_local_spark_session_with_azure

spark = get_local_spark_session_with_azure(
    storage_account="mygovacct",
    auth_type="azure_cli",
    azure_cloud="gov"
)

# Test read
df = spark.read.parquet("abfss://data@mygovacct.dfs.core.usgovcloudapi.net/test/")
assert df.count() > 0
print("✓ Government Cloud connection successful!")
```

## Important Notes

1. **Tenant IDs differ** between clouds
2. **Service principals** must be created in the target cloud
3. **Storage accounts** must exist in the target cloud
4. **Azure CLI** must be set to correct cloud before login
5. **ABFSS paths** must use the correct endpoint suffix

## Files Modified

- `/workspace/tests/spark_test_helper.py` - Added cloud support
- `/workspace/docs/azure_storage_configuration.md` - Updated with cloud examples
- `/workspace/docs/azure_storage_quickref.md` - Updated quick reference

## References

- [Azure Government Cloud Documentation](https://docs.microsoft.com/en-us/azure/azure-government/)
- [Azure China Cloud Documentation](https://docs.microsoft.com/en-us/azure/china/)
- [Azure Storage ABFSS Documentation](https://hadoop.apache.org/docs/stable/hadoop-azure/abfs.html)
