# Azure Storage Quick Reference

## 🚀 Quick Setup (5 minutes)

### 1. Install & Login
```bash
# Install Azure CLI (one-time setup)
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

# Login with your account
az login

# Install Python package
pip install azure-identity
```

### 2. Use in Your Code
```python
from azure.identity import DefaultAzureCredential

# Get token
credential = DefaultAzureCredential()
token = credential.get_token("https://storage.azure.com/.default")

# Configure Spark
storage_account = "mystorageacct"
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.AccessTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.access.token.{storage_account}.dfs.core.windows.net",
               token.token)

# Access data
df = spark.read.parquet(f"abfss://data@{storage_account}.dfs.core.windows.net/path/")
```

---

## 📝 ABFSS Path Format

### Azure Commercial Cloud (default)
```
abfss://<container>@<storage-account>.dfs.core.windows.net/<path>
```

### Azure Government Cloud
```
abfss://<container>@<storage-account>.dfs.core.usgovcloudapi.net/<path>
```

### Azure China Cloud
```
abfss://<container>@<storage-account>.dfs.core.chinacloudapi.cn/<path>
```

### Examples (Commercial Cloud)
```python
# Parquet files
"abfss://data@mystorageacct.dfs.core.windows.net/bronze/sales/"

# Delta tables
"abfss://data@mystorageacct.dfs.core.windows.net/delta/customers/"

# Specific file
"abfss://data@mystorageacct.dfs.core.windows.net/raw/2024/sales.csv"
```

### Examples (Government Cloud)
```python
# Parquet files
"abfss://data@mygovacct.dfs.core.usgovcloudapi.net/bronze/sales/"

# Delta tables
"abfss://data@mygovacct.dfs.core.usgovcloudapi.net/delta/customers/"
```

---

## 🧪 Testing with Azure Storage

### Simple Test (Commercial Cloud)
```python
from tests.spark_test_helper import get_local_spark_session_with_azure

# Create Spark session with Azure auth
spark = get_local_spark_session_with_azure(
    storage_account="mystorageacct",
    auth_type="azure_cli",  # Uses 'az login'
    azure_cloud="public"    # default
)

# Read data
df = spark.read.parquet("abfss://data@mystorageacct.dfs.core.windows.net/test/")
print(f"Rows: {df.count()}")
```

### Simple Test (Government Cloud)
```python
# Create Spark session for Gov Cloud
spark = get_local_spark_session_with_azure(
    storage_account="mygovacct",
    auth_type="azure_cli",
    azure_cloud="government"  # or "gov"
)

# Read data (note the .usgovcloudapi.net endpoint)
df = spark.read.parquet("abfss://data@mygovacct.dfs.core.usgovcloudapi.net/test/")
print(f"Rows: {df.count()}")
```

### Pytest Fixture
```python
@pytest.fixture(scope="module")
def spark_azure():
    spark = get_local_spark_session_with_azure(
        storage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
        auth_type="azure_cli"
    )
    yield spark
    spark.stop()

def test_read_data(spark_azure):
    df = spark_azure.read.parquet("abfss://...")
    assert df.count() > 0
```

---

## 🔑 Authentication Methods

### Local Development (Recommended)
```python
# Azure Commercial Cloud
auth_type="azure_cli"
azure_cloud="public"  # default

# Azure Government Cloud
auth_type="azure_cli"
azure_cloud="government"  # or "gov"
```

### CI/CD Pipeline
```python
# Commercial Cloud
auth_type="oauth"
azure_cloud="public"
tenant_id=os.getenv("AZURE_TENANT_ID")
client_id=os.getenv("AZURE_CLIENT_ID")
client_secret=os.getenv("AZURE_CLIENT_SECRET")

# Government Cloud
auth_type="oauth"
azure_cloud="government"
tenant_id=os.getenv("AZURE_GOV_TENANT_ID")  # Note: Different tenant ID
client_id=os.getenv("AZURE_CLIENT_ID")
client_secret=os.getenv("AZURE_CLIENT_SECRET")
```

### Testing Only
```python
# Uses: Storage Account Key
auth_type="key"
account_key="your-key-here"
```

---

## ⚠️ Common Issues

### "Authentication failed"
```bash
# Commercial Cloud
az login

# Government Cloud
az cloud set --name AzureUSGovernment
az login

# Verify you're logged in
az account show
```

### "Container not found"
```python
# Solution: Check container name and permissions
# Ensure you have "Storage Blob Data Contributor" or "Reader" role
```

### "Token expired"
```bash
# Solution: For long-running sessions, refresh token
# Tokens expire after ~1 hour
# See docs/azure_storage_configuration.md for auto-refresh helper
```

### "azure-identity not installed"
```bash
pip install azure-identity
```

---

## 📚 More Information

- Full Guide: `docs/azure_storage_configuration.md`
- Example Tests: `tests/integration/test_azure_storage_example.py`
- Test Helper: `tests/spark_test_helper.py`

---

## 🎯 Complete Example

```python
#!/usr/bin/env python3
"""Complete example of reading/writing Azure storage"""
from tests.spark_test_helper import get_local_spark_session_with_azure

# Setup
STORAGE_ACCOUNT = "mystorageacct"
CONTAINER = "data"

# Create Spark session
spark = get_local_spark_session_with_azure(
    storage_account=STORAGE_ACCOUNT,
    auth_type="azure_cli"
)

# Read data
input_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/sales/"
df = spark.read.parquet(input_path)
print(f"Read {df.count()} rows")

# Transform
transformed = df.filter(df.amount > 100)

# Write back
output_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/sales/"
transformed.write.mode("overwrite").parquet(output_path)
print(f"Wrote {transformed.count()} rows")

spark.stop()
```

Run it:
```bash
az login
export AZURE_STORAGE_ACCOUNT=mystorageacct
python example.py
```

✅ Done!
