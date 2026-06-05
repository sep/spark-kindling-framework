# Platform Storage Utilities Reference

## Overview

Each Spark platform provides storage utilities for file operations and credentials. These utilities are **automatically injected into the runtime environment** by the platform and are **NOT imported** as regular Python packages.

## Platform-Specific Utilities

### Microsoft Fabric

**Utility Name:** `mssparkutils`

**How It's Available:**
- Automatically injected into `__main__` module at runtime
- Access via: `import __main__; mssparkutils = __main__.mssparkutils`
- Also available via: `import notebookutils`

**Key Operations:**
```python
# File System Operations
mssparkutils.fs.ls(path)                    # List files
mssparkutils.fs.exists(path)                # Check existence
mssparkutils.fs.cp(src, dst, overwrite)     # Copy files
mssparkutils.fs.mv(src, dst)                # Move files
mssparkutils.fs.rm(path, recurse)           # Delete files
mssparkutils.fs.head(path, maxBytes)        # Read file content
mssparkutils.fs.put(path, content, overwrite) # Write file content

# Credentials
mssparkutils.credentials.getToken(audience) # Get OAuth token

# Runtime Context
notebookutils.runtime.context.get("currentWorkspaceId")  # Get workspace ID
```

**Storage Paths:**
- Format: `abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Files/...`
- OneLake uses ADLS Gen2 protocol (abfss)

**When Used:**
- File operations in notebooks
- Job deployment (uploading files to OneLake)
- Bootstrap shim (downloading framework wheels)
- Authentication token retrieval

---

### Azure Synapse

**Utility Name:** `mssparkutils`

**How It's Available:**
- Automatically injected into runtime
- Import via: `from notebookutils import mssparkutils`
- Access via: `import __main__; mssparkutils = __main__.mssparkutils`

**Key Operations:**
```python
# File System Operations (same as Fabric)
mssparkutils.fs.ls(path)
mssparkutils.fs.exists(path)
mssparkutils.fs.cp(src, dst, overwrite)
mssparkutils.fs.mv(src, dst)
mssparkutils.fs.rm(path, recurse)
mssparkutils.fs.head(path, maxBytes)
mssparkutils.fs.put(path, content, overwrite)

# Credentials
mssparkutils.credentials.getToken(audience)

# Notebook Operations
mssparkutils.notebook.run(path, timeout, arguments)
mssparkutils.notebook.exit(value)
```

**Storage Paths:**
- Workspace primary storage: `abfss://<container>@<storage-account>.dfs.core.windows.net/...`
- Uses Azure Data Lake Storage Gen2

**When Used:**
- File operations in Synapse notebooks
- Job deployment (uploading to workspace storage)
- Bootstrap shim (downloading wheels)
- Notebook chaining

---

### Databricks

**Utility Name:** `dbutils`

**How It's Available:**
- Automatically injected into `__main__` module at runtime
- Access via: `import __main__; dbutils = __main__.dbutils`
- Can also import: `from pyspark.dbutils import DBUtils` (for initialization)

**Key Operations:**
```python
# File System Operations (DBFS)
dbutils.fs.ls(path)                         # List files
dbutils.fs.head(path, maxBytes)             # Read file content
dbutils.fs.put(path, content, overwrite)    # Write file content
dbutils.fs.cp(src, dst, recurse)            # Copy files
dbutils.fs.mv(src, dst)                     # Move files
dbutils.fs.rm(path, recurse)                # Delete files
dbutils.fs.mkdirs(path)                     # Create directory

# Notebook Operations
dbutils.notebook.run(path, timeout, arguments)
dbutils.notebook.exit(value)

# Secrets
dbutils.secrets.get(scope, key)             # Get secret value
dbutils.secrets.list(scope)                 # List secrets

# Authentication
dbutils.notebook.entry_point.getDbutils()   # Get DB utils context
    .notebook().getContext()                 # Get notebook context
    .apiToken().get()                        # Get API token
```

**Storage Paths:**
- DBFS: `dbfs:/...` or `/dbfs/...`
- External storage: `s3://...`, `wasbs://...`, `abfss://...`

**When Used:**
- File operations in Databricks notebooks
- Job deployment (uploading to DBFS)
- Bootstrap shim (downloading wheels)
- Secrets management
- Notebook workflows

---

## Kindling Framework Usage

### Platform Detection

The framework automatically detects which utility is available:

```python
def get_storage_utils():
    """Get platform-specific storage utilities"""
    try:
        import __main__
        # Try mssparkutils (Fabric/Synapse)
        mssparkutils = getattr(__main__, "mssparkutils", None)
        if mssparkutils:
            return mssparkutils
    except:
        pass

    try:
        # Try dbutils (Databricks)
        import __main__
        dbutils = getattr(__main__, "dbutils", None)
        if dbutils:
            return dbutils
    except:
        pass

    raise RuntimeError("Could not find storage utils (mssparkutils or dbutils)")
```

### File Operations Abstraction

Platform services provide unified file operation methods:

```python
class PlatformService:
    def exists(self, path: str) -> bool:
        # Uses mssparkutils or dbutils internally
        pass

    def read(self, path: str) -> str:
        # Uses mssparkutils.fs.head or dbutils.fs.head
        pass

    def write(self, path: str, content: str) -> None:
        # Uses mssparkutils.fs.put or dbutils.fs.put
        pass

    def list(self, path: str) -> List[str]:
        # Uses mssparkutils.fs.ls or dbutils.fs.ls
        pass
```

### Bootstrap Shim Usage

The bootstrap shim uses storage utilities to install framework wheels:

```python
# In bootstrap_shim.py (generated)
storage_utils = get_storage_utils()

# List wheel files
files = storage_utils.fs.ls(artifacts_path)
wheel_files = [f.path for f in files if f.name.endswith('.whl')]

# Install each wheel
for wheel_path in wheel_files:
    subprocess.check_call([
        sys.executable, "-m", "pip", "install",
        wheel_path,
        "--disable-pip-version-check"
    ])
```

---

## Common Patterns

### Pattern 1: Conditional Import

```python
# At module level (Synapse)
try:
    from notebookutils import mssparkutils
    add_to_registry = True
except:
    mssparkutils = None
    add_to_registry = False
```

### Pattern 2: Runtime Detection

```python
# At method level (Fabric/Databricks)
def some_method(self):
    import __main__
    utils = getattr(__main__, "mssparkutils", None)
    if not utils:
        utils = getattr(__main__, "dbutils", None)

    if utils:
        # Use the utility
        files = utils.fs.ls(path)
```

### Pattern 3: Lazy Initialization

```python
# Cache utility reference
class PlatformService:
    def __init__(self):
        self._storage_utils = None

    def _get_storage_utils(self):
        if not self._storage_utils:
            import __main__
            self._storage_utils = getattr(__main__, "mssparkutils", None)
        return self._storage_utils
```

---

## Important Notes

### ⚠️ DO NOT Import Directly

**Wrong:**
```python
import mssparkutils  # ❌ This will fail
import dbutils       # ❌ This will fail
```

**Correct:**
```python
import __main__
mssparkutils = getattr(__main__, "mssparkutils", None)  # ✅
dbutils = getattr(__main__, "dbutils", None)             # ✅
```

### ⚠️ Only Available in Runtime

These utilities are **only available when running on the platform**:
- ✅ Available: In notebooks, Spark jobs, interactive clusters
- ❌ Not available: Local development, unit tests, IDE

For local testing, mock these utilities:
```python
class MockStorageUtils:
    class fs:
        @staticmethod
        def ls(path):
            return []  # Mock implementation
```

### ⚠️ Platform-Specific Differences

| Feature | Fabric | Synapse | Databricks |
|---------|--------|---------|------------|
| Utility Name | `mssparkutils` | `mssparkutils` | `dbutils` |
| Import Method | `__main__` or `notebookutils` | `notebookutils` | `__main__` |
| File System | OneLake (ADLS Gen2) | ADLS Gen2 | DBFS |
| Storage Prefix | `abfss://` | `abfss://` | `dbfs://` |
| Secrets | Via credentials API | Via credentials API | `dbutils.secrets` |
| Token Auth | `credentials.getToken()` | `credentials.getToken()` | `apiToken().get()` |

---

## Testing Without Platform Utilities

### Unit Tests

Mock the utilities:

```python
def test_something(mocker):
    # Mock mssparkutils
    mock_utils = mocker.MagicMock()
    mock_utils.fs.ls.return_value = [...]

    mocker.patch("__main__.mssparkutils", mock_utils)

    # Test code
    result = my_function()
```

### System Tests

Run on actual platform or use platform service fixtures:

```python
@pytest.fixture
def fabric_service(fabric_credentials):
    """Create real Fabric service for testing"""
    from kindling.platform_fabric import FabricService
    return FabricService(config, logger, credential)
```

---

## References

- [Fabric mssparkutils documentation](https://learn.microsoft.com/fabric/data-engineering/microsoft-spark-utilities)
- [Synapse mssparkutils documentation](https://learn.microsoft.com/azure/synapse-analytics/spark/apache-spark-azure-portal-add-libraries)
- [Databricks dbutils documentation](https://docs.databricks.com/dev-tools/databricks-utils.html)
