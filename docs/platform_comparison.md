# Platform Service Comparison

## Overview

The Spark Kindling Framework supports multiple execution platforms through a unified `PlatformService` interface. This document compares the implementations across Fabric, Synapse, Databricks, and Local platforms.

## Platform Services Summary

| Feature | Fabric | Synapse | Databricks | Local |
|---------|--------|---------|------------|-------|
| **Authentication** | mssparkutils.credentials | Azure SDK TokenCredential | dbutils/API token | N/A (dummy token) |
| **Workspace API** | Fabric REST API | Synapse Artifacts SDK | Databricks REST API | Local filesystem |
| **File Operations** | mssparkutils.fs | mssparkutils.fs | dbutils.fs | Python pathlib |
| **Notebook Format** | Fabric definition (JSON/Base64) | Synapse SDK models | Databricks SOURCE/JUPYTER | Python files (.py) |
| **Cache System** | Items + Folders | Notebooks + Folders | Recursive workspace scan | Filesystem scan |
| **Spark Mode** | Fabric Spark Pool | Synapse Spark Pool | Databricks Cluster | Local[*] mode |

## Key Implementation Patterns

### 1. Initialization & Configuration

#### Fabric
```python
def __init__(self, config, logger):
    self.workspace_id = self._get_workspace_id()  # From config or runtime
    self._base_url = "https://api.fabric.microsoft.com/v1/"
    self._initialize_cache()  # Fetch items + folders via REST
```

#### Synapse
```python
def __init__(self, config, logger):
    self._base_url = self._build_base_url()  # From Spark config
    self.credential = SynapseTokenCredential()
    self.client = ArtifactsClient(endpoint, credential)
    self._initialize_cache()  # Use SDK to list notebooks
```

#### Databricks
```python
def __init__(self, config, logger):
    self._base_url = self._build_base_url()  # From workspace_id or auto-detect
    self._initialize_cache()  # Recursive workspace list via REST API
```

#### Local
```python
def __init__(self, config, logger):
    self.workspace_id = None
    self.local_workspace_path = Path('./local_workspace')
    self._initialize_cache()  # Scan filesystem for .py files
```

### 2. Token/Authentication

#### Fabric
- Uses `mssparkutils.credentials.getToken(audience)`
- Caches token with 24-hour expiration
- Audience: `"https://api.fabric.microsoft.com/.default"`

#### Synapse
- Custom `SynapseTokenCredential` class implementing Azure SDK interface
- Uses `mssparkutils.credentials.getToken("Synapse")`
- Returns `AccessToken` object with token + expiration

#### Databricks
- Uses `dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()`
- Fallback to `DATABRICKS_TOKEN` environment variable
- Caches token with 23-hour expiration

#### Local
- Returns dummy token `"local-dummy-token"`
- No real authentication needed for local development

### 3. File Operations

#### Fabric & Synapse
Both use `mssparkutils.fs` utilities:
- `exists(path)` → `mssparkutils.fs.exists(path)`
- `copy(src, dst)` → `mssparkutils.fs.cp(src, dst, overwrite)`
- `read(path)` → `mssparkutils.fs.head(path, max_bytes)` (Fabric) or `open(path)` (Synapse)
- `write(path, content)` → `mssparkutils.fs.put(path, content)` (Fabric) or `open(path)` (Synapse)
- `list(path)` → `mssparkutils.fs.ls(path)`

#### Databricks
Uses `dbutils.fs` for DBFS paths:
- `exists(path)` → Try `dbutils.fs.ls(path)`, catch exception
- `copy(src, dst)` → `dbutils.fs.cp(src, dst, overwrite)`
- `read(path)` → `dbutils.fs.head(path)` for DBFS, `open()` for local
- `write(path, content)` → Temp file + `dbutils.fs.cp()` for DBFS
- `list(path)` → `dbutils.fs.ls(path)`

#### Local
Uses Python standard library:
- `exists(path)` → `Path(path).exists()`
- `copy(src, dst)` → `shutil.copy2(src, dst)`
- `read(path)` → `open(path).read()`
- `write(path, content)` → `open(path).write(content)`
- `list(path)` → `Path(path).iterdir()`

### 4. Notebook Operations

#### Fabric
- **List**: REST GET `/workspaces/{id}/notebooks`
- **Get**: REST GET + POST `/items/{id}/getDefinition` (async operation)
- **Create/Update**: REST POST with async polling
- **Format**: Base64-encoded notebook definition with parts (ipynb/py)

#### Synapse
- **List**: SDK `client.notebook.get_notebooks_by_workspace()`
- **Get**: SDK `client.notebook.get_notebook(name)`
- **Create/Update**: SDK `client.notebook.begin_create_or_update_notebook()`
- **Format**: Synapse `NotebookResource` with properties (cells, metadata)

#### Databricks
- **List**: REST GET `/api/2.0/workspace/list` (recursive)
- **Get**: REST GET `/api/2.0/workspace/get-status` + `/export`
- **Create/Update**: REST POST `/api/2.0/workspace/import`
- **Format**: Base64-encoded JUPYTER or SOURCE format

#### Local
- **List**: Filesystem glob `*.py` files
- **Get**: Read Python file, parse cells
- **Create/Update**: Write Python file with MAGIC markers
- **Format**: Python source with `# COMMAND ----------` separators

### 5. Cache Strategy

#### Fabric
```python
# Fetch all items
items = GET /workspaces/{id}/items
# Separately fetch folders (items endpoint doesn't return them)
folders = GET /workspaces/{id}/folders
# Cache notebooks and folders separately
```

#### Synapse
```python
# Get notebooks via SDK
notebooks = client.notebook.get_notebooks_by_workspace()
# Extract folder info from notebook properties
for notebook in notebooks:
    if notebook.properties.folder:
        cache_folder(notebook.properties.folder.name)
```

#### Databricks
```python
# Start with root workspace
items = GET /api/2.0/workspace/list?path=/
# Recursively process subdirectories
for item in items:
    if item.object_type == 'DIRECTORY':
        process_directory(item.path)
```

#### Local
```python
# Scan filesystem recursively
notebooks = local_workspace_path.rglob("*.py")
# Infer folder structure from file paths
for notebook_file in notebooks:
    folder_name = notebook_file.parent.name
```

### 6. Spark Session Access

All platforms follow similar pattern:
```python
def get_spark_session(self):
    import __main__
    return getattr(__main__, 'spark', None)
```

#### Local Enhancement
```python
def get_cluster_info(self):
    spark = self.get_spark_session()
    return {
        'master': spark.sparkContext.master,  # e.g., "local[*]"
        'executor_memory': spark.conf.get('spark.executor.memory'),
        'driver_memory': spark.conf.get('spark.driver.memory')
    }
```

### 7. Workspace Information

#### Fabric
```python
return {
    'workspace_id': mssparkutils.env.getWorkspaceId(),
    'environment': 'fabric'
}
```

#### Synapse
```python
return {
    'workspace_id': mssparkutils.env.getWorkspaceId(),
    'workspace_name': mssparkutils.env.getWorkspaceName(),
    'workspace_url': self._base_url,
    'environment': 'synapse'
}
```

#### Databricks
```python
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
return {
    'workspace_id': context.workspaceId().get(),
    'workspace_url': context.browserHostName().get(),
    'cluster_id': context.clusterId().get(),
    'environment': 'databricks'
}
```

#### Local
```python
return {
    'workspace_id': None,
    'workspace_url': 'http://localhost',
    'workspace_path': str(self.local_workspace_path),
    'environment': os.environ.get('PROJECT_ENV', 'local'),
    'platform': 'local'
}
```

## Platform Registration

All platforms now use the same registration decorator pattern:

```python
@PlatformServices.register(
    name="platform_name", 
    description="Platform description")
def create_platform_service(config, logger):
    return PlatformService(config, logger)
```

### Registration Examples

- **Fabric**: `@PlatformServices.register(name="fabric", ...)`
- **Synapse**: `@PlatformServices.register(name="synapse", ...)` (conditional on Azure SDK import)
- **Databricks**: `@PlatformServices.register(name="databricks", ...)`
- **Local**: `@PlatformServices.register(name="local", ...)`

## Local Platform Enhancements

The revised `platform_local.py` includes:

1. **Better path handling**: Support for both absolute and relative paths
2. **Config flexibility**: Handles both dict and SimpleNamespace config
3. **Enhanced Spark info**: Reports local Spark configuration details
4. **Robust file operations**: Better error handling and directory creation
5. **Platform registration**: Follows same pattern as cloud platforms
6. **Cache consistency**: Same caching mechanism as other platforms
7. **Environment detection**: Falls back to ENVIRONMENT if PROJECT_ENV not set

## Use Cases by Platform

### Fabric
- Microsoft Fabric notebooks and lakehouses
- OneLake data processing
- Power BI integration scenarios

### Synapse
- Azure Synapse Analytics workspaces
- SQL/Spark hybrid workloads
- Azure-native data warehousing

### Databricks
- Databricks workspace notebooks
- Delta Lake operations
- Multi-cloud data engineering

### Local
- **Development**: Test notebooks without cloud costs
- **CI/CD**: Run unit tests in pipelines
- **Debugging**: Quick iteration without deployment
- **Education**: Learn framework without cloud account
- **Validation**: Pre-flight checks before cloud deployment

## Best Practices

1. **Use environment-specific config**: Pass platform-appropriate workspace_id/paths
2. **Leverage caching**: Cache initialization reduces API calls
3. **Handle async operations**: Fabric/Databricks use async patterns for long operations
4. **Test locally first**: Use local platform before deploying to cloud
5. **Abstract platform details**: Use PlatformService interface, not platform-specific code

## Future Improvements

- [ ] Add AWS EMR platform support
- [ ] Implement notebook versioning across platforms
- [ ] Support for lakehouse/catalog operations
- [ ] Enhanced notebook conversion between formats
- [ ] Streaming/incremental notebook execution
- [ ] Platform-specific optimization hints
