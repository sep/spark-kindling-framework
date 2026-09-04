# Kindling Framework - DevContainer Setup

This development container provides a complete environment for working with the Kindling framework, including Spark, Azure CLI, and all necessary Python packages.

## Quick Start

1. Open this repository in VS Code
2. Click "Reopen in Container" when prompted
3. Wait for container to build (~2-5 minutes)
4. Start developing!

## What's Included

- **Python 3.11** - Latest stable Python
- **PySpark 3.4.3** - Apache Spark in local mode
- **Java 11** - LTS version compatible with Spark 3.4
- **Azure CLI** - For Azure authentication and resource management
- **Azure Python SDKs** - `azure-identity`, `azure-storage-blob`, `azure-core`
- **Poetry** - Dependency management
- **VS Code Extensions** - Python, Pylance, Jupyter, YAML, Docker

## Spark Local Mode

The devcontainer uses **Spark Local Mode** for development - all processing happens in-memory within the container, no cluster needed.

### Why Local Mode?

- ✅ **Simple** - No cluster management or networking
- ✅ **Fast** - Direct memory access, no network overhead
- ✅ **Easy Debugging** - All logs in one place, breakpoints work
- ✅ **Matches Production** - Same Spark API as Synapse/Fabric/Databricks

### Using Spark

```python
from pyspark.sql import SparkSession

# Create session in local mode (uses all CPU cores)
spark = SparkSession.builder \
    .appName("My App") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# Use Spark normally
df = spark.read.parquet("data/my_data.parquet")
df.show()

spark.stop()
```

### Spark UI

While a Spark job is running, the Spark UI is available at: http://localhost:4040

## Azure Setup

### Authentication

The container includes Azure CLI for authentication:

```bash
# Commercial Cloud
az login

# Government Cloud
az cloud set --name AzureUSGovernment
az login

# China Cloud
az cloud set --name AzureChinaCloud
az login
```

### Using Azure Storage

After logging in with `az login`, Azure storage access works automatically:

```python
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

# Authenticate using az login credentials
credential = DefaultAzureCredential()

# Access Azure storage
account_url = "https://mystorageacct.dfs.core.windows.net"
service_client = BlobServiceClient(account_url, credential=credential)
```

## Java Version

The container uses **Java 21** for Spark 3.5-based local development.

## Rebuilding the Container

If you modify the Dockerfile or docker-compose.yml:

1. In VS Code: Press `F1` → Select **"Dev Containers: Rebuild Container"**
2. Or from terminal:
   ```bash
   docker compose down
   docker compose build --no-cache
   docker compose up -d
   ```

## Development Workflow

```bash
# Install the framework in development mode
poetry install

# Run tests
poe test-quick

# Run specific test suite
poe test-unit
poe test-integration

# Format code
poe format

# Lint code
poe lint
```

## Troubleshooting

### Java Heap Space Error
Increase driver memory in your SparkSession config:
```python
.config("spark.driver.memory", "4g")
```

### Azure CLI Not Found
Rebuild the container to ensure Azure CLI is installed.

### Reopen/Rebuild Fails With Missing `/tmp/devcontainercli-*` Compose Files
If VS Code logs mention old files under `/tmp/devcontainercli-...` or tries to
reuse a container created from missing temp compose files, remove the stale
container and rebuild from the repo's current config:

```bash
docker ps -a --filter name=spark-kindling-framework_devcontainer-devcontainer
docker rm -f spark-kindling-framework_devcontainer-devcontainer-1
docker compose -f .devcontainer/docker-compose.yml build devcontainer
```

Then reopen the workspace in the container again. This failure is usually stale
Docker state, not a broken repo Dockerfile.

### Port Conflicts
If port 4040 (Spark UI) is already in use, Spark will automatically try 4041, 4042, etc.

## Files in This Directory

- **`devcontainer.json`** - VS Code devcontainer configuration
- **`Dockerfile`** - Container image definition
- **`docker-compose.yml`** - Docker Compose setup
- **`README.md`** - This file

## Next Steps

1. Install dependencies: `poetry install`
2. Run tests to verify setup: `poe test-quick`
3. Start developing your Spark data pipelines!

For more information, see the main [README.md](../README.md) and [documentation](../docs/).
