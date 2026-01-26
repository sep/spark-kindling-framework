# Developer Workflow

## Quick Start with poethepoet

This project uses [poethepoet](https://poethepoet.natn.io/) as a task runner, similar to npm scripts. All development tasks are defined in `pyproject.toml` under `[tool.poe.tasks]`.

### Available Tasks

```bash
# Build platform-specific wheels
poetry run poe build

# Deploy wheels to Azure Storage
poetry run poe deploy                           # Deploy all platforms
poetry run poe deploy --platform synapse        # Deploy only synapse wheel
poetry run poe deploy --platform databricks     # Deploy only databricks wheel
poetry run poe deploy --platform fabric         # Deploy only fabric wheel

# Or use dedicated tasks:
poetry run poe deploy-synapse                   # Same as --platform synapse
poetry run poe deploy-databricks                # Same as --platform databricks
poetry run poe deploy-fabric                    # Same as --platform fabric

# Deploy from GitHub release (production)
poetry run poe deploy-release                   # Deploy all platforms from release

# Run tests with coverage
poetry run poe test

# Format code with black
poetry run poe format

# Lint code with pylint
poetry run poe lint

# Run all checks (format, lint, test)
poetry run poe check
```

## Build & Deploy Workflow

### 1. Local Development Testing

```bash
# Make your changes to packages/kindling/

# Build wheels
poetry run poe build

# Deploy to Azure Storage for testing
az login  # One-time authentication
poetry run poe deploy                         # Deploy all platforms

# OR deploy individual platforms:
poetry run poe deploy --platform synapse      # Deploy only synapse
poetry run poe deploy --platform databricks   # Deploy only databricks
poetry run poe deploy --platform fabric       # Deploy only fabric
```

This builds all 3 platform wheels and uploads them to:
```
sepstdatalakedev/artifacts/packages/
  ├── kindling_databricks-0.2.0-py3-none-any.whl
  ├── kindling_fabric-0.2.0-py3-none-any.whl
  └── kindling_synapse-0.2.0-py3-none-any.whl
```

**Platform-specific deployment** is useful when:
- Testing changes that only affect one platform
- Faster iteration during development
- Deploying hotfixes to specific platforms

### 2. Release Deployment

```bash
# After creating a GitHub release with wheels attached:
poetry run poe deploy-release

# Or deploy a specific release version:
poetry run poe deploy-release 0.2.0
```

### 3. Advanced Usage (Direct Python)

You can also call the scripts directly for more control:

```bash
# Deploy all platforms from local
python scripts/deploy.py

# Deploy specific platform from local
python scripts/deploy.py --platform synapse
python scripts/deploy.py --platform databricks
python scripts/deploy.py --platform fabric

# Deploy from GitHub release
python scripts/deploy.py --release
python scripts/deploy.py --release 0.2.0
python scripts/deploy.py --release --platform synapse
```

## Build System Architecture

### Python Scripts (scripts/ directory)

The build and deployment tools are Python modules in the `scripts/` package:

- **`scripts/build.py`**: Builds platform-specific wheels using isolated Poetry environments
- **`scripts/deploy.py`**: Deploys wheels to Azure Storage using azure-identity SDK
- **`scripts/generate_platform_config.py`**: Generates platform-specific pyproject.toml files

### Key Features

1. **Isolated Builds**: Each platform wheel is built in a temporary directory with its own pyproject.toml
2. **Single Source Version**: Version is read from root `pyproject.toml` only
3. **Platform File Filtering**: Each wheel contains only its platform-specific implementation
4. **Azure Identity Integration**: Uses DefaultAzureCredential (supports Azure CLI, environment variables, managed identity)

### Directory Structure

```
scripts/                    # Build/deployment tools (not distributed)
  __init__.py
  build.py                 # Platform wheel builder
  deploy.py                # Azure Storage deployment
  generate_platform_config.py

packages/kindling/          # The actual library (distributed in wheels)
  __init__.py
  bootstrap.py
  platform_synapse.py
  platform_databricks.py
  platform_fabric.py
  ...
```

## Authentication Methods

### DefaultAzureCredential

The deploy script uses Azure Identity's `DefaultAzureCredential`, which automatically tries authentication methods in this order:

1. **Environment Variables** (service principal)
2. **Managed Identity** (when running in Azure)
3. **Azure CLI** (`az login`)
4. **Visual Studio Code**
5. **Azure PowerShell**

### Local Development with Service Principal

```bash
# Set environment variables
export AZURE_CLIENT_ID="<service-principal-client-id>"
export AZURE_CLIENT_SECRET="<service-principal-secret>"
export AZURE_TENANT_ID="<tenant-id>"

# Deploy
poetry run poe deploy
```

Or use a `.env` file (add to `.gitignore`!):

```bash
# Create .env
cat > .env << 'ENV'
AZURE_CLIENT_ID=<client-id>
AZURE_CLIENT_SECRET=<secret>
AZURE_TENANT_ID=<tenant-id>
ENV

# Load and deploy
source .env
poetry run poe deploy
```

### Local Development with Azure CLI

```bash
# One-time login
az login

# Deploy (uses cached credentials)
poetry run poe deploy
```

### Required Azure Permissions

The service principal needs the **Storage Blob Data Contributor** role:

```bash
az role assignment create \
  --assignee <client-id> \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/sepstdatalakedev"
```

## CI/CD Integration

### GitHub Actions

The Python scripts work seamlessly in GitHub Actions:

```yaml
name: Build and Deploy

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install Poetry
        run: pip install poetry

      - name: Install dependencies
        run: poetry install

      - name: Build wheels
        run: poetry run poe build

      - name: Deploy to Azure
        env:
          AZURE_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
          AZURE_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
          AZURE_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
        run: poetry run poe deploy
```

**Setup GitHub Secrets:**
1. Go to repository Settings → Secrets and variables → Actions
2. Add secrets:
   - `AZURE_CLIENT_ID`
   - `AZURE_CLIENT_SECRET`
   - `AZURE_TENANT_ID`

### Testing Authentication

Test service principal authentication without deploying:

```bash
python -c "
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

credential = DefaultAzureCredential()
client = BlobServiceClient(
    'https://sepstdatalakedev.blob.core.windows.net',
    credential=credential
)
container = client.get_container_client('artifacts')
print('✅ Authentication successful!')
print(f'Container: {container.container_name}')
"
```

## Version Management

The version is managed in a single location:

```toml
# pyproject.toml
[tool.poetry]
version = "0.2.0"  # Single source of truth
```

Platform-specific builds automatically use this version.

## Platform-Specific Dependencies

Dependencies are configured in `scripts/generate_platform_config.py`:

```python
PLATFORM_DEPS = {
    "synapse": ['azure-synapse-artifacts = ">=0.17.0"'],
    "databricks": [],  # All in runtime
    "fabric": [],      # All in runtime
}
```

Only Synapse requires additional packages not provided by the runtime.

## Testing

```bash
# Run all tests with coverage
poetry run poe test

# Run specific test file
poetry run pytest tests/test_bootstrap.py -v

# Run with coverage report
poetry run pytest --cov=kindling --cov-report=html
```

## Code Quality

```bash
# Format code (auto-fix)
poetry run poe format

# Lint code (check only)
poetry run poe lint

# Run all checks
poetry run poe check
```
