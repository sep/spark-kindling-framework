# Configuration Examples

This directory contains example YAML configuration files demonstrating Kindling's hierarchical configuration system.

## Quick Start

Upload these example configs to your artifacts storage to get started:

```bash
# Upload to your artifacts storage
# For Azure storage (all platforms):
az storage blob upload-batch \
  --account-name <storage-account> \
  --destination artifacts \
  --destination-path config \
  --source ./examples/config \
  --auth-mode login
```

## File Structure

```
config/
├── platform_fabric.yaml        # Microsoft Fabric platform settings
├── platform_synapse.yaml       # Azure Synapse Analytics platform settings
├── platform_databricks.yaml    # Databricks platform settings
└── workspace_example.yaml      # Workspace-specific settings template
```

## Configuration Priority

Files are loaded in this order (lowest to highest priority):

1. **settings.yaml** - Base framework settings
2. **platform_{platform}.yaml** - Platform-specific (fabric, synapse, databricks)
3. **workspace_{workspace_id}.yaml** - Workspace-specific
4. **env_{environment}.yaml** - Environment-specific (dev, prod, etc.)
5. **Bootstrap Config** - In-memory overrides (highest priority)

## Usage

### Basic Setup (settings + environment)

Minimal configuration with just base settings and environment overrides:

```yaml
# config/settings.yaml
kindling:
  version: "0.6.0"
  telemetry:
    logging:
      level: INFO

# config/env_prod.yaml
kindling:
  telemetry:
    logging:
      level: WARN  # Less verbose in production
```

### Platform-Specific Settings

Add platform configs when deploying to multiple platforms:

```yaml
# config/platform_fabric.yaml
kindling:
  platform:
    name: fabric
  TELEMETRY:
    logging:
      level: DEBUG  # More verbose for Fabric diagnostic logs
  extensions:
    - kindling-otel-azure>=0.3.0

# config/platform_databricks.yaml
kindling:
  platform:
    name: databricks
  TELEMETRY:
    logging:
      level: INFO  # Less verbose, stdout is well captured
  SPARK_CONFIGS:
    spark.databricks.delta.optimizeWrite.enabled: "true"
```

### Workspace-Specific Settings

Add workspace configs for team/region/cost-center separation:

```yaml
# config/workspace_abc123.yaml (Team A workspace)
kindling:
  workspace:
    name: "Team A Production"
    team: "team-a"
  DATA:
    bronze: "abfss://bronze-team-a@..."

# config/workspace_def456.yaml (Team B workspace)
kindling:
  workspace:
    name: "Team B Production"
    team: "team-b"
  DATA:
    bronze: "abfss://bronze-team-b@..."
```

## Workspace ID Formats

Workspace IDs are auto-detected per platform:

- **Fabric:** GUID format
  - Example: `workspace_12345678-1234-1234-1234-123456789abc.yaml`
  - From: `notebookutils.runtime.context.get("currentWorkspaceId")`

- **Synapse:** Workspace name
  - Example: `workspace_mysynapsews.yaml`
  - From: `spark.conf.get("spark.synapse.workspace.name")`

- **Databricks:** Sanitized workspace URL
  - Example: `workspace_adb-123456789_azuredatabricks_net.yaml`
  - From: `spark.conf.get("spark.databricks.workspaceUrl")` (dots → underscores)

## Complete Documentation

See [docs/platform_workspace_config.md](../../docs/platform_workspace_config.md) for:
- Complete configuration reference
- Usage patterns and scenarios
- Migration guides
- Troubleshooting tips

## Testing Your Configuration

Verify config loading with this bootstrap code:

```python
BOOTSTRAP_CONFIG = {
    "artifacts_storage_path": "abfss://artifacts@<storage>.dfs.core.windows.net/",
    "environment": "dev",
    "log_level": "DEBUG",  # Override for testing
}

from kindling.bootstrap import initialize_framework
initialize_framework(BOOTSTRAP_CONFIG)

# Check what configs were loaded
from kindling.injection import get_kindling_service
from kindling.spark_config import ConfigService

config = get_kindling_service(ConfigService)
print(f"Effective log level: {config.get('kindling.TELEMETRY.logging.level')}")
print(f"Platform: {config.get('kindling.platform.name')}")
print(f"Workspace: {config.get('kindling.workspace.name')}")
```

You should see output indicating which config files were downloaded and loaded.
