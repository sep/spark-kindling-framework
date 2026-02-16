# Platform and Workspace Configuration

## Overview

Kindling supports a **hierarchical configuration system** with multiple layers of YAML configuration files. This allows you to organize settings from most general (base settings) to most specific (environment overrides).

## Configuration Priority

Configuration files are loaded in the following order (lowest to highest priority):

1. **`settings.yaml`** - Base framework settings (lowest priority)
2. **`platform_{platform}.yaml`** - Platform-specific settings (fabric, synapse, databricks)
3. **`workspace_{workspace_id}.yaml`** - Workspace-specific settings
4. **`env_{environment}.yaml`** - Environment-specific settings (dev, prod, etc.)
5. **SparkConf** - `spark.kindling.*` pool/session settings
6. **Bootstrap Config** - In-memory overrides from BOOTSTRAP_CONFIG dict (highest priority)

Each layer can override values from previous layers, allowing precise control over configuration at different organizational levels.

## Configuration Files

### 1. Base Settings (`settings.yaml`)

Universal framework settings that apply across all platforms, workspaces, and environments.

```yaml
kindling:
  version: "0.6.0"

  bootstrap:
    load_lake: true
    load_local: true
    required_packages: []
    extensions: []
    ignored_folders: [".git", "__pycache__", ".vscode"]

  delta:
    tablerefmode: "forName"
    optimize_write: true

  telemetry:
    logging:
      level: "INFO"
      print: true
    tracing:
      enabled: false
      print: false
```

**Upload to:** `{artifacts_storage_path}/config/settings.yaml`

### 2. Platform-Specific Settings (`platform_{platform}.yaml`)

Settings specific to a platform (Fabric, Synapse, or Databricks). These override base settings.

**Platforms:**
- `platform_fabric.yaml` - Microsoft Fabric settings
- `platform_synapse.yaml` - Azure Synapse Analytics settings
- `platform_databricks.yaml` - Databricks settings

**Example:** `platform_fabric.yaml`

```yaml
kindling:
  platform:
    name: fabric

  TELEMETRY:
    logging:
      level: DEBUG  # More verbose for Fabric diagnostic emitters

  SPARK_CONFIGS:
    spark.sql.adaptive.enabled: "true"

  extensions:
    - kindling-otel-azure>=0.3.0
```

**Upload to:** `{artifacts_storage_path}/config/platform_fabric.yaml`

See [examples/config/](../examples/config/) for complete examples of all platform configs.

### 3. Workspace-Specific Settings (`workspace_{workspace_id}.yaml`)

Settings specific to a particular workspace. Useful for:
- Team-specific configurations
- Geographic region settings
- Cost center allocations
- Security policies per workspace

**Workspace ID Format:**
- **Fabric:** GUID format (e.g., `workspace_12345678-1234-1234-1234-123456789abc.yaml`)
- **Synapse:** Workspace name (e.g., `workspace_mysynapsews.yaml`)
- **Databricks:** Sanitized workspace URL (e.g., `workspace_adb-123456789_azuredatabricks_net.yaml`)

**Example:** `workspace_12345678-1234-1234-1234-123456789abc.yaml`

```yaml
kindling:
  workspace:
    name: "Production Workspace"
    team: "Data Engineering"

  TELEMETRY:
    logging:
      level: INFO
    tracing:
      tags:
        workspace: "production"
        team: "data-eng"

  DATA:
    bronze_layer: "abfss://bronze@mystorageaccount.dfs.core.windows.net/"
    silver_layer: "abfss://silver@mystorageaccount.dfs.core.windows.net/"
```

**Upload to:** `{artifacts_storage_path}/config/workspace_{workspace_id}.yaml`

### 4. Environment-Specific Settings (`env_{environment}.yaml`)

Settings specific to an environment (dev, test, staging, prod). **Highest priority** among YAML files.

**Example:** `env_prod.yaml`

```yaml
kindling:
  TELEMETRY:
    logging:
      level: WARN  # Less verbose in production

  SPARK_CONFIGS:
    spark.executor.memory: "16g"
    spark.executor.cores: "4"

  DELTA:
    optimize_write: true
    auto_compact: true
```

**Upload to:** `{artifacts_storage_path}/config/env_prod.yaml`

## Usage Patterns

### Pattern 1: Base + Environment (Simplest)

Minimal setup with just base settings and environment overrides:

```
config/
├── settings.yaml     # Base settings
├── env_dev.yaml      # Development overrides
└── env_prod.yaml     # Production overrides
```

### Pattern 2: Base + Platform + Environment

Add platform-specific settings for multi-platform deployments:

```
config/
├── settings.yaml           # Base settings
├── platform_fabric.yaml    # Fabric-specific settings
├── platform_synapse.yaml   # Synapse-specific settings
├── platform_databricks.yaml # Databricks-specific settings
├── env_dev.yaml            # Development overrides
└── env_prod.yaml           # Production overrides
```

### Pattern 3: Full Hierarchy (Most Control)

Complete hierarchy with workspace-specific settings:

```
config/
├── settings.yaml                           # Base settings
├── platform_fabric.yaml                    # Fabric settings
├── platform_synapse.yaml                   # Synapse settings
├── workspace_abc123.yaml                   # Team A workspace
├── workspace_def456.yaml                   # Team B workspace
├── env_dev.yaml                            # Development env
├── env_staging.yaml                        # Staging env
└── env_prod.yaml                           # Production env
```

### Pattern 4: Workspace Per Environment

Workspaces aligned with environments (common pattern):

```
config/
├── settings.yaml                           # Base settings
├── platform_fabric.yaml                    # Fabric settings
├── workspace_dev-workspace-id.yaml         # Dev workspace
├── workspace_staging-workspace-id.yaml     # Staging workspace
├── workspace_prod-workspace-id.yaml        # Prod workspace
├── env_dev.yaml                            # Dev env overrides
├── env_staging.yaml                        # Staging env overrides
└── env_prod.yaml                           # Prod env overrides
```

## Configuration Detection

Kindling automatically detects platform and workspace information:

### Platform Detection

1. Checks for explicit `platform` or `platform_environment` in bootstrap config
2. Detects from storage utilities (`mssparkutils`, `dbutils`)
3. Detects from Spark session configuration

### Workspace ID Detection

**Fabric:**
```python
# From notebookutils
notebookutils.runtime.context.get("currentWorkspaceId")

# From mssparkutils
mssparkutils.env.getWorkspaceId()
```

**Synapse:**
```python
# From Spark config
spark.conf.get("spark.synapse.workspace.name")

# From mssparkutils
mssparkutils.env.getWorkspaceName()
```

**Databricks:**
```python
# From dbutils context
dbutils.entry_point.getDbutils().notebook().getContext().workspaceId().get()

# From Spark config (fallback)
spark.conf.get("spark.databricks.workspaceUrl")
```

## Bootstrap Configuration

Bootstrap config (in-memory dict) always has **highest priority** and overrides all YAML and SparkConf settings:

```python
BOOTSTRAP_CONFIG = {
    "artifacts_storage_path": "abfss://artifacts@mystorageaccount.dfs.core.windows.net/",
    "environment": "prod",
    "log_level": "DEBUG",  # Overrides all YAML log_level settings
    "use_lake_packages": True,
}

initialize_framework(BOOTSTRAP_CONFIG)
```

Spark pool/session config can also provide defaults via `spark.kindling.*`:

```text
spark.kindling.bootstrap.environment=prod
spark.kindling.bootstrap.use_lake_packages=true
spark.kindling.extensions=["kindling-otel-azure>=0.3.0"]
```

Mapping rules:
- `spark.kindling.bootstrap.<key>` -> bootstrap key `<key>`
- `spark.kindling.<key>` -> `kindling.<key>`

## Example Scenarios

### Scenario 1: Multi-Team Fabric Deployment

You have multiple teams using the same Fabric capacity with different workspaces:

```yaml
# settings.yaml - Base for all teams
kindling:
  telemetry:
    logging:
      level: INFO

# platform_fabric.yaml - Fabric-specific
kindling:
  platform:
    name: fabric
  extensions:
    - kindling-otel-azure>=0.3.0

# workspace_team-a-workspace-id.yaml - Team A settings
kindling:
  workspace:
    team: "team-a"
  DATA:
    bronze: "abfss://bronze-team-a@..."

# workspace_team-b-workspace-id.yaml - Team B settings
kindling:
  workspace:
    team: "team-b"
  DATA:
    bronze: "abfss://bronze-team-b@..."

# env_prod.yaml - Production overrides
kindling:
  telemetry:
    logging:
      level: WARN
```

**Result:** Each team gets their own data paths, but shares platform settings and production logging levels.

### Scenario 2: Cross-Platform Application

Your app runs on both Fabric and Databricks with different configurations:

```yaml
# settings.yaml - Universal settings
kindling:
  delta:
    tablerefmode: "forName"

# platform_fabric.yaml - Fabric tuning
kindling:
  TELEMETRY:
    logging:
      level: DEBUG  # Need verbose logs for diagnostic emitters

# platform_databricks.yaml - Databricks tuning
kindling:
  TELEMETRY:
    logging:
      level: INFO  # Less verbose, stdout is captured
  SPARK_CONFIGS:
    spark.databricks.delta.optimizeWrite.enabled: "true"
```

**Result:** Same app, optimized configs per platform.

### Scenario 3: Dev/Prod Separation

Separate workspaces for dev and prod environments:

```yaml
# settings.yaml - Base settings
kindling:
  delta:
    optimize_write: true

# workspace_dev-workspace.yaml - Dev workspace
kindling:
  TELEMETRY:
    logging:
      level: DEBUG
  LIMITS:
    max_executors: 5

# workspace_prod-workspace.yaml - Prod workspace
kindling:
  TELEMETRY:
    logging:
      level: ERROR
  LIMITS:
    max_executors: 50

# env_prod.yaml - Production environment overrides
kindling:
  SPARK_CONFIGS:
    spark.executor.memory: "32g"
```

**Result:** Dev workspace gets debug logging with limited executors; prod workspace gets error-only logging with high executor limits, plus larger memory from environment config.

## Best Practices

1. **Keep settings.yaml minimal** - Only truly universal settings
2. **Use platform configs for platform-specific tuning** - Spark configs, resource limits, platform features
3. **Use workspace configs for organizational boundaries** - Teams, regions, cost centers
4. **Use environment configs for deployment stages** - Dev, staging, prod
5. **All configs are optional** - Framework works with just bootstrap config if needed
6. **Document your hierarchy** - Add comments explaining why settings are at each level
7. **Version control your configs** - Track changes to configuration over time

## Troubleshooting

### Config Not Loading

Check bootstrap output for config file status:

```
Using artifacts path: abfss://artifacts@...
Platform: fabric
Workspace ID: 12345678-1234-1234-1234-123456789abc
Environment: prod
✓ Downloaded: settings.yaml
✓ Downloaded: platform_fabric.yaml
✓ Downloaded: workspace_12345678-1234-1234-1234-123456789abc.yaml
✓ Downloaded: env_prod.yaml
```

### Workspace ID Not Detected

If workspace-specific config isn't loading, check detection:

```python
# Check what workspace ID is detected
from kindling.bootstrap import _get_workspace_id_for_platform

platform = "fabric"
workspace_id = _get_workspace_id_for_platform(platform)
print(f"Detected workspace ID: {workspace_id}")
```

### Config Priority Verification

Check final config values:

```python
from kindling.injection import get_kindling_service
from kindling.spark_config import ConfigService

config = get_kindling_service(ConfigService)

# Check specific value
log_level = config.get("kindling.TELEMETRY.logging.level")
print(f"Effective log level: {log_level}")

# Check all config
all_config = config.get_all()
print(f"All config: {all_config}")
```

## Migration Guide

### From Single Config File

**Before:**
```
config/
└── settings.yaml  # Everything in one file
```

**After:**
```
config/
├── settings.yaml           # Base settings
├── platform_fabric.yaml    # Fabric-specific
└── env_prod.yaml          # Environment-specific
```

**Migration Steps:**
1. Keep existing `settings.yaml` as base
2. Extract platform-specific settings to `platform_{platform}.yaml`
3. Extract environment-specific settings to `env_{environment}.yaml`
4. Test with each layer to verify behavior

### From Bootstrap Config Only

**Before:**
```python
BOOTSTRAP_CONFIG = {
    "log_level": "INFO",
    "platform": "fabric",
    "artifacts_storage_path": "...",
    # ... 50 more settings
}
```

**After:**
```python
# Minimal bootstrap with YAML doing the work
BOOTSTRAP_CONFIG = {
    "artifacts_storage_path": "...",
    "environment": "prod",
    # Only runtime-specific overrides here
}
```

Move settings to YAML files for better maintainability and version control.

## See Also

- [Platform API Architecture](./platform_api_architecture.md) - Technical deep dive
- [Setup Guide](./setup_guide.md) - Initialization and configuration workflow
- [Example Configs](../examples/config/) - Complete working examples
