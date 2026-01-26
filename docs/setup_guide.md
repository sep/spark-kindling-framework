# Setup Guide

This guide explains how to install, configure, and start using the Spark Kindling Framework across Microsoft Fabric, Azure Synapse Analytics, and Databricks environments.

## Prerequisites

### Required
- One of the following platforms:
  - **Microsoft Fabric** (with Spark runtime)
  - **Azure Synapse Analytics** (with Spark pools)
  - **Databricks** (Azure, AWS, or GCP)
- **Python 3.10+**
- **Apache Spark 3.4+**
- **Delta Lake 2.0+**

### Optional
- Azure Storage Account (for artifacts storage)
- Azure Monitor workspace (for telemetry extension)

## Installation Options

### Option 1: Deploy Pre-built Wheels (Recommended)

The framework is distributed as platform-specific wheels:

1. **Upload wheels to artifacts storage:**
   ```bash
   # From releases or build output
   az storage blob upload \
     --account-name <storage-account> \
     --container artifacts \
     --name packages/kindling_fabric-0.2.0-py3-none-any.whl \
     --file dist/kindling_fabric-0.2.0-py3-none-any.whl
   ```

2. **Install in notebook:**
   ```python
   BOOTSTRAP_CONFIG = {
       "artifacts_storage_path": "abfss://artifacts@<storage>.dfs.core.windows.net/",
       "environment": "dev",
       "use_lake_packages": True,  # Install from artifacts
   }

   %run /path/to/kindling_bootstrap.py
   ```

### Option 2: Install from Artifacts Storage

If wheels are already deployed to your artifacts storage:

```python
# In your notebook
BOOTSTRAP_CONFIG = {
    "artifacts_storage_path": "Files/artifacts",  # Or full abfss:// path
    "environment": "production",
    "use_lake_packages": True,
}

## Configuration

### Hierarchical Configuration System

Kindling uses a layered configuration approach with YAML files:

**Priority (lowest → highest):**
1. `settings.yaml` - Base framework settings
2. `platform_{platform}.yaml` - Platform-specific (fabric/synapse/databricks)
3. `workspace_{workspace_id}.yaml` - Workspace-specific
4. `env_{environment}.yaml` - Environment-specific (dev/prod/etc)
5. `BOOTSTRAP_CONFIG` - Runtime overrides

See [Hierarchical Configuration Guide](./platform_workspace_config.md) for complete details.

### Bootstrap Configuration

Minimal bootstrap example:

```python
BOOTSTRAP_CONFIG = {
    # Required
    'artifacts_storage_path': "abfss://artifacts@<storage>.dfs.core.windows.net/",
    'environment': 'dev',  # Loads env_dev.yaml if exists

    # Package loading
    'use_lake_packages': True,   # Install from artifacts storage
    'load_local_packages': True,  # Load workspace notebooks as packages

    # Optional overrides
    'log_level': 'INFO',  # Override YAML log level
    'platform': 'fabric',  # Force platform (auto-detected if omitted)

    # Extensions (can also be in YAML)
    'extensions': ['kindling-otel-azure>=0.2.0'],

    # Spark configs
    'spark_configs': {
        'spark.sql.adaptive.enabled': 'true'
    }
}
    }
}

%run environment_bootstrap
```

## Required Dependencies

The framework requires these Python packages:

- **injector**: For dependency injection
- **delta-spark**: For Delta Lake functionality
- **dynaconf**: For configuration management
- **pytest**: For testing (optional for production)

## Provider Configuration

### 1. Entity Path Locator

Implement a custom `EntityPathLocator` for your environment:

```python
@GlobalInjector.singleton_autobind()
class MyEntityPathLocator(EntityPathLocator):
    def get_table_path(self, entity):
        # Example: Map entity IDs to cloud storage paths
        return f"abfss://data@storage.dfs.core.windows.net/tables/{entity.entityid}"
```

### 2. Entity Name Mapper

Implement a custom `EntityNameMapper` for your naming convention:

```python
@GlobalInjector.singleton_autobind()
class MyEntityNameMapper(EntityNameMapper):
    def get_table_name(self, entity):
        # Example: Convert entity IDs to table names
        return entity.entityid.replace(".", "_")
```

### 3. Watermark Entity Finder

Implement a custom `WatermarkEntityFinder` for watermark storage:

```python
@GlobalInjector.singleton_autobind()
class MyWatermarkEntityFinder(WatermarkEntityFinder):
    def get_watermark_entity_for_entity(self, context):
        return "system.watermarks"

    def get_watermark_entity_for_layer(self, layer):
        return "system.watermarks"
```

## Directory Structure

TODO: Rewrite this hallucination
For optimal organization, structure your notebooks following this pattern:

```
/workspace
  /project
    /bronze
      # Bronze layer transformation notebooks
    /silver
      # Silver layer transformation notebooks
    /gold
      # Gold layer transformation notebooks
    /common
      # Shared utility notebooks and entity definitions
    /orchestration
      # Pipeline orchestration notebooks
```

## Entity and Pipe Naming Conventions

For better organization and discoverability:

- Entity IDs: `<domain>.<entity_name>` (e.g., `sales.transactions`)
- Pipe IDs: `<stage>.<domain>.<operation>` (e.g., `validate.sales.check_amounts`)

## Testing Setup

To enable testing:

1. Create test notebooks for each component
2. Configure test data paths
3. Import the test framework:

```python
notebook_import("kindling.test_framework")

# Define a test case
@test_case("My test case")
def test_my_pipe():
    # Test implementation
    assert result == expected

# Run tests
run_tests()
```

## Common Issues and Solutions

### Delta Table Access Mode

If you encounter issues with Delta table access, configure the appropriate access mode:

```python
# In your configuration
'spark_configs': {
    'delta_table_access_mode': 'forPath'  # or 'forName', 'auto'
}
```

### Schema Evolution

To enable schema evolution for Delta tables:

```python
'spark_configs': {
    'spark.databricks.delta.schema.autoMerge.enabled': 'true'
}
```

### Dependency Injection Issues

If you encounter dependency injection issues, check:

1. Provider implementation and binding
2. Import order in notebooks
3. Provider scope (singleton vs. transient)

## Getting Help

For additional assistance:

- Check the framework documentation
- Review the test notebooks for examples
- Open an issue in the GitHub repository
