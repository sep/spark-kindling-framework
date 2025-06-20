# Setup Guide

This guide explains how to install, configure, and start using the Spark Kindling Framework in Microsoft Fabric or Azure Databricks environments.

## Prerequisites

- Microsoft Fabric or Azure Databricks workspace
- Python 3.8+
- Apache Spark 3.2+
- Delta Lake 2.0+

## Installation Options

### Option 1: Direct Notebook Import

1. Clone the repository to your local machine
2. Upload the notebook files to your Fabric/Databricks workspace
3. Import the notebooks using the notebook_import function

### Option 2: Package Installation

The framework can be packaged and installed as a Python package:

```python
# Run the build framework packages notebook
%run /fabric/build_framework_packages

# Then in your notebooks
import kindling
```

## Configuration

### Environment Bootstrap

For easy setup, you can use the provided bootstrap notebook:

```python
BOOTSTRAP_CONFIG = {
    'log_level': 'INFO',
    'is_interactive': True,
    'use_lake_packages': False,
    'load_local_packages': False,
    'workspace_endpoint': "your-workspace-endpoint-id",
    'platform_environment': 'fabric',  # or 'databricks'
    'artifacts_storage_path': "Files/artifacts",
    'required_packages': ["injector", "dynaconf", "pytest"],
    'ignored_folders': ['utilities'],
    'spark_configs': {
        'spark.databricks.delta.schema.autoMerge.enabled': 'true'
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
