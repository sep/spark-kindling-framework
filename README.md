# Spark Kindling Framework

**Version:** 0.2.1
**Platforms:** Microsoft Fabric, Azure Synapse Analytics, Databricks

## Overview

Spark Kindling Framework is a comprehensive solution for building robust data pipelines on Apache Spark, specifically designed for cross-platform solutions, via notebook or python-first development. It provides a declarative, dependency-injection-driven approach to defining and executing data transformations while maintaining strong governance and enabling robust observability.

### Key Capabilities

- **Multi-Platform Support** - Unified API across Fabric, Synapse, and Databricks
- **Data Apps & Job Deployment** - Package and deploy apps as Spark jobs
- **Hierarchical Configuration** - Platform, workspace, and environment-specific configs
- **Extensibility** - Plugin system for custom telemetry and integrations
- **Enterprise Observability** - Built-in logging, tracing, and Azure Monitor integration

## Documentation

### Core Features
- [Introduction](./docs/intro.md) - Framework overview and architecture
- [Data Entities](./docs/data_entities.md) - Data entity management system
- [Data Pipes](./docs/data_pipes.md) - Transformation pipeline system
- [Entity Providers](./docs/entity_providers.md) - Storage abstraction system
- [Setup Guide](./docs/setup_guide.md) - Installation and configuration

### Advanced Features
- [Job Deployment](./docs/job_deployment.md) - Deploy apps as Spark jobs
- [Hierarchical Configuration](./docs/platform_workspace_config.md) - Multi-level YAML config system
- [Logging & Tracing](./docs/logging_tracing.md) - Observability foundation
- [Watermarking](./docs/watermarking.md) - Change tracking and incremental processing
- [File Ingestion](./docs/file_ingestion.md) - Built-in file ingestion capabilities
- [Stage Processing](./docs/stage_processing.md) - Pipeline stage orchestration

### Platform & Development
- [Platform API Architecture](./docs/platform_api_architecture.md) - Multi-platform abstraction
- [Platform Storage Utils](./docs/platform_storage_utils.md) - Storage operations
- [Utilities](./docs/utilities.md) - Common utilities and helper functions
- [Build System](./docs/build_system.md) - Platform-specific wheel building
- [CI/CD Setup](./docs/ci_cd_setup.md) - Continuous integration and deployment
- [Testing](./docs/testing.md) - Unit, integration, and system testing

## Core Modules

The framework consists of several modular components:

- **Dependency Injection Engine** - Provides IoC container for loose coupling
- **Data Entities** - Entity registry and storage abstraction
- **Data Pipes** - Transformation pipeline definition and execution
- **Data Apps** - Package and deploy apps as .kda archives
- **Job Deployment** - Deploy apps as Spark jobs across platforms
- **Configuration System** - Hierarchical YAML configuration (platform/workspace/environment)
- **Platform Services** - Unified abstraction for Fabric/Synapse/Databricks
- **Watermarking** - Change tracking for incremental processing
- **File Ingestion** - File pattern discovery and loading
- **Stage Processing** - Orchestration of multi-stage pipelines
- **Common Transforms** - Reusable data transformation utilities
- **Logging & Tracing** - Comprehensive observability features

## Extensions

- **[kindling-otel-azure](./packages/kindling_otel_azure/)** - Azure Monitor OpenTelemetry integration
- **[kindling-databricks-dlt](./packages/kindling_databricks_dlt/)** - Databricks Delta Live Tables integration

## Quickstart

```python
# Import Kindling framework
from kindling.data_entities import *
from kindling.data_pipes import *
from kindling.injection import get_kindling_service

# Define data entity
@DataEntities.entity(
    entityid="customers.raw",
    name="Raw Customer Data",
    partition_columns=["country"],
    merge_columns=["customer_id"],
    tags={"domain": "customer", "layer": "bronze"},
    schema=customer_schema
)

# Create data transformation pipe
@DataPipes.pipe(
    pipeid="customers.transform",
    name="Transform Customers",
    tags={"layer": "silver"},
    input_entity_ids=["customers.raw"],
    output_entity_id="customers.silver",
    output_type="table"
)
def transform_customers(customers_raw):
    # Transformation logic here
    return customers_raw.filter(...)

# Execute pipeline
executer = get_kindling_service(DataPipesExecution)
executer.run_datapipes(["customers.transform"])
```

## Key Features

### Core Framework
- **Declarative definitions** for data entities and transformations
- **Dependency injection** for loose coupling and testability
- **Delta Lake integration** for reliable storage and time travel
- **Watermarking** for change tracking and incremental loads
- **Pluggable providers** for storage, paths, and execution strategies
- **Observability** through logging and tracing

### Multi-Platform Support
- **Unified API** across Microsoft Fabric, Azure Synapse, and Databricks
- **Platform abstraction** - write once, deploy anywhere
- **Platform-specific optimizations** via configuration
- **Smart platform detection** - automatic platform identification

### Data Apps & Deployment
- **Package apps** as .kda archives (Kindling Data Apps)
- **Deploy as Spark jobs** - automated job creation and execution
- **Cross-platform deployment** - same app runs on all platforms
- **Artifact management** - wheels, configs, and dependencies

### Configuration System
- **Hierarchical YAML configs** - settings, platform, workspace, environment layers
- **Auto-detection** - platform and workspace ID discovery
- **Flexible overrides** - bootstrap config for runtime changes
- **Multi-team support** - workspace-specific configurations

### Extensibility
- **Extension system** - load custom packages via configuration
- **Azure Monitor integration** - via kindling-otel-azure extension
- **Custom providers** - implement your own storage backends
- **Signal/event system** - blinker-based pub/sub for custom workflows

## Requirements
- Python 3.10+
- Apache Spark 3.4+
- One of: Microsoft Fabric, Azure Synapse Analytics, or Databricks

## Dependencies
This framework builds upon several excellent open source projects:

- **Apache Spark** - Unified analytics engine for large-scale data processing (Apache 2.0)
- **Delta Lake** - Storage framework for reliable data lakes (Apache 2.0)
- **inject** - Python dependency injection framework (Apache 2.0)
- **blinker** - Python signal/event framework for pub/sub (MIT)
- **dynaconf** - Configuration management for Python (MIT)
- **pytest** - Testing framework (MIT)

### Platform SDKs (auto-detected, only one required):
- **notebookutils** / **mssparkutils** - Microsoft Fabric & Synapse (MIT)
- **dbutils** - Databricks utilities (Databricks)

### Extensions (optional):
- **azure-monitor-opentelemetry** - Azure Monitor integration (MIT)
- **opentelemetry-api/sdk** - OpenTelemetry tracing (Apache 2.0)


## Contributing
We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Support
This is open source software provided without warranty or guaranteed support.

**Commercial Support:** Professional support, training, and consulting services are available from Software Engineering Professionals, Inc. Contact us at [contact information](mailto:engineering@sep.com).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Developed By
**Software Engineering Professionals, Inc.**
16080 Westfield Blvd.
Carmel, IN 46033
[www.sep.com](https://www.sep.com)

## Acknowledgments
This framework was developed to solve real-world data processing challenges encountered across multiple enterprise engagements. We're grateful to our clients who have helped shape the requirements and validate the approach.

---
**Note:** This framework is maintained by SEP and used across multiple projects. If you're using this framework and encounter issues or have suggestions, please open an issue or submit a pull request.
