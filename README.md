<!-- polecat smoke test: workflow verified ok -->
# Spark Kindling Framework

**Import name:** `kindling`
**Platforms:** Microsoft Fabric, Azure Synapse Analytics, Databricks

## Overview

Spark Kindling Framework is a comprehensive solution for building robust data pipelines on Apache Spark, specifically designed for cross-platform solutions, via notebook or python-first development. It provides a declarative, dependency-injection-driven approach to defining and executing data transformations while maintaining strong governance and enabling robust observability.

### Key Capabilities

- **Multi-Platform Support** - Unified API across Fabric, Synapse, and Databricks (including UC shared/standard access mode clusters) plus local/standalone Spark
- **Data Apps & Job Deployment** - Package and deploy apps as Spark jobs
- **Batch & Streaming Pipelines** - Incremental watermarked reads, streaming pipes with SCD1/SCD2 merge sinks
- **Pluggable Entity Providers** - Delta, Parquet, CSV, SQL views, in-memory, Event Hubs, and Azure Data Explorer out of the box
- **Hierarchical Configuration** - Platform, workspace, and environment-specific configs; execution options resolve config-first
- **Schema Reconciliation** - `kindling migrate` plans and applies drift between declared entities and live tables
- **Extensibility** - Plugin system for custom telemetry and integrations
- **Enterprise Observability** - Built-in logging, tracing, and Azure Monitor integration

## Documentation

### Core Features
- [Introduction](./docs/intro.md) - Framework overview and architecture
- [Setup Guide](./docs/guide/setup_guide.md) - Installation and configuration
- [Local Python-First Development](./docs/guide/local_python_first.md) - Scaffold, run, and test without cloud credentials
- [CLI Reference](./docs/reference/cli_reference.md) - Every `kindling` command (scaffolding, apps, migrate, notebooks, workspace)
- [Data Entities](./docs/guide/data_entities.md) - Data entity management system
- [Entity Configuration](./docs/guide/entity_configuration.md) - Tags-first declaration conventions
- [Data Pipes](./docs/guide/data_pipes.md) - Transformation pipeline system
- [Derived Datasets](./docs/guide/derived_datasets.md) - Replacement writes: derived datasets, slice replace, insert-only
- [Entity Providers](./docs/contributing/entity_providers.md) - Storage abstraction system
- [Migrating from runMultiple](./docs/guide/migrating_from_runmultiple.md) - Move Synapse/Fabric notebook DAGs to Kindling pipes

### Advanced Features
- [Job Deployment](./docs/contributing/job_deployment.md) - Deploy apps as Spark jobs
- [Hierarchical Configuration](./docs/contributing/platform_workspace_config.md) - Multi-level YAML config system
- [Logging & Tracing](./docs/contributing/logging_tracing.md) - Observability foundation (including JVM-free telemetry for UC shared clusters)
- [Watermarking](./docs/contributing/watermarking.md) - Change tracking and incremental processing
- [File Ingestion](./docs/guide/file_ingestion.md) - Built-in file ingestion capabilities
- [Stage Processing](./docs/contributing/stage_processing.md) - Pipeline stage orchestration
- [Temporal End-to-End](./docs/guide/temporal_end_to_end.md) - Events, conditions, and episodes (kindling-ext-temporal)
- [Lakeflow App Selection](./docs/guide/lakeflow_app_selection.md) - Run Kindling data apps inside Databricks Lakeflow pipelines
- [Dynamic Registration](./docs/guide/dynamic_registration.md) - Register entities and pipes at runtime

### Platform & Development
- [Platform API Architecture](./docs/contributing/platform_api_architecture.md) - Multi-platform abstraction
- [Platform Storage Utils](./docs/contributing/platform_storage_utils.md) - Storage operations
- [Utilities](./docs/guide/utilities.md) - Common utilities and helper functions
- [Build System](./docs/contributing/build_system.md) - Platform-specific wheel building
- [CI/CD Setup](./docs/contributing/ci_cd_setup.md) - Continuous integration and deployment
- [Testing](./docs/contributing/testing.md) - Unit, integration, and system testing

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
- **Streaming Orchestration** - Streaming queries with health monitoring, recovery, and merge sinks
- **Migration** - Plan/apply reconciliation of declared entities against live Delta tables and views
- **File Ingestion** - File pattern discovery and loading
- **Stage Processing** - Orchestration of multi-stage pipelines
- **Common Transforms** - Reusable data transformation utilities
- **Logging & Tracing** - Comprehensive observability features

## Extensions

- **[kindling-ext-otel-azure](./packages/extensions/kindling_ext_otel_azure/)** - Azure Monitor OpenTelemetry integration
- **[kindling-ext-sdp](./packages/extensions/kindling_ext_sdp/)** - Spark Declarative Pipelines (SDP) declaration engine
- **[kindling-ext-databricks](./packages/extensions/kindling_ext_databricks/)** - Databricks Lakeflow adapter for the SDP declaration engine
- **[kindling-ext-temporal](./packages/extensions/kindling_ext_temporal/)** - Temporal event, condition, and episode primitives
- **[kindling-ext-adx](./packages/extensions/kindling_ext_adx/)** - Azure Data Explorer entity provider (Kusto Spark connector; an API-based `adx-api` provider ships in core)
- **[kindling-ext-cosmos](./packages/extensions/kindling_ext_cosmos/)** - Azure Cosmos DB entity provider (idempotent upsert writes)
- **[kindling-ext-visualization](./packages/extensions/kindling_ext_visualization/)** - Matplotlib visualization helpers

## Install

One distribution, platform-specific extras:

```bash
pip install 'spark-kindling[synapse]'      # Azure Synapse Analytics
pip install 'spark-kindling[databricks]'   # Databricks
pip install 'spark-kindling[fabric]'       # Microsoft Fabric
pip install 'spark-kindling[standalone]'   # Local development / generic Spark
```

The Python import name is `kindling` (unchanged):

```python
from kindling.data_entities import DataEntities
```

Design-time tooling ships separately:

```bash
pip install spark-kindling-cli    # `kindling` CLI for scaffolding and deploy
pip install spark-kindling-sdk    # Programmatic access to platform APIs
```

See [docs/release_process.md](./docs/contributing/release_process.md) for install-from-release examples and [docs/developer_workflow.md](./docs/contributing/developer_workflow.md) for local development.

## CLI Quick Start

Install the CLI and scaffold a repo, package, and app explicitly:

```bash
pip install 'spark-kindling[standalone]' spark-kindling-cli

kindling repo init my-app --output-dir ./my_app
cd my_app
kindling package init my-app
kindling app init my-app --package my-app
cd apps/my_app
kindling app run . --env local
```

No Azure credentials needed — the scaffold uses in-memory entity providers by default.
See [Local Python-First Development](./docs/guide/local_python_first.md) for the full local workflow.

## Notebook Quick Start

```python
# Import Kindling framework
from kindling.data_entities import *
from kindling.data_pipes import *
from kindling.injection import get_kindling_service

# Define data entity (a plain call — registration happens immediately)
DataEntities.entity(
    entityid="customers.raw",
    name="Raw Customer Data",
    partition_columns=["country"],
    merge_columns=["customer_id"],
    tags={"domain": "customer", "layer": "bronze"},
    schema=customer_schema,
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
- **Declarative definitions** for data entities and transformations (tags-first conventions)
- **Dependency injection** for loose coupling and testability
- **Delta Lake integration** for reliable storage and time travel
- **Built-in entity providers** - `delta`, `parquet`, `csv`, `memory`, `sql` (views), `current_view` (SCD2), `eventhub`, `adx-api`; ADX (connector) and Cosmos DB via extensions
- **Watermarking** for change tracking and incremental loads
- **Streaming pipes** with per-micro-batch SCD1/SCD2 merge sinks (`write.mode` tag)
- **Config-first execution options** - parallelism, error strategies (incl. `skip_dependents`), per-pipe retry via `kindling.execution.*`
- **Schema reconciliation** - `kindling migrate plan|apply` converges live tables to declared entities
- **Notebook round-tripping** - `kindling notebook list|pull|push` workspace notebooks as git-friendly `.py` source
- **Observability** through logging and tracing (JVM-free fallback on UC shared clusters)

### Multi-Platform Support
- **Unified API** across Microsoft Fabric, Azure Synapse, and Databricks
- **Platform abstraction** - write once, deploy anywhere
- **Platform-specific optimizations** via configuration
- **Smart platform detection** - automatic platform identification

### Data Apps & Deployment
- **Package apps** as .kda archives (Kindling Data Apps)
- **Run through the durable runner** - apps submit to one managed platform runner
- **Cross-platform deployment** - same app runs on all platforms
- **Artifact management** - wheels, configs, and dependencies

### Configuration System
- **Hierarchical YAML configs** - settings, platform, workspace, environment layers
- **Auto-detection** - platform and workspace ID discovery
- **Flexible overrides** - bootstrap config for runtime changes
- **Multi-team support** - workspace-specific configurations

### Extensibility
- **Extension system** - load custom packages via configuration
- **Azure Monitor integration** - via kindling-ext-otel-azure extension
- **Declarative engines** - SDP declaration engine with a Databricks Lakeflow adapter (kindling-ext-sdp / kindling-ext-databricks)
- **Temporal primitives** - events, conditions, and episode lifecycle (kindling-ext-temporal)
- **Custom providers** - implement your own storage backends
- **Signal/event system** - blinker-based pub/sub for custom workflows

## Requirements
- Python 3.10+
- Apache Spark 3.4+
- Microsoft Fabric, Azure Synapse Analytics, Databricks — or local/standalone Spark for development

## Dependencies
This framework builds upon several excellent open source projects:

- **Apache Spark** - Unified analytics engine for large-scale data processing (Apache 2.0)
- **Delta Lake** - Storage framework for reliable data lakes (Apache 2.0)
- **injector** - Python dependency injection framework (BSD)
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
