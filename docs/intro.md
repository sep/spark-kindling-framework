# Kindling Framework

**Version:** 0.2.0
**Platforms:** Microsoft Fabric, Azure Synapse Analytics, Databricks

## Overview

Kindling is a comprehensive framework for building data solutions on top of Apache Spark with PySpark. Designed for multi-platform notebook-based development, Kindling provides a declarative, dependency-injection-driven approach to data pipeline development, entity management, and execution orchestration that works seamlessly across Microsoft Fabric, Azure Synapse Analytics, and Databricks.

The framework consists of core modules that work together:

- **[Data Pipes Module](./data_pipes.md)** - Declarative pipeline definition and execution engine
- **[Data Entities Module](./data_entities.md)** - Data entity management and storage abstraction layer
- **[Data Apps Module](./job_deployment.md)** - Package and deploy apps across platforms
- **[Configuration System](./platform_workspace_config.md)** - Hierarchical multi-level configuration
- **[Platform Services](./platform_api_architecture.md)** - Unified API for Fabric/Synapse/Databricks

## Architecture

Kindling follows a layered architecture built on dependency injection principles:

```
┌─────────────────────────────────────────────────────────────┐
│                    Notebook Interface                       │
├─────────────────────────────────────────────────────────────┤
│             Kindling Framework Core                         │
│  ┌─────────────────────┬─────────────────────────────────┐  │
│  │   Data Pipes        │     Data Entities               │  │
│  │   • @pipe()         │     • @entity()                 │  │
│  │   • Execution       │     • Registry                  │  │
│  │   • Dependencies    │     • CRUD Operations           │  │
│  └─────────────────────┴─────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                 Dependency Injection Layer                  │
│              (GlobalInjector & Service Providers)           │
├─────────────────────────────────────────────────────────────┤
│     Storage Layer             │    Infrastructure Layer     │
│     • Delta Lake              │    • Logging                │
│     • Entity Providers        │    • Tracing                │
│     • Path Locators           │    • Configuration          │
└─────────────────────────────────────────────────────────────┘
```

## Key Features

### 🚀 **Declarative Development Model**
- Define data entities and transformations using Python decorators
- Automatic dependency resolution and execution ordering
- Built-in error handling and pipeline orchestration

### 🌐 **Multi-Platform Support**
- Unified API across Microsoft Fabric, Azure Synapse, and Databricks
- Platform abstraction layer with automatic detection
- Write once, deploy anywhere - same code runs on all platforms
- Platform-specific optimizations via configuration

### 📦 **Data Apps & Job Deployment**
- Package apps as .kda archives with dependencies
- Deploy as Spark jobs across any supported platform
- Automated job creation, execution, and monitoring
- Bootstrap system for framework initialization

### ⚙️ **Hierarchical Configuration**
- YAML-based multi-level configuration (settings/platform/workspace/environment)
- Auto-detection of platform and workspace
- Runtime overrides via bootstrap config
- Team and environment separation

### 📊 **Unified Data Management**
- Centralized entity registry with metadata management
- Seamless integration between data definitions and transformations
- Support for Delta Lake with versioning and time travel capabilities

### 🔧 **Enterprise-Ready Infrastructure**
- Dependency injection for loose coupling and testability
- Distributed tracing and comprehensive logging throughout
- Pluggable storage backends and flexible naming strategies
- Extension system for custom integrations (Azure Monitor, etc.)

### 📈 **Data Governance & Observability**
- Metadata tagging for classification and discovery
- Pipeline execution monitoring and debugging capabilities
- Schema evolution and version management
- OpenTelemetry-compatible tracing

### 🧩 **Modular & Extensible**
- Clean separation between data definitions and transformations
- Abstract interfaces for custom storage and execution strategies
- Extension loading system for custom packages
- Signal/event system for workflow hooks

## How Kindling Works

### 1. Define Your Data Entities

Use the Data Entities module to register your data structures:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define entity schema
customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("region", StringType(), True)
])

# Register entity with Kindling
@DataEntities.entity(
    entityid="bronze.customers",
    name="Raw Customer Data",
    partition_columns=["region"],
    merge_columns=["customer_id"],
    tags={"layer": "bronze", "domain": "customer"},
    schema=customer_schema
)
```

### 2. Create Data Transformation Pipes

Use the Data Pipes module to define your transformations:

```python
@DataPipes.pipe(
    pipeid="clean_customers",
    name="Clean Customer Data",
    tags={"category": "cleaning", "domain": "customer"},
    input_entity_ids=["bronze.customers"],
    output_entity_id="silver.customers_clean",
    output_type="table"
)
def clean_customer_data(bronze_customers):
    return bronze_customers.filter(col("email").isNotNull()) \
                           .dropDuplicates(["customer_id"])
```

### 3. Execute Your Pipelines

Kindling automatically manages the execution:

```python
# Get the execution engine
executer = GlobalInjector.get(DataPipesExecuter)

# Run your data pipeline
executer.run_datapipes(["clean_customers"])
```

## Core Benefits

### **Simplified Development**
- Eliminates boilerplate code for common data operations
- Provides consistent patterns for data processing workflows
- Reduces complexity in managing dependencies between transformations

### **Improved Maintainability**
- Clear separation of concerns between data definitions and logic
- Centralized metadata management for all data assets
- Standardized approaches to error handling and logging

### **Enhanced Productivity**
- Notebook-friendly development experience
- Automatic discovery and registration of entities and pipes
- Built-in best practices for Spark and Delta Lake operations

### **Enterprise Scalability**
- Dependency injection enables easy testing and mocking
- Pluggable architecture supports different storage backends
- Comprehensive observability for production monitoring

## Getting Started

To start using Kindling in your Spark notebooks:

1. **Set up Configuration**: Create YAML config files or use bootstrap config
   - [Configuration Guide](./platform_workspace_config.md)
   - [Setup Guide](./setup_guide.md)

2. **Learn the Data Entities Module**: Understand how to define and manage your data structures
   - [Data Entities Documentation](./data_entities.md)

3. **Explore the Data Pipes Module**: Learn to build transformation pipelines
   - [Data Pipes Documentation](./data_pipes.md)

4. **Package and Deploy**: Create data apps and deploy as Spark jobs
   - [Job Deployment Documentation](./job_deployment.md)

5. **Extend and Monitor**: Add custom integrations and observability
   - [Logging & Tracing](./logging_tracing.md)
   - [Azure Monitor Extension](../packages/kindling_otel_azure/README.md)

## Use Cases

Kindling is ideal for:

- **Multi-Cloud Data Platforms**: Deploy data solutions across Fabric, Synapse, and Databricks
- **Data Lake Development**: Building medallion architectures (bronze/silver/gold layers)
- **ETL/ELT Pipelines**: Creating maintainable data transformation workflows
- **Analytics Engineering**: Developing data models for analytical workloads
- **Data Science Workflows**: Managing feature engineering and model training pipelines
- **Data Governance**: Implementing metadata-driven data management practices
- **Enterprise Data Teams**: Multi-team, multi-environment deployments with workspace isolation

## Framework Philosophy

Kindling is built on principles for modern data infrastructure:
- **Declarative**: Focus on what you want, not how to implement it
- **Platform-Agnostic**: Write once, deploy to Fabric, Synapse, or Databricks
- **Discoverable**: All entities and transformations are automatically registered and findable
- **Testable**: Dependency injection enables comprehensive testing strategies
- **Observable**: Built-in logging and tracing provide visibility into execution
- **Extensible**: Clean abstractions and extension system for customization
- **Configuration-Driven**: YAML-based hierarchical configs for flexibility

Ready to get started? Dive into the [Setup Guide](./setup_guide.md) to begin building your multi-platform data solutions with Kindling.
