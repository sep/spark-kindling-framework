# Spark Kindling Framework

## Overview

Spark Kindling Framework is a comprehensive solution for building robust data pipelines on Apache Spark, specifically designed for notebook-based environments like Microsoft Fabric and Azure Databricks. It provides a declarative, dependency-injection-driven approach to defining and executing data transformations while maintaining strict governance and observability.

## Documentation

- [Introduction](./docs/intro.md) - Framework overview and architecture
- [Data Entities](./docs/data_entities.md) - Data entity management system
- [Data Pipes](./docs/data_pipes.md) - Transformation pipeline system
- [Entity Providers](./docs/entity_providers.md) - Storage abstraction system
- [Logging & Tracing](./docs/logging_tracing.md) - Observability foundation
- [Utilities](./docs/utilities.md) - Common utilities and helper functions
- [Watermarking](./docs/watermarking.md) - Change tracking and incremental processing
- [File Ingestion](./docs/file_ingestion.md) - Built-in file ingestion capabilities
- [Stage Processing](./docs/stage_processing.md) - Pipeline stage orchestration
- [Setup Guide](./docs/setup_guide.md) - Installation and configuration

## Core Modules

The framework consists of several modular components:

- **Dependency Injection Engine** - Provides IoC container for loose coupling
- **Data Entities** - Entity registry and storage abstraction
- **Data Pipes** - Transformation pipeline definition and execution
- **Watermarking** - Change tracking for incremental processing
- **File Ingestion** - File pattern discovery and loading
- **Stage Processing** - Orchestration of multi-stage pipelines
- **Common Transforms** - Reusable data transformation utilities
- **Logging & Tracing** - Comprehensive observability features

## Quickstart

```python
# Import Kindling framework
import kindling

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
executer = GlobalInjector.get(DataPipesExecution)
executer.run_datapipes(["customers.transform"])
```

## Key Features

- **Declarative definitions** for data entities and transformations
- **Dependency injection** for loose coupling and testability
- **Delta Lake integration** for reliable storage and time travel
- **Watermarking** for change tracking and incremental loads
- **Pluggable providers** for storage, paths, and execution strategies
- **Observability** through logging and tracing
- **Flexible configuration** for different environments