# Kindling Framework

## Overview

Kindling is a comprehensive framework for building data solutions on top of Apache Spark with PySpark. Designed for notebook-based development environments, Kindling provides a declarative, dependency-injection-driven approach to data pipeline development, entity management, and execution orchestration.

The framework consists of two core modules that work together seamlessly:

- **[Data Pipes Module](./data_pipes.md)** - Declarative pipeline definition and execution engine
- **[Data Entities Module](./data_entities.md)** - Data entity management and storage abstraction layer

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
│     Storage Layer              │    Infrastructure Layer     │
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

### 📊 **Unified Data Management**
- Centralized entity registry with metadata management
- Seamless integration between data definitions and transformations
- Support for Delta Lake with versioning and time travel capabilities

### 🔧 **Enterprise-Ready Infrastructure**
- Dependency injection for loose coupling and testability
- Distributed tracing and comprehensive logging throughout
- Pluggable storage backends and flexible naming strategies

### 📈 **Data Governance & Observability**
- Metadata tagging for classification and discovery
- Pipeline execution monitoring and debugging capabilities
- Schema evolution and version management

### 🧩 **Modular & Extensible**
- Clean separation between data definitions and transformations
- Abstract interfaces for custom storage and execution strategies
- Notebook-friendly development workflow

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

1. **Learn the Data Entities Module**: Understand how to define and manage your data structures
   - [Data Entities Documentation](./data_entities.md)

2. **Explore the Data Pipes Module**: Learn to build transformation pipelines
   - [Data Pipes Documentation](./data_pipes.md)

3. **Implement Required Providers**: Set up your storage and infrastructure providers based on your environment

4. **Start Building**: Begin with simple entities and pipes, then compose them into complex workflows

## Use Cases

Kindling is ideal for:

- **Data Lake Development**: Building medallion architectures (bronze/silver/gold layers)
- **ETL/ELT Pipelines**: Creating maintainable data transformation workflows
- **Analytics Engineering**: Developing data models for analytical workloads
- **Data Science Workflows**: Managing feature engineering and model training pipelines
- **Data Governance**: Implementing metadata-driven data management practices

## Framework Philosophy

Kindling is built on the principle that data infrastructure should be:
- **Declarative**: Focus on what you want, not how to implement it
- **Discoverable**: All entities and transformations are automatically registered and findable
- **Testable**: Dependency injection enables comprehensive testing strategies
- **Observable**: Built-in logging and tracing provide visibility into execution
- **Extensible**: Clean abstractions allow customization for specific needs

Ready to get started? Dive into the detailed documentation for each module to begin building your data solutions with Kindling.
