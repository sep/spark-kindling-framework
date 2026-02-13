# Domain-Specific Package Development on Kindling

## Evaluation & Design Proposal

**Date:** 2025-01-27 (Revised 2026-02-12)
**Status:** Proposal
**Scope:** CLI bootstrapping, stub generation, app lifecycle, Python-first local development

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Core Insight: You Don't Need a Full Bootstrap for Local Dev](#2-core-insight)
3. [Developer Use Cases & Testing Tiers](#3-developer-use-cases--testing-tiers)
4. [Current State Analysis](#4-current-state-analysis)
5. [CLI for Project Bootstrapping](#5-cli-for-project-bootstrapping)
6. [Stub Generation for Entities, Pipes, and Providers](#6-stub-generation-for-entities-pipes-and-providers)
7. [Development & Testing Workflow](#7-development--testing-workflow)
8. [Test Infrastructure & Fixtures](#8-test-infrastructure--fixtures)
9. [Implementation Roadmap](#9-implementation-roadmap)
10. [Appendices](#appendices)

---

## 1. Executive Summary

Kindling provides a rich framework for building data solutions on Azure Spark platforms, but building **domain-specific packages** on top of it today requires deep familiarity with its DI system, decorator patterns, provider interfaces, and app packaging conventions. There is no guided path from "new project" to "deployed data app."

This document evaluates what it would take to support domain-specific package development with:

- A **`kindling` CLI** for bootstrapping project templates, generating stubs, and managing the development lifecycle
- **Stub generators** for entities, pipes, providers, and other framework primitives
- **App lifecycle** tooling covering package → deploy → run → monitor
- A **Python-first local development** experience that doesn't require a Spark cluster or a fully bootstrapped framework

### Key Findings

| Area | Current State | Gap Severity | Effort to Close |
|------|--------------|--------------|-----------------|
| Project scaffolding | No templates or CLI | **High** | Medium (2-3 weeks) |
| Entity/pipe stubs | Manual, copy from docs | **Medium** | Low (1 week) |
| App lifecycle tooling | `poe` tasks exist for framework devs, not consumers | **High** | Medium (2-3 weeks) |
| Local development | Pipe functions are already testable; missing fixtures/harness | **Medium** | Low-Medium (1-2 weeks) |
| Documentation for consumers | Intro docs exist, no tutorial | **Medium** | Low-Medium (1-2 weeks) |

**Recommendation:** Build a `kindling` CLI using `click` or `typer` that serves as the single entry point for domain package developers. Start with `kindling init`, `kindling add entity`, and `kindling dev test` commands. Critically, **don't** try to replicate the full bootstrap locally — instead lean into the fact that pipe transforms are plain PySpark functions and make that the primary local dev loop.

---

## 2. Core Insight: You Don't Need a Full Bootstrap for Local Dev {#2-core-insight}

### What Domain Developers Actually Do

The overwhelming majority of domain development work is **writing pipe transforms** — PySpark functions that take DataFrames and return DataFrames:

```python
@DataPipes.pipe(
    pipeid="clean_orders",
    input_entity_ids=["bronze.orders"],
    output_entity_id="silver.clean_orders",
    ...
)
def clean_orders(bronze_orders: DataFrame) -> DataFrame:
    return (
        bronze_orders
        .filter(col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
        .withColumn("processed_at", current_timestamp())
    )
```

**The `@DataPipes.pipe()` decorator is just metadata registration.** The actual `clean_orders` function is a plain Python function. It takes a DataFrame, returns a DataFrame. It has zero dependency on:

- Platform services (Fabric/Synapse/Databricks)
- Cloud storage (mssparkutils/dbutils)
- Bootstrap configuration
- DI container
- Config files
- Extension loading
- Workspace packages

### What This Means for Local Dev

**You don't need `initialize_framework()` to develop and test transforms.** The testing tiers are:

| Tier | What You Test | What You Need | Kindling Bootstrap? |
|------|--------------|---------------|---------------------|
| **Unit** | Pipe function logic | pytest + mock DataFrames | **No** |
| **Integration** | Pipes + real Spark | pytest + local Spark + Delta | **No** |
| **Component** | Pipes + entity registration + DI | pytest + local Spark + minimal DI wiring | **Partial** (just DI + registries) |
| **System** | Full app deployed as Spark job | Cloud platform | **Yes** (full bootstrap) |

**90% of domain development happens in Tiers 1-2.** The framework's existing unit tests already prove this — `tests/unit/test_data_pipes.py` tests pipe execution with nothing but mocks. No Spark session, no platform, no config.

### Implications for Tooling

Instead of building a `kindling dev run` that recreates the full bootstrap locally (fragile, diverges from cloud behavior), we should:

1. **Make pipe functions trivially testable in isolation** (they already are)
2. **Provide test fixtures** that set up local Spark + Delta for integration tests
3. **Provide a lightweight DI harness** for component tests that need entity/pipe registries
4. **Leave system testing to actual cloud deployment** (where it belongs)

This is the same pattern as web frameworks: you unit test your handlers without starting a web server, integration test with a test client, and system test against a deployed instance.

---

## 3. Developer Use Cases & Testing Tiers

### 3.1 The Key Use Cases for Domain Development

Based on analysis of the framework's API surface, existing test apps, and how pipes/entities work, here are the concrete use cases for domain developers:

| Use Case | Frequency | Framework Coupling | Local Testable? |
|----------|-----------|-------------------|-----------------|
| **Write a pipe transform** (DataFrame → DataFrame) | Daily | **None** — pure function | Yes, trivially |
| **Define an entity** (register with decorator) | Weekly | **Shallow** — decorator calls `GlobalInjector.get()` | Yes, with DI reset |
| **Configure entity tags** (provider, path, access mode) | Weekly | **Shallow** — configuration only | Yes, YAML files |
| **Test pipe with real Spark** | Daily | **None** — just needs SparkSession + Delta | Yes, local Spark |
| **Test entity registration + pipe execution together** | Weekly | **Moderate** — needs DI registries | Yes, light DI wiring |
| **Build custom entity provider** | Rarely | **Moderate** — implement ABC, register | Yes, mock-based |
| **Package app as .kda** | Per release | **None** — pure file operations | Yes, filesystem only |
| **Deploy and run as Spark job** | Per release | **Full** — needs cloud platform | No, cloud only |
| **Debug production issues** | As needed | **Full** — needs platform logs | No, cloud only |

### 3.2 Testing Pyramid for Domain Packages

```
                        ┌─────────────────┐
                        │  System Tests   │  Deploy to cloud, run as Spark job
                        │  (cloud only)   │  Validate end-to-end via stdout markers
                        ├─────────────────┤
                    ┌───┤  Component      │  Entity + pipe registries + DI
                    │   │  Tests          │  MemoryEntityProvider, light harness
                    │   ├─────────────────┤
                ┌───┤   │  Integration    │  Real Spark + Delta, test pipe output
                │   │   │  Tests          │  No Kindling DI needed at all
                │   │   ├─────────────────┤
            ┌───┤   │   │  Unit Tests     │  Mock DataFrames or simple assertions
            │   │   │   │  (pure Python)  │  No Spark, no Kindling, just pytest
            │   │   │   └─────────────────┘
    Effort: Low ─────────────────────────────────── High
     Speed: Fast ────────────────────────────────── Slow
```

### 3.3 What Each Tier Needs

#### Tier 1: Unit Tests (No Spark, No Kindling)

Test pipe logic with mock DataFrames. This is where 60%+ of development testing lives.

```python
# tests/test_transforms.py
from pipes.clean_orders import clean_orders  # Just import the function

def test_clean_orders_drops_nulls(spark):
    """Test with a tiny local Spark session or mock."""
    df = spark.createDataFrame([
        ("ord-1", "us-west", 100),
        (None, "us-east", 200),       # null order_id
        ("ord-1", "us-west", 100),    # duplicate
    ], ["order_id", "region", "amount"])

    result = clean_orders(df)       # Call function DIRECTLY, no framework

    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ord-1"
```

**Requirements:** `pytest`, optionally `pyspark` for real DataFrames (or mock if testing logic only). **Zero Kindling imports needed.**

#### Tier 2: Integration Tests (Local Spark + Delta)

Test that transforms work with real Delta Lake reads/writes, schema enforcement, partitioning.

```python
# tests/test_pipeline_integration.py
def test_end_to_end_pipeline(spark, tmp_path):
    """Test full pipeline with real Delta tables."""
    # Write test input
    input_path = str(tmp_path / "bronze" / "orders")
    test_df = spark.createDataFrame([...])
    test_df.write.format("delta").save(input_path)

    # Read, transform, write — just like production but local paths
    source = spark.read.format("delta").load(input_path)
    result = clean_orders(source)

    output_path = str(tmp_path / "silver" / "clean_orders")
    result.write.format("delta").save(output_path)

    # Verify
    output = spark.read.format("delta").load(output_path)
    assert output.count() > 0
    assert "processed_at" in output.columns
```

**Requirements:** `pytest`, `pyspark`, `delta-spark`. **Still zero Kindling DI needed.**

#### Tier 3: Component Tests (Light DI Harness)

Test entity registration, pipe execution engine, entity tag configuration — the framework plumbing.

```python
# tests/test_component.py
from kindling.injection import GlobalInjector
from kindling.data_entities import DataEntities, DataEntityRegistry
from kindling.data_pipes import DataPipes, DataPipesRegistry
from kindling.entity_provider_memory import MemoryEntityProvider
from kindling.entity_provider_registry import EntityProviderRegistry

@pytest.fixture(autouse=True)
def reset_di():
    GlobalInjector.reset()
    yield
    GlobalInjector.reset()

def test_pipe_execution_through_framework(spark):
    """Test that entity registration + pipe execution works end-to-end."""
    # Register entities (this touches DI)
    DataEntities.entity(
        entityid="test.input", name="Input",
        partition_columns=[], merge_columns=["id"],
        tags={"provider_type": "memory"}, schema=None,
    )
    DataEntities.entity(
        entityid="test.output", name="Output",
        partition_columns=[], merge_columns=["id"],
        tags={"provider_type": "memory"}, schema=None,
    )

    # Register pipe
    @DataPipes.pipe(
        pipeid="test_pipe", name="Test Pipe",
        input_entity_ids=["test.input"], output_entity_id="test.output",
        output_type="table", tags={},
    )
    def test_transform(test_input: DataFrame) -> DataFrame:
        return test_input.filter(col("id").isNotNull())

    # Wire memory provider + execute through framework
    registry = GlobalInjector.get(EntityProviderRegistry)
    registry.register_provider("memory", MemoryEntityProvider)
    # ... exercise execution engine ...
```

**Requirements:** `kindling` as a dependency, but **NO bootstrap, NO config files, NO cloud platform**.

#### Tier 4: System Tests (Cloud Deployment)

Build `.kda`, deploy to platform, run as Spark job, validate via stdout markers. This is the existing system test pattern.

```bash
# Package and deploy
kindling build app --platform fabric
poe deploy-app --app-path dist/my-app-fabric-v1.0.0.kda

# Run system tests
poe test-system --platform fabric --test my_app
```

**Requirements:** Cloud credentials, deployed infrastructure. Run in CI/CD.

### 3.4 What Kindling Should Provide for Each Tier

| Tier | What Kindling Should Provide |
|------|------------------------------|
| **Unit** | Nothing (pipe functions are already plain Python). Just document the pattern. |
| **Integration** | `conftest.py` template with local Spark + Delta fixtures (session-scoped). |
| **Component** | Light DI harness: `kindling.testing.reset_framework()`, pre-wired memory providers. |
| **System** | Already exists: `poe test-system`, stdout marker validation, platform runners. |

## 4. Current State Analysis

### 4.1 What Domain Developers Must Know Today

To build a domain-specific package on Kindling, a developer currently needs to understand:

**Core Patterns (mandatory):**
- `@DataEntities.entity()` decorator with `entityid`, `name`, `tags`, `schema`, `partition_columns`, `merge_columns`
- `@DataPipes.pipe()` decorator with `pipeid`, `input_entity_ids`, `output_entity_id`, `output_type`
- `get_kindling_service(ServiceType)` for DI-based service access
- `get_or_create_spark_session()` for Spark session
- `BOOTSTRAP_CONFIG` dictionary for framework initialization
- `app.yaml` structure for data app configuration
- Platform-specific config overlays (`app.fabric.yaml`, etc.)

**Infrastructure Patterns (for anything beyond trivial):**
- `EntityProvider` interface composition (`BaseEntityProvider`, `StreamableEntityProvider`, `WritableEntityProvider`, `StreamWritableEntityProvider`)
- `EntityProviderRegistry.register_provider()` for custom data sources
- `EntityPathLocator` and `EntityNameMapper` ABCs that users must implement
- `WatermarkEntityFinder` ABC for incremental processing
- `@GlobalInjector.singleton_autobind()` for DI registration of custom services
- Signal system (`SignalEmitter` mixin, `EMITS` list) for observability hooks
- `ExecutionStrategy` selection (batch vs streaming vs config-based)

**Deployment Patterns (to ship anything):**
- `.kda` packaging via `DataAppPackage.create()`
- `app.yaml` + `requirements.txt` + `lake-reqs.txt` conventions
- Bootstrap shim pattern for Spark job execution
- Module-level execution (no `if __name__ == "__main__"`, no `main()` function)
- Dual logging (`app_logger.info()` + `print()`) for cross-platform log capture

### 4.2 What Exists Today

| Capability | Implementation | Consumer-Ready? | Useful At Tier |
|-----------|---------------|-----------------|----------------|
| Local Spark session | `get_or_create_spark_session()` | Yes | Integration (T2) |
| Standalone platform | `StandaloneService` (642 lines) | Partial — works but no guided setup | Component (T3) |
| Memory entity provider | `MemoryEntityProvider` — all 4 interfaces | Yes — great for testing | Component (T3) |
| Unit test helpers | `spark_test_helper.py`, `conftest.py` fixtures | Framework-internal only | Unit (T1) |
| Test framework | `NotebookTester`, `MemoryTestCollector` | Notebook-specific, not file-based | — |
| Build/deploy tasks | `poe build`, `poe deploy`, `poe deploy-app` | Framework maintainer-oriented | System (T4) |
| Example apps | `tests/data-apps/` (6 apps) | Reference only, not templates | — |
| Documentation | `docs/intro.md`, `docs/setup_guide.md` | Platform-centric, not dev-centric | — |

### 4.3 What's Missing (Prioritized by Testing Tier)

**Critical for Tier 1-2 (Unit + Integration — daily dev loop):**

1. **No consumer-facing `conftest.py` template** — Framework's own test fixtures exist but aren't packaged for domain devs. Domain developers need a pre-built conftest with local Spark session, Delta Lake config, and `tmp_path` helpers.
2. **No `kindling init` or project generator** — Developers start from scratch or copy examples. Even a basic project template with proper test structure would eliminate most friction.
3. **No tutorial/walkthrough** — Docs explain concepts but not "build your first app, write a pipe, test it locally."
4. **No project structure convention** — No guidance on where entities, pipes, config, tests go.

**Useful for Tier 3 (Component tests — weekly):**

5. **No test harness for DI** — `GlobalInjector.reset()` exists but there's no pre-built fixture that wires up memory providers, mock loggers, and config for component-level testing.
6. **No `py.typed` marker or type stubs** — Missing for IDE autocomplete and mypy.

**Nice-to-have for Tier 3-4 (Component + System — per release):**

7. **No standalone bootstrap path** — `initialize_framework()` assumes cloud storage. This only matters for full component tests; unit and integration tests don't need it.
8. **No stub generators** — Every entity, pipe, and provider is written from zero.
9. **No consumer-facing CLI** — `poe` tasks target framework maintainers.

> **Key observation:** Items 1-4 are low effort and unlock ~90% of the daily dev loop. Items 5-9 add value but are not blockers for productive domain development.

---

## 5. CLI for Project Bootstrapping

### 5.1 CLI Framework Choice

| Option | Pros | Cons | Recommendation |
|--------|------|------|----------------|
| **`click`** | Mature, widely used, composable, good for complex CLIs | More verbose than typer | **Recommended** |
| `typer` | Modern, auto-generates help from type hints | Depends on click anyway, less control | Good alternative |
| `argparse` | Stdlib, no deps | Verbose, poor UX for complex CLIs | Not recommended |

**Recommendation:** Use `click` — it's battle-tested, composable, and the kindling project already has complex task patterns. Add as an optional dependency (`kindling[cli]`).

### 5.2 CLI Command Design

```
kindling
├── init                    # Scaffold a new domain package project
│   ├── --template app      # Data app project (default)
│   ├── --template package  # Reusable library package
│   ├── --template notebook # Notebook-first project
│   └── --platform fabric|synapse|databricks|all
│
├── add                     # Generate framework primitives
│   ├── entity              # Add entity definition stub
│   ├── pipe                # Add pipe definition stub
│   ├── provider            # Add custom entity provider stub
│   ├── ingestion           # Add file ingestion entry stub
│   └── config              # Add config file (settings.yaml, app.yaml, etc.)
│
├── dev                     # Local development & testing
│   ├── test                # Run pytest with Spark/Delta fixtures (PRIMARY)
│   ├── validate            # Validate project structure and config
│   └── run                 # Run app locally with standalone Spark (OPTIONAL)
│
├── build                   # Packaging
│   ├── app                 # Build .kda package
│   └── wheel               # Build .whl package (for libraries)
│
└── deploy                  # Deployment
    ├── app                 # Deploy .kda to platform
    └── job                 # Create and submit Spark job
```

> **Note:** `kindling dev test` is the primary local command, not `kindling dev run`. The vast majority of domain development is transform code that is tested via pytest with local Spark, not via full app execution. `kindling dev run` exists for occasional full-stack testing but is NOT the daily workflow.

### 5.3 `kindling init` — Project Scaffolding

#### Data App Template (default)

```
my-data-app/
├── pyproject.toml              # Poetry project with kindling dependency
├── README.md                   # Auto-generated with getting started guide
├── .gitignore                  # Python + Spark artifacts
├── app.yaml                    # App configuration
├── app.fabric.yaml             # Fabric-specific overrides (if --platform fabric|all)
├── app.synapse.yaml            # Synapse-specific overrides (if --platform synapse|all)
├── app.databricks.yaml         # Databricks-specific overrides (if --platform databricks|all)
├── settings.yaml               # Framework settings (logging, delta config, etc.)
├── requirements.txt            # PyPI dependencies
├── lake-reqs.txt               # Lake wheel dependencies (kindling)
├── main.py                     # Entry point (module-level execution pattern)
├── entities/
│   ├── __init__.py             # Entity registration module
│   └── bronze.py               # Example bronze layer entities
├── pipes/
│   ├── __init__.py             # Pipe registration module
│   └── clean_data.py           # Example transformation pipe
├── providers/                  # Custom entity providers (optional)
│   └── __init__.py
├── config/                     # Additional config files (optional)
│   └── entity_tags.yaml        # Entity tag overrides
└── tests/
    ├── __init__.py
    ├── conftest.py             # Spark session + DI fixtures for local testing
    ├── test_entities.py        # Entity registration tests
    └── test_pipes.py           # Pipe transformation tests
```

#### Library Package Template

For reusable packages consumed by other Kindling apps:

```
my-kindling-lib/
├── pyproject.toml
├── README.md
├── .gitignore
├── src/
│   └── my_kindling_lib/
│       ├── __init__.py         # Package exports
│       ├── entities.py         # Shared entity definitions
│       ├── pipes.py            # Shared pipe definitions
│       └── providers/          # Custom providers
│           └── __init__.py
└── tests/
    ├── conftest.py
    └── test_entities.py
```

### 5.4 Generated File Contents

#### `main.py` (Data App Entry Point)

```python
#!/usr/bin/env python3
"""
{project_name} - Data App

Kindling data application. This file is the entry point executed by the
bootstrap system. Code executes at module level (no main() function needed).
"""
import sys
from kindling.injection import get_kindling_service
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session

# Import entity and pipe definitions (triggers decorator registration)
from entities import *
from pipes import *

# ---- Framework Services ----
logger = get_kindling_service(SparkLoggerProvider).get_logger("{project_name}")
config = get_kindling_service(ConfigService)
spark = get_or_create_spark_session()

logger.info("Starting {project_name}")

# ---- Run Pipeline ----
from kindling.data_pipes import DataPipesExecution
executer = get_kindling_service(DataPipesExecution)

try:
    # Execute all registered pipes in dependency order
    executer.run_datapipes([
        # List pipe IDs to execute, or use discovery
        "clean_data",
    ])
    logger.info("{project_name} completed successfully")
    sys.exit(0)
except Exception as e:
    logger.error(f"{project_name} failed: {e}", exc_info=True)
    sys.exit(1)
```

#### `entities/bronze.py` (Entity Stub)

```python
"""Bronze layer entity definitions for {project_name}."""
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from kindling.data_entities import DataEntities


# ---- Schema Definitions ----

raw_events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("payload", StringType(), True),
])


# ---- Entity Registration ----

@DataEntities.entity(
    entityid="bronze.raw_events",
    name="Raw Events",
    partition_columns=["event_type"],
    merge_columns=["event_id"],
    tags={{
        "layer": "bronze",
        "domain": "{domain}",
        "provider_type": "delta",       # Uses Delta Lake provider
    }},
    schema=raw_events_schema,
)
```

#### `pipes/clean_data.py` (Pipe Stub)

```python
"""Data transformation pipes for {project_name}."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from kindling.data_pipes import DataPipes


@DataPipes.pipe(
    pipeid="clean_data",
    name="Clean Raw Events",
    tags={{"category": "cleaning", "domain": "{domain}"}},
    input_entity_ids=["bronze.raw_events"],
    output_entity_id="silver.clean_events",
    output_type="table",
)
def clean_data(bronze_raw_events: DataFrame) -> DataFrame:
    \"\"\"Clean raw events: deduplicate and filter nulls.

    Args:
        bronze_raw_events: Raw events DataFrame (auto-injected by framework)

    Returns:
        Cleaned DataFrame written to silver.clean_events
    \"\"\"
    return (
        bronze_raw_events
        .filter(col("event_id").isNotNull())
        .dropDuplicates(["event_id"])
        .withColumn("processed_at", current_timestamp())
    )
```

#### `tests/conftest.py` (Local Test Fixtures — The Most Important Generated File)

This is the highest-value artifact from `kindling init`. It enables Tiers 1-3 of local testing:

```python
"""Test fixtures for local development and CI.

Provides 3 tiers of testing support:
  - Tier 1 (Unit): No fixtures needed — call pipe functions directly
  - Tier 2 (Integration): `spark` fixture for local Spark + Delta
  - Tier 3 (Component): `kindling_env` fixture for DI + memory providers
"""
import pytest
from unittest.mock import MagicMock


# ---- Tier 2: Local Spark Session ----

@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session with Delta Lake support.

    Usage:
        def test_clean_orders(spark):
            df = spark.createDataFrame([...])
            result = clean_orders(df)
            assert result.count() == 1
    """
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("{project_name}-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---- Tier 3: Kindling DI Harness ----

@pytest.fixture(autouse=True)
def reset_injector():
    """Reset DI container between tests (prevents cross-test pollution)."""
    from kindling.injection import GlobalInjector
    GlobalInjector.reset()
    yield
    GlobalInjector.reset()


@pytest.fixture
def kindling_env(spark):
    """Set up Kindling DI with memory providers for component testing.

    Usage:
        def test_pipeline_through_framework(kindling_env):
            import entities.bronze
            import pipes.clean_data
            # entities and pipes are now registered in DI
    """
    from kindling.injection import GlobalInjector
    from kindling.entity_provider_memory import MemoryEntityProvider
    from kindling.entity_provider_registry import EntityProviderRegistry

    registry = GlobalInjector.get(EntityProviderRegistry)
    registry.register_provider("delta", MemoryEntityProvider)
    registry.register_provider("memory", MemoryEntityProvider)
    yield registry


@pytest.fixture
def mock_logger():
    """Provide a mock logger for unit tests that don't need Spark logging."""
    logger = MagicMock()
    logger.info = MagicMock()
    logger.error = MagicMock()
    logger.warning = MagicMock()
    return logger
```

#### `app.yaml` (App Configuration)

```yaml
name: {project_name}
description: "{description}"
entry_point: main.py
environment: development

dependencies: []

metadata:
  version: "0.1.0"
  domain: "{domain}"
```

---

## 6. Stub Generation for Entities, Pipes, and Providers

### 6.1 `kindling add entity`

Interactive or argument-driven entity creation:

```bash
# Interactive
kindling add entity

# Non-interactive
kindling add entity \
  --id "bronze.orders" \
  --name "Raw Orders" \
  --partition-columns "region,order_date" \
  --merge-columns "order_id" \
  --provider delta \
  --layer bronze \
  --domain sales \
  --file entities/bronze.py
```

**Generated code** appended to the target file:

```python
@DataEntities.entity(
    entityid="bronze.orders",
    name="Raw Orders",
    partition_columns=["region", "order_date"],
    merge_columns=["order_id"],
    tags={
        "layer": "bronze",
        "domain": "sales",
        "provider_type": "delta",
    },
)
```

### 6.2 `kindling add pipe`

```bash
kindling add pipe \
  --id "transform_orders" \
  --name "Transform Orders" \
  --input "bronze.orders" \
  --output "silver.clean_orders" \
  --file pipes/transform.py
```

**Generated code:**

```python
@DataPipes.pipe(
    pipeid="transform_orders",
    name="Transform Orders",
    tags={"category": "transformation"},
    input_entity_ids=["bronze.orders"],
    output_entity_id="silver.clean_orders",
    output_type="table",
)
def transform_orders(bronze_orders: DataFrame) -> DataFrame:
    """Transform bronze.orders → silver.clean_orders.

    Args:
        bronze_orders: Input DataFrame (auto-injected)

    Returns:
        Transformed DataFrame
    """
    # TODO: Implement transformation logic
    return bronze_orders
```

### 6.3 `kindling add provider`

Custom provider scaffolding is the highest-value generator because the interface is complex:

```bash
kindling add provider \
  --name "parquet" \
  --capabilities read,write \
  --file providers/parquet_provider.py
```

**Generated code** based on capabilities:

```python
"""Custom Parquet entity provider."""
from kindling.entity_provider import BaseEntityProvider, WritableEntityProvider
from kindling.data_entities import EntityMetadata
from pyspark.sql import DataFrame


class ParquetEntityProvider(BaseEntityProvider, WritableEntityProvider):
    """Entity provider for Parquet format.

    Register with:
        registry.register_provider("parquet", ParquetEntityProvider)
    """

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        config = self._get_provider_config(entity_metadata)
        path = config.get("path")
        # TODO: Implement Parquet read
        from kindling.spark_session import get_or_create_spark_session
        return get_or_create_spark_session().read.parquet(path)

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        config = self._get_provider_config(entity_metadata)
        path = config.get("path")
        # TODO: Check if path exists
        return path is not None

    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        config = self._get_provider_config(entity_metadata)
        path = config.get("path")
        # TODO: Implement Parquet write
        df.write.mode("overwrite").parquet(path)

    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        config = self._get_provider_config(entity_metadata)
        path = config.get("path")
        # TODO: Implement Parquet append
        df.write.mode("append").parquet(path)
```

### 6.4 `kindling add config`

```bash
kindling add config settings            # Generate settings.yaml template
kindling add config platform --platform fabric  # Generate platform_fabric.yaml
kindling add config env --env production        # Generate env_production.yaml
```

### 6.5 Capability Matrix

| Stub Type | Complexity | Parameters Required | Lines Generated |
|-----------|-----------|-------------------|-----------------|
| Entity | Low | entityid, name, optional tags/schema | ~15-25 |
| Pipe | Low | pipeid, inputs, output | ~20-30 |
| Provider (read-only) | Medium | name, provider_type | ~30-40 |
| Provider (full) | Medium-High | name, all capabilities | ~60-100 |
| Ingestion entry | Low | entry_id, patterns, dest_entity | ~15 |
| Config file | Low | config type | ~20-30 (YAML) |

---

## 7. Development & Testing Workflow

### 7.1 The Daily Dev Loop (Tiers 1-2)

This is where 90%+ of domain developer time is spent. **No kindling bootstrap required.**

```
    ┌─────────────────────────────────────────────────────────────────┐
    │  Developer's Daily Loop                                        │
    │                                                                 │
    │  1. Write/edit pipe function (pure PySpark: DataFrame → DF)    │
    │  2. Run `pytest tests/test_transforms.py`                      │
    │     - Spark session created from conftest.py fixture            │
    │     - Test creates input DataFrame                              │
    │     - Test calls pipe function DIRECTLY                         │
    │     - Test asserts on output DataFrame                          │
    │  3. See test pass/fail in ~2-5 seconds                          │
    │  4. Repeat                                                      │
    │                                                                 │
    │  With CLI:  `kindling dev test`  (wraps pytest with correct     │
    │             Spark/Delta classpath & pytest markers)              │
    └─────────────────────────────────────────────────────────────────┘
```

**Example test workflow:**

```bash
# Write a pipe
cat > pipes/clean_orders.py << 'EOF'
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from kindling.data_pipes import DataPipes

@DataPipes.pipe(
    pipeid="clean_orders", name="Clean Orders",
    tags={"category": "cleaning"},
    input_entity_ids=["bronze.orders"],
    output_entity_id="silver.clean_orders",
    output_type="table",
)
def clean_orders(bronze_orders: DataFrame) -> DataFrame:
    return (
        bronze_orders
        .filter(col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
        .withColumn("processed_at", current_timestamp())
    )
EOF

# Write the test
cat > tests/test_clean_orders.py << 'EOF'
from pipes.clean_orders import clean_orders

def test_clean_orders_drops_nulls(spark):
    df = spark.createDataFrame([
        ("ord-1", "us-west", 100),
        (None, "us-east", 200),
        ("ord-1", "us-west", 100),
    ], ["order_id", "region", "amount"])

    result = clean_orders(df)

    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ord-1"
    assert "processed_at" in result.columns
EOF

# Run it
kindling dev test  # or just: pytest tests/
```

### 7.2 Framework Integration Testing (Tier 3 — Weekly)

When you need to verify entity registration, pipe wiring, or framework plumbing:

```
    ┌─────────────────────────────────────────────────────────────────┐
    │  Component Integration Loop                                     │
    │                                                                 │
    │  1. Import entity and pipe modules (triggers DI registration)  │
    │  2. Kindling_env fixture wires MemoryEntityProvider             │
    │  3. Seed input data via memory provider                         │
    │  4. Execute pipe through DataPipesExecuter (framework engine)   │
    │  5. Read output via memory provider                             │
    │  6. Assert on output                                            │
    │                                                                 │
    │  NO bootstrap. NO config files. NO cloud platform.              │
    │  Just DI + registries + memory providers.                       │
    └─────────────────────────────────────────────────────────────────┘
```

**When to use Tier 3:**
- Verifying `input_entity_ids` wiring is correct (entity IDs match parameter names)
- Testing that entity metadata (tags, schema, partition columns) is registered correctly
- Testing custom `EntityProvider` implementations
- Testing pipe ordering/dependencies in a multi-pipe pipeline
- Testing the `EntityNameMapper` / `EntityPathLocator` implementations

### 7.3 Release Validation (Tier 4 — Per Release)

System tests validate the full stack by deploying to a cloud platform. This is the existing system test infrastructure.

```
    ┌─────────────────────────────────────────────────────────────────┐
    │  Release Validation (CI/CD)                                     │
    │                                                                 │
    │  1. `kindling build app --platform fabric`  → .kda              │
    │  2. `poe deploy --platform fabric`  → upload to artifacts      │
    │  3. `poe test-system --platform fabric`  → deploy Spark job    │
    │     - Bootstrap shim initializes framework                      │
    │     - Full DI, config, platform services                        │
    │     - Validates stdout markers for test pass/fail               │
    │  4. Results reported in CI/CD pipeline                          │
    │                                                                 │
    │  CLOUD REQUIRED. Run in CI/CD, not local dev.                   │
    └─────────────────────────────────────────────────────────────────┘
```

### 7.4 Complete Lifecycle Comparison

| Phase | Today | Proposed |
|-------|-------|----------|
| Project creation | Copy examples manually | `kindling init my-app` |
| Entity/pipe definition | Write from scratch | `kindling add entity/pipe` |
| **Transform dev (Tier 1-2)** | `pytest` (must configure manually) | `kindling dev test` (or `pytest` with generated conftest) |
| **Framework integration (Tier 3)** | Not supported cleanly | `pytest` with `kindling_env` fixture |
| **Cloud validation (Tier 4)** | `poe test-system` | Same (already works) |
| Configuration | Manual YAML files | `kindling add config` |
| Packaging | `DataAppPackage.create()` | `kindling build app` |
| Deployment | `poe deploy-app` | `kindling deploy app` (or same poe tasks) |

### 7.5 Why NOT `kindling dev run` as Primary

The original proposal considered `kindling dev run` (full local app execution) as the primary dev command. This was over-engineered because:

1. **Full bootstrap is fragile locally** — `initialize_framework()` expects cloud storage, config download, platform detection. Replicating all of this for `standalone` is significant effort.
2. **Transforms don't need bootstrap** — The `@DataPipes.pipe()` decorator stores a plain function reference. Calling it doesn't touch DI, config, or platform services.
3. **pytest is faster feedback** — A single pipe test runs in 2-5 seconds. Full app execution (Spark init + DI + all pipes) takes 15-30 seconds even locally.
4. **pytest is more precise** — Test one pipe at a time, with specific inputs, checking specific outputs. Full app execution tests the whole pipeline or nothing.

`kindling dev run` can still exist as an optional command for occasional full-stack testing, but it is NOT the daily workflow and should be deprioritized.

---

## 8. Test Infrastructure & Fixtures

### 8.1 What Kindling Should Ship for Domain Testing

The framework should provide a small, focused **testing support module** that domain developers use but don't need to understand internally.

#### `kindling.testing` Module (New)

```python
# kindling/testing.py — Published as part of kindling package
"""Test utilities for domain packages built on Kindling.

Provides pytest fixtures and helpers for local development.
Import in conftest.py or test files.
"""
from kindling.injection import GlobalInjector
from kindling.entity_provider_memory import MemoryEntityProvider
from kindling.entity_provider_registry import EntityProviderRegistry


def reset_framework():
    """Reset all DI registrations. Call in autouse fixture between tests."""
    GlobalInjector.reset()


def create_test_harness(spark_session=None, provider_type="memory"):
    """Create a light DI harness for component testing.

    Wires up:
    - MemoryEntityProvider for all entity types
    - Mock logger provider
    - ConfigService with empty/default config

    Returns a TestHarness with helper methods for seeding data and
    executing pipes.
    """
    harness = TestHarness(spark_session)
    harness.register_provider(provider_type, MemoryEntityProvider)
    return harness


class TestHarness:
    """Light-weight test harness for Tier 3 component testing.

    Usage:
        harness = create_test_harness(spark)
        harness.seed("bronze.orders", test_dataframe)
        harness.execute_pipe("clean_orders")
        result = harness.read("silver.clean_orders")
        assert result.count() == expected
    """

    def __init__(self, spark_session=None):
        self.spark = spark_session
        self._providers = {}

    def register_provider(self, name, provider_class):
        registry = GlobalInjector.get(EntityProviderRegistry)
        registry.register_provider(name, provider_class)

    def seed(self, entity_id, dataframe):
        """Write test data to an entity via memory provider."""
        from kindling.data_entities import DataEntityRegistry
        registry = GlobalInjector.get(DataEntityRegistry)
        entity = registry.get_entity_definition(entity_id)
        provider = GlobalInjector.get(MemoryEntityProvider)
        provider.write_to_entity(dataframe, entity)

    def read(self, entity_id):
        """Read data from an entity via memory provider."""
        from kindling.data_entities import DataEntityRegistry
        registry = GlobalInjector.get(DataEntityRegistry)
        entity = registry.get_entity_definition(entity_id)
        provider = GlobalInjector.get(MemoryEntityProvider)
        return provider.read_entity(entity)

    def execute_pipe(self, pipe_id):
        """Execute a single pipe through the framework engine."""
        from kindling.data_pipes import DataPipesExecution
        executer = GlobalInjector.get(DataPipesExecution)
        executer.run_datapipes([pipe_id])
```

### 8.2 Testing Patterns by Tier (with Code Examples)

#### Tier 1: Pure Unit Tests (Zero Dependencies)

Test transformation logic with mock DataFrames. Fastest feedback possible.

```python
# tests/unit/test_transforms.py
"""Tier 1: Test pipe logic with mocks. No Spark, no Kindling."""
from unittest.mock import MagicMock

def test_clean_orders_calls_filter():
    """Verify the pipe calls expected DataFrame operations."""
    from pipes.clean_orders import clean_orders

    mock_df = MagicMock()
    mock_df.filter.return_value = mock_df
    mock_df.dropDuplicates.return_value = mock_df
    mock_df.withColumn.return_value = mock_df

    result = clean_orders(mock_df)

    mock_df.filter.assert_called_once()
    mock_df.dropDuplicates.assert_called_once_with(["order_id"])
```

#### Tier 2: Integration Tests with Real Spark

Test transforms produce correct output with real DataFrames and Delta.

```python
# tests/integration/test_transforms.py
"""Tier 2: Test pipe output with real Spark. No Kindling DI."""

def test_clean_orders_removes_nulls_and_dupes(spark):
    """End-to-end transform test with real DataFrames."""
    from pipes.clean_orders import clean_orders

    input_df = spark.createDataFrame([
        ("ord-1", "us-west", 100),
        (None, "us-east", 200),          # null order_id → filtered
        ("ord-1", "us-west", 100),       # duplicate → deduped
        ("ord-2", "eu-north", 50),
    ], ["order_id", "region", "amount"])

    result = clean_orders(input_df)

    assert result.count() == 2
    ids = [row.order_id for row in result.collect()]
    assert set(ids) == {"ord-1", "ord-2"}
    assert "processed_at" in result.columns


def test_clean_orders_with_delta_roundtrip(spark, tmp_path):
    """Test that output is Delta-compatible (schema, partitioning)."""
    from pipes.clean_orders import clean_orders

    input_df = spark.createDataFrame([
        ("ord-1", "us-west", 100),
        ("ord-2", "eu-north", 50),
    ], ["order_id", "region", "amount"])

    result = clean_orders(input_df)

    # Write to Delta and read back
    output_path = str(tmp_path / "silver" / "clean_orders")
    result.write.format("delta").save(output_path)
    roundtrip = spark.read.format("delta").load(output_path)

    assert roundtrip.count() == 2
    assert roundtrip.schema == result.schema
```

#### Tier 3: Component Tests with DI Harness

Test entity wiring, pipe execution engine, and framework plumbing.

```python
# tests/component/test_pipeline.py
"""Tier 3: Test framework integration with light DI harness."""
from kindling.testing import create_test_harness, reset_framework

def test_pipeline_end_to_end(spark):
    """Test entity → pipe → output through framework engine."""
    reset_framework()

    # Import entities and pipes (triggers decorator registration)
    import entities.bronze
    import pipes.clean_orders

    # Create harness and seed test data
    harness = create_test_harness(spark)
    test_data = spark.createDataFrame([
        ("ord-1", "us-west", 100),
        (None, "us-east", 200),
        ("ord-1", "us-west", 100),
    ], ["order_id", "region", "amount"])

    harness.seed("bronze.orders", test_data)
    harness.execute_pipe("clean_orders")
    result = harness.read("silver.clean_orders")

    assert result.count() == 1
    assert result.collect()[0]["order_id"] == "ord-1"

    reset_framework()
```

### 8.3 Existing Framework Patterns That Prove This Works

The Kindling framework's own test suite already demonstrates every tier:

| Pattern | Location | Tier |
|---------|----------|------|
| Mock-based pipe execution | `tests/unit/test_data_pipes.py:TestDataPipesExecuter` | Tier 1 |
| Entity registration with mocks | `tests/unit/test_data_pipes.py:TestDataPipesRegistration` | Tier 1 |
| Memory provider for testing | `packages/kindling/entity_provider_memory.py` | Tier 3 |
| Full app execution on cloud | `tests/system/core/test_platform_job_deployment.py` | Tier 4 |
| Minimal test app (zero imports) | `tests/data-apps/minimal-test-app/main.py` | Reference |
| Streaming pipes test app | `tests/data-apps/streaming-pipes-test-app/main.py` | Reference |

The `TestDataPipesExecuter` test class at line 488 of `test_data_pipes.py` is particularly instructive — it creates the entire execution engine with nothing but `Mock()` objects:

```python
# From tests/unit/test_data_pipes.py — existing framework test
def setup_method(self):
    self.mock_logger_provider = Mock()
    self.mock_entity_registry = Mock()
    self.mock_pipes_registry = Mock()
    self.mock_erps = Mock()
    self.mock_trace_provider = Mock()

    self.executer = DataPipesExecuter(
        logger_provider=self.mock_logger_provider,
        entity_registry=self.mock_entity_registry,
        pipes_registry=self.mock_pipes_registry,
        erps=self.mock_erps,
        trace_provider=self.mock_trace_provider,
    )
```

This proves the framework is already designed for testability. The missing piece is **packaging these patterns for domain developers** via the `conftest.py` template and `kindling.testing` module.

### 8.4 Optional: Standalone Bootstrap for Full Local Execution

For teams that need full local app execution (rare), these changes to `bootstrap.py` would enable `kindling dev run`:

| Change | Effort | Why Optional |
|--------|--------|-------------|
| `detect_platform()` fallback to `"standalone"` | 2h | Only needed for Tier 3+ with full DI |
| `download_config_files()` local filesystem path | 4h | Only needed for hierarchical config locally |
| `LocalEntityPathLocator` default | 4h | Only needed for Delta entity paths locally |
| `initialize_framework()` standalone mode | 1d | Only needed for `kindling dev run` |

**These are NOT blockers for Tiers 1-2** (daily dev loop). They can be implemented later if demand emerges.

### 8.5 IDE Experience Improvements

| Enhancement | Effort | Impact | Tier Affected |
|------------|--------|--------|---------------|
| Add `py.typed` marker to `kindling` package | Trivial | IDE type checking | All |
| Type annotations on decorator return types | Low | Autocomplete for `@DataEntities.entity()` | T1-T3 |
| Export all public APIs from `kindling.__init__` | Already done | ✅ Works today | All |
| VS Code snippets for entity/pipe patterns | Low | Faster coding in VS Code | T1-T2 |
| `.vscode/settings.json` in scaffolded project | Trivial | Correct Python path, test config | All |

---

## 9. Implementation Roadmap

### Phase 1: Test-First Foundation (1-2 weeks)

**Goal:** Domain developers can `kindling init` a project and immediately test transforms locally.

This phase delivers the highest-value items: project scaffolding and the `conftest.py` template that enables Tiers 1-2 (daily dev loop).

| Item | Effort | Priority | Tier Enabled |
|------|--------|----------|-------------|
| Add `click` as optional dependency (`kindling[cli]`) | 2h | P0 | — |
| Create `kindling/cli/` module with `init` command | 2d | P0 | — |
| Create data app project template (with test structure) | 2d | P0 | T1-T2 |
| **Generate `conftest.py` with Spark + Delta fixtures** | 4h | P0 | **T1-T2** |
| Add `py.typed` marker | 1h | P0 | T1-T3 |
| Create `kindling.testing` module (`reset_framework()`, `TestHarness`) | 1d | P1 | T3 |
| `kindling dev test` command (wraps pytest with correct classpath) | 4h | P1 | T1-T2 |
| `kindling dev validate` command (check project structure) | 1d | P2 | — |

**Deliverable:** `kindling init my-app` → edit `pipes/clean_data.py` → `pytest tests/` (or `kindling dev test`) → see pass/fail in seconds.

**NOT in Phase 1:** No bootstrap fixes, no `kindling dev run`, no standalone platform changes. These are not needed for the daily dev loop.

### Phase 2: Stub Generation (1-2 weeks)

**Goal:** `kindling add entity/pipe/provider` generators reduce boilerplate.

| Item | Effort | Priority |
|------|--------|----------|
| `kindling add entity` command | 1d | P0 |
| `kindling add pipe` command (generates matching test stub) | 1d | P0 |
| `kindling add provider` command (all 4 interface combos) | 2d | P1 |
| `kindling add config` command (YAML templates) | 4h | P1 |
| `kindling add ingestion` command | 4h | P2 |

**Deliverable:** Developers never memorize decorator signatures. Adding a pipe also generates a test file.

### Phase 3: Packaging & Deployment (1-2 weeks)

**Goal:** `kindling build` and `kindling deploy` commands wrap existing infrastructure.

| Item | Effort | Priority |
|------|--------|----------|
| `kindling build app` — wraps `DataAppPackage.create()` | 1d | P0 |
| `kindling build wheel` — for library packages | 1d | P1 |
| `kindling deploy app` — wraps platform deploy | 2d | P1 |
| `kindling deploy job` — wraps `PlatformAPI.create_job()` | 2d | P2 |
| Library package template (`kindling init --template package`) | 1d | P2 |

**Deliverable:** Full lifecycle from `kindling init` to deployed Spark job.

### Phase 4: Optional Enhancements (1-2 weeks, as needed)

**Goal:** Address edge cases for teams needing full local execution or advanced DI testing.

| Item | Effort | Priority | Only If Needed |
|------|--------|----------|----------------|
| Fix `detect_platform()` to support `"standalone"` | 2h | P2 | For `kindling dev run` |
| Fix `download_config_files()` local filesystem path | 4h | P2 | For `kindling dev run` |
| Add `LocalEntityPathLocator` default | 4h | P2 | For Delta paths locally |
| `kindling dev run` command (full local execution) | 2d | P2 | For teams that need it |
| Walk-through tutorial document | 2d | P1 | General |
| `kindling doctor` — diagnose environment issues | 1d | P2 | General |

### Revised Total Effort Estimate

| Phase | Duration | FTE Weeks | Tiers Unlocked |
|-------|----------|-----------|----------------|
| Phase 1: Test-First Foundation | 1-2 weeks | 1.5 | **T1, T2, T3** |
| Phase 2: Stub Generation | 1-2 weeks | 1.5 | Developer velocity |
| Phase 3: Packaging & Deployment | 1-2 weeks | 1.5 | T4 (CI/CD) |
| Phase 4: Optional Enhancements | 1-2 weeks | 1.0 | `kindling dev run` |
| **Total** | **4-8 weeks** | **~5.5 weeks** |

> **Compared to original estimate:** Reduced from ~8 FTE weeks to ~5.5 by deprioritizing `kindling dev run` and bootstrap fixes. The daily dev loop (Tiers 1-2) is fully enabled by Phase 1 alone (~1.5 weeks).

---

## Appendices

### A. Framework API Surface Summary

The Kindling framework exposes the following public APIs that domain developers interact with:

#### Decorators (Registration)
| Decorator | Module | Purpose |
|-----------|--------|---------|
| `@DataEntities.entity()` | `data_entities` | Register a data entity with metadata |
| `@DataPipes.pipe()` | `data_pipes` | Register a transformation pipe |
| `@FileIngestionEntries.entry()` | `file_ingestion` | Register file ingestion patterns |
| `@GlobalInjector.singleton_autobind()` | `injection` | Register DI singleton |

#### Abstract Base Classes (Extension Points)
| ABC | Module | Purpose | Must Implement? |
|-----|--------|---------|-----------------|
| `BaseEntityProvider` | `entity_provider` | Read entities | Only for custom providers |
| `StreamableEntityProvider` | `entity_provider` | Stream reads | Optional |
| `WritableEntityProvider` | `entity_provider` | Batch writes | Optional |
| `StreamWritableEntityProvider` | `entity_provider` | Streaming writes | Optional |
| `EntityPathLocator` | `data_entities` | Map entity ID → path | **Yes** (for Delta) |
| `EntityNameMapper` | `data_entities` | Map entity ID → table name | **Yes** (for Delta forName mode) |
| `WatermarkEntityFinder` | `watermarking` | Find watermark entity | Only if using watermarks |
| `ExecutionStrategy` | `execution_strategy` | Custom execution ordering | Rarely needed |
| `ConfigService` | `spark_config` | Custom config backend | No (DynaconfConfig default) |
| `SignalProvider` | `signaling` | Custom event system | No (BlinkerSignalProvider default) |

#### Service Access (DI)
| Function | Module | Purpose |
|----------|--------|---------|
| `get_kindling_service(Type)` | `injection` | Get service from DI container |
| `GlobalInjector.get(Type)` | `injection` | Same, lower-level |
| `GlobalInjector.reset()` | `injection` | Reset container (for testing) |

#### Key Services (Retrieved via DI)
| Service Type | Purpose |
|-------------|---------|
| `ConfigService` | Read/write configuration |
| `PlatformServiceProvider` | Get platform service |
| `SparkLoggerProvider` | Get Spark-compatible logger |
| `PythonLoggerProvider` | Get Python logger |
| `DataEntityRegistry` | Look up registered entities |
| `DataPipesRegistry` | Look up registered pipes |
| `DataPipesExecution` | Execute pipes |
| `EntityProviderRegistry` | Look up/register providers |
| `WatermarkService` | Watermark operations |
| `SparkTraceProvider` | Distributed tracing |
| `SignalProvider` | Create/get signals |

#### Functions (Direct Import)
| Function | Module | Purpose |
|----------|--------|---------|
| `get_or_create_spark_session()` | `spark_session` | Get Spark session |
| `initialize_framework(config)` | `bootstrap` | Bootstrap framework |
| `bootstrap_framework(config)` | `bootstrap` | Alias for above |
| `is_framework_initialized()` | `bootstrap` | Check init status |

### B. Comparable Tools in Other Ecosystems

| Ecosystem | CLI Tool | What It Does | Kindling Equivalent |
|-----------|----------|-------------|---------------------|
| Django | `django-admin startproject` | Scaffold project | `kindling init` |
| Django | `manage.py startapp` | Add app module | `kindling add entity/pipe` |
| dbt | `dbt init` | Scaffold dbt project | `kindling init` |
| dbt | `dbt run` | Execute models | `kindling dev test` (primary) / `kindling dev run` (optional) |
| Rails | `rails generate model` | Scaffold model + migration | `kindling add entity` |
| Nx | `nx generate` | Scaffold component | `kindling add pipe` |
| Angular | `ng generate component` | Scaffold component | `kindling add pipe` |
| FastAPI (cookiecutter) | `cookiecutter template` | Scaffold project | `kindling init` |

### C. Entity Tag Configuration Reference

Tags are the primary configuration mechanism for entities. Domain developers need to understand these:

```python
@DataEntities.entity(
    entityid="bronze.orders",
    tags={
        # Provider selection
        "provider_type": "delta",           # Which EntityProvider to use

        # Delta provider tags
        "provider.path": "abfss://...",     # Explicit storage path
        "provider.access_mode": "forPath",  # forName | forPath | auto

        # CSV provider tags
        "provider.path": "/data/orders.csv",
        "provider.header": "true",
        "provider.delimiter": ",",

        # EventHub provider tags
        "provider.connection_string": "...",
        "provider.consumer_group": "$Default",

        # Domain tags (custom, no framework behavior)
        "layer": "bronze",
        "domain": "sales",
        "pii": "true",
        "owner": "data-team",
    }
)
```

### D. Project Structure Comparison

**Minimal viable project (2 files):**
```
my-app/
├── main.py          # Entry point with entities + pipes inline
└── app.yaml         # App config
```

**Recommended project (scaffolded):**
```
my-app/
├── main.py
├── app.yaml
├── settings.yaml
├── requirements.txt
├── lake-reqs.txt
├── entities/
│   ├── __init__.py
│   ├── bronze.py
│   └── silver.py
├── pipes/
│   ├── __init__.py
│   └── transforms.py
└── tests/
    ├── conftest.py
    └── test_transforms.py
```

**Enterprise project (full-featured):**
```
my-app/
├── main.py
├── app.yaml
├── app.fabric.yaml
├── app.synapse.yaml
├── app.databricks.yaml
├── settings.yaml
├── requirements.txt
├── lake-reqs.txt
├── entities/
│   ├── __init__.py
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
├── pipes/
│   ├── __init__.py
│   ├── ingestion.py
│   ├── cleaning.py
│   ├── enrichment.py
│   └── aggregation.py
├── providers/
│   ├── __init__.py
│   └── custom_source.py
├── config/
│   ├── entity_tags.yaml
│   └── env_production.yaml
├── fixtures/
│   ├── bronze.orders/
│   │   └── sample.parquet
│   └── bronze.customers/
│       └── sample.parquet
└── tests/
    ├── conftest.py
    ├── test_entities.py
    ├── test_pipes.py
    └── test_providers.py
```

### E. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| CLI becomes version-coupled to framework | Medium | High | CLI generates code, doesn't import framework at generation time |
| Standalone mode diverges from cloud behavior | Medium | Medium | System tests (Tier 4) validate cloud behavior; standalone is optional |
| Template maintenance burden | Low | Medium | Templates are minimal, changes infrequent |
| `exec()` execution model confuses devs | High | Medium | Clear docs + daily loop is `pytest` not `exec()` |
| Type checking gaps with `exec()` pattern | High | Low | IDE support via type stubs, not runtime |
| Local Spark version mismatch with cloud | Medium | Medium | `pyproject.toml` pins Spark version |
| Tier 1-2 tests pass but app fails on cloud | Low | High | System tests (Tier 4) in CI/CD catch integration issues |
| DI state leaks between Tier 3 tests | Medium | Medium | `reset_framework()` autouse fixture + test isolation |
