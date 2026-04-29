# Local Code-First Development on Kindling

**Date:** 2026-04-08
**Status:** Proposal / workflow synthesis
**Scope:** Local project structure, entities/pipes authoring, configuration layering, notebook-based validation

---

## 1. Summary

Kindling can already support a mostly code-first local development model, even though much of its runtime story is notebook and workspace oriented.

The key split is:

- **Code owns domain logic**: entities, pipes, helper transforms, app entrypoints
- **Config owns environment details**: storage roots, table namespaces, provider tags, secrets references
- **Notebooks own platform validation**: bootstrap, workspace loading, cloud job execution, and end-to-end smoke tests

That means a Kindling project does **not** need to be notebook-first during day-to-day development. A practical workflow is:

1. define entities and pipes in Python modules
2. define local and platform overlays in YAML
3. test transforms locally with pytest + local Spark
4. optionally validate notebook/workspace behavior with the standalone platform or notebook test harness
5. run final system tests in actual Fabric / Synapse / Databricks environments

This document describes what that project shape would look like and where the current framework already supports it.

---

## 2. Core Observation

Kindling entities and pipes are already code-defined:

- `DataEntities.entity(...)` registers entity metadata
- `@DataPipes.pipe(...)` registers pipe metadata while leaving the wrapped function as a normal Python callable

That makes the local authoring model more like a Python package than a notebook bundle.

The heavier notebook/bootstrap path is mainly needed for:

- loading workspace packages from remote notebook environments
- platform-specific service initialization
- wheel/config download and extension installation
- deployed job execution

So the right local model is not "recreate the full platform bootstrap on a laptop." It is "treat Kindling apps as Python projects first, and use notebooks as one validation surface."

---

## 3. Recommended Project Shape

Below is a concrete project layout for a Kindling consumer project that wants a local code-first workflow.

```text
my_kindling_project/
  pyproject.toml
  README.md
  src/
    sales_ops/
      __init__.py
      app.py
      entities/
        __init__.py
        bronze.py
        silver.py
        gold.py
      pipes/
        __init__.py
        bronze_to_silver.py
        silver_to_gold.py
      transforms/
        __init__.py
        quality.py
        aggregations.py
      config/
        settings.yaml
        env.local.yaml
        platform.fabric.yaml
        platform.synapse.yaml
        platform.databricks.yaml
  notebooks/
    smoke_test.py
    dev_bootstrap.py
  tests/
    unit/
      test_transforms.py
      test_entities.py
    integration/
      test_pipeline_local.py
    component/
      test_kindling_registration.py
    notebook/
      test_notebook_smoke.py
```

### Why this shape works

- `src/.../entities` keeps entity registration close to domain code
- `src/.../pipes` keeps decorated pipe definitions thin and readable
- `src/.../transforms` holds reusable plain PySpark functions that are easiest to unit test directly
- `config/` makes environment overlays visible and versioned
- `notebooks/` remains present, but as an adapter layer for workspace validation rather than the primary authoring surface
- `tests/` mirrors the testing pyramid already visible in the Kindling repo: unit, integration, component, and system/notebook style validation

---

## 4. Authoring Model

### 4.1 Entities

Entities should be defined in Python modules and imported intentionally during app startup.

Example shape:

```python
from pyspark.sql.types import StructField, StringType, StructType
from kindling.data_entities import DataEntities

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
])

DataEntities.entity(
    entityid="bronze.orders",
    name="bronze_orders",
    partition_columns=[],
    merge_columns=["order_id"],
    tags={
        "provider_type": "delta",
        "layer": "bronze",
    },
    schema=orders_schema,
)
```

Recommended rule:

- keep **stable semantics** in code: schema, merge keys, layer, intent, logical identity
- keep **deploy-time concerns** in config: path, table name, access mode, environment-specific provider tags

This matches Kindling's existing `entity_tags` merge behavior, where YAML overrides are applied at retrieval time.

### 4.2 Pipes

Pipe modules should stay thin and call plain transform functions where possible.

Example shape:

```python
from kindling.data_pipes import DataPipes
from sales_ops.transforms.quality import clean_orders_df

@DataPipes.pipe(
    pipeid="bronze_to_silver_orders",
    name="Bronze to Silver Orders",
    tags={"layer": "silver"},
    input_entity_ids=["bronze.orders"],
    output_entity_id="silver.orders",
    output_type="table",
)
def bronze_to_silver_orders(bronze_orders):
    return clean_orders_df(bronze_orders)
```

Recommended rule:

- keep decorators in pipe modules
- keep business logic in plain functions under `transforms/`

That gives three easy test surfaces:

- test transform functions directly
- test pipe registration metadata
- test Kindling execution against local Spark

### 4.3 App bootstrap module

The project should have one explicit Python entrypoint, for example `src/sales_ops/app.py`, responsible for:

1. importing entity modules for registration side effects
2. importing pipe modules for registration side effects
3. selecting which pipes to run
4. optionally calling Kindling execution services when running inside the framework

This is preferable to scattering registration across notebooks because it creates one reusable module that:

- local tests can import
- notebook wrappers can call
- deployed jobs can call

---

## 5. Configuration Model

Kindling already has the right primitives for a code-first project if they are used consistently.

### 5.1 Put environment-specific provider details in YAML

Use YAML for:

- `kindling.storage.*`
- platform-specific namespace/path defaults
- secret references
- `entity_tags` overrides

Example:

```yaml
kindling:
  storage:
    table_root: "Tables"
    checkpoint_root: "Files/checkpoints"

entity_tags:
  bronze.orders:
    provider.path: "Tables/bronze/orders"

  silver.orders:
    provider.path: "Tables/silver/orders"
```

### 5.2 Keep local config first-class

A code-first project should treat local config as a real target, not as an afterthought.

Suggested overlays:

- `settings.yaml`: shared defaults
- `env.local.yaml`: local Spark and filesystem-oriented overrides
- `platform.fabric.yaml`
- `platform.synapse.yaml`
- `platform.databricks.yaml`

`env.local.yaml` is especially important because it gives developers a place to express:

- local table root under `/tmp` or a project temp folder
- local checkpoint root
- local secrets via environment-variable backed conventions
- simpler provider settings for standalone/local execution

### 5.3 Treat config as deployable, not notebook-owned

The notebook should load config, not define it.

That avoids a common failure mode where:

- entity definitions live in notebooks
- config logic lives in notebooks
- pipe logic lives in notebooks

At that point local testing becomes hard because the notebook is both source code and environment wrapper. The better split is:

- Python package = source of truth
- YAML = environment overlay
- notebook = runner

---

## 6. Local Development Loop

### 6.1 Tier 1: pure code iteration

Best for:

- new transforms
- schema shaping
- joins and aggregations
- data quality logic

Workflow:

1. edit `transforms/*.py`
2. run unit tests with local Spark fixtures or mocks
3. inspect resulting DataFrames locally

This is the fastest loop and should be the default path for most daily work.

### 6.2 Tier 2: local Spark integration

Best for:

- entity schema compatibility
- realistic read/write behavior
- pipe chaining
- Delta behavior and local temp-path execution

Workflow:

1. create test input DataFrames
2. register entities/pipes in test code or import the app package
3. execute transforms or `DataPipesExecuter` using local Spark
4. validate output tables or captured DataFrames

The existing Kindling integration tests already show this pattern clearly.

### 6.3 Tier 3: local component validation of framework wiring

Best for:

- decorator registration
- DI wiring
- provider selection
- config-based tag overrides

Workflow:

1. import entity and pipe modules
2. bind lightweight providers such as memory or local Delta-backed providers
3. exercise registry and execution behavior

This is the right place to test "does my app wire into Kindling correctly?" without needing a real workspace.

### 6.4 Tier 4: notebook/workspace validation

Best for:

- verifying bootstrap assumptions
- validating workspace-loading behavior
- checking notebook-specific packaging or `%run` style integration

This should not be the primary loop. It is the compatibility loop between the code-first package and the notebook-hosted runtime.

---

## 7. Where Notebooks Still Fit

A code-first project still benefits from notebooks, but in a narrower role.

### Recommended notebook roles

- **Smoke notebook**: imports the package, initializes config, runs a small pipeline
- **Exploration notebook**: ad hoc data inspection against real platform data
- **Platform bootstrap notebook**: platform-specific wrapper for Fabric / Synapse / Databricks
- **Notebook-based tests**: validate workspace/package loading patterns when needed

### Notebook responsibilities to avoid

- defining the canonical entity list
- defining the canonical pipe graph
- embedding environment config directly in notebook cells
- being the only place where business logic exists

If the notebook is the only place an entity or pipe is defined, local code-first development breaks down immediately.

---

## 8. Testing Strategy for a Code-First Project

The practical testing pyramid for a Kindling consumer project should be:

### Unit tests

Test:

- transform functions
- schema helpers
- config parsing helpers

Tools:

- `pytest`
- mock DataFrames or very small local Spark fixtures

### Integration tests

Test:

- entities + pipes + real Spark DataFrames
- local Delta reads/writes
- simple execution order

Tools:

- local Spark
- temp directories
- Delta-enabled test session

### Component tests

Test:

- decorator registration
- `DataEntityRegistry` lookups with config tag overrides
- `DataPipesExecuter` behavior with test strategies/providers

Tools:

- Kindling DI container
- memory provider or local provider bindings

### Notebook tests

Test:

- notebook wrapper imports package correctly
- notebook config surface matches package expectations
- standalone workspace scanning or notebook harness behavior

Tools already present in repo:

- `kindling.test_framework`
- `platform_standalone`

### System tests

Test:

- cloud platform deployment
- job packaging
- secrets, storage, cluster policy, and runtime-specific behavior

This is where real notebooks and platform APIs remain essential.

---

## 9. Suggested Notebook Testing Model

If the team still wants "testing via notebooks," the cleanest approach is:

1. keep the real logic in the package
2. make the notebook a very thin wrapper
3. test that wrapper separately

A notebook should ideally do little more than:

```python
from sales_ops.app import register_all, run_smoke

register_all()
run_smoke()
```

That gives notebook validation without forcing business logic into notebook cells.

For local validation there are two good options:

- use `platform_standalone` with a local workspace path containing `.py` notebooks
- use `kindling.test_framework` for notebook-oriented execution/testing where notebook loading behavior matters

This keeps notebook tests meaningful without making them the only development path.

---

## 10. What Kindling Already Supports Well

The current repo already provides strong building blocks for this model:

- entities and pipes are Python-first registration APIs
- YAML config can override entity tags cleanly
- local Spark integration testing is already used heavily in `tests/integration`
- `platform_standalone` already provides a local workspace abstraction
- `test_framework` already provides notebook-oriented testing helpers
- system test apps already show that notebooks/jobs can be thin runners around registered entities and pipes

This means a code-first consumer experience is less about inventing new runtime behavior and more about:

- documenting the intended split
- scaffolding projects into that split
- adding a few thin templates and fixtures

---

## 11. Biggest Gaps

Even though the underlying pieces exist, a consumer project would still feel friction in a few places.

### 11.1 No single "consumer project template"

There is no one obvious starter that says:

- put entities here
- put pipes here
- put local config here
- put notebook wrappers here

The proposed patterns CLI is a good match for this gap.

### 11.2 No explicit local app harness

There is still some ambiguity around the cleanest supported way to run a Kindling app locally as a project, especially for component-level execution with config loading.

A lightweight local harness could standardize:

- config loading
- registry import order
- local Spark session setup
- optional local execution of selected pipes

### 11.3 Notebook testing guidance is still framework-developer oriented

The repo has notebook testing machinery, but consumer-facing guidance is still sparse. A project template should show:

- one smoke notebook
- one notebook test
- one local package-import path

### 11.4 Config ergonomics could be more opinionated

The framework supports several overlapping config surfaces. For consumer projects, the simplest guidance should be:

- Python for entities/pipes
- YAML for environment overrides
- minimal bootstrap dict only for entrypoint selection and runtime wiring

---

## 12. Recommended Near-Term Direction

If Kindling wants to support local code-first development well, the most valuable next steps are:

1. publish a reference project template with `src/`, `config/`, `notebooks/`, and `tests/`
2. add a "local-first consumer workflow" doc that points to unit, integration, notebook, and system-test tiers
3. provide one thin local harness for loading config and importing app modules
4. generate notebook wrappers that call package code rather than containing domain logic
5. keep system tests platform-native, but make earlier tiers notebook-optional

---

## 13. Bottom Line

Local code-first development on Kindling should look like standard Python package development with Spark:

- write entities and pipes in modules
- keep business logic in plain functions
- keep environment details in YAML
- use notebooks as wrappers and validation surfaces
- reserve full bootstrap/platform testing for the last mile

Kindling already has most of the runtime pieces needed for this. The missing work is mainly productization: templates, guidance, and a lightweight local harness that makes the intended workflow obvious.
