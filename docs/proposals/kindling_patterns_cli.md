# Kindling Patterns CLI Proposal

**Date:** 2026-03-27
**Status:** Proposal
**Scope:** CLI pattern scaffolding, medallion architecture bootstrapping, starter app generation

---

## Executive Summary

Kindling already helps with:

- config initialization
- environment validation
- workspace validation
- workspace bootstrap notebook generation

What it does **not** yet provide is an opinionated way to say:

> "Create me a sensible data app starter for this architecture and platform."

This proposal introduces **Kindling Patterns** as a thin scaffolding layer above raw config/bootstrap. A pattern is a named, inspectable starter topology that generates config, starter code, and conventions for a common data architecture.

The recommended v1 is:

- one new command family: `kindling pattern ...`
- one production-worthy starter pattern: `medallion`
- two operating modes: `batch` and `streaming`
- optional platform overlays: `fabric`, `synapse`, `databricks`

The goal is not to create a second framework. The goal is to remove repeated setup decisions and generate a starter app that already matches Kindling concepts like entities, pipes, config overlays, bootstrap notebooks, and platform-specific storage defaults.

---

## Problem Statement

Today a new Kindling user must manually decide:

- how to lay out a data app
- how to name entities and pipes
- how to split bronze, silver, and gold stages
- where checkpoints, temp data, and tables should live
- how to seed platform-specific config
- how to add a first runnable example
- how to create a reasonable smoke test

The existing CLI can generate `settings.yaml` and workspace bootstrap files, but it stops short of app architecture. That leaves each team to invent their own starter structure, which creates inconsistency in:

- config shape
- naming conventions
- storage layout
- test coverage
- onboarding experience

---

## Design Principles

1. Patterns should be thin.
   They generate files and defaults, not hidden runtime behavior.

2. Patterns should be inspectable.
   Every generated artifact should be plain code or YAML the user can edit directly.

3. Patterns should compose with existing Kindling commands.
   `pattern init` should build on `config init` and `workspace init`, not replace them conceptually.

4. Patterns should separate architecture from capabilities.
   `medallion` defines the app shape. Optional add-ons like `quarantine` or `eventhub-source` can layer on later.

5. Platform differences should be expressed as overlays.
   Databricks, Fabric, and Synapse should share one architecture model with platform-specific config and storage defaults.

---

## Terminology

### Pattern

A named scaffold that creates a starter app structure for a known architecture.

Examples:

- `medallion`
- `ingestion-only`
- `streaming-orchestrator`

### Capability Add-on

An optional enhancement applied on top of a pattern.

Examples:

- `quarantine`
- `eventhub-source`
- `telemetry`
- `scd2`

### Platform Overlay

A platform-aware config and file variant layered onto the base pattern.

Examples:

- `databricks`
- `fabric`
- `synapse`

---

## Proposed Command Surface

### `kindling pattern list`

Shows available patterns and short descriptions.

Example:

```bash
kindling pattern list
```

Example output:

```text
medallion            Bronze/Silver/Gold starter app
medallion-streaming  Streaming medallion starter
```

For v1, `medallion-streaming` does not need to be a separate internal implementation. It can simply be the display alias for `medallion --mode streaming`.

### `kindling pattern explain`

Shows what a pattern creates before writing files.

Example:

```bash
kindling pattern explain medallion --mode streaming --platform databricks
```

Suggested output sections:

- purpose
- generated files
- assumptions
- optional capabilities not included by default

### `kindling pattern init`

Generates a starter app from a named pattern.

Example:

```bash
kindling pattern init medallion \
  --app sales_ops \
  --mode streaming \
  --platform databricks \
  --output-dir .
```

Recommended options:

- `--app`
- `--mode {batch,streaming}`
- `--platform {fabric,synapse,databricks}`
- `--output-dir`
- `--force`
- `--with-workspace`
- `--with-sample-data`
- `--with-tests`
- `--dry-run`

### Deferred for Later

These are valuable, but should not be part of v1:

- `kindling pattern add <capability>`
- `kindling pattern validate`
- `kindling pattern upgrade`

---

## V1 Pattern: `medallion`

### Purpose

Create a starter Kindling app that models a standard bronze/silver/gold flow using Kindling entities, pipes, config overlays, and workspace bootstrap files.

### User Story

> As a new Kindling user, I want one command that creates a runnable bronze/silver/gold starter app for my platform so I can start editing business logic instead of wiring config and folder layout by hand.

### Supported Modes

#### `medallion --mode batch`

Creates:

- one sample bronze entity
- one silver cleansing/standardization pipe
- one gold aggregation pipe
- starter app entrypoint
- local test fixtures or smoke test

#### `medallion --mode streaming`

Creates:

- one bronze ingestion/landing flow
- one silver normalization pipe
- one gold aggregation/enrichment pipe
- checkpoint defaults
- starter app entrypoint wired for streaming execution shape

The streaming mode should borrow conventions from the existing streaming test apps but remain smaller and easier to read.

---

## Generated File Tree

This is the proposed output for:

```bash
kindling pattern init medallion --app sales_ops --mode streaming --platform databricks
```

```text
sales_ops/
  settings.yaml
  platform_databricks.yaml
  env_dev.yaml
  .kindling/
    workspace/
      environment_bootstrap.py
      starter_notebook.py
  data-apps/
    sales_ops/
      settings.yaml
      main.py
      entities.py
      pipes/
        bronze.py
        silver.py
        gold.py
      tests/
        test_smoke.py
      README.md
```

### Notes on Layout

- Root `settings.yaml` holds shared Kindling defaults.
- `platform_<platform>.yaml` seeds platform-specific paths and access mode defaults.
- `env_dev.yaml` gives the user a visible place for first environment overrides.
- `data-apps/<app>/` contains app-owned logic.
- `.kindling/workspace/` is only generated when `--with-workspace` is enabled, but that flag should default to true in v1.

---

## Generated Content by File

### Root `settings.yaml`

Should include:

- app metadata
- baseline telemetry settings
- bootstrap defaults
- empty `required_packages` and `extensions`
- medallion-friendly storage defaults

Should also include a small pattern marker for future tooling:

```yaml
kindling:
  pattern:
    name: medallion
    mode: streaming
```

This metadata is useful for future `pattern explain`, `validate`, or `upgrade` commands.

### `platform_<platform>.yaml`

Should seed:

- storage table root
- checkpoint root
- temp path
- Delta access mode defaults
- platform-specific settings where already established in the repo

Examples:

- Databricks: prefer `forName` where catalog/schema context is available
- Fabric/Synapse: prefer workspace/lakehouse-friendly table or path conventions already used by Kindling

### `env_dev.yaml`

Should contain only a few visible starter overrides, such as:

- log level
- sample environment name
- optional sample app tags

Keep this intentionally small so users understand what is environment-specific.

### `data-apps/<app>/settings.yaml`

Should contain app-specific config only, such as:

- app name
- pattern metadata
- any app-owned entity IDs or prefixes

This is also the right place to register optional toggles later, such as:

- `sample_data.enabled`
- `quarantine.enabled`

### `main.py`

Should be a short, readable starter entrypoint that:

- imports the app's entities and pipes
- acquires required Kindling services
- runs the sample flow
- emits simple log markers

The file should optimize for comprehension over cleverness. A new user should be able to read it in a few minutes and know where to edit business logic.

### `entities.py`

Should define the starter bronze/silver/gold entities with tags like:

```python
{"layer": "bronze", "domain": "sales_ops"}
```

This file establishes the naming pattern users should follow.

### `pipes/bronze.py`

Should contain one starter bronze ingestion pipe. In batch mode this can represent landing/standard ingest. In streaming mode it can represent an initial streaming source to bronze.

Expected conventions:

- pipe IDs start with `bronze.`
- output entity IDs land in bronze
- tags include `layer=bronze`

### `pipes/silver.py`

Should contain one normalization or cleansing transform.

Expected conventions:

- pipe IDs start with `silver.`
- input reads bronze
- output lands in silver
- tags include `layer=silver`

### `pipes/gold.py`

Should contain one aggregation or enrichment transform.

Expected conventions:

- pipe IDs start with `gold.`
- input reads silver
- output lands in gold
- tags include `layer=gold`

### `tests/test_smoke.py`

Should provide a minimal smoke test for the generated app shape. The test does not need to be exhaustive. It should simply prove the generated app imports and that core functions are wired in a way the user can build on.

### `README.md`

Should tell the user:

1. what was generated
2. which file to edit first
3. how to run the smoke test
4. how to bootstrap the workspace notebook
5. how to switch to another platform or environment

---

## Naming Conventions

The CLI should impose a clear default naming system so teams stop reinventing it.

### Entities

Use:

```text
<layer>.<domain>_<name>
```

Examples:

- `bronze.sales_ops_orders`
- `silver.sales_ops_orders_clean`
- `gold.sales_ops_daily_sales`

### Pipes

Use:

```text
<layer>.<verb>_<domain>_<name>
```

Examples:

- `bronze.ingest_sales_ops_orders`
- `silver.clean_sales_ops_orders`
- `gold.aggregate_sales_ops_daily_sales`

### Tags

Each generated entity and pipe should include at least:

- `layer`
- `domain`
- `pattern`

Example:

```python
{"layer": "silver", "domain": "sales_ops", "pattern": "medallion"}
```

---

## Default Behavior of `kindling pattern init medallion`

Given:

```bash
kindling pattern init medallion --app sales_ops --mode batch --platform fabric
```

The command should:

1. create the target directory if needed
2. generate base and platform config files if missing
3. generate app files under `data-apps/sales_ops/`
4. generate workspace bootstrap notebooks
5. print a short post-create summary

Suggested summary:

```text
Created medallion pattern app: sales_ops
Mode: batch
Platform: fabric

Next steps:
1. Review data-apps/sales_ops/entities.py
2. Edit data-apps/sales_ops/pipes/silver.py
3. Run kindling env check
4. Run kindling workspace check --platform fabric
```

---

## What V1 Should Not Do

To keep the first implementation small and reliable, v1 should avoid:

- interactive prompting
- remote downloads
- generating many optional integrations
- trying to infer business schemas
- adding complex code generation branches for every source type
- updating existing apps in place beyond simple `--force` overwrite behavior

This keeps the first version deterministic and easy to test.

---

## Future Add-ons

After `medallion` is stable, the next useful layer is capability add-ons.

### `quarantine`

Adds:

- invalid-record output entity
- sample validation split
- quarantine-specific config

### `eventhub-source`

Adds:

- source config scaffold
- example ingestion entity/provider settings
- streaming bronze starter using the event hub provider

### `telemetry`

Adds:

- extension config
- starter Azure Monitor or tracing config
- sample log/tracing usage in generated code

### `scd2`

Adds:

- silver-to-gold dimension starter
- merge pattern sample
- config hints for keys/effective timestamps

---

## Implementation Plan

### Phase 1: Internal Template Functions

Add internal helpers inside `kindling_cli` to render:

- root config files
- app files
- workspace bootstrap files

No plugin system yet. Keep templates in code or a small adjacent template directory.

### Phase 2: New CLI Group

Add:

- `pattern list`
- `pattern explain`
- `pattern init`

### Phase 3: V1 Medallion Templates

Implement:

- `batch`
- `streaming`
- platform overlays for all supported platforms

### Phase 4: Tests

Add unit tests covering:

- file generation
- overwrite behavior
- platform selection
- rendered pattern metadata
- `--dry-run` output

---

## Suggested Acceptance Criteria

The v1 feature is successful if:

1. a new user can generate a starter app with one command
2. the generated tree is small enough to understand in under 10 minutes
3. the generated config aligns with existing Kindling bootstrap/config conventions
4. both `batch` and `streaming` starters are testable in CI as generated artifacts
5. the implementation does not introduce hidden runtime magic beyond file generation

---

## Recommendation

Implement `kindling pattern init medallion` as the first architecture-level scaffold in the CLI.

This is the highest-leverage next step because it sits directly between today's low-level config/bootstrap commands and the user's actual goal: starting a real data app with sane conventions.

If the team wants to stay especially conservative, ship only:

- `kindling pattern list`
- `kindling pattern explain medallion`
- `kindling pattern init medallion --mode {batch,streaming}`

That is small enough to build quickly and concrete enough to validate whether "Kindling Patterns" is the right abstraction before adding capability add-ons.
