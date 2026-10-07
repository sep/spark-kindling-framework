---
name: kindling
description: Defines and changes Kindling data entities, data pipes (transforms, file ingestion, streaming, SCD merges), apps and settings.yaml configuration in a Kindling domain project, using the kindling CLI to scaffold, validate and run them locally. Use when a task touches packages/*/src/*/entities or pipes, apps/*, settings*.yaml, DataEntities, DataPipes, or any `kindling` command.
---

# Kindling domain projects

Kindling runs the same declared data pipelines on Databricks, Microsoft Fabric,
Azure Synapse and local Spark. A domain project declares **entities** (datasets:
schema, keys, storage provider) and **pipes** (transformations from input
entities to an output entity) in Python packages, and **apps** select which
packages to run. **Configuration** (`settings.yaml` layers) supplies paths,
providers and per-environment overrides, so code never branches on platform or
environment.

## Layout

```text
<repo>/
  pyproject.toml              # uv workspace root over packages/* (not a package)
  packages/<pkg>/
    pyproject.toml            # Kindling pinned; poe tasks: test, build
    settings.yaml             # package defaults
    src/<pkg>/
      entities/<ns>.py        # DataEntities declarations (bronze.py, silver.py, ...)
      pipes/<ns>_<name>.py    # @DataPipes.pipe declarations
      transforms/             # pure DataFrame -> DataFrame functions
    tests/                    # unit/, component/, integration/, entities/ (CSV fixtures)
  apps/<app>/
    app.py  app.yaml  lake-reqs.txt  settings.yaml
```

The runtime imports only a package's `entities`, `pipes` and `ingestion`
subpackages. A declaration anywhere else never registers.

## Workflow

1. **Scaffold with the CLI**, not by hand. It puts files where the runtime looks
   and creates matching tests and fixtures:

   ```bash
   kindling package add entity bronze.orders --package packages/sales
   kindling package add pipe silver.orders --inputs bronze.orders --package packages/sales
   kindling package add ingestion bronze.sales_csv --package packages/sales
   kindling app init sales-daily --package sales --pattern batch --repo-root .
   ```

2. **Fill in** the generated declarations and transforms (see the references).
3. **Validate** against the real registry, from the repo root:

   ```bash
   kindling app validate --app apps/sales_daily/app.py --env local
   kindling pipeline list --app apps/sales_daily/app.py
   kindling entity show bronze.orders --app apps/sales_daily/app.py --limit 5
   ```

4. **Test** in the package: `uv run poe test` (unit and component tests). Run a
   single pipe end to end with `kindling pipeline run <pipeid> --app ... --env local`.

## References

Read the one that matches the task:

| Task | Read |
|---|---|
| Declare or change an entity, schema, keys, provider, fixtures, clone/extend | [references/entities.md](references/entities.md) |
| Write a pipe or transform, ingestion, streaming, watermarks, SCD merges | [references/pipes.md](references/pipes.md) |
| Create an app, project layout, lake-reqs, packaging, deploy, environment setup | [references/apps.md](references/apps.md) |
| Settings, environment overlays, entity_tags, env vars, secrets | [references/config.md](references/config.md) |

## Rules

- Declarations run at import time and need an initialized framework; never
  import entity or pipe modules before `initialize()` (apps and tests handle this).
- Keep platform and environment differences in configuration, not in `if`
  branches inside entities, pipes or transforms.
- Never reach into the JVM (`spark._jvm`, `spark._jsc`).
- Changing a column's type or keys on an existing entity is a schema migration:
  check `kindling migrate plan` before relying on it.
- Inside `packages/<pkg>/`, use `uv run poe ...`. A bare `uv sync` there removes the
  other workspace packages from the shared `.venv/`; resync with
  `uv sync --all-packages` at the repo root.
