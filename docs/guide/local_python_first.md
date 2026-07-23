# Local Python-First Development with Kindling

This guide covers the current local-development path for Kindling projects.
The generated workflow is Python-first: scaffold locally, run `poe` tasks, and
only move to remote workspaces when you want deployment or end-to-end tests.

## Quick Start

```bash
# Install Kindling from this repo's latest GitHub release.
CURRENT_RUNTIME_URL=$(curl -fsSL https://github.com/sep/spark-kindling-framework/releases/latest/download/spark_kindling-current-url.txt)
CURRENT_CLI_URL="${CURRENT_RUNTIME_URL//spark_kindling-/spark_kindling_cli-}"
CURRENT_SDK_URL="${CURRENT_RUNTIME_URL//spark_kindling-/spark_kindling_sdk-}"

pip install "spark-kindling[standalone] @ ${CURRENT_RUNTIME_URL}"
pip install "spark-kindling-cli @ ${CURRENT_CLI_URL}"
pip install "spark-kindling-sdk @ ${CURRENT_SDK_URL}"

# Then scaffold and work on your local repo, package, and app.
kindling repo init my-pipeline --output-dir ./my_pipeline
cd my_pipeline
kindling package init my-pipeline
kindling app init my-pipeline --package my-pipeline
cd packages/my_pipeline
poetry install
cp .env.example .env
# Update .env with your environment settings
source .env

poetry run poe test
poetry run poe build
```

If you generated integration tests and have Azure credentials available:

```bash
poetry run poe test-integration
```

## What The Scaffold Creates

The explicit scaffold flow creates:

- repo root shared files: `.devcontainer/`, `.github/workflows/ci.yml`, `.gitignore`
- package-local source at `packages/<pkg>/src/<pkg>/...`
- package-local tests at `packages/<pkg>/tests/`
- package-local `pyproject.toml` with `poethepoet` tasks
- app-local entrypoint and config at `apps/<app>/app.py`, `apps/<app>/settings.yaml`, and `apps/<app>/settings.local.yaml`

Run the commands as separate steps so repos, packages, and apps can evolve independently:

```bash
git clone <your-empty-repo-url> data-platform
cd data-platform
kindling repo init data-platform
kindling package init my-pipeline --repo-root .
kindling app init my-pipeline --package my-pipeline --repo-root .
cd apps/my_pipeline
```

If you start from a repo that already has a `.devcontainer/` so the Kindling
CLI is available inside the container, `kindling repo init` will warn and leave
that devcontainer unchanged. Re-run with `--overwrite-devcontainer` when you
intentionally want the generated Kindling devcontainer config.

To add a second package later:

```bash
cd ../..
kindling package init customer-360 --repo-root .
cd packages/customer_360
poetry install
poetry run poe test
```

The generated `pyproject.toml` depends on the published runtime distribution:

```toml
spark-kindling = { version = ">=0.9.2", extras = ["standalone"] }
```

The package import still stays `import kindling`.

## Installing the Framework Locally

Assuming you are installing Kindling from this project's GitHub releases:

```bash
CURRENT_RUNTIME_URL=$(curl -fsSL https://github.com/sep/spark-kindling-framework/releases/latest/download/spark_kindling-current-url.txt)
CURRENT_CLI_URL="${CURRENT_RUNTIME_URL//spark_kindling-/spark_kindling_cli-}"
CURRENT_SDK_URL="${CURRENT_RUNTIME_URL//spark_kindling-/spark_kindling_sdk-}"

pip install "spark-kindling[standalone] @ ${CURRENT_RUNTIME_URL}"
pip install "spark-kindling-cli @ ${CURRENT_CLI_URL}"
pip install "spark-kindling-sdk @ ${CURRENT_SDK_URL}"
```

Other supported paths are:

```bash
# 1. Released package for local/CI use from PyPI
pip install 'spark-kindling[standalone]'

# 2. Editable source install for framework iteration
pip install -e /path/to/kindling

# 3. Local wheel built from this repo
poetry run poe build
pip install 'spark-kindling[standalone] @ file:///path/to/dist/spark_kindling-<version>-py3-none-any.whl'
```

Use the `standalone` extra for local work because it brings in the Spark runtime
packages that managed platforms already provide.

## Local Test Tasks

Generated packages expose these tasks:

```bash
poetry run poe test-unit
poetry run poe test-component
poetry run poe test
poetry run poe build
```

When integration tests are included, the scaffold also adds:

```bash
poetry run poe test-integration
poetry run poe test-all
```

At the repo level, the generated CI workflow loops over `packages/*` and runs
each package independently:

```bash
for pkg in packages/*; do
  if [ -f "$pkg/pyproject.toml" ]; then
    (cd "$pkg" && poetry install --no-interaction && poetry run poe test && poetry run poe build)
  fi
done
```

That means local day-to-day work stays package-scoped, while CI validates all
scaffolded packages in the repo.

## Running an App Locally

Use `kindling app run` to execute all registered pipes locally with the
standalone platform. The positional argument is the app name; kindling discovers
`apps/<name>/` by convention (kebab and snake forms both work):

```bash
# From the repo root — convention lookup finds apps/my_pipeline/
kindling app run my-pipeline
kindling app run my-pipeline --env local

# Non-standard layout: override the lookup with --local-folder
kindling app run my-pipeline --local-folder path/to/app-dir
```

When you want the app to import checked-out package code instead of an installed
or artifact-backed wheel, pass one or more local package roots:

```bash
kindling app run my-pipeline --local-package packages/my_pipeline
kindling app run my-pipeline --local-package packages/my_pipeline --local-package packages/shared_domain
```

Each `--local-package` path may point at a package root with a `src/` directory
or directly at a source directory. The resolved source paths are prepended to
`PYTHONPATH` for that local run only.

Standalone app runs create a Delta-enabled Spark session by default so local
Delta reads, writes, and `DeltaTable.forPath()` use the JVM Delta classes from
the `delta-spark` package. To force a plain Spark session for a non-Delta app,
run with `KINDLING_SPARK_ENABLE_DELTA=false`.

## Running a Pipe Locally

Use `kindling pipeline run` to execute one registered pipe without deploying to
a remote platform:

```bash
kindling pipeline run bronze_to_silver
```

The command auto-discovers `app.py` by walking up from the current directory. You
can also be explicit:

```bash
kindling pipeline run bronze_to_silver --app apps/my_pipeline/app.py --env local
```

`--env` selects the config overlay (defaults to the `KINDLING_ENV` env var, then
`"local"`). On success you will see:

```
Running pipe: bronze_to_silver
Pipe 'bronze_to_silver' completed successfully.
```

If the pipe ID is not registered, the error message lists all available pipe IDs.

## Validating Definitions Without Spark

`kindling app validate` checks that your entity and pipe definitions are internally
consistent — without starting a SparkSession:

```bash
kindling app validate
```

Example output:

```
[PASS] entities_registered — 3 entity/entities
[PASS] pipes_registered — 2 pipe/pipes
[PASS] pipe.bronze_to_silver.input_entities — OK
[PASS] pipe.bronze_to_silver.output_entity — OK
[PASS] entity.silver.records.merge_columns — OK
Validation passed.
```

Checks performed:

- At least one entity and one pipe are registered
- Every pipe's input entities and output entity exist in the registry
- Every delta entity has `merge_columns` set

`kindling app validate` is safe to run in CI before tests because it never creates a
Spark context.

## Local Memory Providers (No Azure Needed)

The generated `settings.local.yaml` now scaffolds entity tags with
`provider_type: memory` by default. This means `kindling app run`,
`kindling pipeline run`, and unit/component tests work out of the box - no Azure
credentials or ABFSS paths required.

To switch to real Azure storage, uncomment the ABFSS block in `settings.local.yaml`
and set the required env vars in your `.env` file.

## Inline Seed Rows for Memory Entities

Memory entities support inline seed data via `provider.seed.rows` in the entity tags. This is
useful for unit and component tests where you want a small, deterministic dataset without a CSV
file or a fixture setup function.

Seed rows are a list of dicts (one per row) defined directly in the entity declaration:

```python
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

DataEntities.entity(
    entityid="ref.statuses",
    name="statuses",
    merge_columns=["id"],
    partition_columns=[],
    schema=StructType([
        StructField("id", IntegerType(), False),
        StructField("label", StringType(), True),
    ]),
    tags={
        "provider_type": "memory",
        "provider.seed.rows": [
            {"id": 1, "label": "active"},
            {"id": 2, "label": "inactive"},
            {"id": 3, "label": "pending"},
        ],
    },
)
```

When a pipe reads `ref.statuses`, the provider materializes those rows on first access and caches
them in the in-memory store for subsequent reads. The entity schema is required — the provider
validates that all keys in each row dict are declared schema fields and raises a clear error if not.

Seed rows are local-only. The `provider_type: memory` tag means this entity is never read from or
written to remote storage. Use them in `settings.local.yaml` overlays to swap real entities for
in-memory fixtures during local development:

```yaml
# settings.local.yaml
entities:
  - entityid: ref.statuses
    tags:
      provider_type: memory
      provider.seed.rows:
        - id: 1
          label: active
        - id: 2
          label: inactive
```

## KindlingNotInitializedError

If you see `KindlingNotInitializedError` it means a `@DataPipes.pipe` or
`@DataEntities.entity` decorator fired before `initialize()` was called. The
fix is to ensure `app.py` calls `initialize()` before importing any module that
registers pipes or entities — i.e. before `register_all()`. The error message
includes a pointer to the correct order.

## Local Spark Prerequisites

For local integration tests against ABFSS you still need:

1. Java 11+ on `PATH`
2. The Python environment installed via `poetry install`
3. Hadoop Azure JARs in `/tmp/hadoop-jars`

The CLI checks all of this for you:

```bash
kindling env check --local --config config/settings.yaml
```

## Packaging and Remote Lifecycle

The CLI covers the full local-to-remote app lifecycle using app names as convention:

```bash
# Package apps/my_pipeline/ into a .kda archive
kindling app package my-pipeline

# Deploy apps/my_pipeline/ to a remote platform
kindling app deploy my-pipeline --platform fabric

# Run all registered pipes locally with standalone Spark
kindling app run my-pipeline

# Run an already-deployed app remotely (deploy must come first)
kindling app deploy my-pipeline --platform synapse
kindling app run my-pipeline --platform synapse
kindling app status <run-id> --platform synapse
kindling app logs <run-id> --platform synapse
```

Non-standard layouts can always override convention lookup with `--local-folder`:

```bash
kindling app deploy my-pipeline --local-folder path/to/app --platform fabric
kindling package deploy my-package --local-folder path/to/package
```

Remote operations use `spark-kindling-sdk`, so install it alongside the CLI when
you want deploy/manage capabilities.

## Artifact Storage and Workspace Bootstrap

### Deploying runtime artifacts to your lake

Use `kindling runtime deploy` to get kindling wheels and the bootstrap script
into your Azure Data Lake Storage. This is the primary path for initial setup and
for promoting between environments (e.g. staging → prod):

```bash
# First-time install from GitHub into your storage account
kindling runtime deploy \
  --source github:latest \
  --dest abfss://artifacts@myacct.dfs.core.windows.net/kindling

# Promote from staging to prod
kindling runtime deploy \
  --source abfss://artifacts@staging.dfs.core.windows.net/kindling \
  --dest abfss://artifacts@prod.dfs.core.windows.net/kindling
```

The `--dest` root becomes your `artifacts_storage_path` in `BOOTSTRAP_CONFIG`.

### Workspace initialization and config deploy

To push `settings.yaml` and optional notebook stubs into the workspace for the
first time:

```bash
kindling workspace init --platform synapse --storage-account <account>
```

To re-deploy config after `settings.yaml` changes:

```bash
kindling workspace deploy --platform synapse --storage-account <account>
```

`workspace deploy` deploys `settings.yaml` + overlay configs to `{base}/config/`
in storage. For runtime wheels and bootstrap script, use `kindling runtime deploy`.
