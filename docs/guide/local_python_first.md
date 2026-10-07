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
kindling env bootstrap   # pins Kindling in the root pyproject.toml, syncs the repo-wide .venv/
cd packages/my_pipeline
cp .env.example .env
# Update .env with your environment settings
set -a; source .env; set +a

uv run poe test
uv run poe build
```

If you generated integration tests and have Azure credentials available:

```bash
uv run poe test-integration
```

## What The Scaffold Creates

The explicit scaffold flow creates:

- repo root shared files: `.devcontainer/`, `.github/workflows/ci.yml`,
  `.gitignore`, `scripts/setup-local-dev.sh`, and empty `packages/` and `apps/`
- a root `pyproject.toml` that is a uv workspace root, not a package — every
  `packages/*` directory is a workspace member, so the repo shares one `.venv/`
  and one `uv.lock` at the root with every package installed editable
- package-local source at `packages/<pkg>/src/<pkg>/...`
- package-local tests at `packages/<pkg>/tests/`
- package-local `pyproject.toml` (uv_build, `src/` layout, Kindling pinned to
  the CLI's release) with `poethepoet` tasks
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
intentionally want the generated Kindling devcontainer config. An existing
root `pyproject.toml` is kept too; add the workspace table to it yourself:

```toml
[tool.uv.workspace]
members = ["packages/*"]
```

Repos scaffolded before `repo init` wrote a root `pyproject.toml` have none.
Add one, then run `kindling env bootstrap` at the repo root:

```toml
[project]
name = "data-platform-workspace"   # must differ from every package name
version = "0.1.0"
requires-python = ">=3.10"
dependencies = []

[tool.uv]
package = false

[tool.uv.workspace]
members = ["packages/*"]
```

`kindling env bootstrap` adopts the Kindling release your packages pin (it
fails if they disagree) and runs `uv sync --all-packages`. In the generated
devcontainer this runs automatically as the `postCreateCommand`.

To add a second package later:

```bash
cd ../..
kindling package init customer-360 --repo-root .
cd packages/customer_360
uv run poe test
```

`uv run` syncs what the package needs before running. Don't run a bare `uv sync` inside a package directory: in the workspace it is an exact sync of that one package and removes the others from the shared `.venv/`. Resync the whole repo with `uv sync --all-packages` (or `kindling env bootstrap`) at the root.

The generated `pyproject.toml` depends on the published runtime distribution,
pinned to a release wheel by URL:

```toml
[project]
dependencies = ["spark-kindling[standalone]"]

[tool.uv.sources]
spark-kindling = { url = "https://github.com/sep/spark-kindling-framework/releases/download/vX.Y.Z/spark_kindling-X.Y.Z-py3-none-any.whl" }
```

`spark-kindling-cli` and `spark-kindling-sdk` are pinned the same way and sit in
the `dev` dependency group. Every package must pin the same Kindling release as
the root `pyproject.toml` (they share one lockfile). Run `kindling env update`
at the repo root (or `uv run poe update-kindling`) to move every pin to a newer
release together.

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

Kindling is not published to PyPI; the release wheel URLs above are the
supported install. For framework development:

```bash
# 1. Editable source install for framework iteration
pip install -e /path/to/kindling

# 2. Local wheel built from this repo
uv run poe build
pip install 'spark-kindling[standalone] @ file:///path/to/dist/spark_kindling-<version>-py3-none-any.whl'
```

Use the `standalone` extra for local work because it brings in the Spark runtime
packages that managed platforms already provide.

## Local Test Tasks

Generated packages expose these tasks:

```bash
uv run poe test-unit
uv run poe test-component
uv run poe test
uv run poe build
```

When integration tests are included, the scaffold also adds:

```bash
uv run poe test-integration
uv run poe test-all
```

At the repo level, the generated CI workflow runs inside the devcontainer image,
loops over `packages/*` and runs each package independently:

```bash
for pkg in packages/*; do
  if [ -f "$pkg/pyproject.toml" ]; then
    (cd "$pkg" && uv run poe test && uv run poe build)
  fi
done
```

That means local day-to-day work stays package-scoped (in the shared repo-root
`.venv/`; `uv build` in a workspace member writes wheels to the repo-root
`dist/`), while CI validates all scaffolded packages in the repo.

## Running an App Locally

Use `kindling app run` to execute all registered pipes locally with the
standalone platform. The positional argument is the app name; kindling discovers
`apps/<name>/` by convention (kebab and snake forms both work):

```bash
# From the repo root — convention lookup finds apps/my_pipeline/
kindling app run my_pipeline
kindling app run my_pipeline --env local

# Non-standard layout: override the lookup with --local-folder
kindling app run my_pipeline --local-folder path/to/app-dir
```

When you want the app to import checked-out package code instead of an installed
or artifact-backed wheel, pass one or more local package roots:

```bash
kindling app run my_pipeline --local-package packages/my_pipeline
kindling app run my_pipeline --local-package packages/my_pipeline --local-package packages/shared_domain
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

Memory entities are never read from or written to remote storage, and nothing persists between
runs: the seed is rebuilt from the declaration each time the process starts, and any writes last
only for that run. This works on any platform, which makes seed rows a fit for small hard-coded
reference data as well as local fixtures.

To swap a real entity for an in-memory fixture during local development without touching code,
use the top-level `entity_tags` override map (keyed by entity ID) in `settings.local.yaml`:

```yaml
# settings.local.yaml
entity_tags:
  ref.statuses:
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

1. Java 11+ on `PATH` (the devcontainer image ships Java 21)
2. The Python environment installed via `kindling env bootstrap` (or
   `uv sync --all-packages`) at the repo root
3. Hadoop Azure JARs in `/tmp/hadoop-jars` — the devcontainer image ships them;
   elsewhere run `kindling env ensure --cloud azure`

The CLI checks all of this for you:

```bash
kindling env check --local
```

## Packaging and Remote Lifecycle

The CLI covers the full local-to-remote app lifecycle using app names as convention:

```bash
# Package apps/my_pipeline/ into a .kda archive
kindling app package my_pipeline

# Deploy apps/my_pipeline/ to a remote platform
kindling app deploy my_pipeline --platform fabric

# Run all registered pipes locally with standalone Spark
kindling app run my_pipeline

# Run an already-deployed app remotely (deploy must come first)
kindling app deploy my_pipeline --platform synapse
kindling app run my_pipeline --platform synapse
kindling app status <run-id> --platform synapse
kindling app logs <run-id> --platform synapse
```

Non-standard layouts can always override convention lookup with `--local-folder`:

```bash
kindling app deploy my_pipeline --local-folder path/to/app --platform fabric
kindling package deploy my-package --local-folder path/to/package
```

Remote operations use `spark-kindling-sdk`. The CLI depends on it, so it is
always installed alongside the CLI.

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

### Getting extensions onto the cluster

Extensions listed under `kindling.extensions` in your settings are installed at
bootstrap from wheels in `{artifacts}/packages/`. `runtime deploy` uploads only
the core runtime wheel unless you ask for extensions, so name each one your
settings list (or pass `--all-extensions`):

```yaml
kindling:
  extensions: [spark-kindling-ext-sdp]
```

```bash
kindling runtime deploy \
  --source github:latest \
  --dest abfss://artifacts@myacct.dfs.core.windows.net/kindling \
  --extension spark-kindling-ext-sdp
```

The extension wheels come from the same GitHub release (or `local:` directory)
as the runtime, so their versions match. Naming an extension the source does
not contain fails and lists what is available. Promoting with a store-to-store
copy carries the extension wheels along with everything else in `packages/`.

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
