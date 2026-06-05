# Developer Workflow

This repo uses Poetry plus Poe the Poet for day-to-day development.

## Core Commands

```bash
poetry install
poetry run poe test-unit
poetry run poe test-integration
poetry run poe test-system --platform synapse
poetry run poe build
poetry run poe deploy --platform fabric
poetry run poe upload
```

## Build Model

`poetry run poe build` produces the current artifact set:

- `spark_kindling-<version>-py3-none-any.whl` — combined runtime wheel
- `spark_kindling_cli-<version>-py3-none-any.whl` — CLI wheel
- `spark_kindling_sdk-<version>-py3-none-any.whl` — SDK wheel

The runtime wheel contains every platform module. Consumers select platform
runtime dependencies with extras such as:

```bash
pip install 'spark-kindling[synapse]'
pip install 'spark-kindling[databricks]'
pip install 'spark-kindling[fabric]'
pip install 'spark-kindling[standalone]'
```

## Deploy Paths

There are two common deployment flows for the kindling project itself:

```bash
# Upload the current runtime artifacts to storage
poetry run poe deploy
poetry run poe upload

# Push workspace bootstrap assets and config
kindling workspace deploy --platform synapse --storage-account <account>
```

The Python deploy helpers now prefer the combined runtime wheel and only fall
back to legacy `kindling_<platform>-*.whl` artifacts when needed.

## Deploying Runtime to User Environments

Kindling users (not the kindling project team) can deploy runtime artifacts to
their own Azure Data Lake Storage using `kindling runtime deploy`. This is the
recommended path for getting wheels and the bootstrap script into a new
environment, or promoting between environments.

```bash
# Install from the latest GitHub release into a storage account
kindling runtime deploy \
  --source github:latest \
  --dest abfss://artifacts@myacct.dfs.core.windows.net/kindling

# Install a specific version
kindling runtime deploy \
  --source github:0.10.15 \
  --dest abfss://artifacts@myacct.dfs.core.windows.net/kindling

# Deploy from a local build
kindling runtime deploy \
  --source local:./dist \
  --dest abfss://artifacts@mydev.dfs.core.windows.net/kindling

# Promote from non-prod to prod (ADLS → ADLS)
kindling runtime deploy \
  --source abfss://artifacts@staging.dfs.core.windows.net/kindling \
  --dest abfss://artifacts@prod.dfs.core.windows.net/kindling
```

The command deploys to the conventional layout under `--dest`:

- `{dest}/packages/` — `spark_kindling-*.whl`
- `{dest}/scripts/` — `kindling_bootstrap.py`

The `artifacts_storage_path` in `BOOTSTRAP_CONFIG` should point to the `--dest`
root. Wheels always overwrite; use `--overwrite` to also replace an existing
bootstrap script. Pass `--skip-bootstrap` to skip the script entirely.

Auth uses `DefaultAzureCredential` — `az login` or service principal env vars
(`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`) both work.

## Running Apps and Pipes Locally

Use `kindling app run` to execute all registered pipes locally with the
standalone platform:

```bash
cd apps/my_pipeline
kindling app run .
kindling app run . --platform standalone --env local
kindling app run . --local-package ../../packages/my_pipeline
```

Use `kindling pipeline run` to execute one pipe from your local package without
deploying:

```bash
# Run from the app directory — app.py is auto-discovered
kindling pipeline run bronze_to_silver

# Explicit app path and environment overlay
kindling pipeline run bronze_to_silver --app apps/my_pipeline/app.py --env local
```

Use `kindling app validate` to check entity/pipe wiring without starting Spark:

```bash
kindling app validate
```

See [Local Python-First Development](local_python_first.md) for full details on
both commands, the memory-first scaffold, and `KindlingNotInitializedError`.

## CLI Lifecycle Commands

The CLI exposes design-time lifecycle commands beyond config/workspace.

### App workflow

```bash
kindling app package <app-path>
kindling app run <app-path>                           # local standalone
kindling app deploy --local-folder <app-dir> --platform fabric
kindling app run <app-name-or-path> --platform synapse  # submits to runner
kindling app status <run-id> --platform synapse
kindling app logs <run-id> --platform synapse
kindling app cancel <run-id> --platform synapse
kindling app cleanup <app-name> --platform fabric
```

### Runner workflow

The durable runner is the remote execution vehicle. Manage it separately from app work:

```bash
kindling runner ensure --platform synapse   # install if not present
kindling runner status --platform synapse   # check health
kindling runner repair --platform synapse   # reinstall
kindling runner delete --platform synapse   # remove
```

Remote `kindling app run` checks that a runner exists before submitting. If none is found it exits with a hint to run `kindling runner ensure`.

Remote app and runner commands require `spark-kindling-sdk`.

## Authentication

Repo deploy and CI flows use Azure identity, not storage keys:

- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_STORAGE_ACCOUNT`
- `AZURE_CONTAINER`
- optional `AZURE_BASE_PATH`

For local work you can either export those values directly or use `az login`
when the command supports `DefaultAzureCredential`.
