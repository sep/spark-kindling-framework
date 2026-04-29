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

There are two common deployment flows:

```bash
# Upload the current runtime artifacts to storage
poetry run poe deploy
poetry run poe upload

# Push workspace bootstrap assets and config
kindling workspace deploy --platform synapse --storage-account <account>
```

The Python deploy helpers now prefer the combined runtime wheel and only fall
back to legacy `kindling_<platform>-*.whl` artifacts when needed.

## CLI Lifecycle Commands

The CLI now exposes design-time lifecycle commands beyond config/workspace:

```bash
kindling app package <app-dir>
kindling app deploy <app-dir-or-kda> --platform fabric
kindling app cleanup <app-name> --platform fabric
kindling job create job.yaml --platform synapse
kindling job run <job-id> --platform synapse
kindling job status <run-id> --platform synapse
kindling job logs <run-id> --platform synapse
kindling job cancel <run-id> --platform synapse
kindling job delete <job-id> --platform synapse
```

Remote app/job commands require `spark-kindling-sdk`.

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
