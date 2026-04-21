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

# Then scaffold and work on your local project.
kindling new my-pipeline
cd my_pipeline
poetry install
cp .env.example .env
source .env

poetry run poe test
poetry run poe build
```

If you generated integration tests and have Azure credentials available:

```bash
poetry run poe test-integration
```

## What `kindling new` Creates

`kindling new` now generates:

- `src/<pkg>/...` application source
- `config/settings.yaml` plus `config/env.local.yaml`
- `tests/unit`, `tests/component`, and optionally `tests/integration`
- `pyproject.toml` with `poethepoet` tasks
- `.github/workflows/ci.yml` with a starter CI job that runs `poe test` and `poe build`
- `.devcontainer/` for local containerized development

The generated `pyproject.toml` depends on the published runtime distribution:

```toml
spark-kindling = { version = ">=0.9.1", extras = ["standalone"] }
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

Generated projects expose these tasks:

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

The CLI now covers the basic local-to-remote app lifecycle:

```bash
# Package an app directory into a .kda archive
kindling app package path/to/app-dir

# Deploy an app directory or .kda package
kindling app deploy path/to/app-dir --platform fabric --app-name my-app

# Create and run remote jobs from YAML/JSON
kindling job create job.yaml --platform synapse
kindling job run <job-id> --platform synapse
kindling job status <run-id> --platform synapse
kindling job logs <run-id> --platform synapse
```

These remote operations use `spark-kindling-sdk`, so install it alongside the
CLI when you want deploy/manage capabilities.

## Artifact Storage and Workspace Bootstrap

For notebook-backed platforms, the storage bootstrap flow still uses:

```bash
kindling workspace deploy --platform synapse --storage-account <account>
```

`workspace deploy` now prefers the combined runtime wheel:

- `dist/spark_kindling-*.whl`
- falls back to legacy `dist/kindling_<platform>-*.whl` if needed

It also uploads `runtime/scripts/kindling_bootstrap.py` plus config overlays.
