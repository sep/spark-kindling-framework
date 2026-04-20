# CI/CD Pipeline Setup Guide

This document describes how CI/CD works in the current repository state.

## Current Workflow Triggers

The main workflow is `.github/workflows/ci.yml`.

It runs on:

- push to `main` and `develop`
- pull requests targeting `main`
- published releases
- manual `workflow_dispatch`

## What Runs When

### On Push / Pull Request

These lanes run as part of normal CI:

- build CI container image
- unit tests / quality lanes
- integration tests
- KDA packaging tests
- summary reporting

### On Release / Manual Dispatch

These additional lanes run only for release publishing or manual dispatch:

- build wheel artifacts for release/system-test use
- deploy candidate artifacts to Azure storage
- system tests for Synapse, Fabric, and Databricks
- attach wheels to the GitHub release, but only if all system tests pass

That means pushes to `main` do **not** automatically run full system tests in
current CI. System tests are reserved for `release` and `workflow_dispatch`.

## Authentication Model

The workflow uses Azure service-principal authentication, not storage-account keys.

Shared Azure auth secrets used by deploy/system-test lanes:

- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`

Shared storage variables:

- `AZURE_STORAGE_ACCOUNT`
- `AZURE_CONTAINER`

Additional platform secrets used by system tests:

- `FABRIC_WORKSPACE_ID`
- `FABRIC_LAKEHOUSE_ID`
- `SYNAPSE_WORKSPACE_NAME`
- `SYNAPSE_SPARK_POOL`
- `DATABRICKS_HOST`
- `DATABRICKS_CLUSTER_ID`
- platform Key Vault / test resource secrets already referenced in the workflow

## Artifact Flow

The repo now builds and deploys a combined runtime wheel:

- `spark_kindling-<version>-py3-none-any.whl`
- `spark_kindling_cli-<version>-py3-none-any.whl`
- `spark_kindling_sdk-<version>-py3-none-any.whl`

For release/manual system-test runs, CI:

1. builds wheel artifacts
2. downloads them into `dist/`
3. deploys them to Azure storage with `python scripts/deploy.py`
4. passes the resolved `AZURE_BASE_PATH` into system tests
5. attaches wheels to the GitHub release only after all platform lanes succeed

## Local Parity Commands

The repo tasks that map most closely to CI are:

```bash
poetry install
poetry run poe test-unit
poetry run poe test-integration
poetry run poe test-system --platform synapse
poetry run poe build
poetry run poe deploy
```

## Manual System Test Dispatch

`workflow_dispatch` supports targeted system-test runs and worker overrides.
The workflow exposes inputs for:

- platform selection
- xdist worker count override
- poll interval override
- stdout stream max wait override
- completion timeout override

That is the supported path for ad hoc end-to-end validation without cutting a release.
