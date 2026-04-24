# spark-kindling-cli

Command-line tooling for the Spark Kindling Framework. Distributed on PyPI as
`spark-kindling-cli`; installs the `kindling` console script.

## Install

```bash
pip install spark-kindling-cli
```

Depends on `spark-kindling-sdk` for remote platform lifecycle operations.
Install the SDK alongside the CLI when you want to deploy apps or manage jobs:

```bash
pip install spark-kindling-sdk
```

## Commands

- `kindling repo init` — scaffold a multi-package repo root with shared dev tooling
- `kindling package init` — scaffold a package under `packages/<name>` in an existing repo
- `kindling new` — convenience command that creates a repo plus its first package
- `kindling config init` — generate a starter `settings.yaml`
- `kindling config set <key> <value>` — set a config value using dot-notation
- `kindling env check` — validate the local Python/config environment
- `kindling workspace check` — validate the configured platform workspace
- `kindling workspace init` — scaffold bootstrap + starter notebook files for a platform
- `kindling workspace deploy` — upload the runtime wheel, bootstrap script, config, and notebooks to artifact storage
- `kindling app package` — build a `.kda` archive from a local app directory
- `kindling app deploy` — deploy an app directory or `.kda` package through the SDK
- `kindling app cleanup` — remove a deployed app from remote storage
- `kindling job create` — create a remote job from a YAML/JSON config file
- `kindling job run` — start a job run, optionally with parameters
- `kindling job status` — fetch the current run status
- `kindling job logs` — fetch or stream run logs
- `kindling job cancel` — cancel an active run
- `kindling job delete` — delete a remote job definition

Run any command with `--help` for full options.

## Scaffolding

The scaffolding commands now target true multi-package repos:

```bash
kindling repo init data-platform
cd data_platform

kindling package init sales-ops --repo-root .
kindling package init customer-360 --repo-root .
```

This produces a repo root with shared `.devcontainer/`, CI, and `.gitignore`
plus independently buildable packages under `packages/`.

For the single-package quick start, `kindling new my-pipeline` still works and
creates a repo with an initial package at `packages/my_pipeline/`.

## Related

- Runtime framework: `pip install 'spark-kindling[<platform>]'` where `<platform>` is one of `synapse`, `databricks`, `fabric`, `standalone`.
- Design-time SDK: `pip install spark-kindling-sdk`.
