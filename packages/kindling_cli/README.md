# spark-kindling-cli

Command-line tooling for the Spark Kindling Framework. Distributed on PyPI as
`spark-kindling-cli`; installs the `kindling` console script.

## Install

```bash
pip install spark-kindling-cli
```

Depends on `spark-kindling-sdk` for remote platform lifecycle operations.
Install the SDK alongside the CLI when you want to deploy apps or manage the
durable runner:

```bash
pip install spark-kindling-sdk
```

## Commands

- `kindling repo init` — scaffold a multi-package repo root with shared dev tooling
- `kindling package init` — scaffold a package under `packages/<name>` in an existing repo
- `kindling config init` — generate a starter `settings.yaml`
- `kindling config set <key> <value>` — set a config value using dot-notation
- `kindling env check` — validate the local Python/config environment
- `kindling workspace check` — validate the configured platform workspace
- `kindling workspace init` — scaffold bootstrap + starter notebook files for a platform
- `kindling workspace deploy` — upload the runtime wheel, bootstrap script, config, and notebooks to artifact storage
- `kindling app package` — build a `.kda` archive from a local app directory
- `kindling app deploy` — deploy an app directory or `.kda` package through the SDK
- `kindling app run` — run all local app pipes on `standalone`, or submit to the durable Kindling runner on a managed platform
- `kindling app status` — fetch the current remote app run status
- `kindling app logs` — fetch or stream remote app run logs
- `kindling app cancel` — cancel an active remote app run
- `kindling app cleanup` — remove a deployed app from remote storage
- `kindling runner ensure` — install or verify the durable Kindling runner on a platform
- `kindling runner status` — check whether the runner is installed and healthy
- `kindling runner repair` — reinstall the runner (delete and recreate)
- `kindling runner delete` — remove the runner from a platform
- `kindling bundle build` — precompile a project into a Databricks bundle (`databricks.yml`, pipeline resources, generic Lakeflow source, manifest) for `databricks bundle validate/deploy/run`

Run any command with `--help` for full options.

## Databricks bundles

`kindling bundle build` assembles a disposable Databricks bundle from a
project's `config/` overlays and `data-apps/` (or `apps/`) directories. It
starts no Spark session and imports no app code. Deployment inputs come from
options or `KINDLING_BUNDLE_*` environment variables (options win; runtime
settings files are never consulted):

```bash
kindling bundle build \
  --name sales --target dev --app orders \
  --workspace-host https://adb-123.azuredatabricks.net \
  --catalog dev_sales --schema orders \
  --dependency 'spark-kindling-ext-databricks==0.1.15' \
  --wheel dist/orders_kindling_app-1.4.0-py3-none-any.whl

cd dist/bundles/databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev orders
```

By default each pipeline resource carries its effective configuration inline
(`kindling.lakeflow.settings_json`): the base, platform, workspace,
environment and app settings files are merged at build time in the runtime's
order, so the deployed pipeline reads no workspace files or volumes.
`--config-transport files` instead stages the settings files in the bundle
and references them through `spark.kindling.bootstrap.config_files`.
`--app-options-json` sets per-app catalog/schema/continuous/pipes or splits
an app into several pipelines (`{"orders": {"pipelines": {"bronze": {...}}}}`).
`manifest.json` records the generator version, inputs, and the SHA-256 of
every settings file and wheel; no timestamps, so identical inputs give
identical output. See `docs/proposals/databricks_bundle_deployment.md`.

## Scaffolding

The scaffolding commands now target true multi-package repos:

```bash
git clone <your-empty-repo-url> data-platform
cd data-platform
kindling repo init data-platform

kindling package init sales-ops --repo-root .
kindling package init customer-360 --repo-root .
```

This produces a repo root with shared `.devcontainer/`, CI, and `.gitignore`
plus independently buildable packages under `packages/`.
If `.devcontainer/` already exists, `kindling repo init` warns and leaves it
unchanged. Pass `--overwrite-devcontainer` to replace the generated
devcontainer config.

To add a second package later, return to the repo root and scaffold another
package:

```bash
cd ../..
kindling package init customer-360 --repo-root .

cd packages/customer_360
poetry install
poetry run poe test
```

The generated CI workflow runs each scaffolded package independently by
iterating `packages/*` and executing:

```bash
poetry install --no-interaction
poetry run poe test
poetry run poe build
```

The CI job fails if no package `pyproject.toml` files are found under
`packages/`.

Apps are scaffolded separately from packages:

```bash
kindling app init sales-ops --package sales-ops --repo-root .
```

## Related

- Runtime framework: `pip install 'spark-kindling[<platform>]'` where `<platform>` is one of `synapse`, `databricks`, `fabric`, `standalone`.
- Design-time SDK: `pip install spark-kindling-sdk`.
