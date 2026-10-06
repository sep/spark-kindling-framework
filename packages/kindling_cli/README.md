# spark-kindling-cli

Command-line tooling for the Spark Kindling Framework. Distributed as
`spark-kindling-cli`; installs the `kindling` console script.

## Install

Kindling packages are not on PyPI; install them from a
[GitHub Release](https://github.com/sep/spark-kindling-framework/releases)'s
wheel URLs. The CLI depends on `spark-kindling-sdk` (used for remote platform
lifecycle operations), and the SDK only resolves from its release URL too, so
install both:

```bash
V=0.13.0  # any release, without the leading v
BASE=https://github.com/sep/spark-kindling-framework/releases/download/v$V
pip install "$BASE/spark_kindling_cli-$V-py3-none-any.whl" \
    "$BASE/spark_kindling_sdk-$V-py3-none-any.whl"
```

Inside the Kindling devcontainer, `kindling` is a shim that runs the project's
own `./.venv/bin/kindling` (pinned in `pyproject.toml`) when present.

## Commands

- `kindling repo init` — scaffold a multi-package repo root (uv workspace root `pyproject.toml`, devcontainer, CI) with shared dev tooling
- `kindling package init` — scaffold a package (a uv workspace member) under `packages/<name>` in an existing repo
- `kindling app init` — scaffold an app under `apps/<name>` that runs a domain package
- `kindling config init` — generate a starter `settings.yaml`
- `kindling config set <key> <value>` — set a config value using dot-notation
- `kindling env check` — validate the local Python/config environment
- `kindling env bootstrap` — pin Kindling if nothing declares it yet and `uv sync --all-packages` (the devcontainer's `postCreateCommand`)
- `kindling env update` / `kindling env add` — move or add Kindling release wheel pins
- `kindling env ensure` — download Hadoop/ABFSS JARs into `/tmp/hadoop-jars/`
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
  --wheel dist/spark_kindling-0.12.49-py3-none-any.whl \
  --wheel dist/spark_kindling_ext_sdp-0.3.4-py3-none-any.whl \
  --wheel dist/spark_kindling_ext_databricks-0.2.0-py3-none-any.whl \
  --wheel dist/orders_kindling_app-1.4.0-py3-none-any.whl

cd dist/bundles/databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev orders
```

The bundle is rendered from a **template**: ordinary Databricks bundle YAML
with Jinja placeholders. The built-in template produces `databricks.yml`, one
`resources/<key>.pipeline.yml` per pipeline and the generic Lakeflow source,
driven by `--app-options-json` (per-app catalog/schema/continuous/pipes, or a
`pipelines` map to split an app). When a project needs its own resource keys,
display names, tags, per-pipeline permissions, clusters or anything else DAB
supports, copy the template and own it:

```bash
kindling bundle template init          # -> bundle-template/
kindling bundle build --template-dir bundle-template ...
```

Inside a template the generator supplies only what it uniquely knows. Settings
are found by convention, never listed: the shared `config/` overlays
(`settings.yaml`, `settings.databricks.yaml`, `workspace_<id>.yaml`,
`settings.<env>.yaml`) and then each app's own `settings*.yaml` are merged at
build time in the runtime's order and exposed as `apps[<name>].settings`.
`kindling.configuration(<app>, pipes=[...], extra={...})` returns a pipeline's
complete `configuration` map: the app selection, the inline
`kindling.lakeflow.settings_json`, the pipe subset, any extra flat overrides,
and a `kindling.lakeflow.config_keys` naming every emitted key a restricted
runtime must point-look-up. Rendered resources are parsed back and a pipeline
whose configuration did not come from the helper, or whose `config_keys` is
incomplete, fails the build. The deployed pipeline reads no settings files and
the resource file is the complete, reviewable description of what it runs
with. Files ending in `.j2` are rendered (`__pipeline__` in a name renders
once per pipeline); other files are copied verbatim. `manifest.json` records
the generator version, inputs, the SHA-256 of every settings file, wheel and
template file; no timestamps, so identical inputs give identical output.
`--app` may be omitted to build every app under `data-apps/` or `apps/`.
See `docs/proposals/databricks_bundle_deployment.md`.

Wheels passed with `--wheel` are uploaded by `databricks bundle deploy` under
the bundle's own workspace root and become the pipeline environment's
dependencies in the order given. Serverless installs them one at a time and
Kindling packages are not on PyPI, so list dependencies first: framework
core, then `spark-kindling-ext-sdp`, then `spark-kindling-ext-databricks`,
then the app wheel.

The default `--workspace-root` is `/Workspace/Shared/kindling/<name>/<target>`:
a stable path that survives redeploys by different principals, which is
what shared dev/prod targets need. `databricks bundle validate` warns that
`/Workspace/Shared` is writable by all workspace users; for a personal
development target pass `--workspace-root /Workspace/Users/<you>/...`, and
for shared targets either accept the warning or grant the intended group
through `--permissions-json`.

## Scaffolding

The scaffolding commands now target true multi-package repos:

```bash
git clone <your-empty-repo-url> data-platform
cd data-platform
kindling repo init data-platform

kindling package init sales-ops --repo-root .
```

This produces a repo root with shared `.devcontainer/`, CI,
`scripts/setup-local-dev.sh` and `.gitignore` plus independently buildable
packages under `packages/`. The root `pyproject.toml` is a non-package uv
workspace root (`[tool.uv] package = false`,
`[tool.uv.workspace] members = ["packages/*"]`), so the repo shares one
`.venv/` and `uv.lock` with every package installed editable. Run
`kindling env bootstrap` at the root (the devcontainer does this on create) to
pin Kindling there and sync. Every package must pin the same Kindling release
as the root; `kindling env update` moves them together.
If `.devcontainer/` already exists, `kindling repo init` warns and leaves it
unchanged. Pass `--overwrite-devcontainer` to replace the generated
devcontainer config. An existing root `pyproject.toml` is also kept; add
`[tool.uv.workspace] members = ["packages/*"]` to it.

To add a second package later, return to the repo root and scaffold another
package:

```bash
kindling package init customer-360 --repo-root .

cd packages/customer_360
uv run poe test
```

`uv run` syncs what the package needs before running. Don't run a bare `uv sync` inside a package directory: in the workspace it is an exact sync of that one package and removes the others from the shared `.venv/`. Resync the whole repo with `uv sync --all-packages` (or `kindling env bootstrap`) at the root.

The generated CI workflow runs in the devcontainer image and tests each
scaffolded package independently by iterating `packages/*` and executing:

```bash
uv run poe test
uv run poe build
```

The CI job fails if no package `pyproject.toml` files are found under
`packages/`.

Apps are scaffolded separately from packages:

```bash
kindling app init sales-ops --package sales-ops --repo-root .
```

## Related

- Runtime framework: `pip install "spark-kindling[<platform>] @ $BASE/spark_kindling-$V-py3-none-any.whl"` where `<platform>` is one of `synapse`, `databricks`, `fabric`, `standalone`.
- Design-time SDK: `pip install "$BASE/spark_kindling_sdk-$V-py3-none-any.whl"`.
