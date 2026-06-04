# Kindling CLI Reference

The `kindling` CLI is installed via `spark-kindling-cli`. Run any command with `-h`
or `--help` for inline help. Use `-V` / `--version` to print CLI, SDK, and runtime
versions.

```
kindling -V
kindling <group> --help
kindling <group> <command> --help
```

---

## pipeline

Locally run and inspect medallion pipeline layers.

### `pipeline run <PIPE_ID>`

Run a single registered pipe locally against already-populated upstream entity
storage. Primary dev iteration loop — run one layer at a time without
re-running upstream layers.

Entity data sources follow the priority stack:
`tests/entities/` fixture CSV → `kindling.yaml` env mapping → registered provider.

| Option | Default | Description |
|---|---|---|
| `--app PATH` | auto | Path to `app.py` (auto-discovered when omitted) |
| `--env TEXT` | `KINDLING_ENV` or `local` | Config environment overlay |
| `--config PATH` | — | Config directory override (if `app.py` supports it) |
| `--quiet` / `-q` | — | Suppress INFO logs; show WARNING and above only |
| `--no-watermark` | — | Bypass watermark tracking; process full dataset |

```bash
kindling pipeline run bronze.ingest_myproject --app apps/my_pipeline --env dev
kindling pipeline run silver.stage_myproject --env dev
```

### `pipeline list`

List all registered pipe IDs for an app.

| Option | Default | Description |
|---|---|---|
| `--app PATH` | auto | Path to `app.py` |
| `--env TEXT` | `KINDLING_ENV` or `local` | Config environment overlay |

---

## migrate

Inspect and apply entity schema migrations. Requires a running Spark session —
call these from within a Kindling notebook or pipeline context.

### `migrate plan`

Show pending schema changes for all registered entities without applying them.
Flags destructive changes (type changes, column removal, partition changes) so
you can review before applying.

### `migrate apply`

Apply pending schema migrations.

- Non-destructive changes (column additions) are always applied.
- Destructive changes require `--destructive`.
- CATALOG entities use a blue-green strategy: old table archived as
  `<name>_migration_blue` until cleanup.
- STORAGE entities rewrite in place using Delta's ACID guarantees.

| Option | Default | Description |
|---|---|---|
| `--destructive` | — | Allow destructive changes |
| `--backup none\|snapshot` | `none` | Backup strategy before destructive changes |

### `migrate rollback <ENTITY_ID>`

Restore a CATALOG entity to its pre-migration state by promoting the
`<name>_migration_blue` archive back to live. Only valid after a blue-green
apply that has not yet been cleaned up.

### `migrate cleanup <ENTITY_ID>`

Drop blue-green artifacts (`_migration_blue`, `_migration_green`) after a
confirmed successful migration.

---

## config

Manage Kindling configuration files.

### `config init`

Generate an initial `settings.yaml` file.

| Option | Default | Description |
|---|---|---|
| `--output PATH` | `settings.yaml` | Output file path |
| `--name TEXT` | inferred from `pyproject.toml` | App name written into the config |
| `--force` | — | Overwrite if already exists |

### `config set <KEY> <VALUE>`

Set a configuration value using dot-notation keys. Supports `base`,
`platform`, and `env` scopes.

| Option | Default | Description |
|---|---|---|
| `--level base\|platform\|env` | `base` | Config scope |
| `--platform databricks\|fabric\|synapse` | — | Required when `--level=platform` |
| `--env TEXT` | — | Environment name (required when `--level=env`) |
| `--app TEXT` | — | App name; targets the app-specific config directory |
| `--config-dir PATH` | `.` | Root directory containing config files |

```bash
kindling config set kindling.telemetry.logging.level DEBUG
kindling config set kindling.bootstrap.load_lake false --level platform --platform fabric
kindling config set kindling.secrets.secret_scope my-scope --level env --env prod
```

---

## env

Validate local environment prerequisites.

### `env check`

Check whether the local environment is ready for Kindling.

| Option | Default | Description |
|---|---|---|
| `--config PATH` | `settings.yaml` | Settings file to validate |
| `--local` | — | Also check Java, PySpark, delta-spark, and hadoop-azure JARs |
| `--platform databricks\|fabric\|synapse` | auto-detected | Check platform credential env vars; reports each as SET or MISSING with `export` hints. Exits 1 if any are missing |

Platform credential vars checked:

| Platform | Required vars |
|---|---|
| `databricks` | `DATABRICKS_HOST`, `DATABRICKS_TOKEN` |
| `fabric` | `FABRIC_WORKSPACE_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` |
| `synapse` | `SYNAPSE_WORKSPACE_NAME`, `SYNAPSE_SPARK_POOL_NAME`, `AZURE_TENANT_ID`, `AZURE_CLIENT_ID` |

```bash
kindling env check --local
kindling env check --platform fabric
```

---

## workspace

Validate and initialize workspace assets for notebook-backed platforms.

### `workspace check`

Check whether workspace configuration is ready for the selected platform.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--config PATH` | `settings.yaml` | Kindling settings file |

### `workspace init`

Create starter bootstrap and app notebook files for workspace setup.

| Option | Default | Description |
|---|---|---|
| `--output-dir PATH` | `.kindling/workspace` | Output directory |
| `--platform databricks\|fabric\|synapse` | auto-detected or `fabric` | Target platform |
| `--force` | — | Overwrite existing files |

### `workspace deploy`

Deploy kindling packages, scripts, and config to Azure Blob Storage. Uploads:

- `spark_kindling-*.whl` → `{base}/packages/`
- `kindling_bootstrap.py` → `{base}/scripts/`
- `settings.yaml` + overlays → `{base}/config/`

With `--create-notebooks` + `--workspace`, also imports starter notebooks into
the platform workspace via platform APIs.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--config PATH` | `settings.yaml` | Settings file to deploy |
| `--dist-dir PATH` | `dist` | Directory containing built wheel files |
| `--storage-account TEXT` | `AZURE_STORAGE_ACCOUNT` | Storage account name, `name.domain`, or full URL |
| `--container TEXT` | `AZURE_CONTAINER` or `artifacts` | Blob container name |
| `--base-path TEXT` | `AZURE_BASE_PATH` | Base path prefix within container |
| `--skip-wheels` | — | Skip wheel upload |
| `--skip-bootstrap-script` | — | Skip bootstrap script upload |
| `--skip-config` | — | Skip config upload |
| `--create-notebooks` | — | Generate and import notebook stubs into the workspace |
| `--workspace TEXT` | — | Target workspace for notebook import |
| `--overwrite` | — | Overwrite existing scripts, config, and notebooks |
| `--allow-missing-bootstrap-script` | — | Don't fail if `kindling_bootstrap.py` is not found |
| `--allow-missing-config` | — | Don't fail if settings file is not found |

```bash
kindling workspace deploy --platform synapse --storage-account mystorageacct
kindling workspace deploy --platform databricks --create-notebooks --workspace https://adb-123.4.azuredatabricks.net
```

> **Note:** `workspace deploy` is for the kindling project team deploying from a
> local build. To publish kindling to your own storage account as a user, use
> [`runtime publish`](#runtime-publish).

---

## runtime

Manage kindling runtime artifacts.

### `runtime publish`

Publish kindling runtime artifacts (wheels + bootstrap script) to Azure Data
Lake Storage. This is the primary path for installing kindling into a new
environment or promoting between environments (e.g. staging → prod).

**Source types:**

| Source | Example | Description |
|---|---|---|
| `github:VERSION` | `github:latest`, `github:0.10.15` | Download from a GitHub release via `gh` CLI |
| `local:PATH` | `local:./dist` | Read wheels from a local directory |
| `abfss://…` | `abfss://artifacts@staging.dfs.core.windows.net/k` | Copy from another ADLS path |

**Destination layout** under `--dest`:

```
{dest}/packages/   ← spark_kindling-*.whl
{dest}/scripts/    ← kindling_bootstrap.py
```

The `--dest` root is your `artifacts_storage_path` in `BOOTSTRAP_CONFIG`.

| Option | Default | Description |
|---|---|---|
| `--source TEXT` | required | Source specifier (see above) |
| `--dest TEXT` | required | Destination `abfss://` URI |
| `--version TEXT` | — | Release version for `github:` source; overrides the version embedded in `--source` |
| `--skip-bootstrap` | — | Skip the bootstrap script |
| `--overwrite` | — | Overwrite existing scripts (wheels always overwrite) |

```bash
# Install the latest release into a storage account
kindling runtime publish \
  --source github:latest \
  --dest abfss://artifacts@myacct.dfs.core.windows.net/kindling

# Install a specific version
kindling runtime publish \
  --source github:0.10.15 \
  --dest abfss://artifacts@myacct.dfs.core.windows.net/kindling

# Publish from a local build
kindling runtime publish \
  --source local:./dist \
  --dest abfss://artifacts@mydev.dfs.core.windows.net/kindling

# Promote staging → prod (ADLS to ADLS)
kindling runtime publish \
  --source abfss://artifacts@staging.dfs.core.windows.net/kindling \
  --dest abfss://artifacts@prod.dfs.core.windows.net/kindling
```

Auth uses `DefaultAzureCredential` — `az login` or
`AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` both work.

---

## app

Validate, package, deploy, run, and inspect Kindling applications.

### `app init <APP_NAME>`

Create a Kindling app under `apps/` in an existing repo. Generates `app.py`,
`app.yaml`, `lake-reqs.txt`, `settings.yaml`, `settings.local.yaml`, and
`QUICKSTART.md`.

| Option | Default | Description |
|---|---|---|
| `--package TEXT` | app name | Domain package imported by the app |
| `--auth oauth\|key\|cli` | `oauth` | ABFSS auth method for generated local config comments |
| `--layers medallion\|minimal` | `medallion` | Entity/pipe template style |
| `--repo-root PATH` | `.` | Repo root that will receive the app under `apps/` |
| `--pattern batch\|streaming\|file-ingestion` | — | Execution pattern to scaffold; omit for a minimal hello-world app |
| `--template-dir PATH` | — | Directory of custom Jinja2 templates overlaying the built-ins |

### `app validate`

Validate entity and pipe definitions without starting Spark. Checks entity and
pipe registration, pipe input/output entity existence, and delta entity
`merge_columns` presence.

| Option | Default | Description |
|---|---|---|
| `--app PATH` | auto | Path to `app.py` |
| `--env TEXT` | `KINDLING_ENV` or `local` | Config environment to load |

### `app package <APP_NAME>`

Package an app directory into a `.kda` archive. Looks up `apps/<app_name>/` by convention;
use `--local-folder` for non-standard layouts.

| Option | Default | Description |
|---|---|---|
| `--local-folder PATH` | — | Override convention lookup with this directory |
| `--output PATH` | `dist/<app>.kda` | Destination `.kda` file or directory |
| `--json` | — | Machine-readable JSON output |

### `app deploy <APP_NAME>`

Deploy an app to a remote platform. Looks up `apps/<app_name>/` by convention and packages
on the fly; use `--local-folder` to override or `--kda-package` for a pre-built archive.

| Option | Default | Description |
|---|---|---|
| `--local-folder PATH` | — | Override convention lookup. Packaged on the fly. |
| `--kda-package PATH` | — | Pre-built `.kda` archive. Skips convention lookup. |
| `--remote-name TEXT` | source dir stem | Remote app name |
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--json` | — | Machine-readable JSON output |

### `app run <APP_NAME>`

Run an app locally (standalone) or submit a run of a deployed app (remote).

**Standalone** (`--platform standalone`, the default): looks up `apps/<app_name>/` by convention
and runs locally with embedded Spark. Use `--local-folder` to override for non-standard layouts.

**Remote** (`--platform databricks|fabric|synapse`): submits a run of the already-deployed app.
The app must have been deployed first with `kindling app deploy`. `--local-folder` has no meaning
for remote runs and will error.

| Option | Default | Description |
|---|---|---|
| `--platform standalone\|databricks\|fabric\|synapse` | `standalone` | Execution platform |
| `--local-folder PATH` | — | Override convention lookup (standalone only) |
| `--app-name TEXT` | — | Remote app name override (remote only) |
| `--env TEXT` | — | Runtime environment |
| `--config PATH` | — | Config directory override (standalone only) |
| `--quiet` / `-q` | — | Suppress INFO logs (standalone only) |
| `--local-package PATH` | — | Prepend a local package root to PYTHONPATH (repeatable; standalone only) |
| `--parameters PATH` | — | YAML/JSON file of runtime parameters |
| `--param KEY=VALUE` | — | Runtime parameter override (repeatable) |
| `--no-wait` | — | Return immediately after starting (remote) |
| `--no-logs` | — | Skip log streaming (remote) |
| `--poll-interval FLOAT` | `5.0` | Status poll interval in seconds |
| `--timeout FLOAT` | `3600.0` | Max wait time in seconds |
| `--fail-on-error` / `--no-fail-on-error` | fail | Exit non-zero on failed run |
| `--dotenv PATH` | `.env` | Dotenv file to load (repeatable) |
| `--no-dotenv` | — | Do not load `.env` |
| `--json` | — | Machine-readable JSON output |

```bash
# Local standalone — convention lookup
kindling app run my-pipeline
kindling app run my-pipeline --local-package packages/my_pipeline --env local

# Local standalone — non-standard layout
kindling app run my-pipeline --local-folder path/to/app

# Remote — deploy first, then run
kindling app deploy my-pipeline --platform synapse
kindling runner ensure --platform synapse
kindling app run my-pipeline --platform synapse
```

### `app status <RUN_ID>`

Fetch the current status for a remote app run.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |

### `app logs <RUN_ID>`

Fetch or stream logs for a remote app run.

| Option | Default | Description |
|---|---|---|
| `--from-line INT` | `0` | Starting line number |
| `--size INT` | `1000` | Number of lines to fetch |
| `--stream` | — | Tail logs until completion or timeout |
| `--poll-interval FLOAT` | `5.0` | Poll interval for streaming |
| `--max-wait FLOAT` | `300.0` | Max streaming wait in seconds |
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |

### `app cancel <RUN_ID>`

Cancel a remote app run.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--json` | — | Machine-readable JSON output |

### `app cleanup <APP_NAME>`

Delete a previously deployed remote application. Pass the app name directly — the same name
used when deploying.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--json` | — | Machine-readable JSON output |

### `app inspect <APP_NAME>`

Show entity resolution information — provider type, storage path, and whether
a `tests/entities/` fixture override is active.

| Option | Default | Description |
|---|---|---|
| `--env TEXT` | `local` | Config environment |
| `--app PATH` | auto | Path to `app.py` |
| `--entities` | — | Show full entity resolution table |

---

## entity

Inspect and validate entity data during local development.

### `entity show <ENTITY_ID>`

Print entity contents from its data source. Reads via the priority stack:
`tests/entities/` fixture CSV first, then the registered provider.

| Option | Default | Description |
|---|---|---|
| `--env TEXT` | `local` | Config environment |
| `--app PATH` | auto | Path to `app.py` |
| `--limit INT` | `20` | Max rows to display |
| `--count` | — | Print row count only |

### `entity validate <ENTITY_ID>`

Run basic data quality checks: row count (ERROR if zero), null check (WARN on
nulls in key/required columns), schema match (WARN if fixture columns differ
from entity schema). Exits 0 on pass or warnings; exits 1 on any ERROR.

| Option | Default | Description |
|---|---|---|
| `--env TEXT` | `local` | Config environment |
| `--app PATH` | auto | Path to `app.py` |

---

## runner

Manage per-app job definitions for remote execution. `runner ensure` creates
durable job definitions so that `kindling app run --platform <platform>` can
submit runs against them.

### `runner ensure`

Create job definitions for one app or all discovered apps. Idempotent —
existing definitions are left unchanged.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--app TEXT` | all | App name to ensure; omit to scan and ensure all discovered apps |
| `--json` | — | Machine-readable JSON output |

### `runner status`

Show runner installation state, platform job ID, version, and health.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--verbose` | — | Include full runner configuration |
| `--json` | — | Machine-readable JSON output |

### `runner repair`

Recreate or update the runner job and its bootstrap/config references. Use
after credential or configuration changes.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--json` | — | Machine-readable JSON output |

### `runner delete`

Delete the runner job definition from the platform workspace. Prevents remote
`kindling app run` until reinstalled with `runner ensure`.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--yes` | — | Skip confirmation prompt |
| `--json` | — | Machine-readable JSON output |

### `runner invoke`

Invoke the runner with a raw parameters YAML file. Advanced/debug use only —
normal execution should use `kindling app run`.

| Option | Default | Description |
|---|---|---|
| `--params PATH` | required | YAML or JSON parameters file |
| `--platform databricks\|fabric\|synapse` | auto-detected | Target platform |
| `--wait` | — | Poll until the run completes |
| `--poll-interval FLOAT` | `10.0` | Poll interval in seconds |
| `--timeout FLOAT` | `3600.0` | Timeout in seconds |
| `--json` | — | Machine-readable JSON output |

---

## test

Run Kindling project test suites.

### `test run`

Run pytest through Kindling's portable test wrapper.

| Option | Default | Description |
|---|---|---|
| `--suite unit\|component\|integration\|system\|extension\|all` | `unit` | Logical test suite |
| `--path PATH` | `tests/<suite>` | Pytest path to run (repeatable) |
| `--platform databricks\|fabric\|synapse` | — | Target platform |
| `--test TEXT` | — | Pytest `-k` expression |
| `--marker TEXT` | — | Pytest `-m` expression |
| `--ci` | — | Emit junit/json reports and fail fast |
| `--results-dir PATH` | `test-results` | Results directory |
| `--workers TEXT` | — | pytest-xdist worker count |
| `--coverage TEXT` | — | Coverage target (`--cov=<target>`; repeatable) |
| `--no-cov` | — | Pass `--no-cov` to pytest |
| `--preflight none\|local\|system` | `none` | Optional preflight check |
| `--dotenv PATH` | `.env` | Dotenv file to load (repeatable) |
| `--no-dotenv` | — | Do not load `.env` |
| `--pytest-arg TEXT` | — | Extra argument passed through to pytest (repeatable) |

### `test check`

Run a Kindling test preflight check without pytest.

| Option | Default | Description |
|---|---|---|
| `--preflight local\|system` | `local` | Preflight type |
| `--platform databricks\|fabric\|synapse` | — | Target platform |

### `test cleanup`

Run project cleanup hooks for system-test resources.

| Option | Default | Description |
|---|---|---|
| `--platform databricks\|fabric\|synapse` | — | Target platform |
| `--all` | — | Clean all configured platforms |
| `--skip-packages` | — | Skip cleanup of old package artifacts |

---

## repo

Scaffold and manage multi-package Kindling repos.

### `repo init <REPO_NAME>`

Create a Kindling repo root with shared dev tooling (`.devcontainer/`,
`.github/workflows/ci.yml`, `.gitignore`).

| Option | Default | Description |
|---|---|---|
| `--output-dir PATH` | `.` | Directory to initialize as the repo root |
| `--template-dir PATH` | — | Custom Jinja2 templates overlaying the built-ins |
| `--overwrite-devcontainer` | — | Replace an existing `devcontainer.json` |

---

## package

Create and deploy Kindling domain packages.

### `package init <PACKAGE_NAME>`

Create a Kindling package under an existing multi-package repo at
`packages/<name>/`.

| Option | Default | Description |
|---|---|---|
| `--auth oauth\|key\|cli` | `oauth` | Auth style for generated test/config examples |
| `--layers medallion\|minimal` | `medallion` | Package template style |
| `--no-integration` | — | Omit `tests/integration/` |
| `--repo-root PATH` | `.` | Repo root to receive the new package |
| `--template-dir PATH` | — | Custom Jinja2 templates |

### `package deploy <PACKAGE_NAME>`

Build a package wheel with Poetry and upload it to artifact storage at
`{base}/packages/`. Looks up `packages/<package_name>/` by convention; use `--local-folder`
for non-standard layouts.

| Option | Default | Description |
|---|---|---|
| `--local-folder PATH` | — | Override convention lookup with this directory |
| `--dist-dir PATH` | `dist` | Directory where Poetry writes the wheel |
| `--storage-account TEXT` | `AZURE_STORAGE_ACCOUNT` | Storage account |
| `--container TEXT` | `AZURE_CONTAINER` or `artifacts` | Container name |
| `--base-path TEXT` | `AZURE_BASE_PATH` | Base path prefix |
| `--json` | — | Machine-readable JSON output |

### `package add entity <ENTITY_ID>`

Scaffold an entity definition and a CSV fixture stub.

- Appends a `DataEntities.entity()` skeleton to `<path>/entities.py`
- Creates `tests/entities/<ns>/<name>.csv`

| Option | Default | Description |
|---|---|---|
| `--package PATH` | required | Package root |

### `package add pipe <PIPE_ID>`

Scaffold a `DataPipes` pipe and matching unit/integration test stubs.

| Option | Default | Description |
|---|---|---|
| `--inputs TEXT` | — | Comma-separated input entity IDs |
| `--package PATH` | required | Package root |

### `package add ingestion <ENTITY_ID>`

Scaffold a file-ingestion pipe and matching test stubs. The `--source-pattern`
is matched against the filename (not the full ABFSS path); named groups are
automatically extracted as columns by the framework.

| Option | Default | Description |
|---|---|---|
| `--source-pattern TEXT` | — | Regex for matching filenames; named groups become columns |
| `--filename-metadata TEXT` | — | Field name to extract from a named capture group (ignored when `--source-pattern` is set) |
| `--package PATH` | required | Package root |

---

## agent

Manage agent instruction files for Claude Code, Copilot, and Codex.

### `agent setup`

Generate (or update) agent instruction files from the Kindling reference doc:

- `CLAUDE.md` — Claude Code
- `.github/copilot-instructions.md` — GitHub Copilot
- `AGENTS.md` — Codex / OpenAI agents

Re-run after pulling a new devcontainer image to pick up updated documentation.

| Option | Default | Description |
|---|---|---|
| `--force` | — | Regenerate even if version is unchanged |
| `--check` | — | Report whether files are up to date without writing |
| `--project PATH` | `.` | Project root directory |

---

## Environment variables

| Variable | Used by | Description |
|---|---|---|
| `AZURE_STORAGE_ACCOUNT` | `workspace deploy`, `package deploy`, `runtime publish` | Storage account name |
| `AZURE_CONTAINER` | same | Blob container (default: `artifacts`) |
| `AZURE_BASE_PATH` | same | Base path prefix within container |
| `AZURE_TENANT_ID` | all Azure auth | Service principal tenant |
| `AZURE_CLIENT_ID` | all Azure auth | Service principal client ID |
| `AZURE_CLIENT_SECRET` | all Azure auth | Service principal secret |
| `AZURE_CLOUD` | Azure auth | Cloud environment (`AzureUSGovernment`, `AzureChinaCloud`, etc.) |
| `DATABRICKS_HOST` | databricks platform | Databricks workspace URL |
| `DATABRICKS_TOKEN` | databricks platform | Databricks PAT |
| `FABRIC_WORKSPACE_ID` | fabric platform | Fabric workspace GUID |
| `FABRIC_LAKEHOUSE_ID` | fabric platform | Fabric lakehouse GUID |
| `SYNAPSE_WORKSPACE_NAME` | synapse platform | Synapse workspace name |
| `SYNAPSE_SPARK_POOL_NAME` | synapse platform | Spark pool name |
| `KINDLING_ENV` | app/pipeline/config | Default environment overlay |
