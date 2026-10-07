# Setup Guide

This guide explains how to install, configure, and start using the Spark Kindling Framework for local development and cloud deployment across Microsoft Fabric, Azure Synapse Analytics, and Databricks.

## Prerequisites

### Required

- **Python 3.10+**
- **Java 11+** — required by PySpark for local development (the devcontainer image ships Java 21)
- **uv** — Kindling projects are uv projects (the devcontainer image ships it)
- **Azure CLI** (`az`) — for authenticating to Azure storage and platform workspaces, unless you use service principal environment variables. It is **not** in the devcontainer image; install it or add the `ghcr.io/devcontainers/features/azure-cli:1` feature to `.devcontainer/devcontainer.json`

### Optional

- **Azure storage account** — for deploying configs and packages to ABFSS; not required for purely local runs using in-memory entity providers
- A target cloud platform workspace (Microsoft Fabric, Azure Synapse Analytics, or Databricks) — for remote deployment

## Installation

Kindling is distributed as three pip packages on
[PyPI](https://pypi.org/project/spark-kindling/):

| Package | Purpose |
|---|---|
| `spark-kindling` | Runtime framework (entities, pipes, bootstrap) |
| `spark-kindling-cli` | CLI tooling (`kindling` command) |
| `spark-kindling-sdk` | Platform API clients (deploy, status, logs) |

`kindling repo init` / `kindling package init` projects pin these for you
(see `kindling env update`); for a one-off install:

```bash
pip install 'spark-kindling[standalone]' spark-kindling-cli
```

The CLI requires the SDK, so installing the CLI brings it along, even for
local-only use. All three are versioned together; to pin a release, pin each
one to it (`pip install 'spark-kindling[standalone]==0.14.0' spark-kindling-cli==0.14.0
spark-kindling-sdk==0.14.0`; the CLI alone only requires a minimum SDK).

For CI or cloud environments where PySpark is already provided by the
platform, drop the `standalone` extra:

```bash
pip install spark-kindling spark-kindling-cli
```

The SDK is what deploys to and manages remote platform workspaces, so the
same packages cover that too.

### Installing from a GitHub Release

Every [GitHub Release](https://github.com/sep/spark-kindling-framework/releases)
also attaches the wheels. Install by wheel URL where PyPI is unreachable (a
locked-down workspace) or for a release before 0.14.0, which exists only on
GitHub:

```bash
V=0.14.0  # any release tag, without the leading v
BASE=https://github.com/sep/spark-kindling-framework/releases/download/v$V
pip install "spark-kindling[standalone] @ $BASE/spark_kindling-$V-py3-none-any.whl" \
    "$BASE/spark_kindling_sdk-$V-py3-none-any.whl" \
    "$BASE/spark_kindling_cli-$V-py3-none-any.whl"
```

`kindling env update` / `env add` / `env bootstrap` fall back to these URLs on
their own when a version is not on PyPI; `--source github` forces them.

### Devcontainer (recommended)

`kindling repo init` generates a `.devcontainer/devcontainer.json` that uses the published image `ghcr.io/sep/spark-kindling-framework/devcontainer:latest`. The image ships Python 3.11, Java 21, uv, poe, the Databricks CLI and the Hadoop Azure JARs (at `/opt/hadoop-jars`, symlinked to `/tmp/hadoop-jars`). It bakes in no Kindling packages: PySpark 3.5 and Delta Lake come from the project's own dependencies (the `standalone` extra), and `kindling` is a shim that runs `./.venv/bin/kindling` when the current directory has one, else a system-installed CLI, else installs the latest CLI from PyPI (falling back to the latest GitHub release).

Open the repo in VS Code and choose **Dev Containers: Reopen in Container**. The `postCreateCommand` runs `kindling env bootstrap` at the repo root: if the root `pyproject.toml` declares no Kindling dependency, it adopts the release your packages pin (failing if they disagree), or pins the latest release for an empty repo, then runs `uv sync --all-packages`. You end up with one repo-wide `.venv/` and `uv.lock` at the root with every package installed editable (including the `dev` group), and VS Code's interpreter set to `.venv/bin/python`.

To pick up a newer Kindling release inside an existing devcontainer without rebuilding, run this at the repo root (it moves the root's and every package's pins together):

```bash
kindling env update
```

---

## 1. Verify the Local Environment

After installation, confirm all local prerequisites are satisfied:

```bash
kindling env check --local
```

This checks Java, PySpark, delta-spark, and the Hadoop/Azure JARs needed for ABFSS access. Fix any reported issues before continuing.

To also check platform credentials (run one of):

```bash
kindling env check --platform fabric
kindling env check --platform synapse
kindling env check --platform databricks
```

---

## 2. Initialize Project Configuration

If you are starting a new project without an existing `settings.yaml`, generate one:

```bash
kindling config init --name my-project
```

This writes a `settings.yaml` in the current directory. The generated file looks like:

```yaml
name: my-project
version: "0.1.0"
description: "Kindling data app"

kindling:
  telemetry:
    logging:
      level: INFO
      print: true
    tracing:
      print: false

  bootstrap:
    load_lake: true
    load_workspace_packages: false

  spark_configs: {}
  required_packages: []
  extensions: []
```

Use `--output` to write to a different path, or `--force` to overwrite an existing file.

A `settings.local.yaml` (gitignored) is the right place for local-only overrides — ABFSS paths, debug log levels, and anything else you do not want committed:

```yaml
# settings.local.yaml — NOT committed
kindling:
  telemetry:
    logging:
      level: DEBUG
  bootstrap:
    load_lake: false
    load_workspace_packages: true
```

You can also set individual config values from the CLI:

```bash
kindling config set kindling.telemetry.logging.level DEBUG
kindling config set kindling.bootstrap.load_lake false --level platform --platform fabric
```

---

## 3. Scaffold a Repo, Package and App

A new repo starts with the repo root and a domain package (skip this in an existing Kindling repo):

```bash
kindling repo init my-project          # .devcontainer/, .github/workflows/ci.yml, .gitignore, root pyproject.toml, packages/, apps/
kindling package init my-domain-app    # packages/my_domain_app/
kindling env bootstrap                 # repo-wide .venv/ (the devcontainer runs this for you)
```

The root `pyproject.toml` is a uv workspace root, not a package (`[tool.uv] package = false`, `[tool.uv.workspace] members = ["packages/*"]`). If the repo already had a root `pyproject.toml`, `repo init` keeps it; add `[tool.uv.workspace] members = ["packages/*"]` to it. Repos created before `repo init` wrote this file have none — add one with the content shown in [Local Python-First Development](./local_python_first.md#what-the-scaffold-creates), then run `kindling env bootstrap` at the root.

Each package is a uv workspace member with its own `pyproject.toml` (uv_build, `src/<pkg>/` layout, Kindling pinned to the CLI's version: `spark-kindling[standalone]==X.Y.Z`, plus the SDK and CLI in the `dev` group) and poe tasks. Every package must pin the same Kindling release as the root; `kindling env update` moves them together. Work in a package with:

```bash
cd packages/my_domain_app
uv run poe test     # unit + component
uv run poe build    # wheel lands in the repo-root dist/
```

`uv run` syncs what the package needs before running. Don't run a bare `uv sync` inside a package directory: in the workspace it is an exact sync of that one package and removes the others from the shared `.venv/`. Resync the whole repo with `uv sync --all-packages` (or `kindling env bootstrap`) at the root.

Then create an app under `apps/` (it uses the package with the same name unless you pass `--package`):

```bash
cd ../..   # back to the repo root

# Batch medallion app (bronze/silver/gold layers)
kindling app init my-domain-app --pattern batch --layers medallion --repo-root .

# Streaming app
kindling app init my-stream-app --pattern streaming --package my-domain-app --repo-root .

# File ingestion app
kindling app init my-ingest-app --pattern file-ingestion --package my-domain-app --repo-root .
```

Names are normalised to snake case on disk, so this creates:

```
apps/my_domain_app/
  app.yaml                  # App metadata and entry point declaration
  app.py                    # Framework entrypoint (pattern-specific)
  settings.yaml             # App-level base config
  settings.local.yaml       # Local overrides (gitignored)
  lake-reqs.txt             # Packages the app loads (and auto-registers)
  .env.example              # Template for .env
  QUICKSTART.md
  tests/
    entities/               # CSV fixtures for local runs
```

Entities, pipes and tests live in the package, not the app:

```
packages/my_domain_app/
  pyproject.toml
  src/
    my_domain_app/
      entities/             # Entity definitions
      pipes/                # Pipe definitions
      transforms/
  tests/
    unit/                   # Unit tests
    component/              # Component (DI wiring) tests
    integration/            # Integration tests (requires Spark + ABFSS; omit with --no-integration)
```

`app.py` needs no imports: the packages listed in `lake-reqs.txt` have their entities and pipes registered automatically.

---

## 4. First Run

Run the app locally with the in-memory entity provider (no Azure credentials needed):

```bash
cd apps/my_domain_app
uv run kindling app run . --env local
```

Or from the repo root (`kindling app run` finds `apps/my_domain_app/` by convention):

```bash
kindling app run my-domain-app --env local
```

Pass runtime parameters:

```bash
uv run kindling app run . --env local --param report_date=2024-01-15
# or from a file
uv run kindling app run . --env local --parameters params.yaml
```

> In the devcontainer, plain `kindling` resolves to the project's `.venv/bin/kindling` only from the repo root; from a subdirectory use `uv run kindling ...`.

Before running the full app, you can validate entity and pipe registrations without starting Spark:

```bash
uv run kindling app validate --env local
```

And smoke-test individual pipes:

```bash
# List registered pipes
kindling pipeline list --app apps/my_domain_app/app.py --env local

# Run a single pipe
kindling pipeline run bronze_to_silver_orders \
    --app apps/my_domain_app/app.py \
    --env local
```

---

## 5. Environment Setup for Local ABFSS Access

Local runs use the in-memory entity provider by default — no Azure credentials needed. To run against real ABFSS storage, you need the Hadoop Azure JARs and Azure credentials.

### Download Required JARs

The devcontainer image already ships the Hadoop Azure JARs. Outside it, or if they are missing:

```bash
kindling env ensure --cloud azure
```

Without `--cloud`, the cloud is detected from the CLIs on `PATH` (`az` → Azure), and nothing is downloaded if none is found. This downloads into `/tmp/hadoop-jars/`:

- `hadoop-azure` and related JARs from Maven Central
- `kindling-abfss-local-auth.jar` from GitHub Releases (enables Azure CLI token auth)

Safe to re-run — already-present JARs are skipped.

Verify JARs are in place:

```bash
kindling env check --local
```

### Authenticate with Azure

```bash
az login
```

The `kindling-abfss-local-auth.jar` enables Azure CLI token-based authentication to ABFSS, so service principal credentials are not required for local development.

### Configure ABFSS Paths

Set your storage credentials and paths in `.env` (gitignored):

```bash
export AZURE_STORAGE_ACCOUNT=<your-storage-account>
export AZURE_CONTAINER=artifacts
export AZURE_BASE_PATH=kindling
```

Then update `settings.local.yaml` to point entity providers at real ABFSS paths instead of memory:

```yaml
# settings.local.yaml
entity_tags:
  bronze.orders:
    provider_type: "delta"
    provider.path: "abfss://artifacts@<storage>.dfs.core.windows.net/tables/bronze/orders"
```

Verify the full local stack (Python, PySpark, JARs, Azure auth) is ready:

```bash
kindling env check --local --platform fabric
```

---

## Troubleshooting

| Symptom | Check |
|---|---|
| `kindling env check` reports missing JARs | Run `kindling env ensure --cloud azure` |
| Spark session fails to start | Java 11+ must be on PATH (the devcontainer ships Java 21): `java -version`; set `JAVA_HOME` if wrong |
| `kindling` / `import kindling` not found | Run `kindling env bootstrap` at the repo root; from a subdirectory use `uv run kindling` |
| `uv sync` fails with conflicting URLs or unsatisfiable requirements for `spark-kindling` | Packages pin different Kindling releases (or pin one by URL and another by version); run `kindling env update` at the repo root |
| ABFSS access denied locally | Run `az login`; confirm `kindling-abfss-local-auth.jar` is in `/tmp/hadoop-jars/` |
| `entity not found` at runtime | Ensure the package defining the entity is listed in the app's `lake-reqs.txt` and installed (`uv sync --all-packages` at the repo root) |
| Merge fails with schema mismatch | Run `kindling migrate plan` to inspect pending schema changes |
| Remote deploy fails with auth error | Re-run `az login`; check `kindling env check --platform <platform>` |

---

## Next Steps

- [Domain Project Quickstart](./domain_project_quickstart.md) — full end-to-end guide: entities, pipes, testing, deployment
- [Local Python-First Development](./local_python_first.md) — developing without cloud credentials
- [Hierarchical Configuration Guide](./platform_workspace_config.md) — platform and environment config overlays
