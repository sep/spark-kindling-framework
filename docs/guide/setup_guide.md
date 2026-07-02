# Setup Guide

This guide explains how to install, configure, and start using the Spark Kindling Framework for local development and cloud deployment across Microsoft Fabric, Azure Synapse Analytics, and Databricks.

## Prerequisites

### Required

- **Python 3.10+**
- **Java 11+** — required by PySpark for local development
- **Azure CLI** (`az`) — for authenticating to Azure storage and platform workspaces

### Optional

- **Azure storage account** — for deploying configs and packages to ABFSS; not required for purely local runs using in-memory entity providers
- A target cloud platform workspace (Microsoft Fabric, Azure Synapse Analytics, or Databricks) — for remote deployment

## Installation

Kindling is distributed as three pip packages:

| Package | Purpose |
|---|---|
| `spark-kindling` | Runtime framework (entities, pipes, bootstrap) |
| `spark-kindling-cli` | CLI tooling (`kindling` command) |
| `spark-kindling-sdk` | Platform API clients (deploy, status, logs) |

For local development (includes PySpark and Delta Lake):

```bash
pip install 'spark-kindling[standalone]' spark-kindling-cli
```

For CI or cloud environments where PySpark is already provided by the platform:

```bash
pip install spark-kindling spark-kindling-cli
```

To also deploy and manage remote platform workspaces:

```bash
pip install spark-kindling spark-kindling-cli spark-kindling-sdk
```

### Devcontainer (recommended)

The supplied devcontainer ships with Python 3.11, Java 11, PySpark 3.4, Delta Lake, Azure CLI, and all tooling pre-installed. Open the repo in VS Code and choose **Dev Containers: Reopen in Container**. The `postCreateCommand` automatically runs `poetry install --with dev --sync`.

To pick up a newer Kindling release inside an existing devcontainer without rebuilding:

```bash
kindling env update
```

---

## 1. Verify the Local Environment

After installation, confirm all local prerequisites are satisfied:

```bash
kindling env check --local
```

This checks Python version, PySpark, delta-spark, and the Hadoop/Azure JARs needed for ABFSS access. Fix any reported issues before continuing.

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

## 3. Scaffold an App

Create a new app under `apps/`:

```bash
# Batch medallion app (bronze/silver/gold layers)
kindling app init my-domain-app --pattern batch --layers medallion --repo-root .

# Streaming app
kindling app init my-stream-app --pattern streaming --repo-root .

# File ingestion app
kindling app init my-ingest-app --pattern file-ingestion --repo-root .
```

This creates:

```
apps/my-domain-app/
  app.yaml                  # App metadata and entry point declaration
  app.py                    # Framework entrypoint — import modules here
  settings.yaml             # App-level base config
  settings.local.yaml       # Local overrides (gitignored)
  lake-reqs.txt             # Remote package requirements
  src/
    my_domain_app/
      entities/             # Entity definitions
      pipes/                # Pipe definitions
  tests/
    entities/               # CSV fixtures for local testing
    unit/                   # Unit tests
    component/              # Component (DI wiring) tests
    integration/            # Integration tests (requires Spark + ABFSS)
```

`app.py` is where you import your domain modules so their entities and pipes are registered with the framework:

```python
def initialize(env: str = None, config_dir: Path = None):
    import my_domain_app.entities.records   # noqa: F401
    import my_domain_app.pipes.bronze       # noqa: F401
```

---

## 4. First Run

Run the app locally with the in-memory entity provider (no Azure credentials needed):

```bash
cd apps/my-domain-app
kindling app run . --env local
```

Or from the repo root:

```bash
kindling app run my-domain-app --env local --local-folder apps/my-domain-app
```

Pass runtime parameters:

```bash
kindling app run . --env local --param report_date=2024-01-15
# or from a file
kindling app run . --env local --parameters params.yaml
```

Before running the full app, you can validate entity and pipe registrations without starting Spark:

```bash
kindling app validate --env local
```

And smoke-test individual pipes:

```bash
# List registered pipes
kindling pipeline list --app apps/my-domain-app/app.py --env local

# Run a single pipe
kindling pipeline run bronze_to_silver_orders \
    --app apps/my-domain-app/app.py \
    --env local
```

---

## 5. Environment Setup for Local ABFSS Access

Local runs use the in-memory entity provider by default — no Azure credentials needed. To run against real ABFSS storage, you need the Hadoop Azure JARs and Azure credentials.

### Download Required JARs

```bash
kindling env ensure
```

This downloads into `/tmp/hadoop-jars/`:

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
| `kindling env check` reports missing JARs | Run `kindling env ensure` |
| Spark session fails to start | Java 11 must be on PATH: `java -version`; set `JAVA_HOME` if wrong |
| ABFSS access denied locally | Run `az login`; confirm `kindling-abfss-local-auth.jar` is in `/tmp/hadoop-jars/` |
| `entity not found` at runtime | Ensure the module defining the entity is imported in `app.py::initialize()` |
| Merge fails with schema mismatch | Run `kindling migrate plan` to inspect pending schema changes |
| Remote deploy fails with auth error | Re-run `az login`; check `kindling env check --platform <platform>` |

---

## Next Steps

- [Domain Project Quickstart](./domain_project_quickstart.md) — full end-to-end guide: entities, pipes, testing, deployment
- [Local Python-First Development](./local_python_first.md) — developing without cloud credentials
- [Hierarchical Configuration Guide](./platform_workspace_config.md) — platform and environment config overlays
