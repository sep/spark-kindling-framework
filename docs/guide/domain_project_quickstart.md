# Domain Project Quickstart

This guide walks a **domain project developer** through standing up a local dev environment, initialising a platform workspace, and building a domain project end-to-end — entities, pipes, and a data app — ready for local testing and remote deployment.

> **Looking to contribute to the kindling framework itself?** See [developer_workflow.md](developer_workflow.md) instead.

> **Assumptions**
> - You have a dev repo already cloned (or are starting a new one).
> - You have a target platform workspace (Microsoft Fabric, Azure Synapse Analytics, or Databricks).
> - You are using the supplied devcontainer (recommended).

---

## 1. Open the Dev Container

If you are starting a new repo, scaffold the repo root and a domain package first (the `kindling` CLI can come from a one-off install — see the [Setup Guide](setup_guide.md#installation)):

```bash
kindling repo init my-project              # .devcontainer/, CI, .gitignore, root pyproject.toml, packages/, apps/
kindling package init my-domain-app        # packages/my_domain_app/ — entities, pipes, tests, its own pyproject.toml
```

The root `pyproject.toml` is not a package: it is a uv workspace root (`members = ["packages/*"]`) so the whole repo shares one `.venv/` and one `uv.lock`.

The devcontainer image (`ghcr.io/sep/spark-kindling-framework/devcontainer:latest`) ships Python 3.11, Java 21, uv, poe, the Databricks CLI and the Hadoop Azure JARs. It bakes in no Kindling packages and no Azure CLI; PySpark 3.5 and Delta Lake come from the project's own dependencies (the `standalone` extra).

In VS Code:

1. Open the repo root.
2. **Command Palette → "Dev Containers: Reopen in Container"**

Once inside the container the `postCreateCommand` automatically runs `kindling env bootstrap` at the repo root. If the root `pyproject.toml` declares no Kindling dependency yet, it adopts the release your packages pin (or, for an empty repo, pins the latest release), then runs `uv sync --all-packages`. The result is one repo-wide `.venv/` with every package installed editable, including the `dev` group and `.venv/bin/kindling`; VS Code's interpreter is set to `.venv/bin/python`. You don't need to run it manually.

> `kindling` in the image is a shim: it runs `./.venv/bin/kindling` when one exists in the **current directory**, otherwise a system-wide CLI. Run `kindling` commands from the repo root, or use `uv run kindling ...` from a subdirectory, so you get the project's pinned CLI and its packages.

Verify the environment:

```bash
kindling env check --local
```

This validates Java, PySpark, delta-spark, and the Hadoop/Azure JARs. Fix any reported issues before continuing.

To pick up a newer Kindling release inside an existing devcontainer without
rebuilding the container, run this from the repo root (it moves the root's pin
and every package's pin together — all of them must pin the same release):

```bash
kindling env update
```

Generated packages also expose the same workflow as:

```bash
uv run poe update-kindling
```

---

## 2. Create Your `.env` File

Nothing creates `.env` automatically. `kindling package init` and `kindling app init` each write a `.env.example` template; copy it and fill in your values:

```bash
cp packages/my_domain_app/.env.example .env
```

Then edit `.env` and populate at minimum:

```bash
export AZURE_STORAGE_ACCOUNT=<your-storage-account>
export AZURE_CONTAINER=artifacts
export AZURE_BASE_PATH=kindling

# Platform-specific — fill in the section that applies to you:
export FABRIC_WORKSPACE_ID=<your-workspace-id>    # Fabric
export SYNAPSE_WORKSPACE_NAME=<name>              # Synapse
export DATABRICKS_HOST=<host>                     # Databricks
```

Nothing loads `.env` for you — the devcontainer does not source it. Load it into your shell before running commands that need the values:

```bash
set -a; source .env; set +a
```

> `.env` is gitignored — never commit it.

---

## 3. Authenticate with Azure

The devcontainer image does not include the Azure CLI. Either install it (for example, add the `ghcr.io/devcontainers/features/azure-cli:1` feature to `.devcontainer/devcontainer.json`) and log in:

```bash
az login
```

or set the `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` service principal variables in `.env`.

For platform-specific credential checks:

```bash
kindling env check --platform fabric    # or databricks / synapse
```

---

## 4. Initialise the Base Configuration

If the repo does not already have a `settings.yaml`, generate one:

```bash
kindling config init --name my-project
```

This produces a minimal `settings.yaml` in the current directory. Open it and set at minimum:

```yaml
name: my-project
version: "0.1.0"
description: "My domain project"

kindling:
  telemetry:
    logging:
      level: INFO
      print: true
  bootstrap:
    load_lake: true
    load_workspace_packages: false
```

A matching `settings.local.yaml` (gitignored) is the right place for local-only overrides — storage paths, credentials, and log verbosity you do not want committed:

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

---

## 5. Initialise the Platform Workspace

This step deploys your configuration to blob storage and, optionally, imports bootstrap notebooks into the platform workspace. **Run this once per target workspace, then again whenever `settings.yaml` changes significantly.**

```bash
kindling workspace init \
  --platform fabric \
  --storage-account <your-storage-account> \
  --container artifacts \
  --base-path kindling
```

To also generate and import bootstrap notebooks:

```bash
kindling workspace init \
  --platform fabric \
  --storage-account <your-storage-account> \
  --container artifacts \
  --base-path kindling \
  --notebook-bootstrap \
  --workspace <fabric-workspace-id>
```

After this command the following are in place in your storage container:

```
artifacts/kindling/config/settings.yaml
artifacts/kindling/config/settings.fabric.yaml   (if present)
```

To re-deploy config after later changes to `settings.yaml`:

```bash
kindling workspace deploy \
  --platform fabric \
  --storage-account <your-storage-account>
```

---

## 6. Create an App

Each deployable unit is an **app**. Entities and pipes live in the domain package under `packages/`; the app selects which packages to run. Create one under `apps/` (by default it uses the package with the same name; pass `--package` otherwise):

```bash
# Batch processing app with bronze/silver/gold medallion scaffold
kindling app init my-domain-app --pattern batch --layers medallion --repo-root .

# Streaming app
kindling app init my-stream-app --pattern streaming --package my-domain-app --repo-root .

# File ingestion app
kindling app init my-ingest-app --pattern file-ingestion --package my-domain-app --repo-root .
```

Names are normalised to snake case on disk, so this creates:

```
apps/my_domain_app/
  app.yaml                  # App metadata
  app.py                    # Framework entrypoint (pattern-specific)
  settings.yaml             # App-level base config
  settings.local.yaml       # Local overrides (gitignored)
  lake-reqs.txt             # Packages the app loads (and auto-registers)
  .env.example              # Template for .env
  QUICKSTART.md
  tests/
    entities/               # CSV fixtures for local runs

packages/my_domain_app/     # from `kindling package init`
  pyproject.toml            # uv workspace member; poe tasks
  settings.yaml
  settings.local.yaml
  src/
    my_domain_app/
      entities/             # Entity definitions
      pipes/                # Pipe definitions
      transforms/
  tests/
    unit/
    component/
    integration/            # omitted with --no-integration
```

`app.py` needs no import wiring: the packages listed in `lake-reqs.txt` have their entities and pipes registered automatically.

---

## 7. Add Entities

Entities are named, schema-typed data sets — Delta tables, views, or in-memory frames. They are registered with the `DataEntities.entity()` decorator and live in the domain package (e.g. `packages/my_domain_app/src/my_domain_app/entities/`).

### Basic entity

```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from kindling.data_entities import DataEntities

orders_schema = StructType([
    StructField("order_id",   StringType(),    nullable=False),
    StructField("customer_id",StringType(),    nullable=True),
    StructField("status",     StringType(),    nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
])

DataEntities.entity(
    entityid="bronze.orders",
    name="Raw Orders",
    partition_columns=["status"],
    merge_columns=["order_id"],
    tags={"layer": "bronze", "domain": "orders"},
    schema=orders_schema,
)
```

Key parameters:

| Parameter | Required | Notes |
|---|---|---|
| `entityid` | Yes | Dot-separated, e.g. `bronze.orders`, `silver.dim_customer` |
| `name` | Yes | Human-readable label |
| `merge_columns` | Yes (Delta) | Primary key(s) for upsert |
| `partition_columns` | No | Physical partitioning columns |
| `tags` | No | Arbitrary key/value metadata |
| `schema` | Yes | Spark `StructType` |

### SCD Type 2 entity

Add `scd.type: "2"` and `scd.tracked` tags; the framework automatically manages `__effective_from`, `__effective_to`, and `__is_current` columns:

```python
DataEntities.entity(
    entityid="silver.dim_customer",
    name="Customer Dimension",
    merge_columns=["customer_id"],
    tags={
        "layer": "silver",
        "scd.type": "2",
        "scd.tracked": "name,email,region",
    },
    schema=customer_schema,
)
```

### Scaffold an entity from the CLI

```bash
kindling package add entity bronze.orders \
    --package packages/my_domain_app/src/my_domain_app
```

Creates the decorator stub in `entities.py` and a CSV fixture under `tests/entities/bronze/orders.csv`.

---

## 8. Add Pipes

Pipes are transformation functions that read one or more entities and write to an output entity. They are registered with `@DataPipes.pipe()` and must return a PySpark DataFrame.

### Basic pipe

```python
from kindling.data_pipes import DataPipes
from pyspark.sql.functions import col, current_timestamp

@DataPipes.pipe(
    pipeid="bronze_to_silver_orders",
    name="Clean Orders",
    tags={"category": "cleaning", "layer": "silver"},
    input_entity_ids=["bronze.orders"],
    output_entity_id="silver.orders_clean",
    output_type="table",
)
def bronze_to_silver_orders(bronze_orders):
    return (
        bronze_orders
        .filter(col("order_id").isNotNull())
        .dropDuplicates(["order_id"])
        .withColumn("processed_at", current_timestamp())
    )
```

**Input parameter naming**: replace `.` with `_`. Entity `bronze.orders` → parameter `bronze_orders`.

### Multi-input pipe

```python
@DataPipes.pipe(
    pipeid="orders_with_customers",
    name="Orders with Customer Details",
    tags={"layer": "gold"},
    input_entity_ids=["silver.orders_clean", "silver.dim_customer"],
    output_entity_id="gold.orders_enriched",
    output_type="table",
)
def orders_with_customers(silver_orders_clean, silver_dim_customer):
    return silver_orders_clean.join(
        silver_dim_customer.filter(col("__is_current") == True),
        on="customer_id",
        how="left",
    )
```

### Scaffold a pipe from the CLI

```bash
# Single-input pipe
kindling package add pipe bronze_to_silver_orders \
    --inputs bronze.orders \
    --package packages/my_domain_app/src/my_domain_app

# File ingestion pipe
kindling package add ingestion bronze.sales_csv \
    --source-pattern 'sales_(?P<report_date>[^.]+)[.]csv' \
    --package packages/my_domain_app/src/my_domain_app
```

Each scaffold creates the pipe stub, a unit test stub, and CSV fixture stubs for all inputs.

---

## 9. Run a Single Pipe Locally

Before running the full app, smoke-test individual pipes:

```bash
# List all registered pipes for the app
kindling pipeline list --app apps/my_domain_app/app.py --env local

# Run a specific pipe
kindling pipeline run bronze_to_silver_orders \
    --app apps/my_domain_app/app.py \
    --env local
```

The Spark UI is available at `http://localhost:4040` while a job is running.

To reprocess the full dataset ignoring watermarks:

```bash
kindling pipeline run bronze_to_silver_orders \
    --app apps/my_domain_app/app.py \
    --env local \
    --no-watermark
```

---

## 10. Validate the App

Check that all entities and pipes are correctly registered and that their configurations are internally consistent — without starting a Spark session:

```bash
cd apps/my_domain_app
uv run kindling app validate --env local
```

Fix any reported issues before continuing.

---

## 11. Run Tests

Tests live in the package and run through its poe tasks:

```bash
cd packages/my_domain_app
uv run poe test               # unit + component
uv run poe test-unit          # unit tests only
uv run poe test-integration   # integration tests (if generated)
uv run poe build              # wheel lands in the repo-root dist/
```

`uv run` syncs what the package needs before running. Don't run a bare `uv sync` inside a package directory: in the workspace it is an exact sync of that one package and removes the others from the shared `.venv/`. Resync the whole repo with `uv sync --all-packages` (or `kindling env bootstrap`) at the root.

---

## 12. Run the Full App Locally

```bash
cd apps/my_domain_app
uv run kindling app run . --env local
```

Or from repo root:

```bash
kindling app run my-domain-app --env local --local-folder apps/my_domain_app
```

Pass runtime parameters:

```bash
uv run kindling app run . --env local --param report_date=2024-01-15
# or from a file
uv run kindling app run . --env local --parameters params.yaml
```

---

## 13. Package and Deploy

### Package into a `.kda` archive

```bash
kindling app package my-domain-app \
    --local-folder apps/my_domain_app \
    --output dist/my-domain-app.kda
```

### Deploy to the platform

```bash
kindling app deploy my-domain-app \
    --local-folder apps/my_domain_app \
    --platform fabric
```

### Run remotely

```bash
kindling app run my-domain-app \
    --platform fabric \
    --env prod
```

Monitor a running job:

```bash
kindling app logs <run_id> --platform fabric
kindling app status <run_id> --platform fabric
```

Cancel if needed:

```bash
kindling app cancel <run_id> --platform fabric
```

---

## 14. Schema Migrations

The migration system detects schema drift between your registered entity definitions and the live Delta tables and applies changes safely.

```bash
# Inspect pending changes (no writes)
kindling migrate plan --app apps/my_domain_app/app.py --env local

# Apply non-destructive changes (column additions, view updates, cluster changes)
kindling migrate apply --app apps/my_domain_app/app.py --env local

# Apply destructive changes too (column removals, type changes, partition changes)
kindling migrate apply --app apps/my_domain_app/app.py --env prod \
    --destructive --backup snapshot

# After a successful blue-green migration, drop the archived table
kindling migrate cleanup silver.dim_customer --app apps/my_domain_app/app.py

# If something went wrong, restore the pre-migration table
kindling migrate rollback silver.dim_customer --app apps/my_domain_app/app.py
```

`--app` auto-discovers `app.py` when omitted (same as `kindling pipeline run`). These commands start a local Spark session, so they are the right tool for pre-deployment schema management and CI/CD pipelines — run `plan` in a PR check, `apply` in the deploy step, then run the app.

**Destructive change strategies:**

| Change type | `--destructive` required | Strategy |
|---|---|---|
| Column addition | No | `ALTER TABLE ADD COLUMNS` |
| Cluster change | No | `ALTER TABLE CLUSTER BY` |
| View create/update | No | `CREATE OR REPLACE VIEW` |
| Column removal | Yes | Full table rewrite |
| Type change | Yes | Full table rewrite; auto-cast for safe widenings (e.g. `int → bigint`) |
| Partition change | Yes | Full table rewrite |

For CATALOG mode entities, destructive rewrites use a **blue-green strategy**: the old table is archived as `<name>_migration_blue` until you run `cleanup`. For STORAGE mode entities they use an **in-place Delta overwrite**.

---

## Command Order Summary

```
# 0 — New repo only
kindling repo init my-project
kindling package init my-domain-app

# 1 — One-time environment setup (the devcontainer runs `kindling env bootstrap` automatically)
az login                                   # or service principal vars in .env
kindling env check --local --platform fabric

# 2 — One-time workspace setup (or after settings.yaml changes)
kindling config init --name my-project     # if no settings.yaml yet
kindling workspace init --platform fabric --storage-account <acct>

# 3 — Create an app (once per app)
kindling app init my-domain-app --pattern batch --layers medallion --repo-root .

# 4 — Develop entities and pipes (iterative)
kindling package add entity bronze.orders --package packages/my_domain_app/src/my_domain_app
kindling package add pipe bronze_to_silver_orders --inputs bronze.orders --package packages/my_domain_app/src/my_domain_app

# 5 — Validate and test (iterative)
kindling app validate --app apps/my_domain_app/app.py --env local
kindling pipeline run bronze_to_silver_orders --app apps/my_domain_app/app.py --env local
(cd packages/my_domain_app && uv run poe test)

# 6 — Run full app locally
kindling app run my-domain-app --env local --local-folder apps/my_domain_app

# 7 — Ship
kindling app package my-domain-app --local-folder apps/my_domain_app --output dist/my-domain-app.kda
kindling app deploy my-domain-app --local-folder apps/my_domain_app --platform fabric
kindling app run my-domain-app --platform fabric --env prod
```

---

## Troubleshooting

| Symptom | Check |
|---|---|
| `kindling env check` reports missing JARs | Run `kindling env ensure --cloud azure` (downloads into `/tmp/hadoop-jars`; without `--cloud` it needs `az` on PATH to detect Azure). The devcontainer image ships them at `/opt/hadoop-jars`, symlinked to `/tmp/hadoop-jars` |
| Spark session fails to start | Java 11+ must be active (the devcontainer ships Java 21): `java -version`; set `JAVA_HOME` if wrong |
| `kindling` or `import kindling` not found | Run `kindling env bootstrap` at the repo root to recreate `.venv/`; run `kindling` from the repo root or via `uv run kindling` |
| `uv sync` fails with conflicting URLs for `spark-kindling` | A package pins a different Kindling release than the root; run `kindling env update` at the repo root |
| `entity not found` at runtime | Ensure the package defining the entity is listed in the app's `lake-reqs.txt` and installed (`uv sync --all-packages` at the repo root) |
| Merge fails with schema mismatch | Run `kindling migrate plan` to inspect pending schema changes |
| Remote deploy fails with auth error | Re-run `az login`; check `kindling env check --platform <platform>` |
| Watermark prevents full reload | Pass `--no-watermark` to `kindling pipeline run` |
