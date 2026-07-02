# Domain Project Quickstart

This guide walks a **domain project developer** through standing up a local dev environment, initialising a platform workspace, and building a domain project end-to-end — entities, pipes, and a data app — ready for local testing and remote deployment.

> **Looking to contribute to the kindling framework itself?** See [developer_workflow.md](developer_workflow.md) instead.

> **Assumptions**
> - You have a dev repo already cloned (or are starting a new one).
> - You have a target platform workspace (Microsoft Fabric, Azure Synapse Analytics, or Databricks).
> - You are using the supplied devcontainer (recommended).

---

## 1. Open the Dev Container

The devcontainer ships with Python 3.11, Java 11, PySpark 3.4, Azure CLI, and all tooling pre-installed.

In VS Code:

1. Open the repo root.
2. **Command Palette → "Dev Containers: Reopen in Container"**

Once inside the container the `postCreateCommand` automatically runs `poetry install --with dev --sync` and installs pre-commit hooks. You don't need to run those manually.

Verify the environment:

```bash
kindling env check --local
```

This validates Python version, PySpark, delta-spark, and the Hadoop/Azure JARs. Fix any reported issues before continuing.

To pick up a newer Kindling release inside an existing devcontainer without
rebuilding the container, run this from a package directory:

```bash
kindling env update
```

Generated packages also expose the same workflow as:

```bash
poetry run poe update-kindling
```

---

## 2. Create Your `.env` File

Nothing creates this automatically. Copy the template and fill in your values:

```bash
cp .env.template .env
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

The devcontainer's `postCreateCommand` adds a `.bashrc` hook that auto-sources `.env` in every new terminal. If you see `⚠️  No .env file found` in the terminal on container start, this step was skipped.

> `.env` is gitignored — never commit it.

---

## 3. Authenticate with Azure

```bash
az login
```

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

Each deployable unit is an **app**. Create one under `apps/`:

```bash
# Batch processing app with bronze/silver/gold medallion scaffold
kindling app init my-domain-app --pattern batch --layers medallion --repo-root .

# Streaming app
kindling app init my-stream-app --pattern streaming --repo-root .

# File ingestion app
kindling app init my-ingest-app --pattern file-ingestion --repo-root .
```

This creates:

```
apps/my-domain-app/
  app.yaml                  # App metadata
  app.py                    # Framework entrypoint (calls initialize())
  settings.yaml             # App-level base config
  settings.local.yaml       # Local overrides (gitignored)
  lake-reqs.txt             # Remote package requirements
  src/
    my_domain_app/
      entities.py           # Entity definitions
      pipes/
        bronze.py           # Bronze layer pipes
        silver.py           # Silver layer pipes
  tests/
    entities/               # CSV fixtures for local testing
    unit/                   # Unit tests
    integration/            # Integration tests
```

`app.py` wires the app to the framework:

```python
def initialize(env: str = None, config_dir: Path = None):
    from my_domain_app import entities
    from my_domain_app.pipes import bronze, silver
```

---

## 7. Add Entities

Entities are named, schema-typed data sets — Delta tables, views, or in-memory frames. They are registered with the `@DataEntities.entity()` decorator and live in `entities.py` (or any module imported by `app.py`).

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

@DataEntities.entity(
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
@DataEntities.entity(
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
    --package apps/my-domain-app/src/my_domain_app
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
    --package apps/my-domain-app/src/my_domain_app

# File ingestion pipe
kindling package add ingestion bronze.sales_csv \
    --source-pattern 'sales_(?P<report_date>[^.]+)[.]csv' \
    --package apps/my-domain-app/src/my_domain_app
```

Each scaffold creates the pipe stub, a unit test stub, and CSV fixture stubs for all inputs.

---

## 9. Run a Single Pipe Locally

Before running the full app, smoke-test individual pipes:

```bash
# List all registered pipes for the app
kindling pipeline list --app apps/my-domain-app/app.py --env local

# Run a specific pipe
kindling pipeline run bronze_to_silver_orders \
    --app apps/my-domain-app/app.py \
    --env local
```

The Spark UI is available at `http://localhost:4040` while a job is running.

To reprocess the full dataset ignoring watermarks:

```bash
kindling pipeline run bronze_to_silver_orders \
    --app apps/my-domain-app/app.py \
    --env local \
    --no-watermark
```

---

## 10. Validate the App

Check that all entities and pipes are correctly registered and that their configurations are internally consistent — without starting a Spark session:

```bash
cd apps/my-domain-app
kindling app validate --env local
```

Fix any reported issues before continuing.

---

## 11. Run Tests

```bash
# All tests
pytest apps/my-domain-app/tests/ -v

# Unit tests only (fast, no Spark)
pytest apps/my-domain-app/tests/unit/ -v

# Integration tests (local Spark, uses CSV fixtures)
pytest apps/my-domain-app/tests/integration/ -v
```

---

## 12. Run the Full App Locally

```bash
cd apps/my-domain-app
kindling app run . --env local
```

Or from repo root:

```bash
kindling app run my-domain-app --env local --local-folder apps/my-domain-app
```

Pass runtime parameters:

```bash
kindling app run . --env local --param report_date=2024-01-15
# or from a file
kindling app run . --env local --parameters params.yaml
```

---

## 13. Package and Deploy

### Package into a `.kda` archive

```bash
kindling app package my-domain-app \
    --local-folder apps/my-domain-app \
    --output dist/my-domain-app.kda
```

### Deploy to the platform

```bash
kindling app deploy my-domain-app \
    --local-folder apps/my-domain-app \
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
kindling migrate plan --app apps/my-domain-app/app.py --env local

# Apply non-destructive changes (column additions, view updates, cluster changes)
kindling migrate apply --app apps/my-domain-app/app.py --env local

# Apply destructive changes too (column removals, type changes, partition changes)
kindling migrate apply --app apps/my-domain-app/app.py --env prod \
    --destructive --backup snapshot

# After a successful blue-green migration, drop the archived table
kindling migrate cleanup silver.dim_customer --app apps/my-domain-app/app.py

# If something went wrong, restore the pre-migration table
kindling migrate rollback silver.dim_customer --app apps/my-domain-app/app.py
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
# 1 — One-time environment setup (devcontainer handles poetry install automatically)
az login
kindling env check --local --platform fabric

# 2 — One-time workspace setup (or after settings.yaml changes)
kindling config init --name my-project     # if no settings.yaml yet
kindling workspace init --platform fabric --storage-account <acct>

# 3 — Create an app (once per app)
kindling app init my-domain-app --pattern batch --layers medallion --repo-root .

# 4 — Develop entities and pipes (iterative)
kindling package add entity bronze.orders --package apps/my-domain-app/src/my_domain_app
kindling package add pipe bronze_to_silver_orders --inputs bronze.orders --package apps/my-domain-app/src/my_domain_app

# 5 — Validate and test (iterative)
kindling app validate --app apps/my-domain-app/app.py --env local
kindling pipeline run bronze_to_silver_orders --app apps/my-domain-app/app.py --env local
pytest apps/my-domain-app/tests/ -v

# 6 — Run full app locally
kindling app run my-domain-app --env local --local-folder apps/my-domain-app

# 7 — Ship
kindling app package my-domain-app --local-folder apps/my-domain-app --output dist/my-domain-app.kda
kindling app deploy my-domain-app --local-folder apps/my-domain-app --platform fabric
kindling app run my-domain-app --platform fabric --env prod
```

---

## Troubleshooting

| Symptom | Check |
|---|---|
| `kindling env check` reports missing JARs | Re-run `poetry install`; the devcontainer post-create script should have fetched them |
| Spark session fails to start | Java 11 must be active: `java -version`; set `JAVA_HOME` if wrong |
| `entity not found` at runtime | Ensure the module defining the entity is imported in `app.py::initialize()` |
| Merge fails with schema mismatch | Run `kindling migrate plan` to inspect pending schema changes |
| Remote deploy fails with auth error | Re-run `az login`; check `kindling env check --platform <platform>` |
| Watermark prevents full reload | Pass `--no-watermark` to `kindling pipeline run` |
