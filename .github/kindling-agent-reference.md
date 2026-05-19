# Kindling Framework — Developer Reference

<!-- VERSION: 0.9.26 -->
<!-- Generated into devcontainer at /opt/kindling/agent-reference.md -->
<!--
  SECTION MARKERS are used by `kindling agent setup` to extract condensed
  versions for tools with size limits (e.g. GitHub Copilot).
  Keep each section self-contained.
-->

<!-- SECTION: overview -->
## Mental Model

Kindling is a PySpark data pipeline framework. The three core primitives are:

- **Entities** — named Delta tables (or views, streams, in-memory stores). Declared once with a decorator; the framework manages schema, storage, and merge.
- **Pipes** — pure transform functions. Read one or more input entities, return a DataFrame, the framework persists it to the output entity.
- **Signal handlers** — hooks that fire at pipeline execution points. Can inspect or transform DataFrames before/after reads and writes.

Everything is wired through dependency injection. Call `initialize()` first, then import entity/pipe modules (decorators fire and register with the DI container). Never import entity/pipe modules before `initialize()`.

<!-- END SECTION -->

<!-- SECTION: entities -->
## Entities

### Declaring an entity

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from kindling.data_entities import DataEntities

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True),
])

@DataEntities.entity(
    entityid="bronze.orders",        # dot-separated: layer.name
    name="bronze_orders",            # snake_case display name
    merge_columns=["order_id"],      # business key for upsert
    partition_columns=["order_date"],# optional Delta partitioning
    cluster_columns=[],              # optional liquid clustering
    tags={
        "provider_type": "delta",    # storage backend
        "layer": "bronze",
    },
    schema=orders_schema,
)
class BronzeOrders:
    pass
```

All fields except `partition_columns`, `cluster_columns` are required.
`tags={}` is valid — just pass an empty dict.

### SQL view entity

```python
@DataEntities.sql_entity(
    entityid="reporting.recent_sales",
    name="recent_sales",
    sql="SELECT * FROM sales.transactions WHERE event_date >= current_date() - 30",
)
# or from a package resource file:
@DataEntities.sql_entity(
    entityid="reporting.recent_sales",
    name="recent_sales",
    sql_source=SqlSource(resource="my_app:sql/recent_sales.sql"),
)
```

SQL entities are read-only. `provider_type: "view"` is set automatically.

### Referencing entity IDs

Prefer `DataEntities.ids.*` over string literals — IDE completion, rename-safe:

```python
DataEntities.ids.bronze_orders   # == "bronze.orders"
DataEntities.ids.silver_orders   # == "silver.orders"
```

Attribute name = entityid with `.` replaced by `_`.

### Provider types

| `provider_type` | Description |
|---|---|
| `delta` | Delta Lake table (default, read/write/merge/stream) |
| `memory` | In-process store, ephemeral (testing, local dev) |
| `csv` | Read-only CSV fixture |
| `view` | Spark catalog view (set by `sql_entity`, read-only) |
| `eventhub` | Azure Event Hub streaming source |

### Common provider tags

```python
tags={
    "provider_type": "delta",
    "provider.path": "abfss://container@account.dfs.core.windows.net/bronze/orders",
    # Secrets resolved at runtime:
    "provider.path": "@secret:BRONZE_ORDERS_PATH",
}
```

### Read-only entities (external tables)

```python
tags={
    "provider_type": "delta",
    "provider.path": "abfss://curated@storage.dfs.core.windows.net/ref/data",
    "read_only": "true",                       # block writes, register not create
    "read_only.on_path_conflict": "error",     # or "reregister"
}
```

### SCD Type 2

```python
tags={
    "provider_type": "delta",
    "scd.type": "2",
    "scd.tracked": "price,status",             # columns to version
    "scd.routing_key": "hash",                 # or "concat"
    "scd.close_on_missing": "false",           # close record when source disappears
    "scd.optimize_unchanged": "false",         # skip merge if no tracked col changed
}
```

### Config-based tag overrides (settings.yaml)

```yaml
entity_tags:
  bronze.orders:
    provider_type: memory          # override for local dev
  silver.orders:
    provider.path: "@secret:SILVER_ORDERS_PATH"
```

<!-- END SECTION -->

<!-- SECTION: pipes -->
## Pipes

### Declaring a pipe

```python
from kindling.data_pipes import DataPipes
from kindling.data_entities import DataEntities

@DataPipes.pipe(
    pipeid="bronze_to_silver_orders",
    name="Bronze to Silver Orders",
    tags={"layer": "silver"},
    input_entity_ids=[DataEntities.ids.bronze_orders],   # list, first drives watermark
    output_entity_id=DataEntities.ids.silver_orders,
    output_type="table",
    use_watermark=False,   # True = track version, only process new rows next run
)
def bronze_to_silver_orders(bronze_orders):
    # Parameter name = input entityid with dots→underscores
    return (
        bronze_orders
        .filter(col("amount") > 0)
        .withColumn("processed_at", current_timestamp())
    )
```

### Multi-input pipes

```python
@DataPipes.pipe(
    pipeid="enrich_orders",
    name="Enrich Orders",
    tags={},
    input_entity_ids=[DataEntities.ids.silver_orders, DataEntities.ids.ref_customers],
    output_entity_id=DataEntities.ids.gold_orders,
    output_type="table",
)
def enrich_orders(silver_orders, ref_customers):
    # First input (silver_orders) uses watermark if use_watermark=True
    # Subsequent inputs (ref_customers) always read full dataset
    return silver_orders.join(ref_customers, "customer_id", "left")
```

### Referencing pipe IDs

```python
DataPipes.ids.bronze_to_silver_orders   # == "bronze_to_silver_orders"
```

### Watermarking

When `use_watermark=True`, after each successful run the framework saves the
current Delta version of the first input entity. Next run reads only rows added
since that version. Watermarks are stored in a system entity (default: `system.watermarks`).

<!-- END SECTION -->

<!-- SECTION: signals -->
## Signal Handlers

Handlers hook into pipeline execution points. Sync handlers can transform
DataFrames; async handlers are fire-and-forget.

```python
from kindling.signaling import DataSignals
from pyspark.sql.functions import current_timestamp, col

# Validate before write — raise to abort the pipe
@DataSignals.handler("persist.before_persist", priority=10)
def validate_no_nulls(sender, df, pipe_id, **kwargs):
    if df.filter(col("order_id").isNull()).count() > 0:
        raise ValueError(f"Null order_ids in {pipe_id}")

# Transform before write — return new df to replace
@DataSignals.handler("persist.before_persist", priority=20)
def add_audit_columns(sender, df, **kwargs):
    return df.withColumn("_loaded_at", current_timestamp())

# Scope to a specific pipe
@DataSignals.handler("persist.before_persist", pipe_id="bronze_to_silver_orders")
def validate_amounts(sender, df, **kwargs):
    if df.filter(col("amount") < 0).count() > 0:
        raise ValueError("negative amounts")

# Fire-and-forget audit log — doesn't block pipeline
@DataSignals.handler("persist.after_persist", mode="async", on_error="log")
def log_row_count(sender, df, pipe_id, **kwargs):
    print(f"[audit] {pipe_id}: {df.count()} rows written")

# Intercept read — transform data before it reaches the pipe function
@DataSignals.handler("read.after_read", priority=10)
def mask_pii(sender, df, entity_id, **kwargs):
    if "email" in df.columns:
        return df.withColumn("email", lit("***"))
```

### Handler parameters

| Parameter | Type | Default | Effect |
|---|---|---|---|
| `signal_name` | str | required | Signal to subscribe to |
| `mode` | `"sync"` \| `"async"` | `"sync"` | sync blocks pipeline, async is fire-and-forget |
| `priority` | int | 50 | Lower runs first (sync only) |
| `on_error` | `"raise"` \| `"log"` | `"raise"` | What to do on exception (sync only; async always logs) |
| `pipe_id` | str \| None | None | Scope to one pipe; None = all pipes |

### Available signals

| Signal | When | Carries `df`? |
|---|---|---|
| `read.after_read` | After entity read, before pipe function | yes |
| `persist.before_persist` | After pipe function, before write | yes |
| `persist.after_persist` | After write completes | yes |
| `persist.watermark_saved` | After watermark updated | no |
| `persist.persist_failed` | On write error | no |
| `datapipes.before_run` | Before all pipes in a run | no |
| `datapipes.after_run` | After all pipes complete | no |
| `datapipes.before_pipe` | Before each pipe | no |
| `datapipes.after_pipe` | After each pipe | no |
| `datapipes.pipe_failed` | On pipe error | no |
| `datapipes.pipe_skipped` | When pipe has no new data | no |
| `stage.before_execute` | Before stage runs | no |
| `stage.after_execute` | After stage completes | no |

<!-- END SECTION -->

<!-- SECTION: config -->
## Configuration

### settings.yaml

```yaml
name: my-kindling-app
version: "0.1.0"
description: "Kindling data app"

kindling:
  telemetry:
    logging:
      level: INFO          # DEBUG, INFO, WARNING, ERROR
      print: true
    tracing:
      print: false

  bootstrap:
    load_lake: true        # load packages from storage
    load_local: false      # load packages from local filesystem

  delta:
    access_mode: storage   # "storage" (path-based) or "catalog" (Unity Catalog)
    optimize_write: false

  spark_configs: {}        # extra Spark conf key/values
  required_packages: []    # extra Python packages to install at bootstrap
  extensions: []           # Kindling extension packages

  # secrets:
  #   secret_scope: "my-scope"              # Synapse/Databricks secret scope
  #   key_vault_url: "https://..."          # Azure Key Vault URL
```

### Environment overlays

```
config/
  settings.yaml        # base config
  env.local.yaml       # local dev overrides
  env.dev.yaml         # dev environment
  env.prod.yaml        # production
```

Active overlay selected by `KINDLING_ENV` env var (default: `local`).

```yaml
# env.local.yaml — override storage paths + use memory providers for local dev
entity_tags:
  bronze.orders:
    provider_type: memory
  silver.orders:
    provider_type: memory
```

### Secret references

Any string value in config can reference a secret:

```yaml
entity_tags:
  bronze.orders:
    provider.path: "@secret:BRONZE_ORDERS_PATH"   # resolves from env var
```

At runtime Kindling resolves `@secret:NAME` from the configured secret provider
(Key Vault, Databricks secret scope, Synapse linked service) or falls back to
the environment variable `NAME`.

<!-- END SECTION -->

<!-- SECTION: app-structure -->
## App Structure

```
my-app/
  app.py                # initialize() and register_all()
  config/
    settings.yaml
    env.local.yaml
  src/
    my_app/
      entities/
        bronze.py       # @DataEntities.entity declarations
        silver.py
      pipes/
        bronze_to_silver.py   # @DataPipes.pipe declarations
      handlers/
        validation.py   # @DataSignals.handler declarations
  tests/
    entities/
      bronze/
        orders.csv      # fixture CSVs for local dev
```

### app.py pattern

```python
from pathlib import Path
import os

_APP_DIR = Path(__file__).parent

def _config_files(env: str) -> list[str]:
    config_dir = _APP_DIR / "config"
    files = [str(config_dir / "settings.yaml")]
    env_file = config_dir / f"env.{env}.yaml"
    if env_file.exists():
        files.append(str(env_file))
    return files

def register_all() -> None:
    """Import all modules — fires decorators, must run AFTER initialize()."""
    import my_app.entities.bronze   # noqa: F401
    import my_app.entities.silver   # noqa: F401
    import my_app.pipes.bronze_to_silver  # noqa: F401
    import my_app.handlers.validation     # noqa: F401

def initialize(env: str | None = None):
    from kindling.bootstrap import initialize_framework
    env = env or os.environ.get("KINDLING_ENV", "local")
    svc = initialize_framework({
        "platform": "standalone",
        "environment": env,
        "config_files": _config_files(env),
    })
    register_all()
    return svc
```

<!-- END SECTION -->

<!-- SECTION: local-dev -->
## Local Development

### Fixture CSV discovery

Place CSV files under `tests/entities/` matching the entity ID path:

```
tests/entities/
  bronze/
    orders.csv        # → entity "bronze.orders"
  silver/
    orders.csv        # → entity "silver.orders"
  ref_customers.csv   # → entity "ref_customers"
```

The framework auto-discovers and prioritises these over the registered provider
when running locally (standalone platform). No config change needed.

### Memory provider with seed data

```python
@DataEntities.entity(
    entityid="bronze.orders",
    name="bronze_orders",
    merge_columns=["order_id"],
    partition_columns=[],
    tags={
        "provider_type": "memory",
        "provider.seed.rows": [
            {"order_id": "001", "amount": 100},
            {"order_id": "002", "amount": 200},
        ],
    },
    schema=orders_schema,
)
class BronzeOrders:
    pass
```

Seed rows are materialised on first read. Requires a schema.
Imperative writes and fixture CSVs take precedence over seed rows.

### env.local.yaml for local dev

```yaml
# config/env.local.yaml
entity_tags:
  bronze.orders:
    provider_type: memory
  silver.orders:
    provider_type: memory
kindling:
  telemetry:
    logging:
      level: DEBUG
      print: true
```

### Running locally

```bash
kindling pipeline run bronze_to_silver_orders --app app.py --env local
kindling app run . --platform standalone
```

<!-- END SECTION -->

<!-- SECTION: cli -->
## CLI Quick Reference

```bash
# Version info
kindling --version           # shows cli / sdk / runtime versions

# Environment checks
kindling env check           # verify Python env, config, credentials
kindling workspace check     # verify platform env vars are set

# Config
kindling config init         # generate settings.yaml scaffold

# Pipeline (local)
kindling pipeline run <pipe_id> --app app.py
kindling pipeline run <pipe_id> --app app.py --no-watermark
kindling pipeline run <pipe_id> --app app.py --env prod

# Deploy to platform
kindling workspace deploy --platform synapse
kindling workspace deploy --platform databricks --skip-wheels

# App run (remote)
kindling app run . --platform synapse          # deploy + run
kindling app run my-app --platform databricks  # run deployed app
kindling app run . --platform synapse --no-logs --no-wait

# Runner management
kindling runner ensure --platform synapse
kindling runner status --platform databricks

# Migrations
kindling migrate plan --app app.py
kindling migrate apply --app app.py
```

<!-- END SECTION -->

<!-- SECTION: testing -->
## Testing Patterns

### Unit test with memory provider

```python
from unittest.mock import MagicMock
from kindling.data_entities import DataEntities
from kindling.data_pipes import DataPipes

def test_bronze_to_silver(spark):
    DataEntities.reset()
    DataPipes.reset()

    mock_registry = MagicMock()
    DataEntities.deregistry = mock_registry
    DataPipes.dpregistry = mock_registry

    # Register entities and pipe...
    # Run pipe function directly (no framework needed)
    result = bronze_to_silver_orders(sample_df)
    assert result.count() == expected_count
```

### Component test (full pipeline, memory providers)

```python
def test_pipeline(initialize_app):
    # initialize_app fixture calls app.initialize()
    # entity_tags in env.local.yaml set provider_type=memory
    # Place fixture CSVs in tests/entities/ for input data

    from kindling.injection import GlobalInjector
    from kindling.data_entities import DataEntityRegistry
    from kindling.simple_stage_processor import execute_process_stage

    execute_process_stage("bronze", "Bronze Stage", {}, "bronze")

    registry = GlobalInjector.get(DataEntityRegistry)
    result = registry.get_entity_definition("silver.orders")
    # read and assert...
```

### Signal handler testing

```python
from kindling.signaling import DataSignals

def test_handler_transforms_df(spark):
    DataSignals.reset()

    captured = []

    @DataSignals.handler("persist.before_persist")
    def capture(sender, df, **kwargs):
        captured.append(df)

    # trigger pipeline...
    assert len(captured) == 1
```

<!-- END SECTION -->
