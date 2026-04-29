# Dynamic Entity & Pipe Registration

## When to use this pattern

Static `@DataPipes.pipe` decorators work well when the set of pipes is known at author time. Dynamic registration is the right choice when:

- A **definition table** (or config file, API call, etc.) enumerates what entities/pipes exist at runtime
- You have a set of tables with **identical processing logic** (e.g., staging every table from a source system the same way)
- The list of targets changes over time and you don't want to modify code to add each one

---

## How registration works

`DataEntities.entity()` and `@DataPipes.pipe()` are both thin wrappers that delegate to singletons managed by `GlobalInjector`:

```python
# These two are equivalent:
DataEntities.entity(entityid="bronze.orders", ...)     # calls DataEntityManager.register_entity()
DataPipes.pipe(pipeid="load_orders", ...)(my_func)     # calls DataPipesManager.register_pipe()
```

Because the managers are singletons, anything registered before `run_datapipes` is picked up — whether registered via decorator or a programmatic call in a loop.

The `execute` function for a pipe receives inputs as keyword arguments. The key name is the entity ID with `.` replaced by `_`:

```
input_entity_id "bronze.orders"    →  kwarg name  bronze_orders
input_entity_id "raw.crm_account"  →  kwarg name  raw_crm_account
```

---

## End-to-end example: staging from persisted table definitions

A common pattern is to store table definitions in a Delta table (e.g. `config.staging_tables`) with columns like `table_name`, `primary_key`, and `source_schema`. The notebook reads those rows at startup and uses them to drive registration before execution begins.

### Step 1 — read the definition table

```python
from kindling.bootstrap import get_or_create_spark_session

spark = get_or_create_spark_session()

table_definitions = (
    spark.table("config.staging_tables")
    .collect()
)
```

`.collect()` is safe here — definition tables are small. This runs before any registration, so it doesn't interact with the pipe execution system.

### Step 2 — register entities in a loop

```python
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from kindling.data_entities import DataEntities


def make_raw_schema(primary_key: str) -> StructType:
    return StructType([
        StructField(primary_key, StringType(), False),
        StructField("payload", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
    ])


for row in table_definitions:
    table = row["table_name"]        # e.g. "customer"
    pk    = row["primary_key"]       # e.g. "customer_id"

    # Raw source entity
    DataEntities.entity(
        entityid=f"raw.{table}",
        name=f"raw_{table}",
        merge_columns=[pk],
        tags={"layer": "raw", "source_schema": row["source_schema"]},
        schema=make_raw_schema(pk),
    )

    # Staging target entity
    DataEntities.entity(
        entityid=f"staging.{table}",
        name=f"staging_{table}",
        merge_columns=[pk],
        partition_columns=["ingested_date"],
        tags={"layer": "staging", "source_schema": row["source_schema"]},
        schema=make_raw_schema(pk),
    )
```

### Step 3 — register pipes in a loop

The execute function must accept kwargs matching the input entity IDs (`.` → `_`). Because Python closures capture variables by reference rather than value, each pipe's execute function must be created via a factory — otherwise every pipe in the loop would close over the *last* values of `table` and `pk`.

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from kindling.data_pipes import DataPipes


def make_staging_execute(primary_key: str, input_kwarg: str):
    """Returns an execute function closed over the PK and kwarg name for this table."""

    def execute(**kwargs):
        raw_df: DataFrame = kwargs[input_kwarg]
        return (
            raw_df
            .withColumn("ingested_date", F.to_date("ingested_at"))
            .dropDuplicates([primary_key])
        )

    return execute


for row in table_definitions:
    table = row["table_name"]
    pk    = row["primary_key"]

    # Entity ID "raw.{table}" maps to kwarg "raw_{table}" inside execute.
    input_kwarg = f"raw_{table}"

    DataPipes.pipe(
        pipeid=f"stage_{table}",
        name=f"Stage {table}",
        tags={"layer": "staging", "domain": row["source_schema"]},
        input_entity_ids=[f"raw.{table}"],
        output_entity_id=f"staging.{table}",
        output_type="table",
    )(make_staging_execute(pk, input_kwarg))
```

### Step 4 — execute

```python
from kindling.injection import GlobalInjector
from kindling.data_pipes import DataPipesExecution

pipe_ids = [f"stage_{row['table_name']}" for row in table_definitions]

executer = GlobalInjector.get(DataPipesExecution)
executer.run_datapipes(pipe_ids)
```

Or using the DAG executor if ordering or parallelism matters:

```python
executer.run_datapipes_dag(pipe_ids, parallel=True, max_workers=4)
```

---

## `DataEntities.entity()` vs `DataEntityManager.register_entity()` directly

Both paths end up in the same singleton. Use `DataEntities.entity()` (the public API) in normal application code — it validates required fields before forwarding to the manager. Use `register_entity()` directly only in tests where you're constructing `DataEntityManager` yourself without the full framework bootstrap.

---

## Common pitfalls

| Pitfall | Fix |
|---|---|
| Pipe executes with wrong `table` or `pk` at runtime | Use a factory function (`make_staging_execute(pk, kwarg)`) to freeze loop values — see Step 3 |
| Kwarg name doesn't match input entity ID | Entity ID `"raw.customer"` → kwarg `raw_customer` (`.` → `_`). Must match exactly. |
| `DataEntities.deregistry` is `None` at call time | The framework must be initialized (`get_or_create_spark_session` or `initialize_framework`) before calling `DataEntities.entity()` |
| Registering the same `entityid` or `pipeid` twice | The manager silently overwrites. Make sure loop values produce unique IDs. |
| Reading the definition table after pipes start running | Read definition data before any `run_datapipes` call, at notebook init time. |
