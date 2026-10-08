# Data pipes

How to define, derive, test and run Kindling data pipes in a domain project.
Entities a pipe reads and writes are declared as in [entities.md](entities.md);
app wiring is in [apps.md](apps.md); `datapipes:` overlays and secrets are in
[config.md](config.md).

- [Where pipe code goes](#where-pipe-code-goes)
- [`@DataPipes.pipe` contract](#datapipespipe-contract)
- [Transforms vs. registered pipes](#transforms-vs-registered-pipes)
- [Running a pipe](#running-a-pipe)
- [Incremental (watermarked) pipes](#incremental-watermarked-pipes)
- [How output is written: append, SCD1 merge, SCD2](#how-output-is-written-append-scd1-merge-scd2)
- [Streaming pipes](#streaming-pipes)
- [Clone, extend and the YAML form](#clone-extend-and-the-yaml-form)
- [File ingestion](#file-ingestion)
- [CLI](#cli)
- [Testing](#testing)
- [Gotchas](#gotchas)

## Where pipe code goes

```
packages/<pkg>/src/<pkg>/
  entities/      # DataEntities declarations          (auto-imported)
  pipes/         # @DataPipes.pipe modules            (auto-imported)
  ingestion/     # FileIngestionEntries.entry modules (auto-imported)
  transforms/    # pure DataFrame -> DataFrame functions (NOT auto-imported)
```

At bootstrap the framework imports every module under `<pkg>.entities`,
`<pkg>.pipes` and `<pkg>.ingestion` (recursively), and nothing else. Registration
happens as an import side effect, so a pipe in any other subpackage is never
registered unless something imports it. `transforms/` is imported by the pipe
modules that use it. `app.py` does not import pipes; see [apps.md](apps.md).

## `@DataPipes.pipe` contract

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from kindling.data_entities import DataEntities
from kindling.data_pipes import DataPipes

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
])

DataEntities.entity(
    entityid="skill_pipes.orders",
    name="orders",
    merge_columns=["order_id"],
    tags={"provider_type": "memory", "provider.seed.rows": [
        {"order_id": "o1", "customer_id": "c1", "amount": 10.0},
        {"order_id": "o2", "customer_id": None, "amount": 5.0},
        {"order_id": "o3", "customer_id": "c2", "amount": -1.0},
    ]},
    schema=orders_schema,
)
DataEntities.entity(
    entityid="skill_pipes.clean_orders",
    name="clean_orders",
    merge_columns=["order_id"],      # keys => persisted by merge (SCD1 upsert)
    tags={"provider_type": "memory"},
    schema=orders_schema,
)


# transforms/orders.py -- pure, no kindling imports
def clean_orders(orders: DataFrame) -> DataFrame:
    return orders.filter(F.col("customer_id").isNotNull() & (F.col("amount") > 0))


# pipes/silver_orders.py -- thin registration
@DataPipes.pipe(
    pipeid="skill_pipes.clean_orders",
    name="Clean orders",
    tags={"layer": "silver"},
    input_entity_ids=["skill_pipes.orders"],
    output_entity_id="skill_pipes.clean_orders",
    output_type="table",
)
def clean_orders_pipe(skill_pipes_orders):
    return clean_orders(skill_pipes_orders)
```

Rules (from `PipeMetadata` in `kindling/data_pipes.py`):

- Required keywords: `pipeid`, `name`, `tags`, `input_entity_ids`,
  `output_entity_id`, `output_type`. Missing any raises `ValueError`. Pass
  `tags={}` when there are none.
- Optional: `use_watermark=False`, `driving_entity_ids=None` (see below). No
  other keywords exist. `PipeMetadata` would reject them.
- `output_type` is free-form metadata. The runtime never reads it, and the
  scaffolds use `"table"`. How the output is written comes from the **output
  entity's** provider and tags.
- Every input is passed **by keyword**. The name is the entity id with `.`
  replaced by `_`, so `bronze.orders` becomes `bronze_orders`. No other
  character is replaced, so keep ids `snake_case.dotted` (a `-` would produce an
  invalid parameter name).
- The function returns one DataFrame, which is persisted to `output_entity_id`.
  One pipe writes one entity. Do not write inside the function.
- Both the input and output entities must be registered. The executor resolves
  each id through the entity registry.
- `DataPipes.ids.<pipeid with . and - as _>` holds the id string, for example
  `DataPipes.ids.skill_pipes_clean_orders`.
- The decorator raises `KindlingNotInitializedError` if the module is imported
  before `initialize_framework`. This is why pipe modules are auto-imported and
  never imported at the top of `app.py`.

## Transforms vs. registered pipes

Put the logic in pure functions under `transforms/` that take and return
DataFrames and import nothing from kindling. The registered pipe should only
map its keyword inputs onto that function. The `kindling repo init` templates
(`transforms/quality.py`, `pipes/bronze_to_silver.py`) work this way. Test the
transform directly on tiny DataFrames:

```python
from kindling.spark_session import get_or_create_spark_session

spark = get_or_create_spark_session()
sample = spark.createDataFrame(
    [("a", "c1", 3.0), ("b", None, 1.0), ("c", "c2", 0.0)], orders_schema
)
assert [r.order_id for r in clean_orders(sample).collect()] == ["a"]
```

`kindling package add pipe` scaffolds `transform_<name>` in the same module as
the pipe. Moving it into `transforms/` once it grows is fine and expected.

## Running a pipe

`DataPipesExecution.run_datapipes([...])` runs the listed pipes **sequentially
in the order given**, and the first failure re-raises. Pass `use_dag=True` to get
dependency order: the edges come from one pipe's `output_entity_id` matching
another pipe's input, and `kindling.execution.*` config applies. In standalone mode,
when `tests/entities/<ns>/<name>.csv` exists under the cwd it replaces the
input's provider read. See [entities.md](entities.md).

```python
from kindling.data_entities import DataEntityRegistry
from kindling.data_pipes import DataPipesExecution
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import get_kindling_service

get_kindling_service(DataPipesExecution).run_datapipes([DataPipes.ids.skill_pipes_clean_orders])

out_entity = get_kindling_service(DataEntityRegistry).get_entity_definition("skill_pipes.clean_orders")
out = get_kindling_service(EntityProviderRegistry).get_provider_for_entity(out_entity).read_entity(out_entity)
assert [r.order_id for r in out.collect()] == ["o1"]
```

A pipe is **skipped**, with no execute and no persist, only when every driving
read returns `None`, which means a watermarked read found no new data. An empty
DataFrame does not count as `None`, so the pipe still runs.

## Incremental (watermarked) pipes

- `use_watermark=True` makes the **driving** inputs read only the changes since
  the stored cursor. The cursor is keyed by (source entity id, pipeid), so
  renaming a pipe id restarts it from a full read. The cursor advances only
  after the persist succeeds (at-least-once), which is why merge-keyed outputs
  are the safe choice.
- The driving inputs are `driving_entity_ids`, which must be a non-empty subset
  of `input_entity_ids`. When it is omitted, the driving set is
  `[input_entity_ids[0]]`. Every other input is reference data that is read in
  full on every run.
- Incremental reads need a provider that supports them (Delta, or a provider
  that implements `IncrementalReadableEntityProvider`). For any other provider,
  such as memory or CSV, a warning is logged and a full read happens instead.
- **Standalone/local runs never watermark.** `WatermarkAspect` is only
  registered on non-standalone platforms, and not when the execution engine
  owns incrementality. Expect a full read every time you run locally. `kindling pipeline run --no-watermark` and
  `run_datapipes(..., no_watermark=True)` force a full read on a platform.

```python
from pyspark.sql.types import StructField, StructType, StringType

DataEntities.entity(
    entityid="skill_pipes.customers", name="customers", merge_columns=["customer_id"],
    tags={"provider_type": "memory"},
    schema=StructType([StructField("customer_id", StringType(), False),
                       StructField("region", StringType(), True)]),
)
DataEntities.entity(
    entityid="skill_pipes.orders_by_region", name="orders_by_region", merge_columns=["order_id"],
    tags={"provider_type": "memory"}, schema=None,
)

@DataPipes.pipe(
    pipeid="skill_pipes.orders_by_region",
    name="Orders with region",
    tags={"layer": "gold"},
    input_entity_ids=["skill_pipes.clean_orders", "skill_pipes.customers"],
    driving_entity_ids=["skill_pipes.clean_orders"],  # customers = full-read reference
    use_watermark=True,
    output_entity_id="skill_pipes.orders_by_region",
    output_type="table",
)
def orders_by_region(skill_pipes_clean_orders, skill_pipes_customers):
    return skill_pipes_clean_orders.join(skill_pipes_customers, "customer_id", "left")
```

## How output is written: append, SCD1 merge, SCD2

The pipe never chooses the write strategy. The persist step reads it from the
**output entity**, both in batch and in streaming:

| Output entity | Write |
| --- | --- |
| entity does not exist yet | `write_to_entity` (create) |
| `merge_columns` non-empty, provider can merge | merge = SCD1 upsert on the keys |
| `merge_columns=[]`, or the provider cannot merge | append |
| tag `write.mode: append` / `merge` / `insert` | forced. `merge`/`insert` raise when the provider cannot merge. `insert` inserts only new keys |
| tag `scd.type: "2"` (+ `scd.*` options) | the merge runs as SCD2 history (`__effective_from`, `__effective_to`, `__is_current`) |
| tag `dataset.kind: derived` | full replace on every run. Not allowed as a streaming sink |

`scd.type` only accepts `"2"`, so SCD1 has no tag of its own: it is simply the
default merge. The SCD2 options and providers are covered in
[entities.md](entities.md).

## Streaming pipes

A streaming pipe is an ordinary `@DataPipes.pipe`. What makes it stream is how
the app runs it: `kindling.apps.run_streaming_app()` (the `app.streaming.py`
template) calls `ExecutionOrchestrator.execute_streaming`, and that starts every
pipe through `SimplePipeStreamStarter`:

- Driving inputs come in as streaming DataFrames (`read_entity_as_stream`), and
  the other inputs come in as static reads, giving stream-static joins. Every
  driving provider must be able to stream.
- The checkpoint is `<base_checkpoint_path or kindling.storage.checkpoint_root>/<pipeid>`.
  Renaming the pipe or adding a driving input needs a new checkpoint.
- The sink mode follows the same table as above. Merge runs per micro-batch
  (`merge_as_stream`), and append otherwise.
- The body must be valid on a streaming DataFrame, so no `count()`, `collect()`
  or `toPandas()`.
- A `processing_mode: streaming` pipe tag only matters to the `config_based`
  DAG strategy. The default DAG strategy and `kindling pipeline run` both run
  batch.

```python
# illustrative -- needs a stream-capable source and a checkpoint root
@DataPipes.pipe(
    pipeid="silver.events_stream",
    name="Events stream",
    tags={"layer": "silver", "processing_mode": "streaming"},
    input_entity_ids=["bronze.events", "ref.devices"],
    driving_entity_ids=["bronze.events"],          # streamed; ref.devices read static
    output_entity_id="silver.events",
    output_type="table",
)
def events_stream(bronze_events, ref_devices):
    return enrich_events(bronze_events, ref_devices)
```

## Clone, extend and the YAML form

- `DataPipes.clone(new_id, from_pipe=..., **changes)` derives a new pipe from
  another pipe's raw declaration. The clone-only replacements are `name`,
  `output_entity_id`, `output_type`, `use_watermark` and `driving_entity_ids`.
- `DataPipes.extend(pipeid, ...)` changes a pipe in place but can only add:
  `tags` (a later value wins), `add_inputs`, and `transform`.
- `transform(previous_output, **added_inputs)` wraps the execute. The added
  inputs arrive by keyword using the same `.`→`_` naming. When several
  extensions stack, the last one registered runs outermost.
- Derivations resolve whenever their source registers, so import order does not
  matter. If a source never registers, `kindling app validate` reports the
  derivation as unresolved.
- A clone has its own watermark (its own pipe id). A pipe id is either
  registered directly or cloned, never both.

```python
DataPipes.clone(
    "skill_pipes.big_orders",
    from_pipe="skill_pipes.clean_orders",
    output_entity_id="skill_pipes.big_orders",
    transform=lambda df: df.filter(F.col("amount") >= 10),
)
DataPipes.extend("skill_pipes.clean_orders", tags={"owner": "sales"})
DataEntities.entity(
    entityid="skill_pipes.big_orders", name="big_orders", merge_columns=["order_id"],
    tags={"provider_type": "memory"}, schema=orders_schema,
)

from kindling.data_pipes import DataPipesRegistry

pipes = get_kindling_service(DataPipesRegistry)
assert pipes.get_pipe_definition("skill_pipes.big_orders").output_entity_id == "skill_pipes.big_orders"
assert pipes.get_pipe_definition("skill_pipes.clean_orders").tags["owner"] == "sales"
```

In settings YAML, put `clone_of` and `add_inputs` (no transform) under an
**exact** pipe id. Any other keys on that entry override metadata fields. A
wildcard id cannot carry a derivation. Plain `datapipes:` and
`datapipes-bytag:` overrides are covered in [config.md](config.md).

```yaml
datapipes:
  silver.build_orders_canary:
    clone_of: silver.build_orders
    output_entity_id: gold.orders_canary
    add_inputs: [ref.regions]   # read and ordered as a dependency; execute ignores it
```

## File ingestion

File ingestion is not a pipe. It is a registry of
`FileIngestionEntries.entry(...)` entries that
`FileIngestionProcessor.process_path(path)` processes. The `app.file-ingestion.py`
template calls `kindling.apps.run_file_ingestion_app()`, which reads
`KINDLING_INGESTION_PATH`.

```python
import re

from kindling.file_ingestion import FileIngestionEntries

FileIngestionEntries.entry(
    entry_id="skill_pipes.sales_report",
    name="sales_report_ingestion",
    patterns=[r"sales_(?P<region>[a-z]+)_(?P<report_date>\d{8})\.csv"],
    dest_entity_id="skill_pipes.sales_report",   # may use {group} placeholders
    tags={"layer": "bronze"},
    infer_schema=False,
    filetype="csv",
)

match = re.match(r"sales_(?P<region>[a-z]+)_(?P<report_date>\d{8})\.csv", "sales_west_20260101.csv")
assert match.groupdict() == {"region": "west", "report_date": "20260101"}
```

- Every pattern is tried in order with `re.match` (anchored at the start)
  against the **file name**, not the full path; the first match wins. An empty
  list, a bare string or an invalid regex is rejected.
- Each named group becomes a string column, `static_values={...}` adds constant
  columns, and an `ingestion_timestamp` column is always added.
  `dest_entity_id` is `.format(**groups)`, so a single entry can route to
  several entities.
- Files are read with `header=true`; `inferSchema` follows `infer_schema=`
  (default `False`, batch path only). The format is a `filetype` named group if
  the pattern has one, else the `filetype=` argument, else `csv`.
- Files bound for the same entity are unioned by name and then **appended**
  through the destination entity's own provider (its `provider_type` tag,
  default delta). Declare the destination entity before running.
- `discovery="autoloader"` requires `source_glob` plus the
  `kindling_ext_databricks_autoloader` extension and
  `kindling.storage.checkpoint_root`.

## CLI

```bash
kindling package add pipe silver.orders_clean --inputs bronze.orders,ref.customers --package packages/sales
kindling package add ingestion bronze.sales_report --source-pattern 'sales_(?P<report_date>[^.]+)[.]csv' --package packages/sales
kindling pipeline list --app apps/sales_batch/app.py
kindling pipeline show silver.orders_clean --app apps/sales_batch/app.py --tags
kindling pipeline run silver.orders_clean --app apps/sales_batch/app.py --no-watermark
kindling app validate --app apps/sales_batch/app.py
```

- `package add pipe <ns.name>` writes `pipes/<ns>_<name>.py` with pipeid
  `<ns>.<name>`, parameters named `<ns>_<name>` for each input, and a
  `transform_<name>` stub. If `<ns>.<name>_output` is not declared yet, it is
  added to `entities/<ns>.py`, and you should rename it to the real target. The
  command also writes skip-marked test stubs and `tests/entities/<ns>/<name>.csv`
  fixture stubs for the inputs.
- `package add ingestion` writes `pipes/<ns>_<name>_ingestion.py`, even though
  `ingestion/` is also auto-imported. It also declares the destination entity
  with no `provider_type` (default delta); ingestion writes through whatever
  provider that entity declares.
- `pipeline run` runs exactly one pipe with `run_datapipes` (batch, no DAG) and
  needs `settings.yaml` in the cwd, `--app` or `--config`. `--env` only selects
  the config overlay. It always runs locally.

## Testing

- **Unit**: call the `transforms/` functions on DataFrames built inline (see
  above), with no registration and no framework. The repo template's
  `spark_local` fixture is a plain local session.
- **Component**: call `initialize_framework({"platform": "standalone", ...})`,
  import `<pkg>.entities...` and `<pkg>.pipes...`, then assert on
  `get_pipe_definition(id)`: inputs, output, `callable(pipe.execute)`. See
  `tests/component/test_registration.py` in the template. Between isolated
  tests, reset with `DataPipes.reset()`, `DataEntities.reset()` and
  `GlobalInjector.reset()`.
- **Local end to end**: put CSVs at `tests/entities/<ns>/<name>.csv` and run
  `kindling pipeline run <pipeid>` from the project root, or use memory
  entities as in the examples above.

```python
pipe = get_kindling_service(DataPipesRegistry).get_pipe_definition("skill_pipes.orders_by_region")
assert pipe.input_entity_ids == ["skill_pipes.clean_orders", "skill_pipes.customers"]
assert pipe.use_watermark and callable(pipe.execute)
```

## Gotchas

- Never touch `spark._jvm` or `spark._jsc`. They fail on Databricks UC
  shared/standard clusters and on Spark Connect. Framework code enforces this
  with `tests/unit/test_architecture_jvm_boundary.py`, and pipes must follow the
  same rule.
- Keep pipe bodies platform-agnostic: no `if platform == ...` and no hard-coded
  paths or table names. Environment differences go in entity tags and in
  `settings.<env>.yaml` overlays (`datapipes:` for pipe metadata).
- Do not call `.count()`, `.collect()` or `.show()` in a pipe body to check for
  "no data". The framework handles skipping, and actions break streaming. Never
  build a SparkSession in a pipe. If one is truly needed, use
  `kindling.spark_session.get_or_create_spark_session()`.
- A pipe that also reads its own output (self-loop) or writes an input entity is
  legal (the `process_records` template does it) but has no DAG ordering. Prefer
  distinct output entities.
- `run_datapipes` without `use_dag` neither orders nor deduplicates. Pass the
  ids in dependency order, or use `use_dag=True`.
- Tag values may be `@secret:` references (resolved at bootstrap). Never put
  literal credentials in `tags=`.
