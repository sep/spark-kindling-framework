# Defining entities

An entity is a declared dataset: an id, a schema, keys and tags. Pipes read
and write entities by id; a provider (chosen by the `provider_type` tag)
does the storage. Related: [pipes.md](pipes.md) (pipes that read/write
entities), [apps.md](apps.md) (package and app layout),
[config.md](config.md) (`dataentities:` overrides, `entity_tags:`, secrets).

Contents

1. Where entity modules go
2. `DataEntities.entity(...)`
3. Providers and their tags
4. Keys, partitioning, clustering, write semantics, SCD2
5. SQL entities
6. Clone and extend (code and YAML)
7. Local fixtures
8. CLI
9. Rules and gotchas

## 1. Where entity modules go

```text
packages/<pkg>/src/<pkg>/entities/__init__.py
packages/<pkg>/src/<pkg>/entities/<namespace>.py   # one module per namespace: bronze.py, silver.py
packages/<pkg>/tests/entities/<namespace>/<name>.csv
```

- Bootstrap imports only `<pkg>.entities`, `<pkg>.pipes` and `<pkg>.ingestion`,
  including every submodule (`pkgutil.walk_packages`). An entity declared
  anywhere else in the package is never registered unless something imports it.
- Declare entities at module level with a plain call. The modules are imported
  after `initialize()`. A declaration that runs earlier raises
  `KindlingNotInitializedError`.
- Don't put a flat `entities.py` next to an `entities/` package. The package
  shadows the flat module.

## 2. `DataEntities.entity(...)`

```python
from kindling.data_entities import DataEntities
from pyspark.sql.types import (
    DecimalType, StringType, StructField, StructType, TimestampType,
)

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("order_ts", TimestampType(), True),
])

DataEntities.entity(
    entityid="bronze.orders",          # required: unique id, "<namespace>.<name>"
    name="orders",                     # required: display name
    merge_columns=["order_id"],        # required: business keys ([] = append-only)
    tags={"provider_type": "delta", "layer": "bronze"},  # required (may be {})
    schema=orders_schema,              # required: StructType, or None to infer at first write
    partition_columns=[],              # optional: physical partitionBy (delta, parquet)
    cluster_columns=[],                # optional: liquid clustering (delta)
)
```

- Required keywords: `entityid`, `name`, `merge_columns`, `tags`, `schema`.
  If any is missing you get `ValueError: Missing required fields in entity
  decorator`. You must pass `tags={}` and `schema=None` explicitly.
- Keywords only. The call returns an identity decorator, so
  `@DataEntities.entity(...)` also works, but the scaffold and the codebase use a
  plain call.
- `entityid` is the identity everything else uses: pipe `input_entity_ids`,
  config keys, fixture paths and the default delta table name (`bronze.orders`
  maps to schema `bronze`, table `orders`, subject to `kindling.storage.*`
  config). Use exactly two lowercase parts, `<layer-or-domain>.<name>`. The
  CLI rejects other shapes, and a three-part id is read as `catalog.schema.table`.
- `name` is display only for table-backed entities. SQL entities are the
  exception: there `name` is the catalog view name (see §5).
- Declaring the same `entityid` twice replaces the first declaration silently.
- Declaring an entity never touches storage. Tables are created by
  `kindling migrate apply` or on first write.

## 3. Providers and their tags

`provider_type` picks the provider (default `delta`). Provider settings
are tags named `provider.<key>`. Other tags (`layer`, `comment`, ...) are
metadata and are never passed to the connector.

| `provider_type` | Use | Key tags |
|---|---|---|
| `delta` (default) | lakehouse tables, full read/write/merge/stream | optional `provider.table_name`, `provider.path`, `provider.access_mode` (`catalog`/`storage`), `provider.table_catalog`, `provider.table_schema`; `read_only: "true"` blocks writes |
| `memory` | tests, local scratch, seed data | optional `provider.table_name` (default: id with `.` changed to `_`), `provider.seed.rows` (list of dicts; needs a schema), `provider.stream_type` (`rate`/`memory`) |
| `csv` | files, batch read/write | `provider.path` required; `provider.header`, `provider.inferSchema`, `provider.delimiter`, ... |
| `parquet` | plain parquet datasets | `provider.path` required; `provider.save_mode`, `provider.merge_schema`, `provider.option.<name>` |
| `eventhub` | streaming/batch source, read-only | `provider.eventhub.connectionString` (use `@secret:`), `provider.eventhub.name`; `provider.startingPosition`, `provider.eventhub.consumerGroup`, `provider.transport`, `provider.preprocess` |
| `adx-api` | Azure Data Explorer via Kusto SDK | `provider.cluster`, `provider.database`, `provider.table` or `provider.query`; `provider.auth`, windowing `provider.time_column`/`provider.lookback` |
| `current_view` | auto-registered SCD2 `.current` companion | none: don't declare it yourself |

Extensions add more types (`kindling_ext_adx` adds `adx`, `kindling_ext_cosmos`
adds cosmos). An unregistered `provider_type` fails when the entity is read or
written, not when it is declared.

```python
from pyspark.sql.types import IntegerType

DataEntities.entity(
    entityid="ref.regions",
    name="regions",
    merge_columns=["region_id"],
    tags={
        "provider_type": "memory",
        "provider.seed.rows": [
            {"region_id": 1, "region": "EU"},
            {"region_id": 2, "region": "US"},
        ],
    },
    schema=StructType([
        StructField("region_id", IntegerType(), False),
        StructField("region", StringType(), True),
    ]),
)
```

## 4. Keys, partitioning, clustering, write semantics, SCD2

- `merge_columns` set and `write.mode` unset: pipe output is merged (SCD1
  upsert by key). `merge_columns=[]`: append.
- Tag `write.mode`: `append` | `merge` | `insert`. `insert` means insert-if-absent
  and needs `merge_columns`. `DataEntities.insert_only_entity(...)` is sugar for it.
- Tag `dataset.kind: derived` (optional `derived.replace_keys`): each write
  replaces the whole table, or only the slices named by the keys. Can't be
  combined with `write.mode` or `scd.*`. Sugar: `DataEntities.derived_entity(replace_keys=[...], ...)`.
- Tag `schema.drift`: `evolve` (default) | `warn` | `fail`.
- `cluster_columns` wins over `partition_columns` on delta (when both are set,
  `partitionBy` is skipped with a warning). `["auto"]` requires the feature flag
  `kindling.features.delta.auto_clustering`.
- Tag values are checked when the entity is declared: a bad `write.mode`,
  `scd.*`, `dataset.kind` or `schema.drift` raises `ValueError` immediately.

SCD Type 2 is enabled by tags. The schema holds business columns only.
`__effective_from`, `__effective_to` and `__is_current` are added to the table,
and a read-only `<entityid>.current` companion is registered automatically.

```python
DataEntities.entity(
    entityid="silver.customers",
    name="customers",
    merge_columns=["customer_id"],           # business key, required for SCD2
    tags={
        "scd.type": "2",                     # only "2" is accepted
        "scd.tracked": "email,region",       # optional; default = all non-key columns
    },
    schema=StructType([
        StructField("customer_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("region", StringType(), True),
    ]),
)

from kindling.data_entities import DataEntityRegistry
from kindling.injection import GlobalInjector

registry = GlobalInjector.get(DataEntityRegistry)
assert "silver.customers.current" in registry.get_entity_ids()
assert registry.get_entity_definition("silver.customers.current").tags["provider_type"] == "current_view"
```

Other SCD tags: `scd.sequence_by` (an ordering column from the data),
`scd.source_kind` (`snapshot` | `change_feed`), `scd.delete_when` (a SQL
predicate, change_feed only), `scd.close_on_missing`, `scd.optimize_unchanged`,
`scd.routing_key` (`hash` | `concat`), `scd.current_entity_id`, and
`scd.effective_from_col` / `scd.effective_to_col` / `scd.current_col`. Never put
`scd.tracked` columns in `merge_columns`, and never name a schema column
`__merge_key` or one of the temporal columns.

## 5. SQL entities

```python
from kindling.data_entities import SqlSource

DataEntities.sql_entity(
    entityid="reporting.big_orders",
    name="reporting.big_orders",      # catalog view name (or tag provider.table_name)
    sql="SELECT * FROM bronze.orders WHERE amount > 1000",
    tags={"layer": "reporting"},
)
```

- Pass exactly one of `sql=` or `sql_source=SqlSource(inline=... | resource="pkg:sql/x.sql" | file=...)`.
  In packages, prefer `resource=` and ship the `.sql` file as package data.
- The entity gets `provider_type: "view"`, has no schema or keys, and is
  read-only. `kindling migrate apply` creates or replaces the view, and
  `migrate plan` notices SQL changes by hash.
- The core runner has no `view` provider. Reading a SQL entity as a pipe input
  fails with `Unknown provider type: 'view'`. Use it as a published view, or
  express the logic as a pipe instead (see [pipes.md](pipes.md)).

## 6. Clone and extend

`clone` declares a new id with another entity's declaration as its template.
`extend` adds to an existing id in place. Both only add. They resolve
whenever the source is registered, so import order doesn't matter.

```python
DataEntities.clone(
    "bronze.orders_eu",
    from_entity="bronze.orders",
    name="orders_eu",                                  # clone-only override
    add_columns=[StructField("region", StringType())],
    tags={"region": "eu"},                             # merged, later wins
)
DataEntities.extend("bronze.orders", tags={"owner": "sales"})

eu = registry.get_entity_definition("bronze.orders_eu")
assert eu.schema.fieldNames()[-1] == "region" and eu.merge_columns == ["order_id"]
assert registry.get_entity_definition("bronze.orders").tags["owner"] == "sales"
```

- `extend` accepts only `tags`, `add_columns`, `add_partition_columns` and
  `add_cluster_columns`. `clone` also accepts the replacements `name`,
  `merge_columns`, `partition_columns` and `cluster_columns`. Anything else
  raises `DerivationError`.
- Adding a column whose name already exists with a different type is an error.
  Changing a type is a migration, not an extension. Adding columns needs a
  `StructType` source schema, not `None`.
- An id can't be both declared with `entity(...)` and cloned.
- If the source is never registered, the target stays pending: bootstrap logs
  `Declaration derivation ... is unresolved`, and looking it up raises.

The YAML form goes on an exact id under `dataentities:` in settings. It supports
`clone_of` and `add_columns` (types: atomic Spark SQL names,
`decimal(p,s)`, `array<type>`). Other keys on the entry are ordinary overrides:

```yaml
dataentities:
  bronze.orders_us:
    clone_of: bronze.orders
    add_columns:
      - { name: state, type: string }
      - { name: tax, type: "decimal(18,2)" }
    tags: { region: us }
```

Glob keys (`bronze.*`) can't carry `clone_of` or `add_columns`. Config
derivations apply after code ones on every overlay pass.

## 7. Local fixtures

When the platform is standalone, a pipe reading entity `bronze.orders` checks
`tests/entities/bronze/orders.csv` **relative to the current working
directory** before it asks the provider. The rules:

- The id's dots become directories. The last segment is the file name.
- The CSV is read with `header=true, inferSchema=true`. The declared schema is
  not applied.
- A fixture with no data rows (empty, headers only, or only `#` comment lines)
  is ignored with a warning naming the file, and the entity's provider is read
  instead. The stubs `kindling package add entity` (a header row) and
  `package add pipe --inputs` (a comment line) write are like that until you
  add rows. `app validate` warns and `app inspect --entities` marks them
  "ignored".
- Lines starting with `#` are comments in a fixture.
- Fixtures only replace **reads**. Writes still go to the entity's provider.
- `kindling entity show` and `kindling entity validate` use the same lookup.

## 8. CLI

```bash
kindling package add entity bronze.orders --package packages/my_pkg  # appends to entities/bronze.py + tests/entities/bronze/orders.csv stub
kindling entity list --app apps/my_app/app.py --tags                 # registered ids, resolved tags
kindling entity tags bronze.orders --env dev                         # each tag plus the config layer that set it
kindling entity show bronze.orders --limit 50                        # data: fixture first, then provider
kindling entity validate bronze.orders --env local                   # row count / nulls / fixture-vs-schema
kindling migrate plan --app apps/my_app/app.py --env dev             # pending delta/view schema changes
kindling migrate apply --env dev                                     # add columns, create tables and views
kindling migrate apply --env dev --destructive --backup snapshot     # type changes, drops, partition changes
```

The scaffold writes `provider_type: "delta"`, `merge_columns=["id"]`, a
one-column schema and a TODO. Replace all of them. Migrations cover only
`delta` entities and SQL views; other providers show as skipped.

## 9. Rules and gotchas

- Every required keyword is present, even when empty (`tags={}`, `schema=None`).
- Prefer an explicit `StructType`. `schema=None` breaks `add_columns`,
  memory seed rows and typed empty reads, and a streaming merge sink without
  a schema fails at query start if its table doesn't exist yet.
- Changing a column's type (even widening it), removing a column or
  changing partitioning on an existing delta table is destructive: run
  `kindling migrate plan` then `migrate apply --destructive`. Adding columns
  and changing clustering are safe.
- Keep per-environment values (paths, catalogs, secrets) in tags overlaid
  from config ([config.md](config.md)), not in `if env == ...` code.
  A `dataentities:` override whose `merge_columns` is a list replaces the
  declared keys.
- Pipe inputs bind as keyword arguments named after the id with dots changed
  to underscores: `bronze.orders` becomes `bronze_orders`, and
  `silver.customers.current` becomes `silver_customers_current`.
- Don't write to `*.current` companions, `eventhub` entities, SQL entities, or
  entities tagged `read_only: "true"`.
