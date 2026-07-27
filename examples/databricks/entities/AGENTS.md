# Kindling entity instructions

Use this file when creating or changing data entities, schemas, provider tags,
write semantics, SCD2 contracts, or schema migrations. Do not rely on external
documentation; the supported declaration contract is below.

## Normal entity declaration

"DataEntities.entity(...)" is a decorator. Required keyword arguments are
"entityid", "name", "merge_columns", "tags", and "schema". Optional keyword
arguments are "partition_columns", "cluster_columns", and "sql" (the SQL helper
sets "sql"). The decorator registers metadata; the decorated body is a
declaration anchor, not an execution function.

~~~python
from pyspark.sql.types import LongType, StringType, StructField, StructType
from kindling.data_entities import DataEntities

ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("amount_cents", LongType(), nullable=True),
])

@DataEntities.entity(
    entityid="silver.orders",
    name="Orders",
    merge_columns=["order_id"],
    schema=ORDER_SCHEMA,
    tags={
        "provider_type": "delta",
        "layer": "silver",
        "write.mode": "merge",
        "schema.drift": "fail",
    },
)
def orders():
    pass
~~~

Choose a stable logical ID, explicit schema, and business keys before writing a
pipe. "merge_columns" are the business keys for merge, insert-only, and SCD2
behavior; they are not an incidental implementation detail.

## Supported helpers and tag vocabulary

- "DataEntities.derived_entity(replace_keys=None, **entity_args)" sets the
  default "dataset.kind: derived" tag and maps "replace_keys" to the
  comma-separated "derived.replace_keys" tag.
- "DataEntities.insert_only_entity(**entity_args)" sets "write.mode: insert".
  It requires non-empty "merge_columns"; existing keys are left untouched.
- "DataEntities.sql_entity(entityid, name, tags=None, sql=None,
  sql_source=None)" creates a read-only permanent catalog SQL view. Supply
  exactly one of "sql" or "sql_source"; migration uses "CREATE OR REPLACE VIEW".

| Tag | Valid values / purpose |
| --- | --- |
| "provider_type" | Provider selector: "delta" (default), "csv", "eventhub", "parquet", "memory", or "current_view". |
| "provider.read_only" | String "true" prevents Kindling persistence to an external/source-owned entity. |
| "provider.access_mode" | Delta override: "catalog" or "storage". |
| "provider.table_name" / "provider.path" | Catalog-table or storage-path override. |
| "write.mode" | "append", "merge", or "insert". "insert" needs keys and cannot coexist with SCD2. |
| "schema.drift" | "evolve" (default), "warn", or "fail". |
| "dataset.kind" / "derived.replace_keys" | Derived-dataset semantics and optional replacement slice keys. |

For managed Databricks outputs, use "provider_type: delta" unless the source is
truly another provider. Use "provider.read_only: true" for source-owned
entities. A permanent catalog view is "sql_entity"; a memory SQL transform is a
pipe view, not an entity view.

## SCD Type 2

Use SCD tags rather than manually opening and closing records in every pipe.
Only "scd.type: 2" is supported and "merge_columns" must be business keys.

- "scd.tracked": comma-separated non-key changed attributes; specify it for a
  stable business contract.
- "scd.source_kind": "snapshot" or "change_feed". A snapshot closes records
  absent from the latest source; a change feed uses "scd.delete_when" for
  deletes.
- "scd.close_on_missing: true" is snapshot shorthand and conflicts with
  "scd.source_kind: change_feed".
- "scd.sequence_by": optional schema-defined, non-temporal business column for
  deterministic ordering.
- "scd.optimize_unchanged: true": avoid unnecessary work for unchanged rows.
- "scd.current_entity_id": read-only current-row companion; default is
  "<entityid>.current", never the base ID.

Managed temporal columns default to "__effective_from", "__effective_to", and
"__is_current". Rename only with "scd.effective_from_col",
"scd.effective_to_col", or "scd.current_col"; do not declare managed columns in
the business schema.

## Schema changes and migrations

Treat every change as compatibility and data correction, not a field-list edit.
Additive nullable fields and some safe widening can reconcile under the drift
policy. Renames, drops, arbitrary type/key/partition changes, non-null additions,
and semantic backfills require an explicit rollout.

Migration does not infer defaults, renames, or historical backfills. Use a
reviewed, idempotent pipe or SQL operation for data correction; state its scope,
derivation, rerun behavior, validation, and containment. Validate first in a
representative non-production target and classify every change as additive,
backfill-required, or breaking. Never silently invent values for existing rows.
