# Kindling configuration instructions

Use this file when changing Kindling YAML, Databricks Asset Bundle targets, or
environment promotion. This is self-contained framework guidance.

## Configuration surfaces and precedence

Kindling uses three configuration surfaces:

1. A flat bootstrap dictionary passed to "kindling.initialize(...)".
2. Layered YAML keys beneath "kindling.*".
3. A top-level YAML "entity_tags" mapping for per-entity environment overrides.

Entity tag precedence, low to high:

~~~text
helper defaults -> declared tags -> YAML entity_tags ->
run_datapipes(..., entity_tags=...) for one run
~~~

Per-run overrides are restored after that run. Use them for bounded backfill
parameters such as a provider read window, never for durable business meaning.

Keep the same logical structure in every environment. Overlays may change
deployment values, not business rules such as dataset keys, SCD behavior, or
semantic layer.

## Delta and Unity Catalog baseline

For a Unity Catalog-backed application, use catalog mode and a fully qualified
table override where the default name mapper is insufficient:

~~~yaml
kindling:
  delta:
    access_mode: catalog
  storage:
    table_catalog: example_catalog
    table_schema: example_schema
    checkpoint_root: /Volumes/example_catalog/example_schema/example_volume/checkpoints
  secrets:
    secret_scope: example_secret_scope

entity_tags:
  silver.orders:
    provider.table_name: example_catalog.example_schema.orders
~~~

Core keys:

| Key | Meaning |
| --- | --- |
| "kindling.delta.access_mode" | "catalog" for Spark/UC names; "storage" for direct Delta paths. |
| "kindling.storage.table_catalog" | Default catalog in catalog mode. Leave unset for Hive-metastore-only workspaces. |
| "kindling.storage.table_schema" | Default catalog-mode schema/database. |
| "kindling.storage.table_root" | Default path root in storage mode. |
| "kindling.storage.checkpoint_root" | Default checkpoint root; required for streaming unless supplied as a run option. |
| "kindling.databricks.volume_staging_root" | Optional governed Volume root for bootstrap wheel/config staging. |
| "kindling.secrets.secret_scope" | Databricks secret scope for the platform secret resolver. |

For a non-UC storage-mode workspace, use direct approved cloud-storage paths:

~~~yaml
kindling:
  delta:
    access_mode: storage
  storage:
    table_root: abfss://<container>@<account>.dfs.core.windows.net/example/tables
    checkpoint_root: abfss://<container>@<account>.dfs.core.windows.net/example/checkpoints
~~~

Do not mix catalog and direct-path access for one entity unless its provider
contract explicitly supports it. In UC, the executing principal needs the
appropriate catalog, schema, create, select, modify, and volume privileges.

## Entity overrides and environment design

"entity_tags" is the correct place to vary "provider.table_name",
"provider.path", or "provider.access_mode" by environment without changing the
logical entity declaration. Keep "write.mode", "schema.drift", "dataset.kind",
and "scd.*" in source declarations.

Do not make one entity per machine, tenant, or site merely to vary a threshold
or namespace. Model that scope as data, for example "machine_id", and configure
the relevant condition/pipe. Separate entities only when contracts, ownership,
security, retention, or lifecycle genuinely differ.

Databricks Asset Bundles transport and select complete configuration overlays;
they must not reimplement Kindling's merge and precedence behavior. Before a
new environment overlay, identify the target catalog/schema, checkpoint and
staging location, service principal, UC grants, namespace rules, and secrets.

## Table creation and secrets

For a Delta batch output with a declared schema, Kindling ensures a missing
target by default. Do not set "kindling.delta.ensure_on_write: true" just to
create tables. A schema-less first write can create a table through Spark, but
that is not an acceptable production contract; a missing schema-less merge
target is unsupported.

Never store secret values in YAML. Store only secret identifiers/scopes and
resolve values through Databricks secret facilities at runtime.
