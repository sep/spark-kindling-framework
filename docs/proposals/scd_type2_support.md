# SCD Type 2 Support for Kindling

**Status:** Draft (revised)
**Author:** System Analysis
**Created:** 2026-02-26
**Updated:** 2026-04-20
**Related:** obsolete/signal_dag_streaming_proposal.md, obsolete/dag_execution_implementation_plan.md, obsolete/config_based_entity_providers.md

## Revision Summary (2026-04-20)

This proposal was revised based on design feedback after confirming no SCD2 support exists in the current codebase:

1. **Tag-driven configuration, no new `EntityMetadata` fields.** The original proposal added `scd_type` and `scd2_config` fields to `EntityMetadata`. Revised design expresses all SCD configuration through the existing `tags` map under a `scd.*` namespace — no schema change to `EntityMetadata`, no new dataclasses, consistent with how other cross-cutting concerns (`layer`, `domain`, `provider.*`) are already carried in Kindling.
2. **Automatic current-view companion entity.** The original proposal exposed `read_entity_current()` as a helper method users must remember to call. Revised design auto-registers a companion entity at `{entityid}.current` whenever an entity is tagged SCD2, so pipes can declare `input_entity_ids=["silver.dim_customer.current"]` and get the filtered-to-current view transparently via the normal registry lookup.

Both changes keep SCD2 declarative and observable while minimizing the framework surface area and aligning with existing Kindling conventions.

## Executive Summary

This proposal adds **Slowly Changing Dimension Type 2 (SCD2)** write semantics to Kindling's entity provider layer. Today Kindling's merge operation implements SCD Type 1 — it overwrites matched rows in place. SCD2 preserves history by closing the current row (setting an end date and clearing the `is_current` flag) and inserting a new version row whenever tracked columns change.

The revised design introduces SCD2 through **tags on the entity** — no new `EntityMetadata` fields, no new dataclasses. When the framework sees `tags["scd.type"] == "2"` on an entity:

- The staged-updates merge path runs instead of overwrite-in-place.
- Schema is auto-augmented with temporal columns.
- A companion read-only entity `{entityid}.current` is auto-registered that filters to `is_current=true` and can be referenced by downstream pipes the same way any other entity is.

All changes are backward-compatible — entities without the `scd.type` tag continue to behave exactly as they do today.

## Problem Statement

### Current Limitations

Dimension tables in lakehouse architectures frequently need historical tracking. Common scenarios include:

1. **Customer attributes** — name, address, or segment changes over time; downstream analytics need point-in-time accuracy
2. **Product catalog** — price or category changes must be correlated with historical transactions
3. **Configuration / reference data** — regulatory or business rule changes need an audit trail
4. **Compliance** — regulations (GDPR, SOX) require demonstrating what data looked like at a given point in time

### Current State in Kindling

Kindling currently supports:
- **SCD Type 1 (overwrite)** via `merge_to_entity` using `whenMatchedUpdateAll().whenNotMatchedInsertAll()`
- **Change Data Feed** enabled by default (`delta.enableChangeDataFeed=true`)
- **Version tracking** via `get_entity_version()` and `read_entity_since_version()`
- **Watermarking** for incremental processing of upstream changes
- **Dimension change detection** proposed in signal_dag_streaming_proposal.md (detection only, not write semantics)

However, Kindling lacks:
- Built-in SCD2 merge logic (close-old-row + insert-new-version)
- Temporal metadata columns (`effective_date`, `end_date`, `is_current`)
- Entity-level declaration of SCD type
- Convenience methods for reading current-state or point-in-time snapshots
- Signals specific to SCD2 operations (rows closed, rows versioned, rows unchanged)

### Impact

Without framework support, teams must:
- Hand-roll the Delta Lake staged-updates merge pattern inside individual pipe transforms
- Manually manage temporal columns and ensure consistency across pipes
- Duplicate boilerplate SCD2 logic across every dimension entity
- Lose observability into how many rows were versioned vs. unchanged
- Risk subtle bugs when the merge condition or column tracking drifts between pipes

## Proposed Solution

### Design Principles

1. **Declarative** — SCD behavior is declared on the entity, not embedded in pipe logic.
2. **Tags, not fields** — SCD config rides the existing `tags: Dict[str, str]` map under the `scd.*` namespace, consistent with how `layer`, `domain`, and `provider.*` already travel. No new `EntityMetadata` fields, no new dataclasses, no schema change to the registry.
3. **Current view is automatic** — any entity tagged SCD2 gets a companion `{entityid}.current` entity auto-registered. Downstream pipes depend on the companion by ID; there is no separate API to remember.
4. **Backward-compatible** — entities without `scd.type` tag behave exactly as today.
5. **Convention over configuration** — sensible defaults for column names and tracking scope.
6. **Observable** — dedicated signals for SCD2 merge outcomes.
7. **Composable** — works with existing watermarking, streaming restart, and multi-provider architecture.

### Architecture Overview

```
Entity Declaration                      DeltaEntityProvider
┌──────────────────────────────┐        ┌──────────────────────────────┐
│ tags = {                     │        │ _merge_to_delta_table()      │
│   "scd.type": "2",           │───────▶│   ├─ no scd.type: overwrite  │
│   "scd.tracked": "a,b,c",    │        │   └─ scd.type=="2": staged   │
│   "scd.effective_col": "…",  │        │       _merge_scd2()          │
│   "scd.end_col": "…",        │        ├──────────────────────────────┤
│   "scd.current_col": "…",    │        │ ensure_entity_table()        │
│ }                            │        │   auto-augments schema with  │
└──────────────────────────────┘        │   temporal columns when      │
         │                              │   scd.type == "2"            │
         │  DataEntities.entity()       └──────────────────────────────┘
         ▼
  DataEntityManager.register_entity()
         │
         │  if scd.type == "2":
         │    also register companion entity at
         │    "{entityid}.current" with a CurrentViewEntityProvider
         │    that filters is_current=true
         ▼
  Registry now contains:
    silver.dim_customer           (SCD2 table, all history)
    silver.dim_customer.current   (read-only view, current rows only)

SimpleReadPersistStrategy: unchanged.
  Provider's merge_to_entity branches on tags — strategy layer never needs to know.
```

The strategy layer does not change — SCD2 logic is encapsulated entirely in the provider and the registry-time auto-registration, both driven by tags on the entity.

## Detailed Design

### 1. Entity Declaration via Tags

No changes to `EntityMetadata`. SCD configuration lives entirely in the existing `tags: Dict[str, str]` map under the `scd.*` namespace.

#### Tag convention

| Tag key | Required | Default | Description |
|---|---|---|---|
| `scd.type` | yes (to enable) | — | `"2"` enables SCD Type 2. Any other value (or absence) → current overwrite-in-place behavior. |
| `scd.tracked` | no | all non-key, non-temporal columns | Comma-separated list of columns to monitor for changes. |
| `scd.effective_col` | no | `__effective_date` | Name of the timestamp column storing when a version became active. |
| `scd.end_col` | no | `__end_date` | Name of the timestamp column storing when a version was superseded. Null means current. |
| `scd.current_col` | no | `__is_current` | Name of the boolean flag column for fast current-state queries. |
| `scd.current_entity_id` | no | `{entityid}.current` | Override the auto-registered companion entity's id. |
| `scd.routing_key` | no | `hash` | How the staging-only `__merge_key` routing column is computed. `hash` uses `sha2(to_json(struct(*merge_columns)), 256)` — safe for any value type (strings with delimiters, nulls, mixed types) and composite keys. `concat` uses `concat_ws("\|\|", ...)` — cheaper and debuggable, but silently collides when a key value contains `\|\|`. Default `hash` is the right call for a framework that doesn't know the shape of consumers' business keys; `concat` is an opt-in for teams whose keys are controlled types (int ids, UUIDs, short codes) where debuggability matters more than the ~0.5–2% merge-time overhead of a hash. |
| `scd.close_on_missing` | no | `false` | When `"true"`, the SCD2 merge also closes any current row in the target whose business key is absent from the source batch — treating the source as a full snapshot where absence means "deleted upstream." Default `false` is safer for incremental feeds, where absence means "nothing new to report." |
| `scd.optimize_unchanged` | no | `false` | (Phase 4) When `"true"`, the merge pre-filters unchanged rows via hash-based change detection instead of closing and re-inserting them. Opt-in because the extra scan isn't worth it for dimensions under ~1M rows; set to `"true"` for very large dimensions where no-op row churn matters. |

Because tags are `Dict[str, str]`, list values use a simple comma-separator convention (already used elsewhere in the framework for tag-scoped lists). A helper `scd_config_from_tags(entity) -> SCDConfig` extracts and validates at registration time; callers never read tags directly.

```python
# packages/kindling/data_entities.py (new helper, not a new EntityMetadata field)

from dataclasses import dataclass
from typing import List, Optional


ROUTING_KEY_METHODS = ("hash", "concat")


@dataclass(frozen=True)
class SCDConfig:
    """Parsed SCD configuration derived from an entity's tags.

    Constructed by ``scd_config_from_tags(entity)``; not stored on the entity.
    """
    enabled: bool
    tracked_columns: Optional[List[str]]
    effective_date_column: str
    end_date_column: str
    is_current_column: str
    current_entity_id: str
    routing_key_method: str  # one of ROUTING_KEY_METHODS


def scd_config_from_tags(entity) -> SCDConfig:
    """Extract and validate SCD config from entity.tags.

    Returns SCDConfig(enabled=False, ...) if no scd.type tag is set.
    Raises ValueError on misconfiguration (e.g., invalid scd.type value).
    """
    tags = entity.tags or {}
    scd_type = tags.get("scd.type", "").strip()

    if not scd_type:
        return SCDConfig(
            enabled=False,
            tracked_columns=None,
            effective_date_column="__effective_date",
            end_date_column="__end_date",
            is_current_column="__is_current",
            current_entity_id=f"{entity.entityid}.current",
            routing_key_method="hash",
        )

    if scd_type != "2":
        raise ValueError(
            f"Entity '{entity.entityid}': scd.type must be '2' (only SCD Type 2 "
            f"is supported), got '{scd_type}'"
        )

    tracked_raw = tags.get("scd.tracked", "").strip()
    tracked = [c.strip() for c in tracked_raw.split(",") if c.strip()] if tracked_raw else None

    routing = tags.get("scd.routing_key", "hash").strip().lower()
    if routing not in ROUTING_KEY_METHODS:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.routing_key must be one of "
            f"{ROUTING_KEY_METHODS}, got '{routing}'"
        )

    return SCDConfig(
        enabled=True,
        tracked_columns=tracked,
        effective_date_column=tags.get("scd.effective_col", "__effective_date"),
        end_date_column=tags.get("scd.end_col", "__end_date"),
        is_current_column=tags.get("scd.current_col", "__is_current"),
        current_entity_id=tags.get("scd.current_entity_id", f"{entity.entityid}.current"),
        routing_key_method=routing,
    )
```

User declaration (no new parameters, no new dataclass import):

```python
@DataEntities.entity(
    entityid="silver.dim_customer",
    name="Customer Dimension",
    merge_columns=["customer_id"],
    partition_columns=[],
    tags={
        "layer": "silver",
        "domain": "customer",
        "scd.type": "2",
        "scd.tracked": "name,email,region,segment",
    },
    schema=customer_dim_schema,
)
```

Entities that don't set `scd.type` behave identically to today.

#### Design note: tags vs. fields

The original proposal added `scd_type` and `scd2_config` fields to `EntityMetadata`. The revised approach deliberately avoids new fields because:

- **Consistency** — `layer`, `domain`, `provider.access_mode`, etc. are already tags. Cross-cutting classification belongs in tags; structural identity (ids, keys, schema) belongs in fields.
- **Extensibility** — third-party extensions can introduce their own tag conventions (`myext.feature_x`) without requiring a schema change to the core registry.
- **No registry migration** — the existing `EntityMetadata` dataclass is unchanged, so no migration of entity registrations, snapshots, or introspection tooling.
- **Ergonomic cost is small** — list values become comma-separated strings. The `scd_config_from_tags` helper centralizes parsing so callers stay clean.

### 2. Schema Auto-Augmentation

When an entity has `scd.type == "2"`, `ensure_entity_table` automatically appends the three temporal columns to the user-provided schema if they are not already present. Column names come from the tag config (defaults: `__effective_date`, `__end_date`, `__is_current`).

```python
def _augment_schema_for_scd2(self, schema, cfg: SCDConfig):
    """Add SCD2 temporal columns to schema if not already present."""
    from pyspark.sql.types import BooleanType, StructField, StructType, TimestampType

    existing_names = {f.name for f in schema.fields}
    extra_fields = []

    if cfg.effective_date_column not in existing_names:
        extra_fields.append(
            StructField(cfg.effective_date_column, TimestampType(), False)
        )
    if cfg.end_date_column not in existing_names:
        extra_fields.append(
            StructField(cfg.end_date_column, TimestampType(), True)
        )
    if cfg.is_current_column not in existing_names:
        extra_fields.append(
            StructField(cfg.is_current_column, BooleanType(), False)
        )

    return StructType(schema.fields + extra_fields) if extra_fields else schema
```

Keeps entity declarations clean — users define business columns in the schema, the framework manages the history plumbing based on the `scd.*` tags.

### 2.5. Automatic Current-View Registration

When `DataEntityManager.register_entity` sees an entity with `scd.type == "2"`, it **auto-registers a companion entity** at the id `{entityid}.current` (overridable via `scd.current_entity_id` tag). The companion:

- Carries the same schema (minus the temporal columns, which are filtered out of projection).
- Uses a dedicated `CurrentViewEntityProvider` that delegates reads to the base entity's provider and applies `filter(is_current == true)`.
- Is registered read-only: attempting to write to it raises — writes always target the historized base entity.
- Is flagged via tags (`scd.companion_of: {base_entityid}`, `scd.view_type: current`) so it shows up in introspection tooling as derived, not user-declared.

Downstream pipes reference it like any other entity:

```python
@DataPipes.pipe(
    pipeid="gold_current_customers",
    input_entity_ids=["silver.dim_customer.current"],  # ← auto-registered companion
    output_entity_id="gold.current_customers",
    ...
)
def gold_current_customers(dim_customer_current):
    # Already filtered to is_current=true rows — no temporal columns in projection.
    return dim_customer_current.select("customer_id", "name", "email", "region", "segment")
```

#### Why a companion entity (not a helper method)

| Aspect | Helper method (`read_entity_current()`) | Companion entity (proposed) |
|---|---|---|
| **Discovery** | Users must know to call it | Shows up in registry; visible in DAG views and introspection |
| **DAG participation** | Hidden — pipes reading SCD2 tables all depend on the base entity | Pipes depending on the current view have explicit dependency edges on `{entityid}.current` |
| **Watermarking** | Doesn't interact — helper returns a DataFrame | Companion has its own watermark cursor if read incrementally |
| **Change detection** | Detector sees the base entity | Detector can watch the companion's version stream independently |
| **Type parity** | Just a DataFrame — loses entity metadata | Same metadata shape as any entity (id, name, schema, tags) |
| **Third-party tooling** | Must know about the helper | Consumes the registry the same way for every entity |

The companion pattern is a better fit for a registry-based framework. The helper method is kept as a convenience (it's what `CurrentViewEntityProvider` calls under the hood), but it is no longer the primary API.

#### Implementation sketch

```python
# packages/kindling/data_entities.py (inside DataEntityManager)

def register_entity(self, entityid, **decorator_params):
    metadata = EntityMetadata(entityid, **decorator_params)
    self.registry[entityid] = metadata
    self.emit("entity.registered", {"entity_id": entityid})

    cfg = scd_config_from_tags(metadata)
    if cfg.enabled:
        self._register_scd2_current_companion(metadata, cfg)


def _register_scd2_current_companion(self, base: EntityMetadata, cfg: SCDConfig):
    """Auto-register a read-only companion entity filtered to current rows."""
    if cfg.current_entity_id in self.registry:
        # User declared the companion explicitly — defer to their definition.
        return

    companion = EntityMetadata(
        entityid=cfg.current_entity_id,
        name=f"{base.name} (current)",
        partition_columns=base.partition_columns,
        merge_columns=base.merge_columns,
        tags={
            **{k: v for k, v in base.tags.items() if not k.startswith("scd.")},
            "scd.companion_of": base.entityid,
            "scd.view_type": "current",
            "provider.read_only": "true",
        },
        schema=base.schema,  # framework will drop temporal cols in projection
    )
    self.registry[cfg.current_entity_id] = companion
    self.emit(
        "entity.scd2_companion_registered",
        {"base_entity_id": base.entityid, "companion_entity_id": cfg.current_entity_id},
    )
```

Provider resolution (`EntityProviderRegistry`) routes lookups for `scd.companion_of`-tagged entities to `CurrentViewEntityProvider`, which wraps the base entity's provider and filters on read.

### 3. SCD2 Merge Logic

The core implementation in `entity_provider_delta.py` uses Delta Lake's **staged updates** pattern — the standard approach for SCD2 with Delta merge.

#### Synthetic keys: staging-only merge routing

The `_merge_scd2` implementation below adds a column named `__merge_key` to the staged source DataFrame. This is a **staging-only routing handle**, not a persisted column — it exists for the duration of the merge and is dropped before any row is written to the target.

Why it's needed: Delta Lake's `MERGE` expresses, for each source row, either "MATCH → update the target row" *or* "NOT MATCH → insert the source row." It cannot express "for this one source row, both close the existing target row *and* insert a new version." SCD2 needs both behaviors for a changed business key. The staged-updates pattern gets around this by duplicating the source: one copy is keyed so that it matches (and closes the old row via `whenMatchedUpdate`), one copy is keyed so that it cannot match (and inserts the new version via `whenNotMatchedInsert`). The synthetic key is what makes the second copy deterministically fail the merge predicate.

**Not a surrogate key.** Dimensional modeling calls a stable, meaningless integer/UUID assigned to each dimension row a "surrogate" (sometimes "synthetic") key — e.g., `customer_sk`. That is a *persistent* column, lives in the target table forever, and gives fact tables a stable pointer to a specific version of a dimension row. This proposal does not introduce surrogate keys — business keys plus the `is_current` / `effective_date` columns uniquely identify a version. If future work wants fact→dimension version linkage (e.g., storing `customer_sk` on a fact row so it always points at the dimension version that was current when the fact occurred), adding a `{entityid}_sk` column is a separate design decision.

**Pattern prevalence.** The staging-only routing column is the canonical way to get atomic SCD2 with a single `MERGE` statement on any lakehouse engine that enforces the "one action per source row" constraint. Delta Lake's [official SCD2 documentation](https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation), Iceberg and Hudi tutorials, Databricks training materials, and community tooling (`dbt_utils.snapshot`, open-source SCD2 frameworks) all use close variants of the same row-duplication-with-routing-column approach. The only real design variable is how the routing column is computed — see open question 2 below.

#### Routing

Branch on the entity's SCD config (read from tags) in the existing merge entry point:

```python
def _merge_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference):
    cfg = scd_config_from_tags(entity)
    if cfg.enabled:
        self._merge_scd2(df, entity, cfg, table_ref)
    else:
        # Existing overwrite-in-place logic — unchanged
        merge_condition = self._build_merge_condition("old", "new", entity.merge_columns)
        (
            table_ref.get_delta_table()
            .alias("old")
            .merge(source=df.alias("new"), condition=merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
```

#### Staged Updates Merge

The staged-updates pattern works in a single Delta `MERGE` by splitting the source into two sets of rows:

1. **Rows with real merge keys** — these match existing current rows and close them (set `end_date`, clear `is_current`)
2. **Rows with null merge keys** — these force the NOT MATCHED path, inserting new version rows

```python
def _merge_scd2(self, df: DataFrame, entity, cfg: SCDConfig, table_ref: DeltaTableReference):
    """Merge using SCD Type 2 staged updates pattern.

    For each incoming row:
    - If business key does not exist in target → insert as new current row
    - If business key exists but tracked columns unchanged → no-op
    - If business key exists and tracked columns changed → close current row, insert new version
    """
    from pyspark.sql.functions import (
        col, concat_ws, current_timestamp, lit, md5, struct,
    )

    biz_keys = entity.merge_columns
    target = table_ref.get_delta_table()
    target_df = target.toDF()

    # Determine which columns to track for changes
    scd2_meta_cols = {cfg.effective_date_column, cfg.end_date_column, cfg.is_current_column}
    tracked = cfg.tracked_columns or [
        c for c in df.columns if c not in biz_keys and c not in scd2_meta_cols
    ]

    # Build change detection expression
    change_checks = [f"source.`{c}` != target.`{c}`" for c in tracked]
    # Handle nulls: also detect change when one side is null and other is not
    null_checks = [
        f"(source.`{c}` IS NULL != target.`{c}` IS NULL)" for c in tracked
    ]
    change_condition = " OR ".join(
        [f"({ck} OR {nk})" for ck, nk in zip(change_checks, null_checks)]
    )

    # --- Stage 1: Build staged updates DataFrame ---
    # Current records in target that we might need to close
    current_target = target_df.filter(col(cfg.is_current_column) == True)

    # Build the routing-key expression per cfg.routing_key_method
    # (see Synthetic keys subsection above for rationale; default is "hash")
    def _routing_key_expr_for(alias: Optional[str] = None):
        from pyspark.sql.functions import sha2, to_json, struct
        ref = lambda k: col(f"{alias}.`{k}`") if alias else col(f"`{k}`")
        if cfg.routing_key_method == "hash":
            return sha2(to_json(struct(*[ref(k) for k in biz_keys])), 256)
        # "concat"
        return concat_ws("||", *[ref(k).cast("string") for k in biz_keys])

    # Identify source rows that represent actual changes
    # These get a NULL mergeKey so they hit the NOT MATCHED branch (insert as new version)
    changed_rows = (
        df.alias("source")
        .join(current_target.alias("target"), biz_keys, "inner")
        .where(change_condition)
        .select("source.*")
        .withColumn("__merge_key", lit(None).cast("string"))
    )

    # All source rows get their real routing key — these hit the MATCHED branch
    # (to close old rows) and also the NOT MATCHED branch for truly new keys
    keyed_rows = df.withColumn("__merge_key", _routing_key_expr_for())

    staged = changed_rows.unionByName(keyed_rows, allowMissingColumns=True)

    # --- Stage 2: Execute the merge ---
    # Target side uses the same routing-key expression; NULL staged.__merge_key
    # values for the "changed_rows" duplicates fail this equality and fall
    # through to the NOT MATCHED insert branch.
    target_routing_expr_sql = (
        f"sha2(to_json(named_struct({', '.join(f\"'{k}', target.`{k}`\" for k in biz_keys)})), 256)"
        if cfg.routing_key_method == "hash"
        else f"concat_ws('||', {', '.join(f'CAST(target.`{k}` AS STRING)' for k in biz_keys)})"
    )
    top_merge_condition = (
        f"{target_routing_expr_sql} = staged.__merge_key "
        f"AND target.`{cfg.is_current_column}` = true"
    )

    # Build insert values map — all source columns plus SCD2 metadata
    source_cols = [c for c in df.columns if c not in scd2_meta_cols]
    insert_values = {f"`{c}`": f"staged.`{c}`" for c in source_cols}
    insert_values[f"`{cfg.effective_date_column}`"] = "current_timestamp()"
    insert_values[f"`{cfg.end_date_column}`"] = "NULL"
    insert_values[f"`{cfg.is_current_column}`"] = "true"

    (
        target.alias("target")
        .merge(source=staged.alias("staged"), condition=top_merge_condition)
        .whenMatchedUpdate(
            set={
                f"`{cfg.end_date_column}`": "current_timestamp()",
                f"`{cfg.is_current_column}`": "false",
            }
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
```

#### How the staged updates pattern works

```
Source DataFrame (incoming):     Target Table (existing):
┌──────┬───────┬─────────┐       ┌──────┬───────┬─────────┬────────┬────────┬─────────┐
│ id   │ name  │ region  │       │ id   │ name  │ region  │ eff_dt │ end_dt │ current │
├──────┼───────┼─────────┤       ├──────┼───────┼─────────┼────────┼────────┼─────────┤
│ 1    │ Alice │ West    │ ←changed│ 1  │ Alice │ East    │ Jan 1  │ null   │ true    │
│ 2    │ Bob   │ North   │ ←same │ 2    │ Bob   │ North   │ Jan 1  │ null   │ true    │
│ 3    │ Carol │ South   │ ←new  │      │       │         │        │        │         │
└──────┴───────┴─────────┘       └──────┴───────┴─────────┘────────┴────────┴─────────┘

Staged DataFrame:
┌────────────┬──────┬───────┬─────────┐
│ __merge_key│ id   │ name  │ region  │
├────────────┼──────┼───────┼─────────┤
│ NULL       │ 1    │ Alice │ West    │  ← changed row, NULL key → NOT MATCHED → INSERT
│ 1          │ 1    │ Alice │ West    │  ← real key → MATCHED → UPDATE (close old row)
│ 2          │ 2    │ Bob   │ North   │  ← real key → MATCHED → UPDATE (but no change — skip*)
│ 3          │ 3    │ Carol │ South   │  ← real key → NOT MATCHED (new) → INSERT
└────────────┴──────┴───────┴─────────┘

After merge:
┌──────┬───────┬─────────┬────────┬────────┬─────────┐
│ id   │ name  │ region  │ eff_dt │ end_dt │ current │
├──────┼───────┼─────────┼────────┼────────┼─────────┤
│ 1    │ Alice │ East    │ Jan 1  │ Feb 26 │ false   │  ← closed
│ 1    │ Alice │ West    │ Feb 26 │ null   │ true    │  ← new version
│ 2    │ Bob   │ North   │ Jan 1  │ Feb 26 │ false   │  ← closed (*)
│ 2    │ Bob   │ North   │ Feb 26 │ null   │ true    │  ← re-inserted same (*)
│ 3    │ Carol │ South   │ Feb 26 │ null   │ true    │  ← new row
└──────┴───────┴─────────┘────────┴────────┴─────────┘

(*) Note: The basic staged-updates pattern closes and re-inserts unchanged rows.
    This is acceptable for most dimension sizes. For optimization on very large
    dimensions, see "Optimized Change Detection" in the appendix.
```

### 4. Signals

New signal definitions in `EntityProvider.EMITS`:

```python
EMITS = [
    # ... existing signals ...
    "entity.before_scd2_merge",
    "entity.after_scd2_merge",
    "entity.scd2_merge_failed",
]
```

Signal payloads:

```python
# Before SCD2 merge
"entity.before_scd2_merge" → {
    entity_id: str,
    source_row_count: int,
    tracked_columns: list[str],
}

# After SCD2 merge (from Delta merge metrics)
"entity.after_scd2_merge" → {
    entity_id: str,
    rows_inserted: int,      # New versions + truly new rows
    rows_updated: int,        # Closed (expired) rows
    rows_unchanged: int,      # Matched but no tracked column change
    duration_seconds: float,
}

# SCD2 merge failure
"entity.scd2_merge_failed" → {
    entity_id: str,
    error: str,
    error_type: str,
}
```

### 5. Read APIs

Three ways to read an SCD2 entity, in order of preferred use:

#### 5a. Depend on the auto-registered companion (primary API)

```python
@DataPipes.pipe(
    pipeid="...",
    input_entity_ids=["silver.dim_customer.current"],  # companion
    ...
)
def ...(dim_customer_current):
    # Already filtered to is_current=true rows.
    ...
```

This is the idiomatic path. Pipes never need to know the base table has history — they just depend on the view-shaped companion.

#### 5b. Read the historized table directly

```python
@DataPipes.pipe(
    pipeid="customer_history_audit",
    input_entity_ids=["silver.dim_customer"],  # base entity, full history
    ...
)
def customer_history_audit(dim_customer):
    # All rows including closed versions. Useful for audit, change analysis.
    ...
```

For use cases that actually need the history (audit, change-over-time analytics). The pipe sees all rows including temporal columns.

#### 5c. Point-in-time query helper

For ad-hoc or procedural use, a helper on `DeltaEntityProvider` supports point-in-time reads:

```python
def read_entity_as_of(self, entity, point_in_time) -> DataFrame:
    """Read entity state as it was at a specific point in time.

    For SCD2 entities: returns rows where effective_date ≤ point_in_time and
    (end_date > point_in_time OR end_date IS NULL).

    For non-SCD2 entities: falls back to Delta time travel by timestamp.
    """
    from pyspark.sql.functions import col

    cfg = scd_config_from_tags(entity)
    if not cfg.enabled:
        # Fallback: Delta time travel
        table_ref = self._get_table_reference(entity)
        return (
            self.spark.read.format("delta")
            .option("timestampAsOf", str(point_in_time))
            .load(table_ref.table_path)
        )

    df = self.read_entity(entity)
    return df.filter(
        (col(cfg.effective_date_column) <= point_in_time)
        & (
            col(cfg.end_date_column).isNull()
            | (col(cfg.end_date_column) > point_in_time)
        )
    )
```

Point-in-time is not (yet) exposed as an auto-registered companion — unlike "current", a PIT query is parameterized by timestamp and doesn't map cleanly to a static companion id. A follow-up could add `{entityid}.as_of_{suffix}` parameterized companions if a concrete use case emerges.

### 6. Validation

Validation runs once at registration time, inside `scd_config_from_tags` + a `_validate_scd_config` check in `DataEntityManager.register_entity`:

```python
def _validate_scd_config(self, entity: EntityMetadata):
    """Validate SCD tag configuration at registration time."""
    cfg = scd_config_from_tags(entity)  # raises on invalid scd.type value
    if not cfg.enabled:
        return

    if not entity.merge_columns:
        raise ValueError(
            f"Entity '{entity.entityid}': SCD Type 2 requires merge_columns "
            f"(business keys) to be defined"
        )

    if cfg.tracked_columns:
        overlap = set(cfg.tracked_columns) & set(entity.merge_columns)
        if overlap:
            raise ValueError(
                f"Entity '{entity.entityid}': scd.tracked must not include "
                f"merge_columns (business keys): {overlap}"
            )

    # Temporal column names must not collide with business columns.
    temporal = {cfg.effective_date_column, cfg.end_date_column, cfg.is_current_column}
    schema_names = {f.name for f in entity.schema.fields} if entity.schema else set()
    biz_collision = temporal & (schema_names - temporal)
    if biz_collision:
        raise ValueError(
            f"Entity '{entity.entityid}': temporal column names {biz_collision} "
            f"collide with business columns. Override via scd.effective_col / "
            f"scd.end_col / scd.current_col tags."
        )
```

## Integration With Existing Features

### Watermarking

Watermarks track **source entity versions** and are orthogonal to SCD2. A pipe that reads `bronze.customers` (watermarked) and writes to `silver.dim_customer` (SCD2) works as follows:

1. Watermark reads incremental changes from `bronze.customers` since last processed version
2. Pipe transforms the incoming DataFrame
3. Provider merges using SCD2 logic into `silver.dim_customer`
4. Watermark is saved for `bronze.customers`

No changes needed to `WatermarkService` or `SimpleReadPersistStrategy`.

### Streaming Dimension Restart

The existing `DimensionChangeDetector` proposal (signal_dag_streaming_proposal.md) monitors dimension tables by Delta version. SCD2 writes create new Delta versions just like SCD1 writes, so change detection works unchanged. The detector does not need to know whether the dimension uses SCD1 or SCD2 — it only cares that the version incremented.

### Multi-Provider Architecture

SCD2 support is implemented in `DeltaEntityProvider` specifically, since the staged-updates merge pattern requires Delta Lake's `MERGE` semantics. Other providers (`CSVEntityProvider`, `MemoryEntityProvider`) ignore `scd_type` — attempting SCD2 on a non-Delta provider raises a clear error.

### Entity Provider Registry

No changes needed. The registry routes to the appropriate provider, and SCD2 logic is entirely within the Delta provider.

## Implementation Phases

### Phase 1: Declarative Surface (Small)

**Goal:** Establish the tag-based declaration without behavioral changes.

**Deliverables:**
- `SCDConfig` dataclass + `scd_config_from_tags(entity)` helper in `data_entities.py` (read-only derivation; no new fields on `EntityMetadata`).
- Registration-time validation (`_validate_scd_config`).
- Schema auto-augmentation in `ensure_entity_table` — gated on `cfg.enabled`.

**Success criteria:**
- Existing entities unaffected (no `scd.type` tag → no-op).
- Entities tagged `scd.type=2` register with temporal columns in schema.
- Misconfigurations (invalid `scd.type`, tracked/key overlap, temporal/business column collision) caught at registration.

### Phase 2: Companion Entity Auto-Registration (Small)

**Goal:** Auto-register `{entityid}.current` companions for tagged entities.

**Deliverables:**
- `_register_scd2_current_companion` in `DataEntityManager`.
- `CurrentViewEntityProvider` that wraps a base provider and filters on read.
- `EntityProviderRegistry` routing for companion entities (tag `scd.companion_of`).
- Read-only enforcement (`provider.read_only` tag).
- `entity.scd2_companion_registered` signal.

**Success criteria:**
- Registering an SCD2 entity makes `{entityid}.current` discoverable in the registry.
- Pipes can declare `input_entity_ids=["{entityid}.current"]` and get filtered-to-current data.
- Writing to a companion raises a clear error.
- User-declared companion (if explicit) wins over auto-registration.

### Phase 3: Core Merge Logic (Medium)

**Goal:** Working SCD2 merge via the staged-updates pattern.

**Deliverables:**
- `_merge_scd2` method in `DeltaEntityProvider`.
- Routing logic in `_merge_to_delta_table` (branches on `scd_config_from_tags(entity).enabled`).
- SCD2 signals (`before_scd2_merge`, `after_scd2_merge`, `scd2_merge_failed`).
- Unit tests with `MemoryEntityProvider` mock and integration tests with Delta.

**Success criteria:**
- Changed rows produce a closed record + new version.
- Unchanged rows handled correctly.
- New business keys inserted as current.
- Merge metrics emitted via signals.

### Phase 4: Ergonomics and Optimization (Optional)

**Goal:** Performance tuning and additional ergonomics.

**Deliverables:**
- `read_entity_as_of()` point-in-time helper on `DeltaEntityProvider` (companion-based PIT is out of scope — use the helper).
- Pre-filter optimization: hash-based change detection to skip unchanged rows before the merge.
- Composite business key support (multiple `merge_columns`).
- Z-ORDER recommendation via signals when SCD2 table size exceeds threshold.
- Migration path helper for converting existing non-SCD2 entities to SCD2.

## Trade-offs and Alternatives

### Staged Updates vs. Two-Pass Merge

| Aspect | Staged Updates (proposed) | Two-Pass (separate UPDATE then INSERT) |
|--------|--------------------------|---------------------------------------|
| **Atomicity** | Single MERGE = atomic | Two operations = risk of partial state |
| **Performance** | One scan of target | Two scans of target |
| **Complexity** | Higher (null-key trick) | Lower (straightforward SQL) |
| **Correctness** | Proven Delta Lake pattern | Requires explicit transaction control |

**Decision:** Staged updates. Atomicity and single-scan performance outweigh the slight complexity increase. This is the [documented Delta Lake best practice](https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation).

### Framework-Level vs. Pipe-Level SCD2

| Aspect | Framework-level (proposed) | Pipe-level (user writes SCD2 in transform) |
|--------|---------------------------|-------------------------------------------|
| **Consistency** | Enforced across all SCD2 entities | Each pipe may implement differently |
| **Boilerplate** | None — declare `scd_type=2` | Significant per-entity |
| **Observability** | Built-in signals and metrics | Manual logging |
| **Flexibility** | Covers standard SCD2 | Can handle edge cases |

**Decision:** Framework-level for the common case. Users with exotic SCD2 requirements (e.g., bi-temporal, custom merge predicates) can still implement custom pipe logic.

### SCD2 Column Naming

| Option | Pros | Cons |
|--------|------|------|
| **Dunder prefix (`__effective_date`)** | Won't collide with business columns | Unconventional |
| **Configurable (proposed)** | Users choose names | Slightly more config |
| **Fixed names** | Zero config | Risk of collision |

**Decision:** Configurable with dunder-prefixed defaults. The `SCD2Config` dataclass lets users override column names while the defaults avoid collisions.

## Open Questions

### Resolved

- **Unchanged row handling** (resolved 2026-04-20): deferred to Phase 4 as an opt-in optimization users enable via `scd.optimize_unchanged: "true"` on the entity. Default behavior is the simpler close-and-reinsert approach; the pre-filter hash-based optimization is opt-in for teams managing very large dimensions where row churn matters.

- **Composite business keys / synthetic key construction** (resolved 2026-04-20): made configurable via `scd.routing_key` tag. Default is `hash` — `sha2(to_json(struct(*merge_columns)), 256)` — which is safe for any value type, null handling, and composite keys with negligible (~0.5–2% of merge time) cost. Opt-in `concat` — `concat_ws("||", ...)` — for teams with controlled key types who want debuggability and don't care about the ~5× per-row cost difference. See the "Synthetic keys" subsection in Section 3.

- **Bi-temporal support** (out of scope, 2026-04-20): The framework tracks only **system time** — when the framework observed and wrote each version. `__effective_date` is always `current_timestamp()` at merge time. A bi-temporal design that adds business time (when the change actually occurred in reality, separate from when we learned about it) is explicitly not in scope for this proposal. Teams that need bi-temporal semantics — late-arriving truth, retroactive corrections, "what did the system think was true on date X" audit queries — should handle it in their own pipe logic or open a separate proposal if it becomes a recurring need.

- **Delete handling** (resolved 2026-04-20): Opt-in via `scd.close_on_missing: "true"` tag. Default behavior is unchanged — absence in a source batch does not close a current row, matching the expectation of incremental feeds. When the tag is set, the SCD2 merge also closes any current row in the target whose business key isn't present in the source batch (implemented via Delta's `whenNotMatchedBySourceUpdate` clause setting `is_current=false` and `end_date=current_timestamp()`). This targets teams running full-snapshot feeds where absence genuinely means "deleted upstream." Explicitly opt-in because the default interpretation (silence = no change) is safer for the more common incremental-feed case.

### Open

1. **Reverting SCD2 → non-SCD2** (current leaning: not supported): If a user removes the `scd.type` tag from an entity whose target table carries temporal columns from a prior SCD2 lifetime, historical rows remain physically in the table but become semantically orphaned — no code knows they exist or how to interpret them. Current thinking is that reversion should not be a supported operation at all; the right path is to register a new entity under a different id and migrate data explicitly if needed. Enforce by erroring at registration time when the target table's schema has the temporal columns but the entity no longer declares `scd.type=2`. Revisit if real migration workflows expose friction with the "new entity" requirement, particularly around downstream pipes' input ids.

## Success Criteria

### Functional Requirements

- [ ] Entities can declare `scd_type=2` with optional `SCD2Config`
- [ ] Temporal columns auto-added to schema on `ensure_entity_table`
- [ ] Changed rows produce closed record + new version row
- [ ] Unchanged rows handled without data loss
- [ ] New business keys inserted as current
- [ ] `read_entity_current()` returns only active rows
- [ ] `read_entity_as_of()` returns correct point-in-time snapshot
- [ ] Existing SCD1 entities unaffected
- [ ] Misconfigurations caught at registration time

### Non-Functional Requirements

- [ ] SCD2 merge overhead < 2x SCD1 merge for same data volume
- [ ] Signals emitted for all SCD2 operations
- [ ] Works on all supported platforms (Fabric, Synapse, Databricks)
- [ ] Compatible with existing watermarking pipeline
- [ ] Compatible with existing streaming dimension detection

### Production Readiness

- [ ] Unit tests for metadata, validation, schema augmentation
- [ ] Integration tests for SCD2 merge (changed, unchanged, new, composite keys)
- [ ] Point-in-time read tests
- [ ] Platform compatibility tests
- [ ] Documentation with examples
- [ ] Migration guide for converting SCD1 entities to SCD2

## References

- [Delta Lake SCD Type 2 Documentation](https://docs.delta.io/latest/delta-update.html#slowly-changing-data-scd-type-2-operation)
- [Signal-Driven Streaming Proposal — SCD Section](signal_dag_streaming_proposal.md)
- [DAG Execution Plan — Dimension Change Detection](dag_execution_implementation_plan.md)
- [Entity Providers Documentation](../entity_providers.md)
- [Data Entities Documentation](../data_entities.md)
- [Watermarking Documentation](../watermarking.md)
- [Config-Based Entity Providers](config_based_entity_providers.md)

## Appendix

### A. Optimized Change Detection

For very large dimensions where re-inserting unchanged rows is unacceptable, a pre-filter step can reduce the staged DataFrame to only truly changed and new rows:

```python
def _prefilter_changes(self, source_df, target_df, biz_keys, tracked_cols, cfg):
    """Filter source to only rows that represent actual changes or new keys."""
    from pyspark.sql.functions import col, md5, concat_ws, coalesce, lit

    # Hash tracked columns for efficient comparison
    def hash_cols(df, alias, cols):
        return df.withColumn(
            f"__{alias}_hash",
            md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("__NULL__")) for c in cols]))
        )

    source_hashed = hash_cols(source_df, "src", tracked_cols)
    target_current = target_df.filter(col(cfg.is_current_column) == True)
    target_hashed = hash_cols(target_current, "tgt", tracked_cols)

    # Left anti join: rows in source with no matching key in target (new rows)
    new_rows = source_hashed.join(target_hashed, biz_keys, "left_anti")

    # Inner join where hashes differ (changed rows)
    changed_rows = (
        source_hashed.alias("s")
        .join(target_hashed.alias("t"), biz_keys, "inner")
        .where("s.__src_hash != t.__tgt_hash")
        .select("s.*")
    )

    return new_rows.unionByName(changed_rows).drop("__src_hash")
```

This adds a scan but eliminates unnecessary close+reinsert cycles for unchanged rows. Recommended when dimension tables exceed ~1M rows.

### B. SCD1 to SCD2 Migration Helper

For teams converting an existing SCD1 entity to SCD2, a backfill utility initializes the temporal columns:

```python
def backfill_scd2_columns(spark, entity, table_ref):
    """One-time backfill: add SCD2 columns to existing SCD1 table.

    Sets all existing rows as current with effective_date = table creation time.
    """
    cfg = entity.scd2_config
    delta_table = table_ref.get_delta_table()

    # Get table creation timestamp from history
    creation_time = (
        delta_table.history()
        .orderBy("version")
        .select("timestamp")
        .first()[0]
    )

    # Add columns via ALTER TABLE + UPDATE
    spark.sql(f"""
        ALTER TABLE delta.`{table_ref.table_path}`
        ADD COLUMNS (
            `{cfg.effective_date_column}` TIMESTAMP,
            `{cfg.end_date_column}` TIMESTAMP,
            `{cfg.is_current_column}` BOOLEAN
        )
    """)

    delta_table.update(
        set={
            f"`{cfg.effective_date_column}`": f"CAST('{creation_time}' AS TIMESTAMP)",
            f"`{cfg.end_date_column}`": "NULL",
            f"`{cfg.is_current_column}`": "true",
        }
    )
```

### C. Example: Full Pipeline With SCD2

```python
from kindling.data_entities import DataEntities
from kindling.data_pipes import DataPipes
from pyspark.sql.functions import col, lower, trim, upper
from pyspark.sql.types import StringType, StructField, StructType

# --- Bronze: raw customer feed (overwrite latest) ---
@DataEntities.entity(
    entityid="bronze.customers",
    name="Raw Customer Feed",
    merge_columns=["customer_id"],
    partition_columns=[],
    tags={"layer": "bronze"},
    schema=StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("region", StringType(), True),
        StructField("segment", StringType(), True),
    ]),
)

# --- Silver: customer dimension (SCD Type 2) ---
# `scd.type=2` is all it takes to enable history tracking.
# The framework auto-augments the schema with __effective_date / __end_date /
# __is_current and auto-registers a read-only companion at "silver.dim_customer.current".
@DataEntities.entity(
    entityid="silver.dim_customer",
    name="Customer Dimension",
    merge_columns=["customer_id"],
    partition_columns=[],
    tags={
        "layer": "silver",
        "domain": "customer",
        "scd.type": "2",
        "scd.tracked": "name,email,region,segment",
    },
    schema=StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("region", StringType(), True),
        StructField("segment", StringType(), True),
        # temporal columns added automatically during ensure_entity_table
    ]),
)

# --- Pipe: Bronze → Silver with SCD2 ---
@DataPipes.pipe(
    pipeid="load_dim_customer",
    name="Load Customer Dimension",
    input_entity_ids=["bronze.customers"],
    output_entity_id="silver.dim_customer",
    output_type="table",
    tags={"layer": "silver"},
)
def load_dim_customer(bronze_customers):
    """Clean and standardize customer data.

    No SCD2 logic here — the framework handles historization automatically
    based on the entity's scd.type=2 tag.
    """
    return (
        bronze_customers
        .filter(col("customer_id").isNotNull())
        .withColumn("email", lower(col("email")))
        .withColumn("region", upper(trim(col("region"))))
    )

# --- Gold: current customer view ---
# Reads from the auto-registered companion entity; no filter code needed, no
# knowledge that the base is historized.
@DataPipes.pipe(
    pipeid="gold_current_customers",
    name="Current Customer View",
    input_entity_ids=["silver.dim_customer.current"],  # auto-registered companion
    output_entity_id="gold.current_customers",
    output_type="table",
    tags={"layer": "gold"},
)
def gold_current_customers(dim_customer_current):
    """Snapshot of current customer state.

    Receives only is_current=true rows — the companion filters upstream.
    """
    return dim_customer_current.select(
        "customer_id", "name", "email", "region", "segment"
    )
```
