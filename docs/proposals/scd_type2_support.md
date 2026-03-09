# SCD Type 2 Support for Kindling

**Status:** Draft
**Author:** System Analysis
**Created:** 2026-02-26
**Updated:** 2026-02-26
**Related:** signal_dag_streaming_proposal.md, dag_execution_implementation_plan.md, config_based_entity_providers.md

## Executive Summary

This proposal adds **Slowly Changing Dimension Type 2 (SCD2)** write semantics to Kindling's entity provider layer. Today Kindling's merge operation implements SCD Type 1 — it overwrites matched rows in place. SCD2 preserves history by closing the current row (setting an end date and clearing the `is_current` flag) and inserting a new version row whenever tracked columns change.

The design extends `EntityMetadata` with an optional `scd_type` field and `SCD2Config` dataclass, adds a staged-updates merge path in `DeltaEntityProvider`, auto-augments schemas with temporal columns, and exposes convenience read methods (`read_entity_current`, `read_entity_as_of`). All changes are backward-compatible — entities without `scd_type` continue to behave exactly as they do today.

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

1. **Declarative** — SCD behavior is declared on the entity, not embedded in pipe logic
2. **Backward-compatible** — existing entities default to `scd_type=1` with zero behavioral change
3. **Convention over configuration** — sensible defaults for column names and tracking scope
4. **Observable** — dedicated signals for SCD2 merge outcomes
5. **Composable** — works with existing watermarking, streaming restart, and multi-provider architecture

### Architecture Overview

```
Entity Declaration                    DeltaEntityProvider
┌──────────────────────┐              ┌─────────────────────────────┐
│ merge_columns (keys) │─────────────▶│ _merge_to_delta_table()     │
│ scd_type = 2         │              │   ├─ scd_type==1: current   │
│ scd2_config:         │              │   │   whenMatchedUpdateAll() │
│   tracked_columns    │              │   └─ scd_type==2: staged    │
│   effective_date_col │              │       _merge_scd2()         │
│   end_date_col       │              ├─────────────────────────────┤
│   is_current_col     │              │ read_entity_current()       │
└──────────────────────┘              │ read_entity_as_of()         │
                                      └─────────────────────────────┘

SimpleReadPersistStrategy
┌─────────────────────────────────────┐
│ persist_lambda()                    │
│   checks entity.scd_type            │
│   delegates to provider merge       │
│   (no SCD-specific logic needed)    │
└─────────────────────────────────────┘
```

The strategy layer does not change — SCD2 logic is encapsulated entirely in the provider, triggered by metadata on the entity.

## Detailed Design

### 1. Entity Metadata Extension

Add `scd_type` and `scd2_config` to `EntityMetadata` in `data_entities.py`:

```python
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class SCD2Config:
    """Configuration for SCD Type 2 history tracking.

    Attributes:
        tracked_columns: Columns to monitor for changes. When any of these
            change between source and target, the existing row is closed and
            a new version is inserted. None means all non-key columns.
        effective_date_column: Column storing when this version became active.
        end_date_column: Column storing when this version was superseded.
            Null means the row is current.
        is_current_column: Boolean flag column for fast current-state queries.
    """
    tracked_columns: Optional[List[str]] = None
    effective_date_column: str = "__effective_date"
    end_date_column: str = "__end_date"
    is_current_column: str = "__is_current"


@dataclass
class EntityMetadata:
    entityid: str
    name: str
    partition_columns: List[str]
    merge_columns: List[str]
    tags: Dict[str, str]
    schema: Any
    scd_type: int = 1                           # 1 = overwrite, 2 = historized
    scd2_config: Optional[SCD2Config] = None    # Required when scd_type=2
```

User declaration:

```python
@DataEntities.entity(
    entityid="silver.dim_customer",
    name="Customer Dimension",
    merge_columns=["customer_id"],
    scd_type=2,
    scd2_config=SCD2Config(
        tracked_columns=["name", "email", "region", "segment"],
    ),
    partition_columns=[],
    tags={"layer": "silver", "domain": "customer"},
    schema=customer_dim_schema,
)
```

Entities that omit `scd_type` default to `1` and behave identically to today.

### 2. Schema Auto-Augmentation

When `scd_type=2`, `ensure_entity_table` automatically appends the three temporal columns to the user-provided schema if they are not already present:

```python
def _augment_schema_for_scd2(self, schema, scd2_config: SCD2Config):
    """Add SCD2 temporal columns to schema if not already present."""
    from pyspark.sql.types import BooleanType, StructField, TimestampType

    existing_names = {f.name for f in schema.fields}
    extra_fields = []

    if scd2_config.effective_date_column not in existing_names:
        extra_fields.append(
            StructField(scd2_config.effective_date_column, TimestampType(), False)
        )
    if scd2_config.end_date_column not in existing_names:
        extra_fields.append(
            StructField(scd2_config.end_date_column, TimestampType(), True)
        )
    if scd2_config.is_current_column not in existing_names:
        extra_fields.append(
            StructField(scd2_config.is_current_column, BooleanType(), False)
        )

    if extra_fields:
        from pyspark.sql.types import StructType
        return StructType(schema.fields + extra_fields)

    return schema
```

This keeps entity declarations clean — users define business columns, the framework manages the history plumbing.

### 3. SCD2 Merge Logic

The core implementation in `entity_provider_delta.py` uses Delta Lake's **staged updates** pattern — the standard approach for SCD2 with Delta merge.

#### Routing

Branch on `scd_type` in the existing merge entry point:

```python
def _merge_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference):
    if getattr(entity, "scd_type", 1) == 2:
        self._merge_scd2(df, entity, table_ref)
    else:
        # Existing SCD1 logic — unchanged
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
def _merge_scd2(self, df: DataFrame, entity, table_ref: DeltaTableReference):
    """Merge using SCD Type 2 staged updates pattern.

    For each incoming row:
    - If business key does not exist in target → insert as new current row
    - If business key exists but tracked columns unchanged → no-op
    - If business key exists and tracked columns changed → close current row, insert new version
    """
    from pyspark.sql.functions import (
        col, concat_ws, current_timestamp, lit, md5, struct,
    )

    cfg = entity.scd2_config
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

    # Identify source rows that represent actual changes
    # These get a NULL mergeKey so they hit the NOT MATCHED branch (insert as new version)
    changed_rows = (
        df.alias("source")
        .join(current_target.alias("target"), biz_keys, "inner")
        .where(change_condition)
        .select("source.*")
        .withColumn("__merge_key", lit(None).cast("string"))
    )

    # All source rows get their real key — these hit the MATCHED branch (to close old rows)
    # and also the NOT MATCHED branch for truly new keys
    keyed_rows = df.withColumn(
        "__merge_key",
        concat_ws("||", *[col(f"`{k}`").cast("string") for k in biz_keys]),
    )

    staged = changed_rows.unionByName(keyed_rows, allowMissingColumns=True)

    # --- Stage 2: Execute the merge ---
    # Build merge condition on the synthetic __merge_key and composite business key
    target_key_expr = concat_ws(
        "||", *[col(f"target.`{k}`").cast("string") for k in biz_keys]
    )
    merge_condition = f"target.`{cfg.is_current_column}` = true AND " + " AND ".join(
        [f"target.`{k}` = staged.`{k}`" for k in biz_keys]
    )
    # Use __merge_key for the top-level condition so NULL keys bypass matching
    top_merge_condition = (
        f"concat_ws('||', {', '.join(f'CAST(target.`{k}` AS STRING)' for k in biz_keys)})"
        f" = staged.__merge_key"
        f" AND target.`{cfg.is_current_column}` = true"
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

### 5. Convenience Read Methods

Add to `DeltaEntityProvider` (and the `EntityProvider` abstract base):

```python
def read_entity_current(self, entity) -> DataFrame:
    """Read only current (active) records from an SCD2 entity.

    For non-SCD2 entities, returns the full table (equivalent to read_entity).
    """
    df = self.read_entity(entity)
    if getattr(entity, "scd_type", 1) == 2:
        cfg = entity.scd2_config
        return df.filter(col(cfg.is_current_column) == True)
    return df


def read_entity_as_of(self, entity, point_in_time) -> DataFrame:
    """Read entity state as it was at a specific point in time.

    Returns rows that were active at the given timestamp: their effective_date
    is on or before the point_in_time, and their end_date is after it (or null).

    For non-SCD2 entities, falls back to Delta time travel if the timestamp
    can be resolved to a version.

    Args:
        entity: Entity metadata
        point_in_time: Timestamp to query (datetime or string parseable by Spark)
    """
    if getattr(entity, "scd_type", 1) != 2:
        # Fallback: Delta time travel
        table_ref = self._get_table_reference(entity)
        return (
            self.spark.read.format("delta")
            .option("timestampAsOf", str(point_in_time))
            .load(table_ref.table_path)
        )

    cfg = entity.scd2_config
    df = self.read_entity(entity)
    return df.filter(
        (col(cfg.effective_date_column) <= point_in_time)
        & (
            col(cfg.end_date_column).isNull()
            | (col(cfg.end_date_column) > point_in_time)
        )
    )
```

### 6. Validation

Add validation in `DataEntityManager.register_entity` to catch misconfigurations early:

```python
def _validate_scd_config(self, entity: EntityMetadata):
    """Validate SCD configuration at registration time."""
    if entity.scd_type not in (1, 2):
        raise ValueError(
            f"Entity '{entity.entityid}': scd_type must be 1 or 2, got {entity.scd_type}"
        )

    if entity.scd_type == 2:
        if not entity.merge_columns:
            raise ValueError(
                f"Entity '{entity.entityid}': SCD Type 2 requires merge_columns "
                f"(business keys) to be defined"
            )
        if entity.scd2_config is None:
            # Apply defaults
            entity.scd2_config = SCD2Config()

        # Warn if tracked_columns overlap with merge_columns
        if entity.scd2_config.tracked_columns:
            overlap = set(entity.scd2_config.tracked_columns) & set(entity.merge_columns)
            if overlap:
                raise ValueError(
                    f"Entity '{entity.entityid}': tracked_columns should not include "
                    f"merge_columns (business keys): {overlap}"
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

### Phase 1: Metadata and Validation (Small)

**Goal:** Establish the declarative surface without behavioral changes.

**Deliverables:**
- `SCD2Config` dataclass
- `scd_type` and `scd2_config` fields on `EntityMetadata`
- Registration-time validation
- Schema auto-augmentation in `ensure_entity_table`

**Success criteria:**
- Existing entities unaffected (scd_type defaults to 1)
- SCD2 entities register with temporal columns in schema
- Misconfigurations caught at registration time

### Phase 2: Core Merge Logic (Medium)

**Goal:** Working SCD2 merge via the staged-updates pattern.

**Deliverables:**
- `_merge_scd2` method in `DeltaEntityProvider`
- Routing logic in `_merge_to_delta_table`
- SCD2 signals (`before_scd2_merge`, `after_scd2_merge`, `scd2_merge_failed`)
- Unit tests with `MemoryEntityProvider` mock and integration tests with Delta

**Success criteria:**
- Changed rows produce a closed record + new version
- Unchanged rows handled correctly
- New business keys inserted as current
- Merge metrics emitted via signals

### Phase 3: Read Methods and Ergonomics (Small)

**Goal:** First-class support for querying SCD2 tables.

**Deliverables:**
- `read_entity_current()` on `EntityProvider` and `DeltaEntityProvider`
- `read_entity_as_of()` with point-in-time semantics
- Abstract method stubs on `EntityProvider` base class
- Documentation and examples

**Success criteria:**
- `read_entity_current` returns only `is_current=true` rows
- `read_entity_as_of` returns correct snapshot for any timestamp
- Non-SCD2 entities degrade gracefully (full read or Delta time travel)

### Phase 4: Optimizations (Optional)

**Goal:** Performance tuning for large dimensions.

**Deliverables:**
- Pre-filter optimization: hash-based change detection to skip unchanged rows before the merge
- Composite business key support (multiple merge_columns)
- Z-ORDER recommendation via signals when SCD2 table size exceeds threshold
- Migration path helper for converting existing SCD1 entities to SCD2

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

1. **Unchanged row handling:** The basic staged-updates pattern closes and re-inserts rows even when tracked columns haven't changed. Should Phase 1 include a pre-filter optimization, or is the simpler approach acceptable for typical dimension sizes (< 10M rows)?

2. **Composite business keys:** The `__merge_key` concatenation approach works for most types but may have edge cases with delimiters in values. Should we use `struct()` hashing instead of `concat_ws`?

3. **SCD2 → SCD1 migration:** If a user changes an entity from `scd_type=2` back to `scd_type=1`, should the framework warn, error, or silently revert to overwrite semantics (leaving historical rows orphaned)?

4. **Bi-temporal support:** Some use cases need both "system time" (when the change was recorded) and "business time" (when the change occurred in reality). Should `SCD2Config` support an optional `business_time_column` for user-supplied effective dates, or is this out of scope?

5. **Delete handling:** When a business key is present in the target but absent from the source, should SCD2 close the row (soft delete) or leave it as-is? Current proposal leaves it as-is (no deletes). Should a `close_on_missing` option be added?

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
from kindling.data_entities import DataEntities, SCD2Config
from kindling.data_pipes import DataPipes
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# --- Bronze: raw customer feed (SCD1, overwrite latest) ---
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

# --- Silver: customer dimension (SCD2, historized) ---
@DataEntities.entity(
    entityid="silver.dim_customer",
    name="Customer Dimension",
    merge_columns=["customer_id"],
    scd_type=2,
    scd2_config=SCD2Config(
        tracked_columns=["name", "email", "region", "segment"],
    ),
    partition_columns=[],
    tags={"layer": "silver", "domain": "customer"},
    schema=StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("region", StringType(), True),
        StructField("segment", StringType(), True),
        # __effective_date, __end_date, __is_current added automatically
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

    No SCD2 logic here — the framework handles historization
    automatically based on the entity's scd_type=2 declaration.
    """
    return (
        bronze_customers
        .filter(col("customer_id").isNotNull())
        .withColumn("email", lower(col("email")))
        .withColumn("region", upper(trim(col("region"))))
    )

# --- Gold: current customer view (reads only is_current=true) ---
@DataPipes.pipe(
    pipeid="gold_current_customers",
    name="Current Customer View",
    input_entity_ids=["silver.dim_customer"],
    output_entity_id="gold.current_customers",
    output_type="table",
    tags={"layer": "gold"},
)
def gold_current_customers(dim_customer):
    """Snapshot of current customer state.

    Uses read_entity_current() under the hood — or pipe can filter explicitly.
    """
    return dim_customer.filter(col("__is_current") == True)
```
