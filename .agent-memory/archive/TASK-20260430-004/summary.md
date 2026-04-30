# Summary: TASK-20260430-004 — SCD2 Phase 4: close_on_missing + optimize_unchanged

**Merged:** 2026-04-30 | **Branch:** agent/TASK-20260430-004/scd2-phase4 | **PR:** https://github.com/sep/spark-kindling-framework/pull/92

## What was built

Two deferred Phase 4 SCD2 features from PR #77:
- **`close_on_missing`** (#83): `scd.close_on_missing: "true"` closes rows absent from the source batch via `whenNotMatchedBySourceUpdate`. A `__scd2_changed` sentinel column in staged prevents unchanged-but-present rows from being incorrectly closed.
- **`optimize_unchanged`** (#84): `scd.optimize_unchanged: "true"` replaces N-column change detection with a single `sha2(to_json(struct(...)), 256)` hash comparison. Both features are fully inside `_execute_scd2_merge` to correctly handle the interaction case (both flags active simultaneously).

## Key decisions

- **Sentinel column approach**: `close_on_missing` adds `__scd2_changed` boolean to staged rather than running a second Delta merge — single operation, uses `whenNotMatchedBySourceUpdate` as specified in issue #83.
- **Hash inside `_execute_scd2_merge`**: `optimize_unchanged` implemented as hash-based `changed_rows` computation, not as a pre-filter in `SCD2MergeStrategy.apply()`, to avoid an interaction bug where pre-filtering would make unchanged rows look absent to `close_on_missing`.
- **Copilot fix 1**: `_hash_tracked_columns` switched from `concat_ws('|', ...)` to `sha2(to_json(struct(...)), 256)` — avoids delimiter-collision false "unchanged" detection.
- **Copilot fix 2**: `whenNotMatchedBySourceUpdate` condition uses `_quote_sql_identifier()` for is_current_column, consistent with rest of file.

## Intentionally deferred

- `__scd2_src_hash` column leaks into `changed_rows` when `optimize_unchanged=True` (via `select("source.*")` from `source_hashed`). Not a correctness issue — Delta ignores extra source columns in explicit set/values operations. Could be cleaned up by explicitly selecting only `df.columns` from the join result.

## Files changed

- `packages/kindling/data_entities.py` — `SCDConfig` new fields, `scd_config_from_tags` new tag parsing
- `packages/kindling/entity_provider_delta.py` — `_SCD2_CHANGED_COLUMN`, `_hash_tracked_columns`, modified `_execute_scd2_merge`
- `tests/unit/test_scd2_merge.py` — removed Phase 1 deferral test; added 11 new tests (including both-flags-together)
- `tests/unit/test_scd_config.py` — 6 new tag parsing tests
- `CHANGELOG.md`

## For future agents

- `_SCD2_CHANGED_COLUMN = "__scd2_changed"` is a reserved column added to staged only when `close_on_missing=True`. Do not remove without updating the sentinel logic in `_execute_scd2_merge`.
- `_hash_tracked_columns` uses `sha2(to_json(struct(...sorted columns...)), 256)` — this is deliberately different from `_routing_key_column_expr` which uses the same function but over business keys, not tracked columns.
- The `close_on_missing=False` merge branch in `_execute_scd2_merge` must remain byte-for-byte identical to the pre-Phase-4 code — multiple existing tests assert the exact `whenMatchedUpdate(set={...})` call signature without a `condition` kwarg.
