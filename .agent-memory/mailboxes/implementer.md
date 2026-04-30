STATUS: IDLE
TASK: TASK-20260430-004
BRANCH: agent/TASK-20260430-004/scd2-phase4
FROM: planner
RECEIVED: 2026-04-30T20:45:00Z

## Instruction

Implement SCD2 Phase 4: `close_on_missing` (#83) and `optimize_unchanged` (#84).
Full design is at `.agent-memory/design-TASK-20260430-004.md`. Build exactly from that sketch.

### Summary of changes

**`packages/kindling/data_entities.py`**
1. Add two fields at the end of `SCDConfig` (frozen dataclass, line 49):
   ```python
   close_on_missing: bool = False
   optimize_unchanged: bool = False
   ```
2. In `scd_config_from_tags()` (line 256 return block), add to the `return SCDConfig(...)`:
   ```python
   close_on_missing=tags.get("scd.close_on_missing", "false").strip().lower() == "true",
   optimize_unchanged=tags.get("scd.optimize_unchanged", "false").strip().lower() == "true",
   ```

**`packages/kindling/entity_provider_delta.py`**
1. Add new constant near `_SCD2_MERGE_KEY_COLUMN` (line 23):
   ```python
   _SCD2_CHANGED_COLUMN = "__scd2_changed"
   ```
2. Add new function `_hash_tracked_columns(tracked_columns: list[str]):` after
   `_routing_key_target_sql` (around line 164). Uses `sha2`, `concat_ws`, `coalesce`, `col`, `lit`
   — all already imported at line 12:
   ```python
   def _hash_tracked_columns(tracked_columns: list[str]):
       return sha2(
           concat_ws(
               "|",
               *[coalesce(col(c).cast("string"), lit(_SCD2_NULL_SENTINEL)) for c in sorted(tracked_columns)],
           ),
           256,
       )
   ```
3. Modify `_execute_scd2_merge` (line 166) per the full pseudocode in the design doc.

   Key structural rules:
   - When `cfg.optimize_unchanged=False` (default): existing `changed_rows` join with
     `_build_null_safe_change_condition` is unchanged.
   - When `cfg.optimize_unchanged=True`: replace with hash-based inner join using `_hash_tracked_columns`.
     Guard: if `not tracked_columns`, skip hash and fall through to empty changed_rows.
   - When `cfg.close_on_missing=False` (default): `staged = insert_rows.unionByName(keyed_rows, ...)`.
     Merge uses `whenMatchedUpdate(set={...})` with NO condition kwarg (must preserve exact call to
     keep existing tests green). NO `whenNotMatchedBySourceUpdate` call.
   - When `cfg.close_on_missing=True`: add `__scd2_changed` column to `keyed_rows` (True),
     `insert_rows` (False), and sentinel rows for unchanged keys (False). Merge uses
     `whenMatchedUpdate(condition="staged.`__scd2_changed` = true", set={...})` and adds
     `whenNotMatchedBySourceUpdate(condition="target.`__is_current` = true", set={close_set})`.
   - When BOTH are True: reuse `source_hashed` / `target_hashed` from the `optimize_unchanged`
     block to compute `unchanged_rows` for sentinels. Define these variables before the
     `if cfg.optimize_unchanged` branch so they're in scope for the sentinel block.

### Open question to resolve at implementation time

Open question 1 from design doc: If Delta rejects `staged.\`__scd2_changed\` = true` with backtick
quoting, try `staged.__scd2_changed = true` (no backticks). Verify at runtime.

### What NOT to do

- Do NOT change `SCD2MergeStrategy.apply()` — it stays as `_execute_scd2_merge(delta_table, df, entity)`.
- Do NOT pre-filter `df` in `apply()` — both features must live inside `_execute_scd2_merge`.
- Do NOT modify the `else` (close_on_missing=False) merge branch — keep it byte-for-byte
  identical to today so existing tests require no changes.

## Context Files

- `.agent-memory/design-TASK-20260430-004.md` — full design with pseudocode
- `packages/kindling/data_entities.py` — SCDConfig (line 49), scd_config_from_tags (line 219)
- `packages/kindling/entity_provider_delta.py` — _execute_scd2_merge (line 166), constants (line 22-24)
- `tests/unit/test_scd2_merge.py` — existing test structure to keep green

## On Complete

Write to `mailboxes/tester.md`:
  STATUS: PENDING
  TASK: TASK-20260430-004
  BRANCH: agent/TASK-20260430-004/scd2-phase4
  FROM: implementer
  ## What was implemented / [brief summary]
  ## Tests to add / [list from design doc test strategy]
  ## On Complete / write to mailboxes/reviewer.md
