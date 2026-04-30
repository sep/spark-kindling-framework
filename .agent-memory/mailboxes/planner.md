STATUS: IDLE
TASK: TASK-20260430-004
BRANCH: agent/TASK-20260430-004/scd2-phase4
FROM: coordinator
RECEIVED: 2026-04-30T20:30:00Z

## Instruction

Produce a design doc for TASK-20260430-004 — SCD2 Phase 4: `close_on_missing` and
`optimize_unchanged`. These are the two remaining deferred Phase 4 items from PR #77.

Both features extend `SCDConfig` and `_execute_scd2_merge` in
`packages/kindling/entity_provider_delta.py`. Design them together so the implementation
can be done in a single coherent commit.

### Feature 1: close_on_missing (issue #83)

New tag `scd.close_on_missing: "true"` (default: `"false"`).

When enabled, source rows not present in the incoming batch are "closed":
- `__effective_to` set to the current timestamp
- `__is_current` set to `false`

This is a `whenNotMatchedBySource().update(...)` clause added to the Delta merge builder.
It must only apply to rows where `__is_current == true` to avoid re-closing already-closed rows.

Existing test `test_execute_scd2_merge_does_not_implement_close_on_missing_in_phase_1`
currently asserts that this behavior does NOT exist. It documents the deferral. Update
or remove it as part of this task.

### Feature 2: optimize_unchanged (issue #84)

New tag `scd.optimize_unchanged: "true"` (default: `"false"`).

When enabled, pre-filter the incoming DataFrame before the merge to exclude rows where no
business column changed. This avoids writing no-op SCD2 history rows.

Approach: for each incoming row, compute a hash over all non-temporal, non-key columns.
Join against the current entity table (filtered to `__is_current == true`), compute the
same hash on the current row, and drop incoming rows where hashes match.

This is consistent with how `scd.routing_key` uses hash-based routing. The hash function
should be `sha2(concat_ws("|", col1, col2, ...), 256)` over sorted non-key, non-temporal
column names.

### Design questions for the planner to answer

1. **`whenNotMatchedBySource` availability**: Confirm this Delta API is available in the
   version of `delta-spark` the project uses. Check `pyproject.toml` or lockfile for the
   delta-spark version. `whenNotMatchedBySource` was added in Delta Lake 2.3 / delta-spark 2.3.0.

2. **Column exclusion for optimize_unchanged hash**: Which columns should be excluded from
   the hash? At minimum: merge_columns (business keys), `__effective_from`, `__effective_to`,
   `__is_current`, `__merge_key`. Confirm the right set.

3. **SCDConfig field names**: Confirm the exact Python field names for the two new booleans
   (suggest `close_on_missing: bool = False` and `optimize_unchanged: bool = False`).

4. **Tag parsing**: `scd_config_from_tags()` currently parses string tags to booleans.
   Confirm the parsing pattern (e.g., `tags.get("scd.close_on_missing", "false").lower() == "true"`).

5. **Test strategy**: The existing SCD2 tests in `tests/unit/test_entity_provider_delta.py`
   use mock Delta tables. Design tests that verify: close_on_missing fires the right
   `whenNotMatchedBySource` clause; optimize_unchanged drops unchanged rows before merge.

## Context Files

- `packages/kindling/entity_provider_delta.py` — `SCDConfig`, `scd_config_from_tags`,
  `_execute_scd2_merge`, `DeltaMergeStrategies`, `SCD2MergeStrategy`
- `tests/unit/test_entity_provider_delta.py` — existing SCD2 tests, including
  `test_execute_scd2_merge_does_not_implement_close_on_missing_in_phase_1`
- `.agent-memory/ACTIVE_TASK.md` — task brief with acceptance criteria
- Issue #83: `gh issue view 83`
- Issue #84: `gh issue view 84`
- `pyproject.toml` (root or packages/kindling) — delta-spark version

## On Complete

Write design doc to `.agent-memory/design-TASK-20260430-004.md`, then write to
`mailboxes/implementer.md`:
  STATUS: PENDING
  TASK: TASK-20260430-004
  BRANCH: agent/TASK-20260430-004/scd2-phase4
  FROM: planner
  ## Instruction / [implementation instructions from design doc]
  ## Context Files / [design doc + entity_provider_delta.py + test file]
  ## On Complete / write to mailboxes/tester.md
