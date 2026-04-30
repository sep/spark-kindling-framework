STATUS: PENDING
TASK: TASK-20260430-004
BRANCH: agent/TASK-20260430-004/scd2-phase4
FROM: implementer
RECEIVED: 2026-04-30T21:00:00Z

## What was implemented

Both Phase 4 SCD2 features are live in commit 52b3c12:

- `SCDConfig` has two new optional fields: `close_on_missing: bool = False` and
  `optimize_unchanged: bool = False` (`data_entities.py`)
- `scd_config_from_tags()` parses `scd.close_on_missing` and `scd.optimize_unchanged` tags
- `_SCD2_CHANGED_COLUMN = "__scd2_changed"` added as a private constant
- `_hash_tracked_columns(tracked_columns)` added as a private helper (SHA2-256 over sorted columns)
- `_execute_scd2_merge` modified per design doc:
  - `optimize_unchanged=True`: replaces column-by-column change detection with hash comparison
  - `close_on_missing=True`: adds `__scd2_changed` sentinel to staged, adds
    `whenMatchedUpdate(condition=..., set={...})` and `whenNotMatchedBySourceUpdate(...)`
  - `close_on_missing=False` branch is byte-for-byte identical to previous code

## Tests already added

20 tests total in `tests/unit/test_scd2_merge.py`:
- 9 existing SCD2 merge tests (all still passing)
- 6 new `close_on_missing` tests
- 5 new `optimize_unchanged` tests

6 new tag parsing tests in `tests/unit/test_scd_config.py`

All 1131 unit tests passing (`poe test-unit`).

## What tester should verify

1. Re-run `poe test-unit` to confirm 1131 passing on the branch
2. Check there are no edge cases missing:
   - Both flags together (`close_on_missing=True` + `optimize_unchanged=True`) — the interaction
     is handled via reuse of `source_hashed`/`target_hashed` in scope. A test for this combination
     would be valuable but is not yet present.
3. The removed test `test_execute_scd2_merge_does_not_implement_close_on_missing_in_phase_1` was
   deleted intentionally — it documented the Phase 1 deferral which is now implemented.

## On Complete

Write to mailboxes/reviewer.md:
  STATUS: PENDING
  TASK: TASK-20260430-004
  BRANCH: agent/TASK-20260430-004/scd2-phase4
  FROM: tester
  ## Test results / [pass counts, any gaps]
  ## On Complete / write to mailboxes/ship.md
