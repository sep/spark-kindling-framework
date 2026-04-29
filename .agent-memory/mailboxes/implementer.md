STATUS: IDLE
TASK: TASK-20260429-002
FROM: coordinator
RECEIVED: 2026-04-29T20:48:00Z

## Instruction
Fix 4 correctness/validation gaps in the SCD2 implementation. All fixes are small and well-specified — do not refactor beyond what each issue asks for.

### Fix 1 — Issue #78: `point_in_time` type inconsistency (`entity_provider_delta.py`)
In `read_entity_as_of`, the SCD2 filter path uses `point_in_time` directly in a Spark Column comparison. Wrap it with `lit(point_in_time).cast("timestamp")` so the comparison semantics are stable regardless of whether callers pass `datetime`, `date`, or string.

### Fix 2 — Issue #79: `__merge_key` not validated as reserved (`data_entities.py`)
In `_validate_scd_config()`, add a check that raises `ValueError` if `__merge_key` appears in the entity's declared column names. Message: `"Entity '{entityid}' declares column '__merge_key' which is reserved for SCD2 merge staging."` (or similar).

### Fix 3 — Issue #80: `concat_ws` drops nulls in routing key (`entity_provider_delta.py`)
In `_execute_scd2_merge`, for the `"concat"` routing key method, coalesce each business key column to an explicit null-sentinel string (e.g. `"__null__"`) before passing to `concat_ws`. Apply the same coalesce on the target side of the MERGE condition so both sides are consistent.

### Fix 4 — Issue #81: `scd.current_entity_id` not validated against base id (`data_entities.py`)
In `_validate_scd_config()`, add a check that raises `ValueError` if `current_entity_id` (after stripping) is empty or equals the base `entity.entityid`. Message: `"scd.current_entity_id must differ from the base entity id and be non-empty."` (or similar).

## Rules
- Type hints on all touched functions
- No bare `except:`, no `print()`, no commented-out code
- Run `poe test-unit` before marking done — must stay green
- Do not fix anything not listed above

## Context Files
- `packages/kindling/entity_provider_delta.py` — Fix 1 (`read_entity_as_of`), Fix 3 (`_execute_scd2_merge`)
- `packages/kindling/data_entities.py` — Fix 2 and Fix 4 (`_validate_scd_config`)
- GitHub issues #78, #79, #80, #81 for exact problem statements

## On Complete
Run `poe test-unit`. If green, dispatch to mailboxes/tester.md:
  STATUS: PENDING / TASK: TASK-20260429-002 / FROM: implementer
  Instruction: Write tests for the 4 fixes (issues #78–#81). One test per fix minimum. Run poe test-unit.
  Context Files: entity_provider_delta.py, data_entities.py, test_scd_config.py, test_scd2_merge.py
  On Complete: dispatch to mailboxes/ship.md
