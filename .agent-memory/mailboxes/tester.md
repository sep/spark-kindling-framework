STATUS: PENDING
TASK: TASK-20260429-002
FROM: implementer
RECEIVED: 2026-04-29T21:05:00Z

## Instruction
Write unit tests for the 4 fixes in TASK-20260429-002. Minimum one test per fix. Run `poe test-unit` — must stay at 1080 passing.

### Fix 1 (#78) — read_entity_as_of timestamp cast
In tests/unit/test_scd2_merge.py: assert that the SCD2 filter path wraps point_in_time with lit().cast("timestamp") rather than using the raw value directly.

### Fix 2 (#79) — __merge_key reserved column validation
In tests/unit/test_scd_config.py: assert _validate_scd_config raises ValueError when entity schema contains a field named __merge_key.

### Fix 3 (#80) — null-sentinel coalesce in routing key
In tests/unit/test_scd2_merge.py: assert that for routing_key:"concat", the column-side routing key expression contains COALESCE and __null__ sentinel (complement the existing SQL-side assertion).

### Fix 4 (#81) — current_entity_id validation
In tests/unit/test_scd_config.py: assert _validate_scd_config raises ValueError when scd.current_entity_id equals the base entityid, and when it is empty/whitespace-only.

## Context Files
- packages/kindling/entity_provider_delta.py
- packages/kindling/data_entities.py
- tests/unit/test_scd2_merge.py
- tests/unit/test_scd_config.py

## On Complete
Run poe test-unit. If green, dispatch to mailboxes/ship.md:
  STATUS: PENDING / TASK: TASK-20260429-002 / FROM: tester
  Instruction: Ship TASK-20260429-002. Branch: agent/TASK-20260429-002/scd2-followup. Issues: #78 #79 #80 #81.
  On Complete: merge PR, close issues, log complete.
