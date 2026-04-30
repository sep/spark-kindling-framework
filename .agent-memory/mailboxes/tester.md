STATUS: IDLE
TASK: TASK-20260430-001
FROM: implementer
RECEIVED: 2026-04-30T17:07:04Z

## Instruction
Re-run `poe test-unit`. Confirm all 1105+ tests still pass after the 3 reviewer fixes.
Add a test for the double-wrapping fix: import a pipe before initialize(), assert
exactly one KindlingNotInitializedError is raised (no chained same-type cause).

## Context Files
- `.agent-memory/review-TASK-20260430-001.md`
- `packages/kindling/data_entities.py`
- `packages/kindling/data_pipes.py`
- `packages/kindling_cli/kindling_cli/templates/config/env.local.yaml.j2`
- `tests/unit/test_data_entities.py`
- `tests/unit/test_data_pipes.py`

## On Complete
Dispatch to mailboxes/reviewer.md with STATUS: PENDING
