STATUS: IDLE
TASK: TASK-20260430-002
FROM: planner
RECEIVED: 2026-04-30T18:10:00Z

## Instruction

Implement all 6 items from the design doc at `.agent-memory/design-TASK-20260430-002.md`.
Work on branch `agent/TASK-20260430-002/dx-fixes-round2`.

Follow CONVENTIONS.md: type hints on all public functions, agent tag comments on new code
(`# [implementer] <reason> — TASK-20260430-002`), no bare `except:`.

After all changes, run `poe test-unit`. It must stay green (1106 tests). Note: the new DI wiring
component test (item 6) will FAIL until item 1 is fixed — implement item 1 first, then item 6.

## Context Files
- `.agent-memory/design-TASK-20260430-002.md` — full design with pseudocode for all 6 items
- `packages/kindling/watermarking.py` — add NullWatermarkEntityFinder here (item 1)
- `packages/kindling/bootstrap.py` — remove/route debug prints (item 2)
- `packages/kindling/spark_config.py` — remove/route debug prints (item 2)
- `packages/kindling_cli/kindling_cli/cli.py` — validate --env, next-steps annotation (items 3, 4)
- `packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2` — CLI dep (item 5)
- `tests/unit/test_cli_local_dev_dx.py` — existing tests for reference

## On Complete
Run `poe test-unit`. If green, write to `.agent-memory/mailboxes/tester.md`:
  STATUS: PENDING
  TASK: TASK-20260430-002
  FROM: implementer
  ## Instruction
  Run `poe test-unit`. Add tests for: DI wiring (item 6 component test), validate --env
  acceptance, and assert that `kindling run` in a scaffolded project now reaches pipe
  execution (can be a dry-run or mock-executor test). Confirm total >= 1106.
  ## Context Files
  - `.agent-memory/design-TASK-20260430-002.md`
  - All changed source files
  ## On Complete
  Write to mailboxes/reviewer.md with STATUS: PENDING
