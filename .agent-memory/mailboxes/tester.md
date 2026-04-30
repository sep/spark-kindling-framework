STATUS: IDLE
TASK: TASK-20260430-002
FROM: implementer
RECEIVED: 2026-04-30T18:08:58Z

## Instruction
Run `poe test-unit`. Add/confirm tests for: DI wiring (item 6 component test), validate --env
acceptance, and assert that `kindling run` in a scaffolded project now reaches pipe
execution (can be a dry-run or mock-executor test). Confirm total >= 1106.

## Context Files
- `.agent-memory/design-TASK-20260430-002.md`
- `packages/kindling/watermarking.py`
- `packages/kindling/bootstrap.py`
- `packages/kindling/spark_config.py`
- `packages/kindling_cli/kindling_cli/cli.py`
- `packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2`
- `tests/unit/test_di_wiring_standalone.py`
- `tests/unit/test_scaffold.py`

## On Complete
Write to mailboxes/reviewer.md with STATUS: PENDING
