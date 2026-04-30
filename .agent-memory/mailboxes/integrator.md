STATUS: IDLE
VERDICT: APPROVED
TASK: TASK-20260430-001
FROM: reviewer
RECEIVED: 2026-04-30T17:12:00Z

## Instruction

Wire the approved TASK-20260430-001 implementation. All changes are already on branch
`agent/TASK-20260430-001/local-dev-dx`. Your job is to confirm the branch is clean,
run a final `poe test-unit`, and hand to ship.

Verify these are present on the branch:
- `packages/kindling/data_entities.py` — `KindlingNotInitializedError`, `DataEntities.reset()`
- `packages/kindling/data_pipes.py` — `DataPipes.reset()`, guard in `pipe()` decorator
- `packages/kindling/__init__.py` — `KindlingNotInitializedError` exported
- `packages/kindling_cli/kindling_cli/cli.py` — `run_pipe`, `validate_app`, `_discover_app_py`, `_load_app_module`, fixed `env_check`, fixed `new_project` next-steps
- `packages/kindling_cli/kindling_cli/templates/config/env.local.yaml.j2` — memory-first, single `entity_tags:` key
- `packages/kindling_cli/kindling_cli/templates/tests/conftest.py.j2` — public `reset_kindling` fixture
- `packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2` — plain `pytest` poe tasks

Run `poe test-unit`. Must stay green (1106 expected).

## Context Files
- `.agent-memory/review-TASK-20260430-001.md`
- All files listed above

## On Complete
Write to `.agent-memory/mailboxes/ship.md`:
  STATUS: PENDING
  TASK: TASK-20260430-001
  BRANCH: agent/TASK-20260430-001/local-dev-dx
  FROM: integrator
  ## Instruction
  Create PR for TASK-20260430-001 targeting main. Title: "feat(dx): local-first scaffold, kindling run/validate, KindlingNotInitializedError (#85)"
  Body should summarise all 9 changes (Group A: runtime + CLI commands; Group B: template fixes).
  Reference issue #85. Request Copilot review.
