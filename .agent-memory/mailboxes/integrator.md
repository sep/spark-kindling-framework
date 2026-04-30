STATUS: IDLE
VERDICT: APPROVED WITH NOTES
TASK: TASK-20260430-002
FROM: reviewer
RECEIVED: 2026-04-30T18:20:00Z

## Instruction

Wire the approved TASK-20260430-002 implementation. All changes are on branch
`agent/TASK-20260430-002/dx-fixes-round2`. Confirm branch is clean, run final `poe test-unit`
(must stay at 1110), and hand to ship.

Verify these are present on the branch:
- `packages/kindling/watermarking.py` — `NullWatermarkEntityFinder` class + auto-binding in standalone
- `packages/kindling/bootstrap.py` — debug `print()` calls removed/routed to logger
- `packages/kindling/spark_config.py` — debug `print()` calls removed/routed to logger
- `packages/kindling_cli/kindling_cli/cli.py` — `validate_app` has `--env` option, next-steps `.env` annotation updated
- `packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2` — `spark-kindling-cli` uncommented as dev dependency
- `tests/unit/test_di_wiring_standalone.py` — new DI wiring component test

## Reviewer notes (APPROVED WITH NOTES — not blocking, pass to ship as-is)
1. MINOR: `NullWatermarkEntityFinder` error message could include `GlobalInjector.bind(WatermarkEntityFinder, ...)` example
2. MINOR: `--env`/`--app` decorator order on `validate` reversed vs `run` (cosmetic, wiring correct)
3. MINOR: DI test uses subprocess isolation rather than in-process approach
4. NIT: `NullWatermarkEntityFinder` docstring could add "bind a real implementation" guidance

## Context Files
- `.agent-memory/review-TASK-20260430-002.md`
- All files listed above

## On Complete
Write to `.agent-memory/mailboxes/ship.md`:
  STATUS: PENDING
  TASK: TASK-20260430-002
  BRANCH: agent/TASK-20260430-002/dx-fixes-round2
  FROM: integrator
  ## Instruction
  Create PR for TASK-20260430-002 targeting main.
  Title: "fix(dx): kindling run now works in scaffolded projects; suppress debug noise; validate --env (#87 #88)"
  Body: summarise all 6 fixes (watermark DI binding, print cleanup, validate --env, next-steps, CLI dep, DI test).
  Reference issues #87 and #88. Request Copilot review.
