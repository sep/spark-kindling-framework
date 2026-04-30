STATUS: PENDING
TASK: TASK-20260430-002
FROM: coordinator
RECEIVED: 2026-04-30T17:45:00Z

## Instruction

Produce a design doc for TASK-20260430-002. Six items to address — four of them are well-understood
fixes, two require design decisions. See scope below.

### Item 1 — `WatermarkEntityFinder` unbound in standalone (🔴 Critical)

`DataPipesExecution` DI graph: `SimpleReadPersistStrategy` → `WatermarkManager` → `WatermarkEntityFinder`
(abstract, no concrete binding in standalone platform). The test framework mocks it; the real CLI hits
`injector.CallError` immediately.

Three options were identified in issue #87:
- A) `kindling run` CLI auto-binds a `NullWatermarkEntityFinder` before calling `run_datapipes`
- B) Scaffold `app.py.j2` template includes a stub implementation + binding
- C) Standalone platform (`platform_standalone.py`) auto-registers a no-op during `initialize_framework`

Pick one. Consider: option A is narrowest (only helps `kindling run`, not programmatic use); option C
is broadest (helps all standalone callers but risks masking the need for a real implementation in
production); option B puts user code in charge, consistent with how existing apps do it.

Look at `tests/data-apps/eventhub-provider-test-app/main.py` and
`tests/data-apps/streaming-pipes-test-app/main.py` for the pattern existing apps use.

Provide specific pseudocode for whichever option you recommend.

### Item 2 — 100+ debug `print()` calls in `bootstrap.py` + `spark_config.py` (🔴 Critical)

Every invocation prints 30–50 lines of debug noise unconditionally. The calls are raw `print()`
statements — not behind any flag. Key locations: `bootstrap.py:1408-1420`, `spark_config.py:124-184`.

The fix must not silence legitimate startup errors. The existing `print_logging` config key
(`kindling.telemetry.logging.print: true`) controls the logger, not these raw prints. The bootstrap
itself runs before config is fully loaded in some paths, so a simple "read config flag" approach may
not work for all sites.

Design the suppression strategy: which calls to remove entirely (clearly leftover debug code), which
to route through the existing logger, and whether a `KINDLING_QUIET=1` env var is needed as a
workaround for the bootstrap-before-config ordering problem.

### Item 3 — `kindling validate` missing `--env` option (🟠 High)

`kindling run` accepts `--env TEXT` and passes it to `_load_app_module`. `validate_app` also calls
`_load_app_module` but has no `--env` option. Add one — wire identically to `run_pipe`.

File: `packages/kindling_cli/kindling_cli/cli.py` — `validate_app()` function.

### Item 4 — Next-steps implies Azure creds required for local dev (🟠 High)

`kindling new` prints:
```
cp .env.example .env  # fill in your credentials, then: source .env
```

Annotate to make clear this is optional for local-first (memory provider) dev. Look at
`packages/kindling_cli/kindling_cli/cli.py` — `new_project()` — for the next-steps string.

### Item 5 — `spark-kindling-cli` commented out in scaffold pyproject.toml (🟠 High)

After `poetry install`, `kindling` is not on PATH because `spark-kindling-cli` is commented out as
"optional" in the scaffold's `pyproject.toml.j2`. Decide: uncomment as a real dev dependency, or
add a `poetry run kindling ...` hint in next-steps. Consider that the CLI is a dev-time tool (no
production need), but it's the primary local run mechanism the docs promote.

File: `packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2`

### Item 6 — Missing DI wiring component test (🟠 High, issue #88)

Add a component-level test that exercises real DI graph construction for `DataPipesExecution` on
standalone platform. The test must:
1. Call `initialize_framework(platform="standalone")` with an in-memory config (no files, no Azure)
2. Call `GlobalInjector.get(DataPipesExecution)` with no mocking
3. Assert a concrete `DataPipesExecution` instance is returned (no `CallError`)
4. This test should FAIL before item 1 is fixed and PASS after

This test should live in `tests/unit/` alongside the other CLI/DX tests, or in a new
`tests/component/` area. Check CONVENTIONS.md for guidance on test placement.

## Context Files
- `.agent-memory/ACTIVE_TASK.md` — task brief
- `packages/kindling/bootstrap.py` — lines 1408–1500 and grep for `print(` (86 total)
- `packages/kindling/spark_config.py` — lines 124–190 (17 print calls)
- `packages/kindling/watermarking.py` — `WatermarkEntityFinder` abstract class
- `packages/kindling/platform_standalone.py` — standalone platform (no WatermarkEntityFinder binding)
- `packages/kindling_cli/kindling_cli/cli.py` — `run_pipe`, `validate_app`, `new_project`
- `packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2` — scaffold pyproject
- `tests/data-apps/eventhub-provider-test-app/main.py` — example WatermarkEntityFinder implementation
- `tests/unit/test_cli_local_dev_dx.py` — existing CLI tests (mocked DI)
- GitHub issues #87 and #88

## On Complete
Write to `.agent-memory/mailboxes/implementer.md`:
  STATUS: PENDING
  TASK: TASK-20260430-002
  FROM: planner
  ## Instruction / [implement the design]
  ## Context Files / design-TASK-20260430-002.md + source files
  ## On Complete / write to mailboxes/tester.md
