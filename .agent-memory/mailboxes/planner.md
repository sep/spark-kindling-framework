STATUS: IN_PROGRESS
TASK: TASK-20260430-003
FROM: coordinator
RECEIVED: 2026-04-30T00:00:00Z

## Instruction

Produce a design doc for TASK-20260430-003 — 9 confirmed DX gaps from the post-#85/#87/#88
evaluation (issue #90). This is primarily doc additions, template changes, and print-to-logger
cleanup, with one small framework change (G9). No architectural decisions needed — focus on
implementation order, the G9 sentinel approach, and the G12 template placement.

### Gaps to address

| Gap | Location | Change |
|-----|----------|--------|
| G3 | `packages/kindling_cli/kindling_cli/templates/src/entities/records.medallion.py.j2` and `records.minimal.py.j2` | Add comment block explaining how to add a second entity |
| G4 | `docs/intro.md` | Remove or update pinned `**Version:** 0.6.6` (current is 0.9.x) |
| G5 | `README.md` | Add "Quick start" CLI section: `kindling new my-app` → `cd packages/my_app` → `poetry install` → `kindling run bronze_to_silver --env local` |
| G6 | `packages/kindling/spark_session.py:16` | `print("Creating new spark session ...")` → `logger.info(...)` |
| G7 | `packages/kindling/notebook_framework.py:1945,1966` | Two emoji `print()` → `logger.info(...)` |
| G8 | `packages/kindling/data_apps.py` | 29 bare `print()` (emoji-debug style) → route to logger or delete |
| G9 | `packages/kindling/spark_config.py` — `DynaconfConfig.get()` | Sentinel default so `get(key)` vs `get(key, None)` can be distinguished; log warning when called without explicit default and value is None |
| G12 | `packages/kindling_cli/kindling_cli/templates/tests/integration/test_pipeline.py.j2` | Add test that calls `DataPipesExecution.run_datapipes(["bronze_to_silver"])` via Kindling (not raw Delta) |
| G13 | `docs/setup_guide.md` | Add "Local development" section at top, before cloud platform sections |

### Key design questions for the planner to answer

1. **G9 sentinel approach**: Confirm the sentinel pattern is appropriate. The `get()` method is
   called very frequently — decide whether to use `logger.warning` or `logger.debug` to avoid log
   spam. Also decide: should the warning fire every call, or only in a debug/dev context?

2. **G8 data_apps.py categorisation**: Like the bootstrap.py cleanup in TASK-20260430-002, read
   `data_apps.py` and categorise the 29 prints:
   - Emoji step-tracking prints (🚀, 📋, ✅, 🔧, 📄) → route to `logger.info` or `logger.debug`
   - Error/exception prints → route to `logger.error` or `logger.warning`
   - Any that are clearly leftover → delete

3. **G12 template isolation**: The new integration test calls `initialize()` which modifies global
   state. Should it use the existing `_setup` fixture (which uses `spark_abfss`) or a new
   fixture using standalone? The Kindling execution test should work without ABFSS — it uses
   memory provider entities.

4. **G5 README placement**: Where does the Quick start section go relative to the existing
   notebook-oriented "Quickstart" Python snippet? Recommend replacing or supplementing.

## Context Files
- `packages/kindling_cli/kindling_cli/templates/src/entities/records.medallion.py.j2`
- `packages/kindling_cli/kindling_cli/templates/src/entities/records.minimal.py.j2`
- `packages/kindling_cli/kindling_cli/templates/tests/integration/test_pipeline.py.j2`
- `packages/kindling/spark_session.py` (G6 — check for logger instance)
- `packages/kindling/notebook_framework.py` lines 1940–1970 (G7)
- `packages/kindling/data_apps.py` (G8 — all 29 prints)
- `packages/kindling/spark_config.py` — `DynaconfConfig.get()` ~line 279 (G9)
- `docs/intro.md` (G4)
- `docs/setup_guide.md` (G13)
- `README.md` (G5)
- `docs/local_python_first.md` — review for any further accuracy issues
- `.agent-memory/ACTIVE_TASK.md` (task brief)
- Issue #90: `gh issue view 90`

## On Complete
Write design doc to `.agent-memory/design-TASK-20260430-003.md`, then write to
`mailboxes/implementer.md`:
  STATUS: PENDING
  TASK: TASK-20260430-003
  BRANCH: agent/TASK-20260430-003/dx-eval-remediation
  FROM: planner
  ## Instruction / [implementation instructions from design doc]
  ## Context Files / [design doc + changed files]
  ## On Complete / write to mailboxes/tester.md
