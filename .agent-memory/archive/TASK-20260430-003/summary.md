# Summary: TASK-20260430-003 — DX Eval Remediation

**Merged:** 2026-04-30 | **Branch:** agent/TASK-20260430-003/dx-eval-remediation | **PR:** https://github.com/sep/spark-kindling-framework/pull/91

## What was built

Addressed 9 confirmed developer experience gaps (G3–G9, G12, G13) from a comprehensive post-#85/#87/#88 evaluation. README now has a CLI-first quick-start (`kindling new` → `poetry install` → `kindling run`); `docs/intro.md` no longer pins a stale version; `docs/setup_guide.md` opens with a local dev section; scaffold entity templates include guidance on adding a second entity; bare `print()` calls in `data_apps.py` (29), `spark_session.py`, and `notebook_framework.py` are routed to structured logging; `DynaconfConfig.get()` logs a debug message when a key is missing and no default was supplied; the scaffold integration test template exercises pipe execution via `DataPipesExecution.run_datapipes()` without requiring ABFSS.

## Key decisions

- **G9 sentinel**: `DynaconfConfig.get()` uses `type(self).get.__defaults__[0]` introspection to detect "no default supplied" — reload-safe alternative to module-level `_MISSING` identity check. Logged in DECISIONS.md 2026-04-30.
- **G12 test mark**: `TestKindlingPipeExecution` uses class-level `pytestmark = [pytest.mark.integration]` to override the module-level `requires_azure` so it runs without ABFSS credentials.
- **G8 data_apps.py**: `_execute_app()` debug prints deleted entirely (redundant with existing logger calls); `run_app()` progress prints routed to `self.logger.info/debug`; exception handler uses `logger.exception()` to preserve traceback in structured logs.

## Intentionally deferred

- G9 sentinel: `_MissingType` sentinel class with `isinstance()` check would be cleaner for typing — flagged for future cleanup.
- `NotebookWheelBuilder` bare `print()` at line 1782 — pre-existing, out of scope for G8.
- G13 setup_guide.md local section uses CLI (not Python API) after Copilot correctly identified the original Python snippet as a TypeError.

## Files changed

- `README.md` — CLI Quick Start section
- `docs/intro.md` — removed stale version pin
- `docs/setup_guide.md` — Local Development section
- `packages/kindling/data_apps.py` — print cleanup (G8)
- `packages/kindling/notebook_framework.py` — print cleanup (G7)
- `packages/kindling/spark_session.py` — print cleanup (G6)
- `packages/kindling/spark_config.py` — sentinel + debug log (G9)
- `packages/kindling_cli/.../records.medallion.py.j2` — guidance comments (G3)
- `packages/kindling_cli/.../records.minimal.py.j2` — guidance comments (G3)
- `packages/kindling_cli/.../test_pipeline.py.j2` — pipe execution test class (G12)
- `tests/unit/test_spark_config.py` — 3 new G9 regression tests
- `CHANGELOG.md`

## For future agents

- `DynaconfConfig.get()` now logs `_CONFIG_LOGGER.debug("Config key %s not found and no default supplied", key)` when called without an explicit default and the key is absent. This is intentional — don't remove without updating the test in `test_spark_config.py`.
- `TestKindlingPipeExecution` in the integration test template uses `spark_local` + memory providers — it does NOT require ABFSS. The class-level `pytestmark` is what prevents the module-level `requires_azure` from skipping it.
- The `data_apps.py` `_execute_app()` method previously had extensive debug prints that have been removed. If you need to debug app execution, raise the logger to DEBUG level rather than re-adding prints.
