# Summary: TASK-20260430-002 — DX Round 2: post-#85 eval gaps + DI wiring test

**Merged:** 2026-04-30 | **Branch:** agent/TASK-20260430-002/dx-fixes-round2 | **PR:** https://github.com/sep/spark-kindling-framework/pull/89

## What was built
Fixed 5 DX gaps discovered during post-#85 developer simulation: `kindling run` now executes pipes
in freshly scaffolded projects (NullWatermarkEntityFinder auto-bound in standalone), CLI output is
clean by default (109 debug print() calls removed/routed), `kindling validate` accepts `--env`,
scaffold next-steps annotates .env as optional, and spark-kindling-cli is a real dev dependency.
Added `test_di_wiring_standalone.py` — a real DI construction test with no mocking that catches
abstract-class binding gaps before they ship.

## Key decisions
- **WatermarkEntityFinder**: Option C chosen — standalone platform auto-binds NullWatermarkEntityFinder
  after `initialize_platform_services()`, conditional on no existing binding. Raises NotImplementedError
  with actionable message if watermarking is actually invoked, so it does not mask production gaps.
- **Print suppression**: No KINDLING_QUIET env var needed. 109 prints categorised into: delete entirely
  (leftover debug/emoji), route to logger.debug/info/warning (operational diagnostics), keep (intentional
  fallback logger in spark_config.py ~line 597).
- **_bindings guard**: Copilot correctly identified direct `._bindings` access as fragile; guarded with
  `getattr(..., None)` consistent with other introspection sites.

## Intentionally deferred
- Reviewer notes (non-blocking): NullWatermarkEntityFinder error message could show GlobalInjector.bind
  example; DI test uses subprocess isolation rather than in-process; decorator order cosmetic mismatch.
- Remaining issue #85 medium/low items (items #9-#14, #15-#19).

## Files changed
- `packages/kindling/watermarking.py` — NullWatermarkEntityFinder
- `packages/kindling/bootstrap.py` — debug print cleanup + standalone binding
- `packages/kindling/spark_config.py` — debug print cleanup
- `packages/kindling_cli/kindling_cli/cli.py` — validate --env, .env annotation, KINDLING_ENV fallback
- `packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2` — spark-kindling-cli uncommented
- `tests/unit/test_di_wiring_standalone.py` — new DI wiring test
- `tests/unit/test_cli_local_dev_dx.py`, `test_scaffold.py` — new coverage

## For future agents
- NullWatermarkEntityFinder is in `watermarking.py`. If watermark functionality is ever needed in
  standalone mode, the user must call `GlobalInjector.bind(WatermarkEntityFinder, MyImpl)` in their
  `app.py` before `register_all()`.
- The DI wiring test (`test_di_wiring_standalone.py`) uses subprocess isolation. If it fails, check
  that all abstract classes in the DataPipesExecution DI chain have concrete bindings in standalone mode.
- The intentional print-based fallback logger in `spark_config.py` (~line 597) was deliberately kept —
  it fires only when the structured logger is unavailable.
