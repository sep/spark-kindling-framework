# Task Registry

| ID | Title | Status | Branch |
|----|-------|--------|--------|
| TASK-20260429-001 | SCD Type 2 Entity Support | ✅ MERGED (PR #77) | agent/TASK-20260429-001/scd2-support |
| TASK-20260429-002 | SCD2 Follow-up Fixes (#78–#81) | ✅ MERGED (PR #82) | agent/TASK-20260429-002/scd2-followup |
| TASK-20260430-001 | Local Dev DX: Critical & High gaps (issue #85) | ✅ MERGED (PR #86) | agent/TASK-20260430-001/local-dev-dx |
| TASK-20260430-002 | DX Round 2: WatermarkEntityFinder, debug noise, validate --env, CLI dep, DI test (#87, #88) | ✅ MERGED (PR #89) | agent/TASK-20260430-002/dx-fixes-round2 |
| TASK-20260430-003 | DX Eval Remediation: docs, print cleanup, scaffold test example (#90) | 🔄 IN PROGRESS | agent/TASK-20260430-003/dx-eval-remediation |

---

# Active Task: TASK-20260430-003 — DX Eval Remediation: docs, print cleanup, scaffold test example
**Status:** IN PROGRESS
**Branch:** `agent/TASK-20260430-003/dx-eval-remediation`
**Issue:** #90
**Started:** 2026-04-30

## Goal
Address 9 confirmed developer experience gaps from the post-#85/#87/#88 evaluation. After this task:
- The README has a CLI-first quick-start path (`kindling new` → `poetry install` → `kindling run`)
- `docs/intro.md` no longer shows the stale 0.6.6 version
- `docs/setup_guide.md` has a local development section before the cloud platform sections
- `docs/local_python_first.md` is accurate and current
- Scaffold entity templates include guidance comments for adding a second entity
- `spark_session.py`, `notebook_framework.py`, and `data_apps.py` have no bare `print()` calls
- `DynaconfConfig.get()` warns when a key is not found and no default was supplied
- The scaffold integration test template demonstrates pipe execution via `DataPipesExecution.run_datapipes()`

## Scope
| # | Gap | Priority | Type |
|---|-----|----------|------|
| G3 | No "add entity/pipe" guidance in scaffold | Medium | Template + doc |
| G4 | `docs/intro.md` version 0.6.6 (actual: 0.9.x) | Medium | Doc fix |
| G5 | README has no `kindling new` quickstart | Medium | Doc addition |
| G6 | Bare `print()` in `spark_session.py:16` | Low | Framework cleanup |
| G7 | Bare `print()` in `notebook_framework.py:1945,1966` | Low | Framework cleanup |
| G8 | 29 bare `print()` in `data_apps.py` | Low | Framework cleanup |
| G9 | `DynaconfConfig.get()` silent on missing key | Low | Framework fix |
| G12 | No pipe-execution test example in scaffold | Low | Template |
| G13 | `docs/setup_guide.md` cloud-only | Medium | Doc addition |

## Acceptance Criteria
- [ ] `README.md` has a "Quick start" section: `kindling new my-app` → `cd` → `poetry install` → `kindling run bronze_to_silver --env local`
- [ ] `docs/intro.md` has no pinned version number (or shows current version)
- [ ] `docs/setup_guide.md` has a "Local development" section before cloud platform sections
- [ ] Scaffold entity template file (`records.medallion.py.j2` and `records.minimal.py.j2`) includes a comment block explaining how to add a second entity
- [ ] `spark_session.py:16` uses `logger.info` instead of `print()`
- [ ] `notebook_framework.py:1945,1966` use `logger` instead of `print()`
- [ ] `data_apps.py` has zero bare `print()` calls — all routed to logger
- [ ] `DynaconfConfig.get()` logs a `logger.warning` when the resolved value is `None` and no explicit default was passed (sentinel pattern)
- [ ] Scaffold integration test template (`test_pipeline.py.j2`) includes a test that calls `DataPipesExecution.run_datapipes(["bronze_to_silver"])` via Kindling (not raw Delta)
- [ ] All existing unit tests continue to pass (`poe test-unit`)

## Agent Plan
| Step | Agent | Input | Output | Status |
|------|-------|-------|--------|--------|
| 1 | planner | this brief + issue #90 + source | design doc | ⏳ PENDING |
| 2 | implementer | design doc | code + doc changes | — |
| 3 | tester | implementation | test coverage + regression | — |
| 4 | reviewer | code + tests | verdict | — |
| 5 | ship | approved code | PR to main | — |

## Handoff Log
- 2026-04-30: Task created by coordinator. Dispatched to planner.

---

# Active Task: TASK-20260430-002 — DX Round 2: post-#85 eval gaps + DI wiring test
**Status:** COMPLETE — merged as PR #89
**Branch:** `agent/TASK-20260430-002/dx-fixes-round2`
**Issues:** #87, #88
**Started:** 2026-04-30

## Goal
Fix 5 DX gaps discovered during post-#85 developer simulation (issue #87) and add the missing DI
wiring component test that would have caught them (issue #88). After this task, `kindling run` executes
a scaffolded bronze→silver pipe without Azure credentials, the CLI is quiet by default, `kindling
validate` accepts `--env`, the scaffold pyproject.toml makes the CLI available after `poetry install`,
and any future DI binding regression is caught by a component test before shipping.

## Scope
| # | Item | Severity | Type | Issue |
|---|------|----------|------|-------|
| 1 | `kindling run` fails — `WatermarkEntityFinder` unbound in standalone | 🔴 Critical | Framework + CLI | #87 |
| 2 | 100+ debug `print()` in `bootstrap.py`/`spark_config.py` flood every invocation | 🔴 Critical | Framework cleanup | #87 |
| 3 | `kindling validate` missing `--env` option | 🟠 High | CLI | #87 |
| 4 | Next-steps implies Azure creds required for local dev | 🟠 High | Template | #87 |
| 5 | `spark-kindling-cli` commented out in scaffold pyproject.toml | 🟠 High | Template | #87 |
| 6 | Missing DI wiring component test — DI failures invisible to test suite | 🟠 High | Testing | #88 |

## Acceptance Criteria
- [ ] `kindling run bronze_to_silver --env local` completes successfully in a freshly scaffolded project without Azure credentials
- [ ] `kindling validate`, `kindling run`, and `kindling env check` produce no debug print noise by default; actual results are the only stdout
- [ ] `kindling validate --env local` is accepted (no "No such option" error)
- [ ] `kindling new` next-steps annotates `.env` copy step as optional for local dev
- [ ] After `poetry install` in a scaffolded project, `kindling run`/`validate` are available (either via PATH or via `poetry run kindling`)
- [ ] A component test exercises real `initialize_framework(platform="standalone")` + real `GlobalInjector.get(DataPipesExecution)` with no mocking — passes green
- [ ] All 1106 existing unit tests continue to pass

## Agent Plan
| Step | Agent | Input | Output | Status |
|------|-------|-------|--------|--------|
| 1 | planner | this brief + issues #87 #88 + source | design doc | ⏳ PENDING |
| 2 | implementer | design doc | code fixes across framework + CLI + templates | — |
| 3 | tester | implementation | component test + regression tests | — |
| 4 | reviewer | code + tests | verdict | — |
| 5 | integrator | approved code | verify + clean | — |
| 6 | ship | branch | PR to main | — |

## Handoff Log
- 2026-04-30: Task created by coordinator. Dispatched to planner.

---

# Completed Task: TASK-20260430-001 — Local Dev DX: Critical & High gaps
**Status:** COMPLETE — merged as PR #86
**Branch:** `agent/TASK-20260430-001/local-dev-dx`
**Issue:** #85
**Started:** 2026-04-30

## Goal
Address the Critical and High-severity gaps from the local Python-first developer audit (issue #85). After this task, a developer can run `kindling new`, install dependencies, and execute a pipe locally without Azure credentials, with clear error messages and working test isolation.

## Scope — items in this task
| # | Item | Severity | Type |
|---|------|----------|------|
| 1 | No local-first entity config in scaffold | 🔴 Critical | Template fix |
| 2 | No `kindling run <pipe_id>` command | 🔴 Critical | New CLI command |
| 3 | Silent failure when importing before `initialize_framework()` | 🔴 Critical | Runtime fix |
| 4 | `kindling env check` wrong default config path | 🟠 High | CLI fix |
| 5 | `poe test-unit` requires `kindling` on PATH | 🟠 High | Template fix |
| 6 | No `kindling validate` command | 🟠 High | New CLI command |
| 7 | No test isolation fixture + private API in reset | 🟠 High | Runtime + Template fix |
| 8 | `@secret:` prefix undocumented in scaffold | 🟠 High | Template fix |
| 11 | `kindling new` next-steps output wrong | 🟡 Medium | CLI fix |

## Out of scope (deferred to future tasks)
- `kindling entity list` / `kindling pipe list` (#9)
- `--platform` flag on `kindling new` (#10)
- `kindling upgrade` (#12)
- Local-project integration test using Kindling execution (#13)
- Devcontainer multi-package venv path (#14)
- Low-severity items (#15–#19)

## Acceptance Criteria
- [ ] `kindling new my-app && cd my-app/packages/my-app && poetry install && kindling run bronze_to_silver --env local` succeeds without Azure credentials
- [ ] Importing an entity module before `initialize()` raises `KindlingNotInitializedError` with an actionable message
- [ ] `kindling env check` run from a scaffolded package root passes `config_file_exists`
- [ ] `poetry run poe test-unit` works without `kindling` on PATH
- [ ] `kindling validate` reports mismatched entity IDs and missing required fields without starting Spark
- [ ] Scaffolded `conftest.py` includes `reset_kindling` fixture using public `DataEntities.reset()` / `DataPipes.reset()` API
- [ ] `env.local.yaml` template generates a local-first config (memory provider or local delta path) with ABFSS as a commented overlay
- [ ] `@secret:` resolution from env vars is documented in generated config comments
- [ ] All existing unit tests pass (`poe test-unit`)
- [ ] New unit tests cover: `kindling run` happy path, `kindling validate` with good/bad definitions, `KindlingNotInitializedError`, `DataEntities.reset()`

## Agent Plan
| Step | Agent | Input | Output | Status |
|------|-------|-------|--------|--------|
| 1 | planner | this brief + issue #85 + current source | design doc | ⏳ PENDING |
| 2 | implementer | design doc | code changes across CLI + runtime + templates | — |
| 3 | tester | implementation | unit tests for new commands + runtime fixes | — |
| 4 | reviewer | code + tests | verdict + notes | — |
| 5 | integrator | approved code + reviewer notes | merged to branch | — |
| 6 | ship | branch | PR to main | — |

## Handoff Log
- 2026-04-30: Task created by coordinator. Dispatched to planner.

---

# Completed Task: TASK-20260429-002 SCD2 Follow-up Fixes
**Status:** COMPLETE — merged as PR #82
**Started:** 2026-04-29

## Goal
Fix 4 correctness/validation gaps identified by Copilot review of PR #77. All fixes are in `entity_provider_delta.py` and `data_entities.py`.

## Acceptance Criteria
- [x] #78: `read_entity_as_of` SCD2 filter uses `lit(point_in_time).cast("timestamp")`
- [x] #79: `_validate_scd_config()` raises `ValueError` if `__merge_key` appears in entity's declared columns
- [x] #80: `_execute_scd2_merge` coalesces each business key to `__null__` sentinel before `concat_ws`
- [x] #81: `_validate_scd_config()` raises `ValueError` if `current_entity_id` equals base `entityid` or is empty
- [x] All existing tests pass (`poe test-unit`)
- [x] New tests cover each fix

## Agent Plan
| Step | Agent | Status |
|------|-------|--------|
| 1 | implementer | ✅ DONE |
| 2 | tester | ✅ DONE |
| 3 | ship | ✅ DONE |

## Handoff Log
- 2026-04-29: Task created by coordinator. Dispatched directly to implementer (no planner needed — fixes fully specified in issues #78–#81).
- 2026-04-29: All 4 fixes implemented and tested. PR #82 created, Copilot reviewed, merged.

---

# Completed Task: TASK-20260429-001 SCD Type 2 Entity Support
**Status:** COMPLETE — merged as PR #77
**Requested:** Implement SCD Type 2 merge semantics for Delta-backed entities
**Goal:** Delta entities tagged `scd.type: "2"` automatically get SCD2 merge behavior (close current row, insert new version) via a named strategy applied by `DeltaEntityProvider`, without any changes to the execution layer or `EntityMetadata` schema.
**Started:** 2026-04-29

## Acceptance Criteria
- [ ] `DeltaMergeStrategy` protocol exists with an `apply(delta_table, df, entity, merge_condition)` signature
- [ ] `DeltaMergeStrategies` class-level registry with `@register(name=...)` decorator (raises on duplicate)
- [ ] `SCD1MergeStrategy` registered as `"scd1"` — default; existing `whenMatchedUpdateAll / whenNotMatchedInsertAll` behavior
- [ ] `SCD2MergeStrategy` registered as `"scd2"` — staged-updates: close current row, insert new version
- [ ] `_merge_to_delta_table` resolves strategy by name via `DeltaMergeStrategies.get()`, no internal branching
- [ ] `scd_config_from_tags(entity)` helper extracts and validates `scd.*` tags into `SCDConfig`
- [ ] `ensure_entity_table()` auto-augments schema with temporal columns (`__effective_from`, `__effective_to`, `__is_current`) when `scd.type == "2"`
- [ ] Companion entity `{entityid}.current` auto-registered at `DataEntityManager.register_entity()` time for SCD2 entities; filters to `is_current=true`
- [ ] Existing entities without `scd.type` tag are unaffected (SCD1 is default)
- [ ] Unit tests for: strategy registry, SCD1 strategy, SCD2 merge logic, `scd_config_from_tags`, companion entity registration
- [ ] `docs/proposals/scd_type2_support.md` used as the implementation spec

## Key Files
- `packages/kindling/entity_provider_delta.py` — primary implementation target
- `packages/kindling/data_entities.py` — companion entity auto-registration
- `docs/proposals/scd_type2_support.md` — full spec (tags, SCDConfig, signals, phasing)

## Design Decisions (see DECISIONS.md 2026-04-29)
- Strategy is Delta-scoped — no cross-provider abstraction
- Registry follows `PlatformServices` decorator pattern, not `EntityProviderRegistry` instance pattern
- Duplicate registration raises `ValueError` (unlike `PlatformServices` which silently skips)
- `scd.strategy` tag resolves strategy name; `scd.type: "2"` implies `strategy: "scd2"` as a convenience

## Agent Plan
| Step | Agent | Input | Output | Status |
|------|-------|-------|--------|--------|
| 1 | planner | this task + proposal doc | implementation plan | ✅ DONE |
| 2 | implementer | implementation plan | code in `entity_provider_delta.py`, `data_entities.py` | ✅ DONE |
| 3 | tester | implementation | unit test suite (57 tests, 1080 total passing) | ✅ DONE |
| 4 | reviewer | code + tests | APPROVED WITH NOTES (3 minor findings) | ✅ DONE |
| 5 | integrator | approved code + reviewer notes | merged to branch | ✅ DONE |

## Handoff Log
- 2026-04-29: Task created by coordinator. Proposal finalized with strategy registration design.
  Planner should start from `docs/proposals/scd_type2_support.md` — design is settled,
  focus the plan on implementation order and test strategy, not re-litigating the design.
- 2026-04-29: Planner complete → dispatched to implementer (Codex).
- 2026-04-29: Implementer complete → dispatched to tester (Codex). 57 new unit tests added.
- 2026-04-29: Tester complete → 57 passed, 1080 total. Dispatched to reviewer.
- 2026-04-29: Reviewer complete → APPROVED WITH NOTES. 3 minor findings dispatched to integrator:
  1. Add comment in `_execute_scd2_merge` on unchanged-row exclusion from Group B
  2. Explicit boolean filter: `col(cfg.is_current_column) == lit(True)` at ~line 173
  3. Companion name format: `f"{base.name} (current)"` (parenthetical, per spec)

## Artifacts
- Spec: `docs/proposals/scd_type2_support.md`
- Decision: `DECISIONS.md` — "SCD merge behavior is a Delta-scoped strategy" (2026-04-29)

## Handoff: integrator → coordinator @ 2026-04-29T20:20:00Z
**Did:** Applied 3 reviewer-note integration fixes for TASK-20260429-001; validated provider wiring and companion registration; ran unit+integration suites (all green).
**Touched:** packages/kindling/entity_provider_delta.py; packages/kindling/data_entities.py
**Decided:** No CHANGELOG.md exists; release-note update deferred pending explicit target version file.
**Need from you:** Close out task or request release-note target version for documentation update.
**Blockers:** None.

## Handoff: implementer → tester @ 2026-04-30T19:48:25Z
**Task:** TASK-20260430-003 DX eval remediation.
**Did:** Implemented G3-G9, G12, and G13 from `.agent-memory/design-TASK-20260430-003.md`; kept pre-DI logging on stdlib loggers and removed targeted `print()`/`traceback.print_exc()` paths that could disturb stdout monitors.
**Touched:** README/docs, Spark session/config/framework/app logging paths, entity scaffold templates, and integration pipeline template.
**Verified:** `poetry run poe test-unit` passed with 1110 tests and 2 warnings.
**Next:** Tester should add/confirm G9 sentinel unit coverage and re-run `poe test-unit`.
**Blockers:** None.

## Handoff: tester → reviewer @ 2026-04-30T19:54:01Z
**Task:** TASK-20260430-003 DX eval remediation.
**Did:** Added G9 sentinel regression tests, found a reload-sensitive sentinel identity bug, routed it back, then verified the fix.
**Verified:** `poetry run pytest tests/unit/test_spark_config.py -q` passed 34 tests; `poetry run poe test-unit` passed 1115 tests with 2 warnings.
**Touched:** `tests/unit/test_spark_config.py` plus the implementer fix in `packages/kindling/spark_config.py`.
**Note:** The final fix/test diff is uncommitted because git staging escalation was rejected by the approval usage limit; implementation commit `92fec31` exists for the first batch.
**Blockers:** None.
