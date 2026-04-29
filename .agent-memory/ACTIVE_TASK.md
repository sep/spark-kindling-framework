# Active Task: TASK-20260429-001 SCD Type 2 Entity Support
**Status:** IN PROGRESS
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
| 5 | integrator | approved code + reviewer notes | merged to branch | 🔄 IN PROGRESS |

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
