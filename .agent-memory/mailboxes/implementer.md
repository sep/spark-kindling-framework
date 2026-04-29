STATUS: IDLE
TASK: TASK-20260429-001
FROM: planner
RECEIVED: 2026-04-29T00:00:00Z

## Instruction
Implement SCD Type 2 entity support per the implementation plan. Follow the plan's step order exactly — do not skip ahead.

Step order:
1. `data_entities.py` — SCDConfig, scd_config_from_tags, _validate_scd_config, DataEntityManager companion registration
2. `entity_provider_delta.py` — DeltaMergeStrategy ABC, DeltaMergeStrategies registry, SCD1MergeStrategy (extract), SCD2MergeStrategy stub, refactor _merge_to_delta_table
3. `entity_provider_delta.py` — _execute_scd2_merge, SCD2MergeStrategy full body, _augment_schema_for_scd2, read_entity_as_of
4. `entity_provider_current_view.py` — new file, CurrentViewEntityProvider
5. `entity_provider_registry.py` — register "current_view" provider

Phase 1 only — do NOT implement scd.close_on_missing or scd.optimize_unchanged yet.
Type hints on all public functions. No bare except:. No print(). No commented-out code.

## Context Files
- `docs/proposals/scd_type2_implementation_plan.md` — step-by-step plan with exact signatures
- `docs/proposals/scd_type2_support.md` — full design spec (tags, signals, SCD2 merge detail)
- `packages/kindling/data_entities.py` — modify register_entity, add SCDConfig
- `packages/kindling/entity_provider_delta.py` — primary target; study _merge_to_delta_table (line 947), _create_physical_table (line 712)
- `packages/kindling/notebook_framework.py` — PlatformServices.register pattern (line 1904) to follow for DeltaMergeStrategies
- `packages/kindling/entity_provider_registry.py` — add current_view in _register_builtin_providers
- `.agent-memory/CONVENTIONS.md` — naming, testing, OSS rules

## On Complete
Run: poe test-unit
If green, dispatch to mailboxes/tester.md:
  STATUS: PENDING / TASK: TASK-20260429-001 / FROM: implementer
  Instruction: Write unit tests per plan Section 4 (all 6 test files). Run poe test-unit.
  Context Files: implementation files + plan doc
  On Complete: dispatch to mailboxes/reviewer.md
