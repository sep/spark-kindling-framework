STATUS: IDLE
VERDICT: CHANGES REQUESTED
TASK: TASK-20260430-001
FROM: reviewer
RECEIVED: 2026-04-30T15:42:00Z

## Instruction

Three required fixes from review. Do not change anything else.

### Fix 1 — `env.local.yaml.j2` duplicate `entity_tags:` key (MAJOR)

**File:** `packages/kindling_cli/kindling_cli/templates/config/env.local.yaml.j2`

The template currently renders `entity_tags:` as a live key AND again as `# entity_tags:`
in the commented ABFSS block. A developer who uncomments the ABFSS section gets a duplicate
YAML key — the memory entries are silently overwritten.

Restructure so `entity_tags:` appears exactly once and the ABFSS override is a commented
alternative nested under the same key. Example structure for medallion:

```yaml
entity_tags:
  bronze.records:
    provider_type: "memory"
    # To use ABFSS instead, comment out provider_type and uncomment provider.path:
    # provider.path: "@secret:ABFSS_BRONZE_PATH"
  silver.records:
    provider_type: "memory"
    # provider.path: "@secret:ABFSS_SILVER_PATH"
```

The Jinja `{% if layers == "medallion" %}` / `{% else %}` conditionals must still wrap
the full content.

### Fix 2 — `KindlingNotInitializedError` double-wrapping (MAJOR)

**Files:** `packages/kindling/data_entities.py` (2 locations) and `packages/kindling/data_pipes.py` (1 location)

In every decorator that catches `Exception` and re-raises `KindlingNotInitializedError`,
add a guard so a `KindlingNotInitializedError` from an inner `_raise_if_not_initialized`
call is not caught and re-wrapped:

```python
except Exception as exc:
    if isinstance(exc, KindlingNotInitializedError):
        raise
    raise KindlingNotInitializedError("...") from exc
```

Apply to all three locations where this pattern appears.

### Fix 3 — Missing agent tags on runtime file changes (MINOR, required by CONVENTIONS.md)

**Files:** `packages/kindling/data_entities.py` and `packages/kindling/data_pipes.py`

Add `# [implementer] <brief reason> — TASK-20260430-001` comment above:
- `KindlingNotInitializedError` class definition in data_entities.py
- `DataEntities.reset()` method in data_entities.py
- `DataPipes.reset()` method in data_pipes.py

## Context Files
- `.agent-memory/review-TASK-20260430-001.md` — full review with line numbers
- `packages/kindling_cli/kindling_cli/templates/config/env.local.yaml.j2`
- `packages/kindling/data_entities.py`
- `packages/kindling/data_pipes.py`

## On Complete
Run `poe test-unit`. If green, write to `.agent-memory/mailboxes/tester.md`:
  STATUS: PENDING
  TASK: TASK-20260430-001
  FROM: implementer
  ## Instruction
  Re-run `poe test-unit`. Confirm all 1105+ tests still pass after the 3 reviewer fixes.
  Add a test for the double-wrapping fix: import a pipe before initialize(), assert
  exactly one KindlingNotInitializedError is raised (no chained same-type cause).
  ## Context Files
  - .agent-memory/review-TASK-20260430-001.md
  - packages/kindling/data_entities.py
  - packages/kindling/data_pipes.py
  - packages/kindling_cli/kindling_cli/templates/config/env.local.yaml.j2
  - tests/unit/test_data_entities.py
  - tests/unit/test_data_pipes.py
  ## On Complete
  Dispatch to mailboxes/reviewer.md with STATUS: PENDING
