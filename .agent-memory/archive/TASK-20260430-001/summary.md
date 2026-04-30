# Summary: TASK-20260430-001 — Local Dev DX: Critical & High gaps

**Merged:** 2026-04-30 | **Branch:** agent/TASK-20260430-001/local-dev-dx | **PR:** https://github.com/sep/spark-kindling-framework/pull/86

## What was built
Added `kindling run` and `kindling validate` CLI commands, `KindlingNotInitializedError`, and public
`DataEntities.reset()` / `DataPipes.reset()` API. Fixed 5 scaffold template/config gaps that blocked
local development without Azure credentials: memory-first `env.local.yaml`, plain-pytest `pyproject.toml`,
public `reset_kindling` fixture in `conftest.py`, correct `env check` default config path, and correct
`kindling new` next-steps output.

## Key decisions
- `kindling validate` uses real `initialize_framework(platform="standalone")` — standalone defers SparkSession
  lazily so validation is Spark-free without mocking
- `KindlingNotInitializedError` defined in `data_entities.py`, imported by `data_pipes.py`, exported from
  top-level `kindling` package
- `env.local.yaml` template defaults to `provider_type: memory` (ephemeral, no filesystem), with ABFSS
  as inline commented alternatives per entity — avoids duplicate YAML keys

## Intentionally deferred
- `kindling entity list` / `kindling pipe list` (issue #85 item #9)
- `--platform` flag on `kindling new` (item #10)
- `kindling upgrade` (item #12)
- Local-project integration test using Kindling execution (item #13)
- Devcontainer multi-package venv path (item #14)
- Low-severity items #15–#19
- Copilot follow-ups: reset_kindling `sys.modules` clearing, `validate_app` None guard on
  `get_pipe_definition`, docstring softening on Spark-free claim

## Files changed
- `packages/kindling/__init__.py` — export KindlingNotInitializedError
- `packages/kindling/data_entities.py` — KindlingNotInitializedError, DataEntities.reset()
- `packages/kindling/data_pipes.py` — DataPipes.reset(), guard in pipe() decorator
- `packages/kindling_cli/kindling_cli/cli.py` — kindling run, kindling validate, env check fix, new next-steps fix
- `packages/kindling_cli/kindling_cli/templates/config/env.local.yaml.j2` — memory-first local config
- `packages/kindling_cli/kindling_cli/templates/tests/conftest.py.j2` — reset_kindling fixture
- `packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2` — plain pytest poe tasks
- `tests/unit/test_cli_local_dev_dx.py` — new CLI command tests
- `tests/unit/test_data_entities.py` — reset() and error tests
- `tests/unit/test_data_pipes.py` — reset(), error, no-double-wrap tests
- `tests/unit/test_kindling_initialize_entrypoint.py` — KindlingNotInitializedError export test
- `tests/unit/test_scaffold.py` — next-steps single-cd test
- `docs/local_python_first.md` — kindling run/validate, memory providers, KindlingNotInitializedError
- `docs/developer_workflow.md` — running a pipe locally section
- `CHANGELOG.md` — created

## For future agents
- The `reset_kindling` fixture in conftest.py.j2 does NOT clear `sys.modules` for project-specific modules.
  Developers with circular decorator side-effects at import time may need to add `sys.modules` clearing.
  This is the top Copilot follow-up to address.
- `kindling validate` validates registry state only — it does not run the pipe or touch providers.
  If a developer adds a provider-level check to validate, they must ensure standalone platform
  doesn't accidentally create a SparkSession.
- The deferred items above (#9–#19 from issue #85) are a natural next issue/task.
