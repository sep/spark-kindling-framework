# Changelog

All notable changes to spark-kindling are documented here.

## [Unreleased]

### Added
- `kindling run <pipe_id>` CLI command — executes a registered pipe locally after auto-discovering `app.py`; no cloud credentials required (TASK-20260430-001, #85)
- `kindling validate` CLI command — statically validates entity/pipe definitions without starting Spark (TASK-20260430-001, #85)
- `KindlingNotInitializedError` — raised with actionable message when entity/pipe decorators fire before `initialize()` (TASK-20260430-001, #85)
- `DataEntities.reset()` and `DataPipes.reset()` public classmethods for test isolation (TASK-20260430-001, #85)

### Changed
- Scaffold `env.local.yaml` now uses `provider_type: memory` by default — no Azure credentials needed to run locally (TASK-20260430-001, #85)
- Scaffold `conftest.py` includes `reset_kindling` fixture using public reset API (TASK-20260430-001, #85)
- Scaffold poe tasks use plain `pytest` instead of `kindling test run` wrapper (TASK-20260430-001, #85)
- `kindling env check` auto-probes `config/settings.yaml` before `settings.yaml` when `--config` is omitted (TASK-20260430-001, #85)
- `kindling new` next-steps output uses a single `cd` command (TASK-20260430-001, #85)

### Fixed
- `NullWatermarkEntityFinder` auto-bound in standalone platform — `kindling run` now executes pipes in scaffolded projects without Azure credentials (TASK-20260430-002, #87)
- Removed/routed 109 unconditional debug `print()` calls in `bootstrap.py` and `spark_config.py` — CLI output is clean by default (TASK-20260430-002, #87)
- Added `--env` option to `kindling validate` for consistency with `kindling run` (TASK-20260430-002, #87)
- `kindling new` next-steps now annotates `.env` copy step as optional for local-first dev (TASK-20260430-002, #87)
- `spark-kindling-cli` uncommented as dev dependency in scaffold `pyproject.toml.j2` — `kindling` available after `poetry install` (TASK-20260430-002, #87)

### Tests
- Added `test_di_wiring_standalone.py` — real DI construction test with no mocking; catches abstract-class binding gaps before they ship (TASK-20260430-002, #88)
