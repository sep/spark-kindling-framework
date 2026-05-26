# Changelog

All notable changes to spark-kindling are documented here.

## [Unreleased]

## [0.9.30] - 2026-05-26

### Added
- `kindling._runner` module — standalone app execution now initializes the Kindling framework before running the app entrypoint, mirroring the remote `DataAppManager` execution path; `app.py` no longer contains bootstrap code
- `DataAppConstants.LOCAL_SETTINGS_FILE` — `settings.local.yaml` is excluded from KDA packages at build time

### Changed
- App and package scaffolds no longer generate a `config/` subdirectory; config files now sit at the project root following ASP.NET-style layering: `settings.yaml` (base, deployed) and `settings.local.yaml` (local dev overlay, gitignored, never deployed)
- Generated `app.py` is now a passive entrypoint — contains only domain module import stubs and a startup log line; all framework initialization is owned by the CLI runner or platform
- `kindling app run` (standalone) invokes `python -m kindling._runner` with explicit `--config` args resolved from the app directory instead of passing `KINDLING_CONFIG_DIR`
- Default app entrypoint renamed from `main.py` to `app.py` across CLI, `DataAppConstants`, and `notebook_framework`; `LEGACY_ENTRY_POINT` constant and silent `main.py` fallback removed

### Fixed
- `settings.local.yaml` excluded from KDA packaging in both single-platform and multi-platform build paths

### Added
- `scd.close_on_missing: "true"` tag on SCD2 entities — rows absent from the source batch are logically closed (`__effective_to = now`, `__is_current = false`) via `whenNotMatchedBySourceUpdate`; safe for full-snapshot sources (TASK-20260430-004, #83)
- `scd.optimize_unchanged: "true"` tag on SCD2 entities — SHA2-256 hash comparison over tracked columns replaces per-column change detection; reduces merge cost for large dimensions with many unchanged rows (TASK-20260430-004, #84)
- CLI Quick Start section in `README.md` — covers `kindling new` → `poetry install` → `kindling run` end-to-end local workflow (TASK-20260430-003)
- Local Development section in `docs/setup_guide.md` — placed before cloud-platform sections so developers can run locally without cloud credentials (TASK-20260430-003)
- Sentinel-based missing-key debug logging to `DynaconfConfig.get()` in `spark_config.py` — surfaces misconfigured keys without raising at call-time (TASK-20260430-003)
- Entity guidance comments to scaffold entity templates (`records.medallion.py.j2`, `records.minimal.py.j2`) — inline hints steer developers toward correct field/SCD patterns (TASK-20260430-003)
- `TestKindlingPipeExecution` class to scaffold integration test template — gives generated projects a ready-to-run pipe execution test (TASK-20260430-003)
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
- Stale pinned version `0.6.6` removed from `docs/intro.md` — version line deleted to avoid future staleness (TASK-20260430-003)
- 29 bare `print()` calls in `data_apps.py` routed to `self.logger`; additional bare prints fixed in `spark_session.py` and `notebook_framework.py` — structured log output consistent throughout (TASK-20260430-003)
- `NullWatermarkEntityFinder` auto-bound in standalone platform — `kindling run` now executes pipes in scaffolded projects without Azure credentials (TASK-20260430-002, #87)
- Removed/routed 109 unconditional debug `print()` calls in `bootstrap.py` and `spark_config.py` — CLI output is clean by default (TASK-20260430-002, #87)
- Added `--env` option to `kindling validate` for consistency with `kindling run` (TASK-20260430-002, #87)
- `kindling new` next-steps now annotates `.env` copy step as optional for local-first dev (TASK-20260430-002, #87)
- `spark-kindling-cli` uncommented as dev dependency in scaffold `pyproject.toml.j2` — `kindling` available after `poetry install` (TASK-20260430-002, #87)

### Tests
- Added `test_di_wiring_standalone.py` — real DI construction test with no mocking; catches abstract-class binding gaps before they ship (TASK-20260430-002, #88)
