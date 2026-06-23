# Changelog

All notable changes to spark-kindling are documented here.

## [0.10.33] - 2026-06-23

### Changed

- Removed legacy agent workflow references from repository agent instructions and
  quickstart documentation.
- Removed legacy agent tooling setup from the devcontainer configuration.
- Added the app-local settings overlay proposal document.

## [0.10.31] - 2026-06-19

### Fixed

- `kindling app run` (standalone) no longer incorrectly fetches lake-reqs packages from the
  lake by default. Packages already installed in the current Python environment (editable,
  regular pip, devcontainer pre-install, or published to a registry) are used as-is without
  contacting the lake. This fixes a regression where `artifacts_storage_path` was required
  even for purely local runs.
- Fixed `lake-reqs.txt` spec parsing to correctly handle `~=` and `!=` version operators.
- Added `--load-lake` flag to `kindling app run` (standalone-only) to explicitly force
  downloading lake-reqs packages from the lake regardless of local install state.

## [0.10.32] - 2026-06-23

### Changed

- App runtime configuration now treats `app.yaml` as manifest-only and loads runtime
  settings from `settings.yaml`, `settings.<platform>.yaml`, then
  `settings.<env>.yaml`, with environment overlays taking precedence. Legacy
  `platform_<platform>.yaml`, `env_<env>.yaml`, and app-local `app.<target>.yaml`
  files remain fallback inputs for compatibility, but new config should use the
  dot-style `settings.*.yaml` names.
- `runtime publish` renamed to `runtime deploy`. Same behavior; the new name is consistent with
  `app deploy`, `package deploy`, and `workspace deploy`. **Breaking change**: update scripts
  and CI pipelines to use `kindling runtime deploy`.
- `workspace init` — completely new behavior. Now initializes the platform workspace by deploying
  `settings.yaml` + overlays to `{base}/config/` in storage. With `--notebook-bootstrap`, also
  generates and imports notebook bootstrap files into the platform workspace.
  **Breaking change**: the old local file generation behavior (`--output-dir`, `--force`) is
  removed. Use `--notebook-bootstrap` with `--workspace` for notebook import.
- `workspace deploy` — now deploys config only (`settings.yaml` + overlays to `{base}/config/`).
  Wheel and bootstrap script deployment have been removed; use `kindling runtime deploy` for those.
  **Breaking change**: `--dist-dir`, `--skip-wheels`, `--skip-bootstrap-script`,
  `--allow-missing-bootstrap-script`, `--create-notebooks`, and `--workspace` options removed.
- `workspace check` removed. Use `kindling env check --platform <platform>` instead.

- `app package <APP_NAME>` — positional is now the app name; kindling discovers `apps/<name>/`
  by convention (kebab→snake normalized). `--local-folder` overrides for non-standard layouts.
  **Breaking change** for callers passing a path as the positional; use `--local-folder` instead.
- `app deploy <APP_NAME>` — positional is now the app name with the same convention lookup.
  `--local-folder` overrides; `--kda-package` deploys a pre-built archive. Removed the
  "must supply --local-folder or --kda-package" requirement; passing just the app name is now
  the normal workflow. `--app-name` renamed to `--remote-name` to avoid naming conflict.
- `app run <APP_NAME>` — standalone mode now does convention lookup (`apps/<name>/`) instead of
  requiring a path. `--local-folder` overrides. Remote mode (`--platform`) no longer auto-deploys
  when a local path is detected — the app must be deployed first with `kindling app deploy`.
  Added `--local-folder` option (standalone-only; errors if used with `--platform`).
  **Breaking change** for `app run <path> --platform` workflows; split into separate
  `app deploy` + `app run` calls.
- `app cleanup <APP_NAME>` — removed `--local-folder` and `--kda-package` flags; APP_NAME is now
  required. Pass the app name directly instead of inferring it from a path.
  **Breaking change** for callers using `--local-folder` or `--kda-package`; pass the name directly.
- `package deploy <PACKAGE_NAME>` — positional is now the package name; kindling discovers
  `packages/<name>/` by convention. `--local-folder` overrides.
  **Breaking change** for callers passing a path as the positional; use `--local-folder` instead.

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
