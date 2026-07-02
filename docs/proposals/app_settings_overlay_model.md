# Proposal: App-Local Settings Overlay Model (Manifest-Only `app.yaml`)

**Status:** Mostly implemented. All items done except #6 (removing the legacy
`app.<platform>.yaml` / `app.<env>.yaml` fallback), which is intentionally retained as a
compatibility shim — see Legacy Fallback Behavior below. Revisit #6 only when ready to force
a breaking config migration.

## Goal

Adopt a consistent config model where:

1. `app.yaml` is manifest-only.
2. Runtime settings are app-local:
   1. `settings.yaml`
   2. `settings.<platform>.yaml`
   3. `settings.<env>.yaml`
3. Precedence is:
   1. `settings.yaml`
   2. `settings.<platform>.yaml`
   3. `settings.<env>.yaml` (env wins)
4. Deployment and packaging include only the selected env/platform overlays.

## Current Gaps

1. Workspace config deploy uploads all `platform_*.yaml` and `env_*.yaml` overlays in a directory.
2. App package/deploy includes all deployable YAML files by extension, not selected overlays only.
3. Bootstrap config loading still uses legacy `platform_*` / `env_*` naming and ordering.
4. Some app config loading paths use `app.<env>.yaml` behavior instead of app-local `settings.<...>.yaml`.
5. Several tests/docs assume legacy naming and behavior.

## Implementation Checklist

### 1) Canonical naming + compatibility policy

- [x] Define canonical naming as `settings.<name>.yaml`.
- [x] Decide whether to retain fallback reads of legacy `platform_<x>.yaml` / `env_<x>.yaml` — **retained** (see Legacy Fallback Behavior below).

Files:

- `packages/kindling_cli/kindling_cli/cli.py`
- `packages/kindling/bootstrap.py`
- `packages/kindling/data_apps.py`

Test impact:

- `tests/unit/test_cli.py` (`TestConfigSetLevelRouting`)
- `tests/unit/test_platform_workspace_config.py`

### 2) `config set --level` file routing

- [x] Route `--level platform` to `settings.<platform>.yaml`.
- [x] Route `--level env` to `settings.<env>.yaml`.
- [x] Apply same naming for app-scoped config writes.

Files:

- `packages/kindling_cli/kindling_cli/cli.py` (`_resolve_config_path`, `config_set`)

Test impact:

- Update:
  - `tests/unit/test_cli.py::TestConfigSetLevelRouting::test_platform_level`
  - `tests/unit/test_cli.py::TestConfigSetLevelRouting::test_env_level`
  - `tests/unit/test_cli.py::TestConfigSetAppScope::test_app_with_platform_level`

### 3) Selective app artifact assembly (package/deploy)

- [x] Filter app artifact config files to selected target only:
  - include: `app.yaml`, `settings.yaml`, selected `settings.<platform>.yaml`, selected `settings.<env>.yaml`
  - exclude: unrelated overlays and local-only settings
- [x] Add/confirm CLI options needed for filtering (`--env`, `--platform`).

Files:

- `packages/kindling_cli/kindling_cli/cli.py` (`_prepare_app_files`, `app_package`, `app_deploy`)
- `packages/kindling/app_files.py` (optional helper adjustment)

Test impact:

- Add/update in `tests/unit/test_cli.py`:
  - package file list includes only selected overlays
  - deploy payload includes only selected overlays
  - local-only overlays excluded

### 4) Standalone app run overlay resolution

- [x] Ensure standalone run loads app-local settings in order:
  - `settings.yaml`
  - `settings.<platform>.yaml` (if applicable)
  - `settings.<env>.yaml`

Files:

- `packages/kindling_cli/kindling_cli/cli.py` (`_run_standalone_app`)

Test impact:

- Update command argument/config-file order assertions in `tests/unit/test_cli.py` (standalone app run tests).

### 5) Bootstrap remote config loading order

- [x] Update remote bootstrap download/load order to reflect canonical overlay precedence.
- [x] If compatibility retained, prefer dot-style names and fall back to legacy names.

Files:

- `packages/kindling/bootstrap.py` (`download_config_files`)

Test impact:

- Update `tests/unit/test_platform_workspace_config.py`:
  - ordering assertions
  - selected overlay assertions
  - fallback behavior assertions (if enabled)

### 6) Remove `app.<x>.yaml` as runtime settings source

- [ ] Stop using `app.<env|platform>.yaml` for runtime settings resolution.
- [ ] Keep `app.yaml` manifest-only.

  **Status:** Partially done. `_load_config_content` in `data_apps.py` now reads `settings.yaml` and
  `settings.<platform|env>.yaml` as the primary sources, but it still reads `app.yaml` as the base
  manifest layer and falls back to `app.<platform>.yaml` / `app.<env>.yaml` when the canonical
  `settings.*` files are absent (see Legacy Fallback Behavior below). The fallback path has not yet
  been removed.

Files:

- `packages/kindling/data_apps.py` (`_load_config_content`, `_merge_platform_config_at_deploy_time`, constants/usages)

Test impact:

- Update tests currently expecting `app.<platform>.yaml` merge behavior:
  - `tests/unit/test_kda_integration.py`
  - `tests/unit/test_notebook_utilities.py`

### 7) KDA packaging semantics alignment

- [x] Ensure package creation does not repurpose `app.yaml` for merged runtime settings.
- [x] Keep runtime overlay files explicit and selected.

  `DataAppPackage.create` uses `is_deployable_app_file` (from `app_files.py`) to filter KDA contents,
  which excludes `app.<x>.yaml` legacy files and unselected overlays.

Files:

- `packages/kindling/data_apps.py` (`DataAppPackage.create` and related helpers)

Test impact:

- Update KDA structure assertions in:
  - `tests/unit/test_kda_integration.py`
  - `tests/unit/test_notebook_utilities.py`

### 8) Workspace deploy filtering

- [x] Replace wildcard overlay upload behavior with selected platform/env overlay upload.
- [x] Add `--env` option where needed for workspace flows.

  `_deploy_config` now uploads only `settings.yaml` plus whichever `settings.<platform>.yaml` /
  `settings.<env>.yaml` overlays are explicitly selected. No wildcard upload occurs.

Files:

- `packages/kindling_cli/kindling_cli/cli.py` (`_deploy_config`, `workspace_init`, `workspace_deploy`)

Test impact:

- Update workspace tests in `tests/unit/test_cli.py` that monkeypatch `_deploy_config`.

### 9) Scaffold alignment

- [x] Ensure generated app structure and comments match manifest-only `app.yaml` + settings overlay model.

  `app.yaml.j2` contains only manifest fields (`name`, `entry_point`). `settings.yaml.j2` holds
  runtime Kindling configuration. The separation is correct.

Files:

- `packages/kindling_cli/kindling_cli/scaffold.py`
- `packages/kindling_cli/kindling_cli/templates/app.yaml.j2`
- `packages/kindling_cli/kindling_cli/templates/settings.yaml.j2`
- `packages/kindling_cli/kindling_cli/templates/settings.local.yaml.j2`

Test impact:

- Validate/update `tests/unit/test_scaffold.py` expectations.

### 10) Docs update

- [x] Update naming and precedence examples.
- [x] Update package/deploy descriptions to “selected overlays only.”

  Verified no lingering `platform_<x>.yaml` / `env_<x>.yaml` / `app.<platform>.yaml` /
  `app.<env>.yaml` references in `cli_reference.md`, `setup_guide.md`,
  `domain_project_quickstart.md`, or `local_python_first.md`.

Files:

- `docs/reference/cli_reference.md`
- `docs/guide/setup_guide.md`
- `docs/guide/domain_project_quickstart.md`
- `docs/guide/local_python_first.md`

### 11) Regression test execution

- [x] Run and fix:
  - `pytest -q tests/unit/test_cli.py`
  - `pytest -q tests/unit/test_platform_workspace_config.py`
  - `pytest -q tests/unit/test_kda_integration.py tests/unit/test_notebook_utilities.py`
  - `pytest -q tests/unit/test_scaffold.py`

  277/277 passing as of 2026-07-01.

### 12) Migration notes

- [x] Add migration notes and compatibility guidance.

  See `CHANGELOG.md` [0.10.32] and [0.10.33] entries below, plus the Migration Notes
  section of this document.

Files:

- `CHANGELOG.md`

---

## Legacy Fallback Behavior

Two code paths implement a silent fallback from the new dot-style canonical names to the old
naming conventions. Both are active as of the current implementation and should be explicitly
retained or removed as a follow-up decision.

### `download_config_files` in `packages/kindling/bootstrap.py`

When a canonical overlay file is not found in remote storage, the download function tries the
corresponding legacy path before skipping the file:

| Canonical name (tried first) | Legacy fallback (tried second) |
|---|---|
| `config/settings.<platform>.yaml` | `config/platform_<platform>.yaml` |
| `config/settings.<env>.yaml` | `config/env_<env>.yaml` |
| `data-apps/<app>/settings.<platform>.yaml` | `data-apps/<app>/app.<platform>.yaml` |
| `data-apps/<app>/settings.<env>.yaml` | `data-apps/<app>/app.<env>.yaml` |

The fallback path is logged at INFO level (`”Downloaded legacy config … after … was not found”`).
Files are not found silently at DEBUG level when neither path exists.

### `_load_config_content` in `packages/kindling/data_apps.py`

When loading app config for a deployed or staged app, the method:

1. Reads `app.yaml` as the base manifest layer (if present).
2. Merges `settings.yaml` on top (required if `app.yaml` is absent).
3. If `platform` is given: tries `settings.<platform>.yaml`; on failure falls back to `app.<platform>.yaml`.
4. If `environment` is given: tries `settings.<env>.yaml`; on failure falls back to `app.<env>.yaml`.

This means apps that have not yet migrated to the new overlay naming will continue to load
correctly. The fallback for item 6 above is the remaining work to remove these legacy reads.

---

## Migration Notes

### Renaming existing overlay files

If your project uses the old `platform_<name>.yaml` / `env_<name>.yaml` convention at the workspace
config level, rename the files to `settings.<name>.yaml` before re-deploying:

```
config/platform_fabric.yaml  →  config/settings.fabric.yaml
config/env_prod.yaml         →  config/settings.prod.yaml
```

If your app directory uses `app.<platform>.yaml` or `app.<env>.yaml` as runtime overlay files,
rename them to `settings.<platform>.yaml` / `settings.<env>.yaml`:

```
app.fabric.yaml  →  settings.fabric.yaml
app.prod.yaml    →  settings.prod.yaml
```

Legacy filenames will continue to work as fallbacks during the transition (see Legacy Fallback
Behavior above), but they are not guaranteed to be retained in future releases.

### `kindling config set --level`

The `--level platform` and `--level env` flags now write to `settings.<platform>.yaml` and
`settings.<env>.yaml` respectively. If you have automation that expects the old filenames, update
it to use the new names.

### App packages (`.kda`)

KDA artifacts built with `kindling app package` or `kindling app deploy` now include only the
selected platform/environment overlay (`--platform`, `--env`). Legacy `app.<x>.yaml` files inside
an app directory are excluded from packaged artifacts. Remove them or rename them to
`settings.<x>.yaml` to have them included.

## Acceptance Criteria

1. `app.yaml` remains manifest-only.
2. App runtime settings are resolved from app-local `settings*.yaml` files only.
3. Overlay precedence is deterministic and env wins over platform.
4. Deploy/package include only selected target overlays.
5. Tests and docs reflect new behavior.
