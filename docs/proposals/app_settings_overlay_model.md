# Proposal: App-Local Settings Overlay Model (Manifest-Only `app.yaml`)

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

- [ ] Define canonical naming as `settings.<name>.yaml`.
- [ ] Decide whether to retain fallback reads of legacy `platform_<x>.yaml` / `env_<x>.yaml`.

Files:

- `packages/kindling_cli/kindling_cli/cli.py`
- `packages/kindling/bootstrap.py`
- `packages/kindling/data_apps.py`

Test impact:

- `tests/unit/test_cli.py` (`TestConfigSetLevelRouting`)
- `tests/unit/test_platform_workspace_config.py`

### 2) `config set --level` file routing

- [ ] Route `--level platform` to `settings.<platform>.yaml`.
- [ ] Route `--level env` to `settings.<env>.yaml`.
- [ ] Apply same naming for app-scoped config writes.

Files:

- `packages/kindling_cli/kindling_cli/cli.py` (`_resolve_config_path`, `config_set`)

Test impact:

- Update:
  - `tests/unit/test_cli.py::TestConfigSetLevelRouting::test_platform_level`
  - `tests/unit/test_cli.py::TestConfigSetLevelRouting::test_env_level`
  - `tests/unit/test_cli.py::TestConfigSetAppScope::test_app_with_platform_level`

### 3) Selective app artifact assembly (package/deploy)

- [ ] Filter app artifact config files to selected target only:
  - include: `app.yaml`, `settings.yaml`, selected `settings.<platform>.yaml`, selected `settings.<env>.yaml`
  - exclude: unrelated overlays and local-only settings
- [ ] Add/confirm CLI options needed for filtering (`--env`, `--platform`).

Files:

- `packages/kindling_cli/kindling_cli/cli.py` (`_prepare_app_files`, `app_package`, `app_deploy`)
- `packages/kindling/app_files.py` (optional helper adjustment)

Test impact:

- Add/update in `tests/unit/test_cli.py`:
  - package file list includes only selected overlays
  - deploy payload includes only selected overlays
  - local-only overlays excluded

### 4) Standalone app run overlay resolution

- [ ] Ensure standalone run loads app-local settings in order:
  - `settings.yaml`
  - `settings.<platform>.yaml` (if applicable)
  - `settings.<env>.yaml`

Files:

- `packages/kindling_cli/kindling_cli/cli.py` (`_run_standalone_app`)

Test impact:

- Update command argument/config-file order assertions in `tests/unit/test_cli.py` (standalone app run tests).

### 5) Bootstrap remote config loading order

- [ ] Update remote bootstrap download/load order to reflect canonical overlay precedence.
- [ ] If compatibility retained, prefer dot-style names and fall back to legacy names.

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

Files:

- `packages/kindling/data_apps.py` (`_load_config_content`, `_merge_platform_config_at_deploy_time`, constants/usages)

Test impact:

- Update tests currently expecting `app.<platform>.yaml` merge behavior:
  - `tests/unit/test_kda_integration.py`
  - `tests/unit/test_notebook_utilities.py`

### 7) KDA packaging semantics alignment

- [ ] Ensure package creation does not repurpose `app.yaml` for merged runtime settings.
- [ ] Keep runtime overlay files explicit and selected.

Files:

- `packages/kindling/data_apps.py` (`DataAppPackage.create` and related helpers)

Test impact:

- Update KDA structure assertions in:
  - `tests/unit/test_kda_integration.py`
  - `tests/unit/test_notebook_utilities.py`

### 8) Workspace deploy filtering

- [ ] Replace wildcard overlay upload behavior with selected platform/env overlay upload.
- [ ] Add `--env` option where needed for workspace flows.

Files:

- `packages/kindling_cli/kindling_cli/cli.py` (`_deploy_config`, `workspace_init`, `workspace_deploy`)

Test impact:

- Update workspace tests in `tests/unit/test_cli.py` that monkeypatch `_deploy_config`.

### 9) Scaffold alignment

- [ ] Ensure generated app structure and comments match manifest-only `app.yaml` + settings overlay model.

Files:

- `packages/kindling_cli/kindling_cli/scaffold.py`
- `packages/kindling_cli/kindling_cli/templates/app.yaml.j2`
- `packages/kindling_cli/kindling_cli/templates/settings.yaml.j2`
- `packages/kindling_cli/kindling_cli/templates/settings.local.yaml.j2`

Test impact:

- Validate/update `tests/unit/test_scaffold.py` expectations.

### 10) Docs update

- [ ] Update naming and precedence examples.
- [ ] Update package/deploy descriptions to “selected overlays only.”

Likely files:

- `docs/reference/cli_reference.md`
- `docs/guide/setup_guide.md`
- `docs/guide/domain_project_quickstart.md`
- `docs/guide/local_python_first.md`

### 11) Regression test execution

- [ ] Run and fix:
  - `pytest -q tests/unit/test_cli.py`
  - `pytest -q tests/unit/test_platform_workspace_config.py`
  - `pytest -q tests/unit/test_kda_integration.py tests/unit/test_notebook_utilities.py`
  - `pytest -q tests/unit/test_scaffold.py`

### 12) Migration notes

- [ ] Add migration notes and compatibility guidance.

Files:

- `CHANGELOG.md`

## Acceptance Criteria

1. `app.yaml` remains manifest-only.
2. App runtime settings are resolved from app-local `settings*.yaml` files only.
3. Overlay precedence is deterministic and env wins over platform.
4. Deploy/package include only selected target overlays.
5. Tests and docs reflect new behavior.
