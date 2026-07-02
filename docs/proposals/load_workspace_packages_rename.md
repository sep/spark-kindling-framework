# Proposal: Rename `load_local` → `load_workspace_packages`

**Status:** Implemented (2026-07-01), with one scope correction from the original proposal:
`_runner.py::_load_local_package_modules()` and `packages/kindling/bootstrap.py`'s
`_load_local_package_module_roots()` / `_import_local_package_registrations()` /
`KINDLING_LOCAL_PACKAGE_MODULES` are a **different, unrelated feature** (discovering
already-locally-installed Python packages for a dev/editable-install version shim and
registration import) that happens to share the word "local." They were **not** renamed and
`tests/unit/test_runner_local_packages.py` was **not** touched — renaming them would have
made that feature's naming worse, not better, since it really is about the local filesystem/
environment, not the platform workspace. Actual scope ended up being ~24 files, not the ~10
originally listed, once the correct set was identified.

## Goal

Replace the ambiguous `load_local` config key (and its internal alias `load_local_packages`) with
`load_workspace_packages` everywhere — YAML config, Spark config, flat bootstrap config, docs, and
tests — while retaining a compatibility alias during a migration window.

## Motivation

`load_local` is ambiguous. "Local" could mean:

- the local filesystem
- the local machine (vs. remote)
- local to the notebook

The feature actually controls whether Kindling loads packages from the **platform workspace**
(Databricks / Fabric / Synapse) during bootstrap. The internal function already uses the right name
(`load_workspace_packages()`); the rename surfaces that name at the config layer too.

`load_workspace_packages` is preferred over `load_notebook_packages` because the source is the
platform workspace, not a notebook — the packages happen to be implemented as notebooks, but the
concept is workspace-scoped.

## Current State

| Layer | Current name |
|---|---|
| YAML config key (user-facing) | `kindling.bootstrap.load_local` |
| Flat bootstrap config key | `load_local_packages` |
| Spark config key | `spark.kindling.bootstrap.load_local` |
| Internal function | `load_workspace_packages()` ← already correct |

### Files affected

**Core:**
- `packages/kindling/bootstrap.py` — alias map (line 79), decision logic (lines 1646–1668)
- `packages/kindling/spark_config.py` — key mappings (lines 166, 223)
- `packages/kindling/data_apps.py` — `config.get("load_local_packages", ...)` (line 978)
- `runtime/scripts/kindling_bootstrap.py` — alias map (line 94)

**CLI:**
- `packages/kindling_cli/kindling_cli/cli.py` — default YAML templates and generated flat
  bootstrap-config dicts (several call sites)
- `packages/kindling_cli/kindling_cli/templates/settings.yaml.j2`

**SDK (ad-hoc run generated config):**
- `packages/kindling_sdk/kindling_sdk/platform_fabric.py`,
  `packages/kindling_sdk/kindling_sdk/platform_synapse.py`,
  `packages/kindling_sdk/kindling_sdk/platform_databricks.py`

**Test fixtures:**
- `tests/local-project/apps/sales_ops/settings.yaml`
- `tests/data-apps/migration-test-app/settings.yaml`

**Unit tests:**
- `tests/unit/test_bootstrap_spark_pool_config.py`, `tests/unit/test_bootstrap_standalone.py`,
  `tests/unit/test_spark_config.py`

**Docs:**
- `docs/contributing/platform_workspace_config.md`
- `docs/guide/domain_project_quickstart.md`, `docs/guide/setup_guide.md`, `docs/guide/devstories.md`
- `docs/reference/config_reference.md`

**Explicitly NOT in scope** (different feature, see Status above):
- `packages/kindling_cli/kindling_cli/_runner.py` — `_load_local_package_modules()`
- `packages/kindling/bootstrap.py` — `_load_local_package_module_roots()`,
  `_import_local_package_registrations()`, `_LOCAL_PACKAGE_MODULES_ENV`
- `tests/unit/test_runner_local_packages.py`

## Implementation Checklist

### 1) Bootstrap alias + canonical key

- [x] In `bootstrap.py`, canonical dotted key is now `kindling.bootstrap.load_workspace_packages`;
  canonical flat key (via the Spark-conf alias map) is `load_workspace_packages`.
- [x] Retain a read-alias so `load_local` (dotted) and `load_local_packages` (flat) both resolve
  to the same effective value.
- [x] Emit a deprecation warning (`logger.warning`) when the legacy dotted key is what actually
  supplied the value.

Files: `packages/kindling/bootstrap.py`, `runtime/scripts/kindling_bootstrap.py`

### 2) Spark config key mapping

- [x] Added `kindling.BOOTSTRAP.load_workspace_packages` → `load_workspace_packages` and
  `load_workspace_packages` → `BOOTSTRAP.load_workspace_packages` translations, alongside the
  existing legacy `load_local` / `load_local_packages` translations (left unchanged, additive only).

Files: `packages/kindling/spark_config.py`

### 3) `data_apps.py` reference

- [x] `_override_with_workspace_packages` now reads canonical `load_workspace_packages` first,
  falling back to legacy `load_local_packages` with a deprecation warning.

Files: `packages/kindling/data_apps.py`

### 4) CLI default templates

- [x] Updated generated `settings.yaml` / `settings.local.yaml` and all generated flat
  bootstrap-config dicts to emit `load_workspace_packages`.

Files: `packages/kindling_cli/kindling_cli/cli.py`,
`packages/kindling_cli/kindling_cli/templates/settings.yaml.j2`

### 5) Runner helpers

- [x] **Corrected scope**: `_runner.py::_load_local_package_modules()` is an unrelated feature
  (local-install version shim) and was intentionally left unchanged — see Status above.

### 6) Test fixtures

- [x] Updated `settings.yaml` in `tests/local-project/apps/sales_ops/` and
  `tests/data-apps/migration-test-app/` to use `load_workspace_packages`.
- [x] Updated `tests/unit/test_bootstrap_spark_pool_config.py`,
  `tests/unit/test_bootstrap_standalone.py`, `tests/unit/test_spark_config.py` to assert on the
  new key, plus added new tests for the canonical-key and alias paths.
  `tests/unit/test_runner_local_packages.py` correctly left untouched (unrelated feature).

### 7) Docs update

- [x] Replaced all `load_local: true/false` examples with `load_workspace_packages: true/false`.
- [x] Noted the rename and backward-compat alias in `CHANGELOG.md`.

Files: `docs/contributing/platform_workspace_config.md`, `docs/guide/domain_project_quickstart.md`,
`docs/guide/setup_guide.md`, `docs/guide/devstories.md`, `docs/reference/config_reference.md`,
`CHANGELOG.md`

### 8) Regression tests

- [x] `pytest -q tests/unit/test_bootstrap_spark_pool_config.py tests/unit/test_bootstrap_standalone.py tests/unit/test_spark_config.py`
  — passing.
- [x] `pytest -q tests/unit/` (full unit suite) — 1540/1540 passing (2026-07-01).

### 9) Compatibility alias removal (future)

- [ ] After one release cycle, remove the `load_local` / `load_local_packages` aliases and the
  deprecation-warning path. Track as a follow-up issue.

## Acceptance Criteria

1. `load_workspace_packages` is the canonical key in all user-facing YAML config and docs.
2. `load_local` and `load_local_packages` still work but emit a deprecation warning.
3. No behavioral change — only naming.
4. All unit tests pass with the new key.
5. Scaffolded apps generate `load_workspace_packages` by default.
