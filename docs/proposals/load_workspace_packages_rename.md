# Proposal: Rename `load_local` → `load_workspace_packages`

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
- `packages/kindling_cli/kindling_cli/cli.py` — default YAML templates (lines 84, 2148, 2248)
- `packages/kindling_cli/kindling_cli/_runner.py` — `_load_local_package_modules()` (lines 52–72)

**Test fixtures:**
- `tests/local-project/apps/sales_ops/settings.yaml` (line 14)
- `tests/data-apps/migration-test-app/settings.yaml` (line 17)

**Docs:**
- `docs/contributing/platform_workspace_config.md` (line 34)
- `docs/guide/domain_project_quickstart.md` (lines 98, 111)

## Implementation Checklist

### 1) Bootstrap alias + canonical key

- [ ] In `bootstrap.py`, change the canonical flat key from `load_local_packages` →
  `load_workspace_packages`.
- [ ] Retain a read-alias so `load_local` and `load_local_packages` both map to
  `load_workspace_packages` (backward compatibility).
- [ ] Emit a deprecation warning when either legacy key is read.

Files: `packages/kindling/bootstrap.py`, `runtime/scripts/kindling_bootstrap.py`

### 2) Spark config key mapping

- [ ] Add `spark.kindling.bootstrap.load_workspace_packages` as the canonical Spark key.
- [ ] Retain translation of `spark.kindling.bootstrap.load_local` with a deprecation warning.

Files: `packages/kindling/spark_config.py`

### 3) `data_apps.py` reference

- [ ] Update `config.get("load_local_packages", False)` → `config.get("load_workspace_packages", False)`.
- [ ] Ensure the alias in step 1 means this keeps working even if old config is supplied.

Files: `packages/kindling/data_apps.py`

### 4) CLI default templates

- [ ] Update generated `settings.yaml` / `settings.local.yaml` to emit `load_workspace_packages`.
- [ ] Add a comment in scaffolded files pointing users away from the old key name.

Files: `packages/kindling_cli/kindling_cli/cli.py`, `packages/kindling_cli/kindling_cli/scaffold.py`,
relevant `.j2` templates

### 5) Runner helpers

- [ ] Rename `_load_local_package_modules()` → `_load_workspace_package_modules()` in `_runner.py`.
- [ ] Update all callers.

Files: `packages/kindling_cli/kindling_cli/_runner.py`

### 6) Test fixtures

- [ ] Update `settings.yaml` in test fixtures to use `load_workspace_packages`.
- [ ] Update any unit tests that assert on the old key name.

Key test files:
- `tests/unit/test_runner_local_packages.py`
- `tests/unit/test_bootstrap_spark_pool_config.py`
- `tests/local-project/apps/sales_ops/settings.yaml`
- `tests/data-apps/migration-test-app/settings.yaml`

### 7) Docs update

- [ ] Replace all `load_local: true/false` examples with `load_workspace_packages: true/false`.
- [ ] Note the rename and backward-compat alias in the migration/changelog section.

Files: `docs/contributing/platform_workspace_config.md`,
`docs/guide/domain_project_quickstart.md`, `CHANGELOG.md`

### 8) Regression tests

- [ ] `pytest -q tests/unit/test_runner_local_packages.py`
- [ ] `pytest -q tests/unit/test_bootstrap_spark_pool_config.py`
- [ ] `pytest -q tests/unit/` (full unit suite)

### 9) Compatibility alias removal (future)

- [ ] After one release cycle, remove the `load_local` / `load_local_packages` aliases and the
  deprecation-warning path. Track as a follow-up issue.

## Acceptance Criteria

1. `load_workspace_packages` is the canonical key in all user-facing YAML config and docs.
2. `load_local` and `load_local_packages` still work but emit a deprecation warning.
3. No behavioral change — only naming.
4. All unit tests pass with the new key.
5. Scaffolded apps generate `load_workspace_packages` by default.
