# CLI Analysis — TASK-20260430-001
# Generated: 2026-04-30
# Source files read verbatim; all line numbers are 1-based from the actual file.

---

## 1. Click Group/Command Structure

File: `/workspaces/kindling/packages/kindling_cli/kindling_cli/cli.py`

### Root group

| Line | Decorator | Function | CLI name |
|------|-----------|----------|----------|
| 358 | `@click.group(context_settings={"help_option_names": ["-h", "--help"]})` | `cli` | `kindling` |

### `migrate` sub-group and commands

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 368 | `@cli.group("migrate")` | `migrate_group` | `kindling migrate` |
| 373 | `@migrate_group.command("plan")` | `migrate_plan` | `kindling migrate plan` |
| 409 | `@migrate_group.command("apply")` | `migrate_apply` | `kindling migrate apply` |
| 460 | `@migrate_group.command("rollback")` | `migrate_rollback` | `kindling migrate rollback <entity_id>` |
| 491 | `@migrate_group.command("cleanup")` | `migrate_cleanup` | `kindling migrate cleanup <entity_id>` |

### `config` sub-group and commands

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 519 | `@cli.group("config")` | `config_group` | `kindling config` |
| 524 | `@config_group.command("init")` | `config_init` | `kindling config init` |
| 547 | `@config_group.command("set")` | `config_set` | `kindling config set <key> <value>` |

### `env` sub-group and commands

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 638 | `@cli.group("env")` | `env_group` | `kindling env` |
| 713 | `@env_group.command("check")` | `env_check` | `kindling env check` |

### `workspace` sub-group and commands

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 801 | `@cli.group("workspace")` | `workspace_group` | `kindling workspace` |
| 806 | `@workspace_group.command("check")` | `workspace_check` | `kindling workspace check` |
| 851 | `@workspace_group.command("init")` | `workspace_init` | `kindling workspace init` |
| 1467 | `@workspace_group.command("deploy")` | `workspace_deploy` | `kindling workspace deploy` |

### `app` sub-group and commands

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 1713 | `@cli.group("app")` | `app_group` | `kindling app` |
| 1718 | `@app_group.command("package")` | `app_package` | `kindling app package <app_path>` |
| 1760 | `@app_group.command("deploy")` | `app_deploy` | `kindling app deploy <app_path>` |
| 1808 | `@app_group.command("cleanup")` | `app_cleanup` | `kindling app cleanup <app_name>` |

### `job` sub-group and commands

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 1839 | `@cli.group("job")` | `job_group` | `kindling job` |
| 1844 | `@job_group.command("create")` | `job_create` | `kindling job create <config_path>` |
| 1873 | `@job_group.command("run")` | `job_run` | `kindling job run <job_id>` |
| 1946 | `@job_group.command("status")` | `job_status` | `kindling job status <run_id>` |
| 1967 | `@job_group.command("logs")` | `job_logs` | `kindling job logs <run_id>` |
| 2020 | `@job_group.command("cancel")` | `job_cancel` | `kindling job cancel <run_id>` |
| 2049 | `@job_group.command("delete")` | `job_delete` | `kindling job delete <job_id>` |

### `test` sub-group and commands

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 2078 | `@cli.group("test")` | `test_group` | `kindling test` |
| 2083 | `@test_group.command("run")` | `test_run` | `kindling test run` |
| 2210 | `@test_group.command("check")` | `test_check` | `kindling test check` |
| 2227 | `@test_group.command("cleanup")` | `test_cleanup` | `kindling test cleanup` |

### `repo` sub-group and commands

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 2239 | `@cli.group("repo")` | `repo_group` | `kindling repo` |
| 2244 | `@repo_group.command("init")` | `repo_init` | `kindling repo init <repo_name>` |

### `package` sub-group and commands

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 2290 | `@cli.group("package")` | `package_group` | `kindling package` |
| 2295 | `@package_group.command("init")` | `package_init` | `kindling package init <package_name>` |

### `new` — top-level command (NOT a sub-group)

| Line | Decorator | Function | CLI path |
|------|-----------|----------|----------|
| 2380 | `@cli.command("new")` | `new_project` | `kindling new <project_name>` |

### `main` entrypoint

| Line | Definition |
|------|-----------|
| 2507 | `def main() -> None:` — calls `cli()` |
| 2512 | `if __name__ == "__main__": main()` |

---

## 2. `new` Command — Next-Steps Output

Function `new_project`, lines 2495–2504.

Exact lines:
```
line 2495: click.echo(f"Created {cfg.snake_name}/ ({len(created)} files)")
line 2496: click.echo()
line 2497: click.echo("Next steps:")
line 2498: click.echo(f"  cd {cfg.snake_name}")
line 2499: click.echo(f"  cd packages/{cfg.snake_name}")
line 2500: click.echo("  poetry install")
line 2501: click.echo("  cp .env.example .env  # fill in your credentials, then: source .env")
line 2502: click.echo("  poetry run poe test")
line 2503: if cfg.integration:
line 2504:     click.echo("  poetry run poe test-integration  # requires Azure creds in .env")
```

Rendered output (project_name="my-pipeline", integration=True):
```
Created my_pipeline/ (N files)

Next steps:
  cd my_pipeline
  cd packages/my_pipeline
  poetry install
  cp .env.example .env  # fill in your credentials, then: source .env
  poetry run poe test
  poetry run poe test-integration  # requires Azure creds in .env
```

Note: `cfg.snake_name` is the snake_case form of the project name.

---

## 3. `env check` — `--config` Default

Function `env_check`, lines 713–798.

Option definition (lines 714–720):
```
default="settings.yaml"     <- the default
show_default=True
type=click.Path(path_type=Path, dir_okay=False, exists=False)
```

- Default value: `"settings.yaml"` (relative, resolved via `.expanduser()` at line 750)
- `show_default=True` — `--help` shows `[default: settings.yaml]`
- `exists=False` — Click does NOT require the file to exist; the command handles the missing case itself

Exact check lines using the config path:
```
line 750: resolved_config_path = config_path.expanduser()
line 761: exists = resolved_config_path.exists()
line 762: checks.append(("config_file_exists", exists, str(resolved_config_path)))
line 765: if exists:
line 766:     config_data = _load_yaml_config(resolved_config_path)
```

---

## 4. Lazy vs. Top-Level Import Pattern

All `kindling` runtime imports are **lazy** — inside command function bodies, wrapped in `try/except ImportError`. No top-level `import kindling` anywhere in cli.py.

Canonical pattern:
```python
try:
    from kindling.injection import GlobalInjector
    from kindling.migration import MigrationService
except ImportError as exc:
    raise click.ClickException("kindling package is required …") from exc
```

Examples:
- `migrate_plan` — lines 381–387
- `migrate_apply` — lines 434–437
- `migrate_rollback` — lines 468–472
- `migrate_cleanup` — lines 499–503

`kindling_sdk` is also lazy:
- `_create_platform_api` helper — lines 188–194

Exception: `test_run` (line 2174) imports `kindling_cli.test_runner` lazily inside the function body. That is `kindling_cli` (the CLI package itself), not `kindling` runtime — same lazy-import pattern, different package.

Rule: Any `kindling.*` or `kindling_sdk.*` import is always done lazily inside a function, never at module top level.

---

## 5. `env.local.yaml.j2` — Full Rendered Output for medallion + oauth

Template: `/workspaces/kindling/packages/kindling_cli/kindling_cli/templates/config/env.local.yaml.j2`
Variables: `auth="oauth"`, `layers="medallion"`

```yaml
# Local development overlay — ABFSS paths resolved from environment variables.
#
# Credential env vars (oauth auth):
#   AZURE_STORAGE_ACCOUNT   storage account name
#   AZURE_TENANT_ID          AAD tenant ID
#   AZURE_CLIENT_ID          service principal app ID
#   AZURE_CLIENT_SECRET      service principal secret
#
# Path env vars:
#   ABFSS_BRONZE_PATH=abfss://<container>@<account>.dfs.core.windows.net/<path>
#   ABFSS_SILVER_PATH=abfss://<container>@<account>.dfs.core.windows.net/<path>


entity_tags:
  bronze.records:
    provider.path: "@secret:ABFSS_BRONZE_PATH"
  silver.records:
    provider.path: "@secret:ABFSS_SILVER_PATH"
```

Rendering notes:
- Template lines 4–8 (`{% if auth == "oauth" %}`): oauth block fires — four credential comment lines
- Template lines 18–20 (`{% if layers == "medallion" %}`): medallion path comment block — two ABFSS_* lines
- Template lines 25–30 (`{% if layers == "medallion" %}`): medallion entity_tags block with `bronze.records` + `silver.records`
- There is one blank line between the path-comment block and the entity_tags block (template line 24 inside the if)

---

## 6. `conftest.py.j2` — Does It Include `reset_kindling`?

YES. `reset_kindling` is present and fully scaffolded — but it is conditional on `integration=True`.

Gating condition (template line 256): `{% if integration %}`
Closing condition (template line 289): `{% endif %}`

The fixture body (template lines 257–288):
```python
@pytest.fixture
def reset_kindling():
    """Reset the Kindling DI graph between integration tests.

    initialize_framework() is idempotent (it returns early if already
    initialized). Use this fixture when a test needs a fresh bootstrap.
    """
    import sys

    from kindling.data_entities import DataEntities
    from kindling.data_pipes import DataPipes
    from kindling.injection import GlobalInjector

    _pkg_modules = [k for k in sys.modules if k.startswith("{{ snake_name }}")]

    def _reset():
        from injector import singleton
        from kindling.spark_config import ConfigService, DynaconfConfig

        GlobalInjector.reset()
        DataEntities.deregistry = None
        DataPipes.dpregistry = None
        for mod in _pkg_modules:
            sys.modules.pop(mod, None)

        inj = GlobalInjector.get_injector()
        inj.binder.bind(ConfigService, to=DynaconfConfig, scope=singleton)
        inj.binder.bind(DynaconfConfig, to=DynaconfConfig, scope=singleton)

    _reset()
    yield
    _reset()
```

When `integration=False`: no `reset_kindling`, no `spark_abfss`, no `pytest_collection_modifyitems`.
When `integration=True`: all three are scaffolded.

`reset_kindling` resets: `GlobalInjector`, `DataEntities.deregistry`, `DataPipes.dpregistry`, package sys.modules entries, then re-binds `ConfigService`/`DynaconfConfig` as singletons.

---

## 7. Poe Tasks in `pyproject.toml.j2`

Template: `/workspaces/kindling/packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2`
Lines 23–31 of the template.

Template source (verbatim):
```toml
[tool.poe.tasks]
test-unit = "kindling test run --suite unit --path tests/unit"
test-component = "kindling test run --suite component --path tests/component"
{% if integration %}
test-integration = "kindling test run --suite integration --path tests/integration --preflight local"
test-all = { sequence = ["test-unit", "test-component", "test-integration"] }
{% endif %}
test = { sequence = ["test-unit", "test-component"] }
build = "poetry build"
```

Rendered for `integration=True`:
```toml
[tool.poe.tasks]
test-unit = "kindling test run --suite unit --path tests/unit"
test-component = "kindling test run --suite component --path tests/component"
test-integration = "kindling test run --suite integration --path tests/integration --preflight local"
test-all = { sequence = ["test-unit", "test-component", "test-integration"] }
test = { sequence = ["test-unit", "test-component"] }
build = "poetry build"
```

Rendered for `integration=False`:
```toml
[tool.poe.tasks]
test-unit = "kindling test run --suite unit --path tests/unit"
test-component = "kindling test run --suite component --path tests/component"
test = { sequence = ["test-unit", "test-component"] }
build = "poetry build"
```

Key observations:
- All test tasks delegate to `kindling test run` (the CLI's own command)
- `--preflight local` only on `test-integration`
- `test` (default shortcut) runs unit + component, never integration
- `test-all` only exists when integration scaffolding is on
- No `poe lint`, `poe format`, `poe ci`, `poe check` task exists in the template

---

## 8. Real `app.py` — `sales_ops` Reference Implementation

File: `/workspaces/kindling/tests/local-project/src/sales_ops/app.py`

Module-level imports (lines 17–18): only `os` and `pathlib.Path`. `kindling` is NOT imported at module level.

### `_config_dir()` (lines 21–22)
```python
def _config_dir() -> Path:
    return Path(__file__).parent.parent.parent / "config"
```
Three levels up from `app.py` into the package's `config/` directory.

### `_config_files(env)` (lines 25–31)
```python
def _config_files(env: str) -> list[str]:
    config_dir = _config_dir()
    files = [str(config_dir / "settings.yaml")]
    env_file = config_dir / f"env.{env}.yaml"
    if env_file.exists():
        files.append(str(env_file))
    return files
```
Returns `list[str]` of absolute paths. `env.<env>.yaml` included only if it exists.

### `register_all()` (lines 34–38)
```python
def register_all() -> None:
    import sales_ops.entities.orders           # noqa: F401
    import sales_ops.pipes.bronze_to_silver    # noqa: F401
```
Side-effect-only lazy imports inside the function.

### `initialize(env=None)` (lines 40–69)
```python
def initialize(env: str | None = None):
    from kindling.bootstrap import initialize_framework   # lazy — line 51

    if env is None:
        env = os.environ.get("KINDLING_ENV", "local")   # line 53

    svc = initialize_framework(                           # line 59
        {
            "platform": "standalone",
            "environment": env,
            "config_files": _config_files(env),
        }
    )

    register_all()                                        # line 67

    return svc                                            # line 69
```

Critical ordering: `initialize_framework` BEFORE `register_all()`.
Docstring explanation (lines 55–58): DI container must have `ConfigService` wired before `@DataPipes.pipe` decorators fire on import inside `register_all`.

Return value: the `StandaloneService` instance returned by `initialize_framework`.

Env resolution order: `env` argument → `KINDLING_ENV` env var → `"local"` literal.

### Caller usage (from docstring lines 9–14)
```
Local:   svc = initialize()           # uses env.local.yaml
Job:     svc = initialize(env="prod") # uses env.prod.yaml
```

---

## Summary Table — Key Facts for Implementation

| Item | Finding |
|------|---------|
| `kindling new` structure | `@cli.command("new")` at root, line 2380 — NOT a sub-group |
| Next-steps output mechanism | Plain sequential `click.echo()` calls, lines 2495–2504 |
| `env check --config` default | `"settings.yaml"` (relative), `exists=False`, line 716 |
| Import pattern for `kindling.*` | Always lazy inside function body with `try/except ImportError` |
| `env.local.yaml.j2` output (medallion+oauth) | 2 entity_tags: bronze.records + silver.records |
| `reset_kindling` in conftest.py.j2 | Present, gated on `{% if integration %}`, lines 257–289 |
| Poe tasks (template) | test-unit, test-component, test (sequence), build; test-integration/test-all only if integration=True |
| `initialize()` call order | `initialize_framework()` first, then `register_all()` |
| `initialize()` returns | The `StandaloneService` instance from `initialize_framework` |
| Config path convention | `Path(__file__).parent.parent.parent / "config"` |
