# Configuration

How a Kindling domain project specifies configuration: which files, which
keys, how they layer, and how code reads them. Entity and pipe declarations
live in [entities.md](entities.md) and [pipes.md](pipes.md); app layout in
[apps.md](apps.md).

- [Rules](#rules)
- [Files and layering](#files-and-layering)
- [Precedence across all sources](#precedence-across-all-sources)
- [Keys worth setting](#keys-worth-setting)
- [entity_tags overlays](#entity_tags-overlays)
- [dataentities / datapipes overlays](#dataentities--datapipes-overlays)
- [Environment variables, SparkConf, parameters](#environment-variables-sparkconf-parameters)
- [Secrets](#secrets)
- [Reading config from code](#reading-config-from-code)
- [Gotchas](#gotchas)

## Rules

- Execution options (parallelism, retries, timeouts, storage roots, access
  mode, log level) belong in `kindling.*` config. Call parameters and
  `--param` are just-in-time overrides for one run, not the primary interface.
- Per-environment differences go in overlay files (`settings.<env>.yaml`),
  never in `if env == ...` branches in code.
- Never put a secret value in any settings file. Use `"@secret:<name>"`.
- Point bootstrap at directories (`config_dir`, `app_dir`); never build a
  file list. `config_files` is deprecated and logs a warning.

## Files and layering

Kindling merges the settings files itself (`merge_settings_layers` in
`packages/kindling/spark_config.py`) and hands the result to Dynaconf
(`envvar_prefix="KINDLING"`), which resolves `@format`, secrets and env vars. Bootstrap
discovers files by convention (`settings_hierarchy` / `resolve_settings_files`
in `packages/kindling/bootstrap.py`), lowest precedence first:

| # | Scope | File | Legacy name (used only if canonical is absent) |
|---|-------|------|------|
| 1 | `config_dir` | `settings.yaml` | |
| 2 | `config_dir` | `settings.<platform>.yaml` | `platform_<platform>.yaml` |
| 3 | `config_dir` | `workspace_<workspace_id>.yaml` | |
| 4 | `config_dir` | `settings.<environment>.yaml` | `env_<environment>.yaml` |
| 5 | `app_dir` | `settings.yaml` | |
| 6 | `app_dir` | `settings.<platform>.yaml` | `app.<platform>.yaml` |
| 7 | `app_dir` | `settings.<environment>.yaml` | `app.<environment>.yaml` |

- `<platform>` is `standalone`, `databricks`, `fabric` or `synapse`;
  `<environment>` is the bootstrap `environment` (default `development`;
  `kindling app run` defaults to `KINDLING_ENV` or `local`).
- Deployed jobs download the same table from artifacts storage
  (`config/` then `data-apps/<app>/`); local `config_dir`/`app_dir` files
  layer on top of downloaded ones.
- `kindling app run <app>` passes `app_dir=apps/<app>/`; `--config DIR` adds
  a shared `config_dir`.
- `settings.local.yaml` is gitignored and never packaged or bundled. See
  [Gotchas](#gotchas) for when it loads.

The table is inspectable from code:

```python
from kindling.bootstrap import resolve_settings_files, settings_hierarchy

order = [name for _scope, name, _legacy in settings_hierarchy("prod", platform="databricks")]
assert order == [
    "settings.yaml", "settings.databricks.yaml", "settings.prod.yaml",
    "settings.yaml", "settings.databricks.yaml", "settings.prod.yaml",
]
# Existing files only, in merge order (the test cwd holds just settings.yaml).
assert resolve_settings_files(".", None, "local", platform="standalone") == ["settings.yaml"]
```

## Precedence across all sources

Lowest to highest:

1. Settings files in the order above (artifacts-storage, then local dirs,
   then any legacy `config_files`).
2. `KINDLING_`-prefixed environment variables (Dynaconf env loader).
3. `spark.kindling.*` SparkConf keys (merged into the bootstrap dict).
4. The bootstrap dict passed to `initialize_framework` / `kindling.initialize`,
   which includes `kindling app run` parameters (`CONFIG__*` env vars, then
   the `--parameters` file, then `--param`).
5. Runtime only: `ConfigService.set()`, entity `tag_overrides`, and call
   arguments such as `run_datapipes(parallel=...)`.

Between files, mappings deep-merge (a later layer that sets one leaf keeps
its siblings) and **lists and scalars replace** -- the same rule as config
overlays and `kindling bundle build`. Keys match case-insensitively. To append
to a list on purpose, add `dynaconf_merge` to it or write `"@merge [x]"`
(`"@merge_unique [x]"` skips items already present). `settings.local.yaml` is
the `local` environment's layer and applies only when `environment` is
`local`.

## Keys worth setting

Only keys the framework actually reads (grep `config.get("kindling.` in
`packages/kindling/`):

```yaml
kindling:
  telemetry:
    logging:
      level: INFO           # mirrored to flat log_level (spark_log.py)
      print: true
    tracing:
      enabled: true         # trace_ops.py
      level: standard       # minimal | standard | verbose
  delta:
    access_mode: storage    # catalog | storage; entity_provider_delta.py (default catalog)
  storage:                  # EntityNameMapper / EntityPathLocator defaults
    table_root: Tables
    checkpoint_root: Files/checkpoints
    table_catalog: main
    table_schema: analytics
  execution:                # generation_executor.py EXECUTION_CONFIG_DEFAULTS
    parallel: true
    max_workers: 4
    error_strategy: fail_fast   # fail_fast | continue | skip_dependents
    pipe_timeout: 1800
    auto_cache: false
    retry:
      attempts: 2
      interval_seconds: 30
    pipes:                  # keyed by literal pipe id
      silver.orders:
        retry:
          attempts: 5
  bootstrap:
    load_workspace_packages: false
  extensions: [spark-kindling-ext-sdp]   # installed at cloud bootstrap
  secrets:
    secret_scope: my-scope  # Databricks; Fabric/Synapse use key_vault_url or linked_service
  lakeflow:
    temporal_mode: streaming   # read by the Databricks extension
spark_configs:              # pushed to the live session via spark.conf.set
  spark.sql.shuffle.partitions: 64
```

Full key list: `docs/reference/config_reference.md`. Your own domain settings
can live under any top-level key (for example `orders:`); read them the same
way.

## entity_tags overlays

Override an entity's tags per environment without touching code. Top-level
`entity_tags:`, keyed by the **exact** entity id, values are flat tag keys
(dots are literal, not nesting):

```yaml
# settings.local.yaml or settings.dev.yaml
entity_tags:
  bronze.orders:
    provider_type: memory
  silver.orders:
    provider.path: "@secret:ABFSS_SILVER_PATH"
    provider.access_mode: storage
```

These merge over the declared tags (and over `dataentities:` results) every
time the entity definition is read (`DataEntityManager.get_entity_definition`).
No globs here; use `dataentities:` for families of ids.

## dataentities / datapipes overlays

`dataentities:`, `datapipes:`, `dataentities-bytag:` and `datapipes-bytag:`
override any declared field (not only tags), keyed by glob id patterns
(`bronze.*`, `**`) or by tag value, and can derive new declarations
(`clone_of`, `add_columns`, `add_inputs`). Mappings deep-merge; exact ids beat
wildcards. Shapes and semantics: [entities.md](entities.md),
[pipes.md](pipes.md).

```yaml
dataentities:
  "bronze.*":
    tags:
      layer: bronze
```

## Environment variables, SparkConf, parameters

- **`KINDLING_` env vars**: prefix `KINDLING_`, nesting delimiter `__`. The
  top-level settings key is `kindling`, so the prefix appears twice:
  `KINDLING_KINDLING__EXECUTION__MAX_WORKERS=8`. Values are parsed (`true`,
  `8` become bool/int). Deep-merges into existing sections.
- **SparkConf**: `spark.kindling.<path>` maps to `kindling.<path>`;
  `spark.kindling.bootstrap.<key>` maps to the bootstrap key `<key>` (for
  example `spark.kindling.bootstrap.environment`, `.config_dir`, `.app_dir`).
  Explicit bootstrap-dict values win over SparkConf.
- **`kindling app run` parameters**: `--param a.b=c` (repeatable),
  `--parameters file.yaml`, and `CONFIG__a__b=c` env vars all become
  bootstrap-dict entries for that run.
- `.env` in the current directory is loaded by `kindling app run` (disable
  with `--no-dotenv`, add files with `--dotenv`).

```bash
export KINDLING_KINDLING__EXECUTION__PARALLEL=true
kindling app run my_app --env dev --param kindling.execution.max_workers=8 --param log_level=DEBUG
kindling config show --app apps/my_app/app.py --env prod --platform databricks
kindling config show --app apps/my_app/app.py --key kindling.execution.max_workers
kindling config diff --app apps/my_app/app.py --env dev --diff-env prod
kindling config set kindling.execution.max_workers 8 --level env --env prod --config-dir apps/my_app
```

## Secrets

Write a reference as the whole string value: `"@secret:<name>"` or
`"@secret <name>"` (Databricks also accepts `"@secret:<scope>:<key>"`).
To embed a secret in a larger string, reference a secret-valued key with
`"@format ...{this.<key>}"`.
`packages/kindling/config_loaders.py` resolves references in settings files
and in `dataentities:`/`datapipes:`/`entity_tags:` values. Bootstrap re-resolves
once platform services exist and **fails loudly** if any remain unresolved.

| Platform | Provider |
|----------|----------|
| standalone | env var: `<name>`, then `<NAME>` with `-` `.` `:` replaced by `_`, then `KINDLING_SECRET_<NAME>` |
| databricks | `dbutils.secrets` with `kindling.secrets.secret_scope` (or the `scope:` prefix) |
| fabric / synapse | Key Vault via `kindling.secrets.linked_service` or `key_vault_url`, then the env-var fallback |

Locally, put values in `.env` (gitignored); commit `.env.example` with
placeholder values only.

```python
import os

from kindling.config_loaders import resolve_secret_value
from kindling.injection import get_kindling_service
from kindling.platform_provider import SecretProvider

os.environ["ORDERS_API_TOKEN"] = "dummy-token"  # normally set by .env
secrets = get_kindling_service(SecretProvider)
assert secrets.get_secret("orders-api-token") == "dummy-token"
assert resolve_secret_value("@secret:orders-api-token", secrets) == "dummy-token"
```

## Reading config from code

Get the `ConfigService` singleton (or declare it as an injected constructor
dependency) and read dotted keys. Always pass a default for optional keys.

```python
from kindling.injection import get_kindling_service
from kindling.spark_config import ConfigService

config = get_kindling_service(ConfigService)

assert config.get("kindling.telemetry.logging.level") == "WARN"
assert config.get("kindling.platform.name") == "standalone"
assert config.get("environment") == "local"
max_workers = config.get("kindling.execution.max_workers", 4)  # unset -> default
assert max_workers == 4
assert config.get("orders.not_configured") is None  # missing, no default -> None

# Ids contain dots: never traverse them as config paths.
assert config.get_entity_tags("bronze.orders") == {}
```

Other methods: `get_all()`, `set(key, value)` (runtime only, not persisted),
`reload()` (re-downloads from artifacts storage when bootstrapped from it) and
`get_fresh(key)`.

## Gotchas

- **A list in a later layer replaces the earlier one.** Overlaying
  `required_packages: [c]` on `[a, b]` gives `[c]`. Add the `dynaconf_merge`
  marker (or `"@merge [c]"`) when a layer should add to the list instead.
- **Lists passed as parameters still append** to the files' value (Dynaconf
  merges the bootstrap dict itself); prefer setting lists in settings files.
- **`settings.local.yaml` is gitignored and never deployed**; it only affects
  runs with `environment: local`.
- **Strings starting with `@` are Dynaconf directives** (`@format`, `@json`,
  `@int`, `@secret`). `@format {this.kindling.x}` is evaluated lazily and raises
  `DynaconfFormatError` if its target is missing. Avoid plain values that
  begin with `@`.
- During file load you may see `Failed to resolve secret ... Returning original
  value`. That is the expected early pass before platform services exist.
  Only a bootstrap failure is real.
- `kindling config show` merges only the app directory's base, platform and
  env files. It ignores `--config` shared dirs, `KINDLING_` env vars and
  parameters.
- `kindling config set --app NAME` writes under `<config-dir>/data-apps/NAME/`
  (artifacts layout). For a scaffolded `apps/<app>/`, pass
  `--config-dir apps/<app>` and omit `--app`.
- `kindling.execution.pipes` and `entity_tags` are indexed by literal id.
  Dotted lookups like `config.get("entity_tags.bronze.orders")` mis-traverse.
- Re-calling `initialize_framework` with a different `environment` or
  directories reconfigures the process and logs a warning. Tests should reset
  between configurations.
