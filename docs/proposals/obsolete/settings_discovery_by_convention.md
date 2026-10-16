# Settings Discovery by Convention

**Status:** Implemented (2026-09-28); `config_files` deprecated with a warning.
**Created:** 2026-09-28

## Problem

Kindling settings live in a documented hierarchy: global overlays under a
`config/` directory (`settings.yaml`, `settings.<platform>.yaml`,
`workspace_<id>.yaml`, `settings.<env>.yaml`) and per-app overlays in the app's
own directory (`settings.yaml`, `settings.<platform>.yaml`,
`settings.<env>.yaml`). The runtime already applies that convention itself when
it discovers settings in artifacts storage (`download_config_files`), and
`kindling bundle build` applies it at build time for Lakeflow pipelines.

Every other entry point still asks the caller to spell the hierarchy out as an
explicit list:

- `kindling.initialize(config={"config_files": [...]})` in a local `app.py`
  (the `tests/local-project` fixture builds the list by hand).
- The CLI's `_bootstrap_app` resolves the files with `_load_effective_raw_config`
  and passes them to `initialize_framework` as `config_files`; `kindling app
  deploy` does the same; `kindling_cli._runner` forwards repeated `--config`
  flags into the same key.
- The DAB promotion guide's jobs section tells readers to write the list into
  `kindling.initialize`.

The convention exists in three copies (bootstrap download flow, CLI raw-config
loader, bundle generator) and the list is a user-facing concept anywhere the
runtime cannot discover from artifacts storage.

## Decision

Explicit settings-file lists stop being a user-facing concept. Callers name
*where* settings live, never *which files*:

```python
kindling.initialize(config={
    "environment": "dev",
    "config_dir": "/path/to/config",      # optional; global overlays
    "app_dir": "/path/to/apps/orders",    # optional; per-app overlays
})
```

Bootstrap resolves the ordered file list from those directories with the same
rules as the artifacts-storage flow (base, platform, `workspace_<id>`,
environment, then app base/platform/environment; canonical names first,
documented legacy names `platform_<p>.yaml` / `env_<e>.yaml` as fallback;
`settings.local.*` only for `environment=local`). The resolved list remains an
internal value passed to Dynaconf.

## Scope

1. **Runtime.** Add `resolve_settings_files(config_dir, app_dir, environment,
   platform, workspace_id)` in `kindling.bootstrap`, sharing the ordering table
   with `download_config_files` so the two cannot drift. `initialize_framework`
   accepts `config_dir` / `app_dir` (also via `spark.kindling.bootstrap.*`) and
   resolves them before the artifacts-storage flow; discovered artifact files
   come first, local directories layer on top, as `config_files` does today.
   `config_files` keeps working for one release with a deprecation warning
   naming the replacement; the `spark.kindling.bootstrap.config_files` Spark
   key gets the same warning.
2. **CLI.** `_bootstrap_app`, `kindling app deploy`, and `_runner` pass
   `config_dir` / `app_dir` instead of file lists. `--config` on `app run`,
   `pipeline run/show`, and `env check` keeps its "directory" meaning (it
   already is a directory override) and maps to `config_dir`; the runner's
   repeated `--config <file>` becomes a single `--config-dir`. The bundle
   generator's `resolve_config_sources` and the CLI's `_load_effective_raw_config`
   collapse onto one design-time copy of the same table, with a unit test
   asserting parity against the runtime helper.
3. **Docs and fixtures.** `dab_config_promotion.md` jobs section, the local
   development guide, `config_reference.md`, and `tests/local-project/apps/
   sales_ops/app.py` switch to directories. `CHANGELOG` records the deprecation.

## Non-goals

- Changing the overlay order or file names.
- Removing the internal `config_files` value that bootstrap hands to Dynaconf.
- Lakeflow: already inline via `kindling.lakeflow.settings_json`.

## Acceptance

- A scaffolded app runs locally with no `config_files` anywhere in the project.
- `kindling app run` and `_runner` never construct a file list.
- Passing `config_files` explicitly still works and warns once.
- Unit tests: directory resolution parity between runtime and CLI copies,
  legacy-name fallback, `settings.local.*` gating, precedence of local
  directories over artifacts-storage files, and the deprecation warning.
