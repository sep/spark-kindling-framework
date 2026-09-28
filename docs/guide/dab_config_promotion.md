# Promoting Kindling Config with Databricks Asset Bundles

Store kindling's YAML config in your repo, deploy it to workspace files with
a Databricks Asset Bundle (DAB), and point `kindling.initialize()` at the
deployed path. Promotion is `databricks bundle deploy -t <target>` in CI —
no storage account, no volume mount, works on UC shared/standard access
mode clusters (workspace files are driver-readable via `/Workspace/...`).

## Division of labor

Keep **kindling's environment overlays** as the source of environment
differences, and use **DAB targets purely as transport** — deploy the whole
config directory to every target and let kindling's `environment` select
the overlay:

- Config semantics stay in one system (kindling's hierarchical layering,
  `@secret` resolution, entity tag overrides) instead of splitting between
  Dynaconf and DAB variable substitution.
- Every environment carries the full config set, so diffs between
  environments are visible in one file (`prod.yaml`) rather than scattered
  through `databricks.yml` variables.

Use DAB variables only for values that are *about the workspace itself*
(paths, service principal ids), not for kindling behavior.

## Layout

```
my-solution/
├── databricks.yml
├── config/
│   ├── settings.yaml        # base kindling config
│   ├── dev.yaml             # kindling environment overlays
│   └── prod.yaml
└── apps/ ...
```

## databricks.yml

```yaml
bundle:
  name: my-solution-config

# Deploy to a STABLE path per target — the default .bundle/... path is
# user-scoped and changes with the deploying principal; jobs need a path
# that survives redeploys and principal changes.
targets:
  dev:
    workspace:
      host: https://adb-<dev>.azuredatabricks.net
      file_path: /Workspace/Shared/kindling/dev
  prod:
    workspace:
      host: https://adb-<prod>.azuredatabricks.net
      file_path: /Workspace/Shared/kindling/prod
    presets:
      name_prefix: ""

sync:
  include:
    - config/**
```

## Runtime

```python
import kindling

ENV = "prod"   # or resolve from a job parameter / cluster tag

kindling.initialize(config={
    "environment": ENV,
    "config_files": [
        f"/Workspace/Shared/kindling/{ENV}/config/settings.yaml",
        f"/Workspace/Shared/kindling/{ENV}/config/{ENV}.yaml",
    ],
    "install_bootstrap_dependencies": False,
})
```

Explicit `config_files` bypasses the `artifacts_storage_path` download flow
entirely; the paths are ordinary driver-local reads. Long-running jobs can
pick up a promotion without restart via `ConfigService.reload()`, which
emits `config.pre_reload` / `config.post_reload` with a change diff.

## Lakeflow Pipelines

Lakeflow uses the same files and the same bootstrap key. Put the selected app
and pipe subset in the pipeline configuration, then pass the deployed settings
files through `spark.kindling.bootstrap.config_files`:

```yaml
resources:
  pipelines:
    telemetry_silver:
      name: telemetry-silver
      catalog: dev_silver
      target: cwmdp
      configuration:
        "kindling.data_app": telemetry
        "kindling.lakeflow.allowed_apps": telemetry
        "kindling.lakeflow.pipes": silver.build_telemetry,silver.derive_events,silver.derive_episodes
        "spark.kindling.bootstrap.environment": dev
        "spark.kindling.bootstrap.workspace_id": adb-dev
        "spark.kindling.bootstrap.config_files": '["/Workspace/Shared/kindling/dev/config/settings.yaml", "/Workspace/Shared/kindling/dev/config/settings.databricks.yaml", "/Workspace/Shared/kindling/dev/data-apps/telemetry/settings.yaml"]'
```

The selector sets `declaration_only=true` and calls
`kindling.initialize(..., app_name="telemetry", engine="databricks_sdp")`.
Configuration files are still loaded by Kindling's shared Dynaconf path; the
Lakeflow selector does not parse or validate YAML itself.

### Generating the bundle

`kindling bundle build` produces a bundle like the one above from a project's
`config/` and `data-apps/` directories, including one pipeline resource per
app (or per configured split). Its default `inline` transport merges the
overlays at build time and writes the result into the pipeline configuration
as `kindling.lakeflow.settings_json`, so nothing is read from workspace files
at declaration time; `--config-transport files` reproduces the
`spark.kindling.bootstrap.config_files` layout shown here with staged files
referenced through `${workspace.file_path}`. See the
[CLI README](../../packages/kindling_cli/README.md#databricks-bundles).

## CI/CD promotion

```yaml
# per environment, gated however your pipeline gates promotions
- run: databricks bundle validate -t prod
- run: databricks bundle deploy -t prod
  env:
    DATABRICKS_HOST: ...
    ARM_CLIENT_ID: ...        # deploy as a service principal
    ARM_CLIENT_SECRET: ...
    ARM_TENANT_ID: ...
```

`bundle deploy` is idempotent and atomic enough for config (files replaced
per deploy); the git history of `config/` is the audit trail, and rollback
is redeploying an earlier ref.

## When the storage-account flow is still the right choice

The ABFSS `artifacts_storage_path` flow remains preferable when you rely on
workspace-id–keyed config resolution, ship config alongside deployed wheels
and KDA apps as one artifact set, or serve multiple workspaces from one
storage location. The two coexist: `config_files` layers on top of anything
downloaded from storage.
