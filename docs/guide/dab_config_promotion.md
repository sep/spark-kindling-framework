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

Lakeflow pipelines do not read settings files at all. `kindling bundle build`
finds the project's settings by convention (`config/` overlays, then the
app's `settings*.yaml`), merges them at build time in the runtime's order and
writes the result into each pipeline resource as
`kindling.lakeflow.settings_json`; the selector injects it before
initialization, and flat pipeline keys still act as just-in-time overrides.
The bundle it produces is the pipeline counterpart of the job layout above,
with no `sync` of a `config/` directory and no file paths in the pipeline
configuration. See the [CLI README](../../packages/kindling_cli/README.md#databricks-bundles)
and `examples/lakeflow-telemetry/`.

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
