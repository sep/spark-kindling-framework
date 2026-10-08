# Getting started on Databricks

How a scaffolded Kindling project gets running on Databricks: auth, artifacts,
the two execution paths, extensions, the Databricks settings overlay and
troubleshooting. Layering, `@secret` and `entity_tags` rules are in
[config.md](config.md). App layout and the local loop are in
[apps.md](apps.md). The examples use package `sales_core`, app
`daily_orders`, catalog `main`, schema `sales` and volume `kindling`.

- [Rules](#rules)
- [Prerequisites and auth](#prerequisites-and-auth)
- [Artifacts storage and Unity Catalog](#artifacts-storage-and-unity-catalog)
- [Pick an execution path](#pick-an-execution-path)
- [Path A: Kindling jobs](#path-a-kindling-jobs)
- [Path B: Lakeflow pipelines via bundles](#path-b-lakeflow-pipelines-via-bundles)
- [Extensions](#extensions)
- [settings.databricks.yaml](#settingsdatabricksyaml)
- [Verify and troubleshoot](#verify-and-troubleshoot)

## Rules

- Always pass `--platform databricks`. Auto-detection checks
  `FABRIC_WORKSPACE_ID` and `SYNAPSE_WORKSPACE_NAME` before `DATABRICKS_HOST`.
- Use folder names (snake_case) in deploy and run commands. `app deploy`
  names the remote app after its directory, and `app run --platform`
  resolves a name or path to that same folder name.
- Point every tool at one artifacts root with `KINDLING_ARTIFACTS_STORAGE_PATH`.
  Don't use the legacy `AZURE_STORAGE_ACCOUNT` triple here.
- Databricks-only settings go in `settings.databricks.yaml`, never in code.
  Remote commands never load `.env`, so export it first (`set -a; . ./.env; set +a`).

## Prerequisites and auth

`DATABRICKS_HOST` is always required. The Kindling SDK, and `kindling env check
--platform databricks`, try auth in this order: `DATABRICKS_TOKEN`, then the
Azure service principal (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`,
`AZURE_CLIENT_SECRET`), then `az login`. The devcontainer image has no `az`.

- The last two work only on Azure, and Databricks OAuth M2M
  (`DATABRICKS_CLIENT_ID`) is never read, so on AWS or GCP use a token. If
  PATs are disabled, mint one:
  `export DATABRICKS_TOKEN=$(az account get-access-token --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query accessToken -o tsv)`.
- The Databricks CLI (preinstalled in the devcontainer, needed for bundles) has
  its own auth chain: `DATABRICKS_TOKEN`, then `ARM_TENANT_ID`/`ARM_CLIENT_ID`/`ARM_CLIENT_SECRET`,
  then `az login`. It ignores `AZURE_*`, so copy those values into `ARM_*`.

```bash
# .env (gitignored; placeholder values)
DATABRICKS_HOST=https://adb-1234567890123456.7.azuredatabricks.net
DATABRICKS_TOKEN=<token>
KINDLING_ARTIFACTS_STORAGE_PATH=/Volumes/main/sales/kindling/artifacts
DATABRICKS_CLUSTER_ID=<existing-cluster-id>   # jobs path only; unset it for bundle commands
CONFIG__platform_databricks__kindling__temp_path=/Volumes/main/sales/kindling/tmp
```

`CONFIG__a__b=v` acts like `--param a.b=v` on `kindling app run`, and the
`CONFIG__platform_databricks__` form applies only to Databricks runs.
`KINDLING_DATABRICKS_RUNTIME_*` and `KINDLING_DATABRICKS_SYSTEM_TEST_MODE`
belong to the framework's own system tests. Don't set them.

## Artifacts storage and Unity Catalog

The artifacts root holds `packages/` (wheels), `scripts/kindling_bootstrap.py`,
`config/` and `data-apps/<app>/`. The CLI writes it and jobs read it. Use
`/Volumes/<catalog>/<schema>/<volume>/<path>` with Unity Catalog (create it
first with `databricks volumes create main sales kindling MANAGED`) or
`abfss://<container>@<account>.dfs.core.windows.net/<path>` without UC. The
CLI rejects `dbfs:/` roots because DBFS root storage is deprecated.

- On Databricks, an unset `kindling.delta.access_mode` becomes `catalog` when
  UC is detected at startup and `storage` when it isn't. The scaffolded
  `settings.yaml` pins `storage`, so a UC project sets `catalog` in its
  Databricks overlay to get managed tables by name.
- Without UC, set `kindling.features.databricks.uc_enabled: false`,
  `access_mode: storage` and `abfss://` values for `table_root` and
  `checkpoint_root`. With a Hive database, `catalog` mode also works with
  `table_schema` plus `table_schema_location`.
- `kindling.features.discovery: "false"` skips the startup probes and assumes UC and Volumes.

How table names resolve from the storage keys. The example runs locally,
and nothing touches Spark once a schema is configured:

```python
from types import SimpleNamespace

from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.injection import get_kindling_service
from kindling.spark_config import ConfigService

config = get_kindling_service(ConfigService)
names = get_kindling_service(EntityNameMapper)
paths = get_kindling_service(EntityPathLocator)
config.set("kindling.storage.table_catalog", "main")
config.set("kindling.storage.table_schema", "sales")
config.set("kindling.storage.table_root", "/Volumes/main/sales/kindling/tables")

orders = SimpleNamespace(entityid="bronze.orders", tags={})
assert names.get_table_name(orders) == "main.sales.bronze_orders"  # id flattened to one leaf
assert paths.get_table_path(orders) == "/Volumes/main/sales/kindling/tables/bronze/orders"  # storage mode
tagged = SimpleNamespace(entityid="bronze.orders", tags={"provider.table_catalog": "prod_bronze"})
assert names.get_table_name(tagged) == "prod_bronze.sales.bronze_orders"  # tag beats config
config.set("kindling.storage.table_schema", None)  # catalog only: the id keeps schema.table
assert names.get_table_name(orders) == "main.bronze.orders"
```

## Pick an execution path

| | A: Kindling jobs | B: Lakeflow pipeline (bundle) |
|---|---|---|
| Runs | app.py as a `spark_python_task` | the declared graph as a Lakeflow pipeline |
| Writes data | Kindling providers (Delta, merges, watermarks) | Lakeflow (MVs, streaming tables, AUTO CDC) |
| Compute | classic only: `--cluster-id`/`DATABRICKS_CLUSTER_ID`, else a new job cluster (default 1-worker 13.3 `Standard_DS3_v2`) | serverless |
| Config | downloaded from artifacts at run time | merged at build time, inline in the resource |
| Choose for | imperative pipes, file ingestion (Auto Loader), Delta writes you control | declarative pipes, expectations, SCD2, managed scheduling |

For path A, use an existing UC cluster in dedicated (single-user) access
mode. When the artifacts root is a volume, a new cluster gets `SINGLE_USER`
automatically, but its node type only exists on Azure.

## Path A: Kindling jobs

Start from a repo that `repo init`, `package init` and `app init` created
and that already validates locally ([apps.md](apps.md)).

```bash
set -a; . ./.env; set +a
kindling env check --platform databricks
kindling runtime deploy --source github:0.13.2 --dest "$KINDLING_ARTIFACTS_STORAGE_PATH" \
  --extension spark-kindling-ext-databricks-autoloader   # your Kindling pin, plus kindling.extensions wheels
kindling workspace deploy --platform databricks --config config/settings.yaml --env dev   # optional shared config/
kindling package deploy sales_core                                # each lake-reqs.txt package, first
kindling app deploy daily_orders --platform databricks --env dev  # adds settings.databricks.yaml + settings.dev.yaml
kindling app check --app apps/daily_orders/app.py --env dev --platform databricks   # runtime version skew
kindling app run daily_orders --platform databricks --env dev --param kindling_version=0.13.2
kindling app run daily_orders --platform databricks --env dev --new-cluster --node-type Standard_DS4_v2 --num-workers 4
kindling app logs "$RUN_ID" --platform databricks
kindling runner register --app daily_orders --platform databricks --config environment=dev --config kindling.temp_path=/Volumes/main/sales/kindling/tmp
```

- `runtime deploy` uploads `spark_kindling-*.whl` and the bootstrap script,
  plus the extension wheels named with `--extension` (or `--all-extensions`).
  Each run reinstalls the highest `spark_kindling` wheel in `packages/`, so
  pin with `--param kindling_version=X`.
- Use the same environment for deploy and run. Both default `--env` to
  `KINDLING_ENV`; with neither set, the job runs as `development`.
- Compute: `--cluster-id ID` runs on an existing cluster (overriding
  `DATABRICKS_CLUSTER_ID`); `--new-cluster` with `--spark-version`,
  `--node-type` and `--num-workers` sizes a job cluster. `runner register`
  takes the same options. Serverless jobs are not supported.
- Pass `kindling.temp_path` as a run parameter (`CONFIG__` export or
  `--param`). The framework install and config download run before settings
  files are read, and otherwise stage on `dbfs:/tmp`.
- `runner register` creates a persistent Job for Workflows. It ignores
  `CONFIG__` vars, so pass overrides with `--config`, keyed `environment`
  (not `env`).
- On the cluster, `DataAppManager` installs the `lake-reqs.txt` wheels and
  the app's `requirements.txt`, then imports each package's `entities`,
  `pipes` and `ingestion` subpackages, as a local run does. Declarations
  outside those subpackages register only if something imports them.

## Path B: Lakeflow pipelines via bundles

The generated source calls `kindling_ext_databricks.lakeflow_app_selector.declare_from_pipeline_config()`.
It loads the `spark_kindling.data_apps` entry point named by `kindling.data_app`,
initializes Kindling (`engine="databricks_sdp"`, `declaration_only`), calls
`register_all()` and declares the graph. Nothing is installed at run time, so
every wheel must be a pipeline dependency.

1. Expose the app from the package. The entry-point name must match both
   `--app` and an `apps/<name>/` directory:

   ```toml
   # packages/sales_core/pyproject.toml
   [project.entry-points."spark_kindling.data_apps"]
   daily_orders = "sales_core"
   ```

   ```python
   # illustrative: packages/sales_core/src/sales_core/__init__.py
   import importlib
   import pkgutil


   def register_all() -> None:
       """Declaration-only: import every entities/pipes module so decorators run."""
       for namespace in ("sales_core.entities", "sales_core.pipes"):
           package = importlib.import_module(namespace)
           for info in pkgutil.walk_packages(package.__path__, prefix=f"{namespace}."):
               importlib.import_module(info.name)
   ```

   `register_all()` may only declare: no writes, no installs, no `run_*_app`.
   Keeping the imports inside it means importing the package declares nothing.

2. Build the wheels, generate the bundle and deploy it. Serverless installs
   dependencies one at a time, so pass the wheels in this order: core,
   ext-sdp, ext-databricks, then your own.

   ```bash
   kindling env add spark-kindling-ext-databricks --version 0.13.2   # pins the release's extension version
   kindling runtime deploy --source github:0.13.2 --dest dist/kindling \
     --extension spark-kindling-ext-sdp --extension spark-kindling-ext-databricks   # stage release wheels in dist/kindling/packages/
   (cd packages/sales_core && uv build --wheel --out-dir ../../dist/lakeflow)
   kindling bundle build --name sales --target dev --app daily_orders \
     --workspace-host "$DATABRICKS_HOST" --workspace-root /Workspace/Users/me@example.com/sales \
     --catalog main --schema sales \
     --wheel dist/kindling/packages/spark_kindling-0.13.2-py3-none-any.whl \
     --wheel dist/kindling/packages/spark_kindling_ext_sdp-0.3.4-py3-none-any.whl \
     --wheel dist/kindling/packages/spark_kindling_ext_databricks-0.2.0-py3-none-any.whl \
     --wheel dist/lakeflow/sales_core-0.1.0-py3-none-any.whl
   cd dist/bundles/databricks
   databricks bundle validate -t dev
   databricks bundle deploy -t dev
   databricks bundle run -t dev daily_orders
   ```

   `runtime deploy` to a local directory downloads the release wheels into
   its `packages/`. Use the file names it reports there; the extension
   versions shown are examples only.

- Settings are merged at build time, `config/` first and then `apps/<app>/`
  (`settings.yaml`, `settings.databricks.yaml`, `settings.<env>.yaml`, where
  `--env` defaults to `--target`). The result is inlined as
  `kindling.lakeflow.settings_json`. `settings.local.yaml` is never included,
  and `@merge` directives are not applied.
- Datasets land in `<catalog>.<schema>` with normalized leaf names
  (`silver.orders` becomes `silver_orders`). MVs need `CREATE MATERIALIZED VIEW`.
  Inputs from outside the pipeline resolve as in path A.
- With no `--wheel`/`--dependency`, the bundle depends on an unpinned
  `spark-kindling-ext-databricks` from PyPI, which may not match your Kindling
  release (and releases before 0.14.0 aren't on PyPI). Pass the wheels. The default
  `--workspace-root` is `/Workspace/Shared/kindling/<name>/<target>`, which
  is writable by everyone.
- To own resource keys, permissions or clusters, run
  `kindling bundle template init` and build with `--template-dir bundle-template`
  (keep `kindling.configuration(<app>, ...)`). `--app-options-json` sets
  per-app catalog, schema or pipes. Use one pipeline per app, and never
  repoint an existing pipeline at another app.

## Extensions

Add an extension with `kindling env add <dist> --version <your Kindling version>`.
Never guess a version with `uv add`.

| Distribution | Adds | Enabled by | Reaches compute via |
|---|---|---|---|
| `spark-kindling-ext-sdp` | declaration engine: `DeclarationPlan`, OSS `pyspark.pipelines` emission, write guard | `kindling.initialize(engine="sdp")`; the adapter below builds on it | `--wheel` (path B) |
| `spark-kindling-ext-databricks` | Lakeflow adapter: expectations, SCD2 as AUTO CDC, Event Hubs/Kafka sources, `streaming_inputs`, temporal lowering, app selector | `engine="databricks_sdp"` (the selector sets it) | `--wheel` (path B) |
| `spark-kindling-ext-databricks-autoloader` | `cloudFiles` discovery for `FileIngestionEntries.entry(..., discovery="autoloader", source_glob=...)` | importing it registers the runner; needs `kindling.storage.checkpoint_root` | `kindling.extensions` (path A) |
| `spark-kindling-ext-otel-azure` | Azure Monitor log and trace providers | import, plus `kindling.telemetry.azure_monitor.enable_logging` and `enable_tracing` set to `true` (both default to **false**) and a `connection_string` | `kindling.extensions` (path A) |

- `kindling.extensions` is a list, so a later layer replaces it. It only acts
  during a path A bootstrap: each spec (`==`/`>=` honoured, otherwise the
  highest version) is matched to a wheel in `<artifacts>/packages/`, then
  pip-installed and imported. Standalone runs and Lakeflow ignore it.
- Upload extension wheels with the runtime:
  `kindling runtime deploy --source github:0.13.2 --dest "$KINDLING_ARTIFACTS_STORAGE_PATH" --extension spark-kindling-ext-otel-azure`
  (repeatable; `--all-extensions` uploads every one the release has).
- Expectation, SCD and temporal YAML (`datapipes: <pipe>: engine: databricks_sdp: ...`)
  is in the extension READMEs and [pipes.md](pipes.md).

## settings.databricks.yaml

Put the overlay in `apps/<app>/`, where `app deploy --platform databricks`,
`bundle build` and `config show --platform databricks` all read it. Shared
values go in `config/settings.databricks.yaml`, which `workspace deploy` and
`bundle build` read.

```yaml
# apps/daily_orders/settings.databricks.yaml
kindling:
  delta:
    access_mode: catalog          # overrides the scaffolded storage mode; UC managed tables
  storage:
    table_catalog: main           # with table_schema: bronze.orders -> main.sales.bronze_orders
    table_schema: sales
    checkpoint_root: /Volumes/main/sales/kindling/checkpoints   # streaming pipes, Auto Loader
  temp_path: /Volumes/main/sales/kindling/tmp    # extension installs; also pass it as a run parameter
  secrets:
    secret_scope: sales           # "@secret:<key>" -> dbutils.secrets.get("sales", key)
  extensions:
    - spark-kindling-ext-databricks-autoloader
    - spark-kindling-ext-otel-azure
  telemetry:
    azure_monitor:
      enable_logging: true
      enable_tracing: true
      connection_string: "@secret:appinsights-connection-string"
```

Per-environment catalogs go in the env overlay (`apps/<app>/settings.prod.yaml`),
which takes precedence over the platform overlay:

```yaml
kindling:
  storage:
    table_catalog: prod
dataentities-bytag:               # every entity tagged layer: bronze
  layer:
    bronze:
      tags:
        provider.table_catalog: prod_bronze
entity_tags:                      # exact id, flat tag keys
  ref.regions:
    provider.table_name: shared.reference.regions
```

- Secrets: `"@secret:<key>"` uses `kindling.secrets.secret_scope`, and
  `"@secret:<scope>:<key>"` names the scope directly. Create them with
  `databricks secrets create-scope sales` and `databricks secrets put-secret sales <key>`.
  When `dbutils` isn't available, as in local runs, the provider reads the
  env vars `<key>`, `<KEY>` or `KINDLING_SECRET_<KEY>`. Bundles copy settings
  into the resource YAML, so any literal secret there is published.
- With no `kindling.temp_path`, staging uses `kindling.databricks.volume_staging_root`
  or the volume parent of `checkpoint_root`/`table_root` (only once Volumes
  are detected), and `dbfs:/tmp` otherwise.

## Verify and troubleshoot

```bash
kindling env check --platform databricks          # host + first satisfied auth; exit 1 if not ready
kindling config show --app apps/daily_orders/app.py --env dev --platform databricks
kindling entity tags bronze.orders --app apps/daily_orders/app.py --env dev --platform databricks
kindling app status "$RUN_ID" --platform databricks
```

| Symptom | Cause and fix |
|---|---|
| `Unable to determine platform` / `Artifacts location is not configured` | pass `--platform databricks`; set `KINDLING_ARTIFACTS_STORAGE_PATH` (or `--artifacts-path`) |
| Job fails before Kindling starts | run `runtime deploy` for `scripts/kindling_bootstrap.py`. If the job can't read it from the volume at start, set `KINDLING_DATABRICKS_CLASSIC_BOOTSTRAP_ROOT` to a root (e.g. under `/Workspace`) containing `scripts/kindling_bootstrap.py` |
| `Failed to install kindling from datalake` | `packages/` has no `spark_kindling-*.whl`, or none matching `kindling_version` |
| Staging or `dbfs:/tmp` permission errors | pass `kindling.temp_path` as a run parameter. The principal needs READ VOLUME on artifacts, WRITE VOLUME on temp and checkpoints, and USE CATALOG/USE SCHEMA/CREATE TABLE on the target |
| Run succeeds, no pipes ran | the package isn't in `lake-reqs.txt`, or its declarations live outside `entities/`/`pipes/`/`ingestion/` |
| Stale code on an all-purpose cluster | lake wheels install without `--force-reinstall`, so pip skips a same-version wheel. Bump the version |
| `Failed to find extension wheel` / `No importable module found for extension` | the wheel isn't in `<artifacts>/packages/` (`runtime deploy --extension NAME`), or the spec name is wrong |
| `Missing kindling.storage.checkpoint_root` / `Databricks secret scope is not configured` | set the key in the Databricks overlay, or use `@secret:<scope>:<key>` |
| `Unknown Lakeflow data app 'x'. Discovered apps: ...` | the entry point is missing, or the app wheel isn't passed with `--wheel` |
| Bundle pipeline fails at `pip install` / keeps running an old wheel | wheel order (core, ext-sdp, ext-databricks, app). Serverless caches environments by requirement set, so bump the wheel version on every change |
| `databricks bundle run` reports an active update | a failed update is still retrying. Run `databricks pipelines stop <pipeline-id>`, wait for IDLE, then rerun |

Run `databricks bundle validate -t <target>` before each deploy. Review
`resources/*.pipeline.yml` and `manifest.json` (inputs and settings/wheel hashes).
