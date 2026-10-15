# Apps and project structure

How a Kindling domain repo is laid out, what an app is made of, and the CLI
loop for scaffolding, validating, running and shipping apps. Entity and pipe
definitions are in [entities.md](entities.md) and [pipes.md](pipes.md).
Settings layering, overlays and secrets are in [config.md](config.md).

1. [Repo layout](#repo-layout)
2. [Scaffolding: always use the CLI](#scaffolding-always-use-the-cli)
3. [Anatomy of an app](#anatomy-of-an-app)
4. [How entities and pipes get registered](#how-entities-and-pipes-get-registered)
5. [Execution patterns (the "executor")](#execution-patterns-the-executor)
6. [Validate and run locally](#validate-and-run-locally)
7. [Package and deploy](#package-and-deploy)
8. [Environment: uv workspace rules](#environment-uv-workspace-rules)
9. [Gotchas](#gotchas)

## Repo layout

A domain repo is a **uv workspace**. Reusable code lives in packages, and
deployable units live in apps. An app owns no entities or pipes. It names
the packages it runs in `lake-reqs.txt`.

```text
<repo>/
  pyproject.toml          # NOT a package: name "<repo>-workspace",
                          # [tool.uv] package = false, workspace members = ["packages/*"]
  uv.lock                 # one lockfile for the whole repo
  .venv/                  # one shared env, every package installed editable
  .devcontainer/  .github/workflows/ci.yml  scripts/setup-local-dev.sh
  packages/
    <pkg>/                # workspace member (distribution name <pkg-kebab>)
      pyproject.toml      # runtime dep: plain, unpinned spark-kindling; dev group: spark-kindling/SDK/CLI
                          # ==X.Y.Z pins, pytest, poe, pyspark/delta-spark/pandas/pyarrow; poe tasks
                          # (repos pinned by release URL use [tool.uv.sources] instead)
      settings.yaml  settings.local.yaml  .env.example
      src/<pkg>/
        entities/         # @DataEntities.entity declarations   -> auto-registered
        pipes/            # @DataPipes.pipe declarations        -> auto-registered
        ingestion/        # (optional) ingestion declarations   -> auto-registered
        transforms/       # plain DataFrame functions (NOT walked; import them from pipes)
      tests/unit/  tests/component/  tests/integration/
  apps/
    <app>/
      app.py              # entrypoint (thin; calls a kindling.apps helper)
      app.yaml            # manifest: name, entry_point
      lake-reqs.txt       # packages this app loads (and auto-registers)
      requirements.txt    # (optional) extra PyPI deps for remote runs
      settings.yaml       # deployed base config
      settings.local.yaml # local overlay: gitignored, never deployed
      .env.example  QUICKSTART.md
      tests/entities/<ns>/<name>.csv   # local fixture CSVs
```

## Scaffolding: always use the CLI

Never hand-create the repo, a package, or an app. The generators keep the
names, pins and file set consistent.

```bash
kindling repo init sales-repo --output-dir sales-repo        # repo root (no packages yet)
cd sales-repo
kindling env bootstrap                                       # pin Kindling at root + sync .venv/
kindling package init sales-core --layers medallion          # -> packages/sales_core/
kindling app init daily-orders --package sales-core --pattern batch           # -> apps/daily_orders/
kindling app init orders-stream --package sales-core --pattern streaming
kindling app init orders-landing --package sales-core --pattern file-ingestion
```

- `package init`: `--layers medallion|minimal`, `--auth oauth|key|cli`,
  `--no-integration`, `--repo-root`, `--template-dir`.
- `app init`: `--package` (**defaults to the app name**), `--pattern
  batch|streaming|file-ingestion` (leave it out for a hello-world app.py that
  runs no pipes), `--layers`, `--auth`, `--repo-root`, `--template-dir`.
- Add entities, pipes and ingestion with `kindling package add entity|pipe|ingestion`.
  [entities.md](entities.md) and [pipes.md](pipes.md) cover these.
- `app init` does not check that `--package` exists. A wrong name gives a
  `lake-reqs.txt` that registers nothing.

## Anatomy of an app

**app.py** holds no framework setup. The CLI (locally) or the platform
bootstrap (remotely) calls `initialize_framework()` and registers the
packages' entities and pipes **before** app.py runs. app.py only picks the
execution pattern in its `__main__` block. This is the scaffolded batch app:

```python
# illustrative: app.py runs as __main__ under the runner
"""daily-orders: Kindling batch app entrypoint."""

if __name__ == "__main__":
    from kindling.apps import run_batch_app

    run_batch_app()
```

Rules for app.py:
- Never call `initialize_framework()` and never hardcode a platform.
- Never import entity or pipe modules to register them. `lake-reqs.txt` does that.
- Keep it thin. Put business logic in a package so it can be tested.
- An app's `if __name__ == "__main__"` block runs in both cases: the local runner
  and the remote `DataAppManager` execute app.py with `__name__ == "__main__"`.

**app.yaml** is the manifest:

```yaml
name: daily-orders        # display name; deploy and run use the folder name, daily_orders
entry_point: app.py       # default app.py
# optional, read when packaging: description, version, dependencies, environment, metadata
```

**lake-reqs.txt** has one distribution spec per line. `#` comments are allowed
and version specifiers (`==`, `>=`, `~=` and so on) are accepted:

```text
# Artifact-backed wheels loaded from artifacts/packages/ at runtime.
sales-core
shared-reference-data==0.4.0
```

This file is used in three ways:
1. **Registration.** Each name is normalized (`-` becomes `_`, lowercased) to an
   import name, and its `entities`/`pipes`/`ingestion` subpackages are imported
   (see the next section).
2. **Local install.** `kindling app run` uses packages that are already
   installed (editable workspace members are never reinstalled).
   `--load-lake` fetches them from the artifacts lake.
3. **Remote install.** The remote app manager downloads the matching wheels
   from `<artifacts>/packages/` (picking the highest version when the spec is
   unpinned), pip-installs them, then imports them.

**requirements.txt** (optional) lists plain PyPI dependencies that are installed
for remote runs.

## How entities and pipes get registered

Bootstrap receives `registration_packages` (the normalized `lake-reqs.txt`
names). For each `<pkg>`, it imports `<pkg>.entities`, `<pkg>.pipes` and
`<pkg>.ingestion` and walks **every submodule** recursively (`pkgutil.walk_packages`).
Importing a module runs its decorators.

- A namespace that is missing is skipped. An `ImportError` inside one of your
  modules is raised.
- Decorated code anywhere else, such as `transforms/`, `<pkg>/__init__.py` or
  `utils/`, is **not** walked. Keep `@DataEntities.entity` and `@DataPipes.pipe`
  in those three namespaces. Pipes import transforms, not the other way round.
- A `lake-reqs.txt` entry whose import name differs from its distribution name
  (`-` to `_`) contributes nothing, and you get no error.
- `kindling app run --local-package <dir>` (repeatable) puts a package on the
  path and adds it to the walk without editing `lake-reqs.txt`.
- **Remote caveat (from the code):** `DataAppManager` (used by `app deploy` and
  `app run --platform`) does `import <pkg>` after installing lake wheels. It
  does not run the namespace walker itself. If a remote run registers no pipes,
  check this first. Confirm with a remote run before relying on it.

## Execution patterns (the "executor")

The executor is just the `__main__` block of app.py. It calls a helper from
`kindling.apps`, and `app init --pattern` picks which one.

| `--pattern` | helper | runs |
|---|---|---|
| `batch` | `run_batch_app(pipe_ids=None, *, use_dag=True)` | all registered pipes, sorted, unless `pipe_ids` is given; DAG-ordered |
| `streaming` | `run_streaming_app(pipe_ids=None, *, streaming_options=None)` | `ExecutionOrchestrator.execute_streaming`; checkpoint base from `KINDLING_CHECKPOINT_PATH` when no options are given |
| `file-ingestion` | `run_file_ingestion_app(source_path=None)` | `FileIngestionProcessor.process_path`; path from `KINDLING_INGESTION_PATH` (raises if unset) |

The helpers import from `kindling.apps`:

```python
from kindling.apps import run_batch_app, run_file_ingestion_app, run_streaming_app
```

To narrow or customize, edit app.py by hand rather than adding new files:

```python
# illustrative: app.py runs as __main__ under the runner
from kindling.apps import run_batch_app

if __name__ == "__main__":
    run_batch_app(["bronze_to_silver"], use_dag=True)  # only this pipe
```

`kindling app add executor` is **deprecated** and only prints a pointer to
`app init --pattern`. Do not use it. To change an existing app's pattern,
swap the helper call in app.py.

## Validate and run locally

These commands find app.py through `--app` or from the current directory:
`./app.py`, then `apps/*/app.py`, `src/*/app.py` and `packages/*/src/*/app.py`.
With more than one app at the repo root they fail with "Multiple app.py
files found", so **`cd apps/<app>`** or pass `--app apps/<app>/app.py`.

```bash
cd apps/daily_orders
kindling app validate --env local          # entity/pipe graph checks, no Spark
kindling app check --env local             # validate + app.py import smoke test (+ --platform version skew)
kindling pipeline list --env local         # registered pipe ids
kindling pipeline run bronze_to_silver --env local --no-watermark   # one pipe, upstream data already present
kindling entity list --tags                # registered entities and resolved tags
kindling app inspect daily_orders --entities --env local   # provider/path/fixture per entity
kindling app run daily_orders --env local  # full app in a subprocess (standalone platform)
kindling app run daily_orders --param report_date=2024-01-15 --trace
```

- `app run <app>` takes an app **name** or a **path** to an app directory (one
  containing `app.py`): `kindling app run .` from inside the app directory,
  `kindling app run apps/<app>`, or an absolute path. A name is normalized to
  snake_case and resolved by walking up from the current directory to
  `apps/<app>/`. Use `--local-folder <dir>` for other layouts.
- With `--platform`, APP resolves to the name `app deploy` deploys under: the
  app folder's name (`daily-orders` and `daily_orders` both submit
  `daily_orders`; a path uses its directory name). Use the folder name in
  commands. Nothing is uploaded, so deploy first; `--app-name` targets a custom
  deployed name.
- The `APP_NAME` argument to `app inspect` is only a display label. The app
  itself is still found through `--app` or the current directory.
- Fixture CSVs at `tests/entities/<ns>/<name>.csv` are resolved **relative to the
  current directory**, so run from the app directory to pick up the app's fixtures.
- `app run` loads `.env` from the current directory by default
  (`--dotenv FILE`, `--no-dotenv`). `--config DIR` adds a shared settings
  directory, and `--env` selects the overlay (see [config.md](config.md)).
- Package tests run through poe in the package directory, not through the app
  (see below).

## Package and deploy

Each command gets one line here. Run `--help` or see
`docs/reference/cli_reference.md` for details.

```bash
kindling runtime deploy --source github:0.13.2 --dest /Volumes/main/kindling/artifacts --extension spark-kindling-ext-databricks-autoloader   # Kindling + extension wheels -> <artifacts>/packages/
kindling package check sales-core              # metadata, src layout, wheel builds
kindling package deploy sales-core --artifacts-path /Volumes/main/kindling/artifacts   # build wheel -> <artifacts>/packages/
kindling app package daily_orders --platform databricks --env prod   # -> dist/<app-dir>.kda
kindling app deploy daily_orders --platform databricks --env prod    # upload app to <artifacts>/data-apps/<name>/
kindling app run daily_orders --platform databricks --env prod       # run the deployed app remotely
kindling app run daily_orders --platform databricks --new-cluster --node-type Standard_DS4_v2 --num-workers 4   # size the job cluster
kindling runner register --app daily-orders --platform databricks    # named job for external orchestrators
kindling bundle build --name sales --target dev --app daily-orders   # Databricks Lakeflow bundle (deploy with databricks CLI)
```

- Deploy the packages listed in `lake-reqs.txt` **before** the app that needs them.
- Cloud bootstrap installs the extensions named in `kindling.extensions` from
  `<artifacts>/packages/`; `kindling runtime deploy --extension NAME`
  (repeatable) or `--all-extensions` puts their wheels there.
- Remote `app run` and `app deploy` both default `--env` to `KINDLING_ENV`.
- Databricks job compute: `--cluster-id ID` (existing cluster), or
  `--new-cluster` with `--spark-version`, `--node-type`, `--num-workers`, on
  `app run --platform databricks` and `runner register`. Serverless jobs are
  not supported.
- A `.kda` archive contains only `*.py`, `*.yaml`/`*.yml`, `*.sql`,
  `requirements.txt` and `lake-reqs.txt`. `settings.local.yaml` is never
  included, `settings.<platform>.yaml`/`settings.<env>.yaml` are included only
  when selected, and fixture CSVs are not shipped.
- Follow a remote run with `kindling app status|logs|cancel <run_id>`.
  `kindling app cleanup <app>` deletes a deployed app.

## Environment: uv workspace rules

- At the **repo root**, run `kindling env bootstrap` (the devcontainer's
  `postCreateCommand` does this). If the root declares no Kindling pin, it adopts
  the release the packages pin (or pins latest when there are no packages) and
  syncs one repo-wide `.venv/`. Nested pins that disagree make it fail.
- Inside a package, use `uv run poe test` (unit + component), `uv run poe test-unit`,
  `test-component`, `test-integration`, `test-all`, and `uv run poe build`.
- **Never run a bare `uv sync` inside a package directory.** It does an exact sync
  of that one member and prunes the other packages from the shared `.venv/`. To
  resync, run `uv sync --all-packages` or `kindling env bootstrap` at the root.
- Every package must pin the **same Kindling release** as the root, in the same
  form (PyPI `==` versions, or release wheel URLs in `[tool.uv.sources]` for
  older repos). Otherwise uv fails to resolve the workspace. Move every pin
  together with `kindling env update` (`--version X.Y.Z`) at the root, or
  `uv run poe update-kindling`; it also converts URL pins to PyPI versions.
  Never hand-edit one package's pin.
- Kindling extensions are added with `kindling env add spark-kindling-ext-databricks`,
  never with `uv add` and a version you guessed.
- A package's runtime `dependencies` take plain, unpinned `spark-kindling`.
  Never put `spark-kindling[standalone]`, `spark-kindling==X`, `pyspark` or
  `delta-spark` there: lake wheels are pip-installed with their dependencies
  on clusters that already supply Spark, Delta and the Kindling the bootstrap
  installed. Local Spark comes from the root's `[standalone]` pin and
  each package's `dev` group.
- Run `kindling` from the repo `.venv/` (or `uv run kindling ...`).

```bash
kindling env bootstrap
kindling env update --version 0.13.2
(cd packages/sales_core && uv run poe test)
```

## Gotchas

- **Names are normalized.** `Daily-Orders` becomes `apps/daily_orders/`, with
  `name: daily-orders` in app.yaml. Packages work the same way: the
  distribution is `sales-core`, the import is `sales_core`, and the directory is
  `packages/sales_core/`. CLI name arguments accept either form. Names that
  start with a digit or map to a Python keyword are rejected.
- **`--package` defaults to the app name.** Pass it whenever the app and the
  package have different names.
- **An app has no code of its own.** It cannot "own" entities. A shared
  package used by several apps goes in each app's `lake-reqs.txt`.
- **No registration means no pipes.** With an empty registry, `run_batch_app()`
  quietly runs nothing. Confirm with `kindling pipeline list` before debugging
  the app.
- **`kindling.bootstrap.load_workspace_packages: false`** is in the scaffolded
  settings. Leave it off. Code arrives through `lake-reqs.txt`, not through
  notebook or workspace scanning.
- **Settings per app.** Each app has its own `settings.yaml` and overlays.
  Packages also ship a `settings.yaml` for their own tests. The layering
  rules are in [config.md](config.md).
