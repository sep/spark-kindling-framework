# Kindling Developer Stories

Developer-oriented use cases for the Kindling CLI, organized by phase of the development lifecycle. These stories describe the intended devex and can be used to evaluate CLI design proposals against real workflows.

---

## 1. First-Time Setup

**As a developer, I want to initialize a new Kindling app from a template**
so that I get a working project structure without reading docs first.

```bash
kindling app init ./orders --template default
```

**As a developer, I want to configure environment-specific settings for my app**
so that dev and prod environments point at the right platforms, storage paths, and entity providers without changing code.

```bash
kindling env init --app ./fawkes
```

Expected behavior: generates a `kindling.yaml` stub with `dev` and `prod` environment blocks, including ABFSS storage path placeholders and entity provider defaults for each environment. The developer fills in storage account, container, and platform details.

Example output structure:
```yaml
app: fawkes
environments:
  dev:
    platform: synapse
    storage:
      account: myaccount
      container: dev
    entities:
      default_provider: delta
  prod:
    platform: synapse
    storage:
      account: myaccount
      container: prod
    entities:
      default_provider: delta
```

**As a developer, I want to check whether my local environment is configured correctly**
so that I know if I can run pipelines locally before writing any code.

```bash
kindling env check
```

**As a developer, I want to check whether my platform connection is configured correctly**
so that I can confirm credentials and workspace settings before attempting a remote run.

```bash
kindling env check --platform synapse
```

**As a developer, I want to see what platforms are available and which one is active**
so that I understand where commands will run by default.

```bash
kindling env list
```

---

## 2. Local Development Loop

**As a developer, I want to run a single pipe locally against my dev data**
so that I can iterate quickly without deploying to a remote platform.

```bash
kindling run bronze.ingest_orders --env dev
```

**As a developer, I want to run a pipe locally with a specific config file**
so that I can test different parameter sets without changing my defaults.

```bash
kindling run bronze.ingest_orders --config ./local-test-config.yaml
```

**As a developer, I want to see what apps and pipes are registered in my project**
so that I can verify the registration wiring is correct before running anything.

```bash
kindling app list
kindling pipeline list --app orders
```

**As a developer, I want to run a single layer using an already-populated upstream entity as input**
so that I can iterate on silver or gold transforms without re-running bronze every time.

```bash
kindling pipeline run silver.stage_fawkes --app fawkes --env dev
```

The silver pipe reads from the `bronze.fawkes` Delta entity that already exists in dev storage. No bronze re-execution needed. This is the primary iteration loop when developing mid-pipeline stages — the upstream entity is already there; only the downstream transform changes.

**As a developer, I want to run a pipe with watermarking disabled**
so that I can do a full backfill during development without resetting watermark state manually.

```bash
kindling run bronze.ingest_orders --no-watermark
```

---

## 3. Testing

The test suites map to two execution environments:

- **Local execution** (unit, component, integration) — Spark runs on the dev machine or CI runner. No platform credentials required for unit and component. Integration may use `tests/entities/` CSVs (local, no credentials) or cloud storage (credentials required).
- **Remote execution** (system) — code runs inside the platform Spark pool. Requires runner, platform credentials, and real environment config. This is `kindling app run` with assertions.

**As a developer, I want to run unit tests for my app**
so that I can verify transform logic in isolation without the Kindling runtime or storage.

Unit tests call transform functions directly. Data can be anything — hardcoded rows, CSVs from `tests/entities/`, or in-memory DataFrames. No Kindling runtime involved.

```bash
kindling test run --suite unit
```

**As a developer, I want to run component tests to verify entity and pipe registration**
so that I catch DI wiring errors before running anything that touches data.

```bash
kindling test run --suite component
```

**As a developer, I want to run integration tests with local fixture data**
so that I can verify full pipeline behavior on my machine without cloud storage credentials.

Integration tests run the full Kindling pipeline with local Spark. The `tests/entities/` convention is the primary data source — `tests/entities/orders.csv` is auto-discovered as the `orders` entity source. No platform credentials needed.

```bash
kindling test run --suite integration
```

**As a developer, I want to run integration tests against real cloud storage**
so that I can verify pipeline behavior against the actual data shape and volume in dev storage.

```bash
kindling test run --suite integration --env dev
```

This uses the same local Spark execution but entity providers resolve to cloud storage paths for the `dev` environment rather than `tests/entities/`.

**As a developer, I want to run system tests against a real platform Spark pool**
so that I can verify the full remote execution path before promoting to production.

System tests submit to the platform runner and execute remotely — the same path as `kindling app run`. Requires runner to be installed and platform credentials configured.

```bash
kindling test run --suite system --platform synapse
kindling test run --suite system --platform fabric
```

**As a developer, I want to write an end-to-end medallion integration test that seeds one CSV and asserts on gold output**
so that a single test covers the full bronze → silver → gold chain with controlled data.

The integration test seeds a CSV into `tests/entities/bronze/fawkes_raw/`, runs the full pipeline locally, and asserts on the gold entity output. `kindling app add pipe` scaffolds the per-layer tests, but this story is about the cross-layer test that validates the chain as a whole.

```bash
kindling test run --suite integration --test test_fawkes_medallion
```

The scaffolded skeleton for this test is generated by `kindling app add pipe gold.curate_fawkes --chain`, which produces an additional `tests/integration/test_fawkes_chain.py` covering the full flow end to end.

**As a developer, I want to run preflight checks before integration or system tests**
so that I get a clear error on missing credentials or config rather than a cryptic failure mid-run.

```bash
kindling test check --preflight local
kindling test check --preflight system
```

**As a developer, I want to clean up test artifacts after a run**
so that stale output data does not affect subsequent test runs.

```bash
kindling test cleanup
```

---

## 4. Notebook Import

These stories cover converting notebook-based development work into Kindling app packages. Kindling uses a folder convention for notebook packages: a folder is recognized as a package when it contains an `{name}_init` notebook, and notebooks within it use `notebook_import("package.module")` to reference each other.

**As a developer, I want to import a whole notebook folder that follows the Kindling convention into a new app package**
so that I can take notebook-based work and turn it into a deployable Kindling app without manually converting each notebook.

```bash
kindling notebook import --folder ./orders
```

Expected behavior: scans `./orders` for the `orders_init` notebook, resolves `notebook_import()` dependencies, converts each notebook to a `.py` module, and emits a structured app package ready for `kindling app package`.

**As a developer, I want to import a whole notebook folder into an existing app package**
so that I can merge a new set of notebooks into a package I am already maintaining without creating a new package from scratch.

```bash
kindling notebook import --folder ./bronze_transforms --into ./orders
```

Expected behavior: converts each notebook in `./bronze_transforms` to a module and adds it under the target package, resolving `notebook_import()` calls relative to the merged package. Warns on name collisions rather than silently overwriting.

**As a developer, I want to import a single notebook as a module into an existing package**
so that I can add one new notebook to an existing package without re-importing the whole folder.

```bash
kindling notebook import --notebook ./bronze_transforms/ingest_orders --into ./orders
```

Expected behavior: converts `ingest_orders` to `orders/ingest_orders.py`, rewrites any `notebook_import()` calls to standard Python imports, and leaves the rest of the package untouched.

**As a developer, I want to preview what a notebook import would produce before writing any files**
so that I can verify the conversion before committing it to the package.

```bash
kindling notebook import --folder ./orders --dry-run
```

**As a developer, I want to see which notebooks in a folder Kindling recognizes as a package**
so that I can confirm the `_init` convention is set up correctly before importing.

```bash
kindling notebook scan ./orders
```

Expected output: lists discovered package name, init notebook, member notebooks, and resolved `notebook_import()` dependency graph.

---

## 5. Scaffolding

These commands add new building blocks to an existing app and generate the corresponding test scaffolding so that the new code has a place to be tested immediately.

**As a developer, I want to add a new module to an existing app**
so that I have a correctly structured file and matching unit test skeleton without writing boilerplate.

```bash
kindling app add module bronze --app ./orders
```

Generated:
```
orders/bronze/__init__.py
orders/bronze/bronze.py            # module skeleton with imports
tests/unit/test_bronze.py          # unit test skeleton
```

**As a developer, I want to add a new entity to a module**
so that the entity is registered and a sample CSV fixture is created for use in unit and integration tests.

```bash
kindling app add entity bronze.orders --app ./orders
```

Generated:
```
orders/bronze/entities.py          # entity definition appended or created
tests/entities/bronze/orders.csv   # empty CSV with header row matching entity schema
```

The generated CSV is a stub — column headers only — so unit and integration tests have a file to populate rather than having to create it from scratch.

**As a developer, I want to add a new data pipe to a module**
so that the pipe is registered, a transform function skeleton exists, and all three test tiers have scaffolding.

```bash
kindling app add pipe bronze.ingest_orders --app ./orders
```

Generated:
```
orders/bronze/ingest_orders.py             # pipe + transform function skeleton
tests/unit/test_ingest_orders.py           # unit test: calls transform directly with sample data
tests/integration/test_ingest_orders.py    # integration test: runs full pipeline, asserts on output entity
tests/entities/bronze/orders.csv          # fixture stub if not already present for input entities
```

The unit test skeleton reads from `tests/entities/` directly and calls the transform function. The integration test skeleton runs the full pipeline via the Kindling runtime and reads the output entity to assert on.

**As a developer, I want scaffolded tests to appear in batch test runs immediately**
so that I can see which tests still need to be implemented without having to track them manually.

Scaffolded tests are generated as `pytest.mark.skip` stubs, not empty `pass` bodies. They show up in `kindling test run` output from the moment the pipe is added, with a message indicating what needs to be done:

```python
@pytest.mark.skip(reason="scaffolded — implement transform assertions")
def test_ingest_orders_transform():
    ...

@pytest.mark.skip(reason="scaffolded — populate tests/entities/bronze/orders.csv before running")
def test_ingest_orders_pipeline():
    ...
```

Since generated files follow pytest naming conventions (`test_*.py` in `tests/unit/` and `tests/integration/`), no registration is needed — `kindling test run --suite unit` and `--suite integration` pick them up automatically.

**As a developer, I want to add a file ingestion pipe that reads CSVs from a storage path by filename pattern**
so that I have the correct ingestion scaffolding for a bronze CSV source without writing the file scanning boilerplate.

```bash
kindling app add ingestion bronze.fawkes_raw \
  --source-pattern "abfss://raw@{account}.dfs.core.windows.net/fawkes/*.csv" \
  --filename-metadata frequency \
  --app ./fawkes
```

Generated:
```
fawkes/bronze/fawkes_raw_ingestion.py     # file ingestion entry using file_ingestion.py patterns
                                          # includes filename metadata extraction (e.g. frequency)
fawkes/bronze/entities.py                 # bronze.fawkes_raw entity definition with CSV provider
tests/unit/test_fawkes_raw_ingestion.py   # unit test: filename pattern matching, metadata extraction
tests/integration/test_fawkes_raw_ingestion.py  # integration test: reads from tests/entities/bronze/fawkes_raw/
tests/entities/bronze/fawkes_raw/         # folder for sample CSV files matching the ingestion pattern
```

The unit test covers filename parsing and metadata extraction (e.g. deriving `frequency` from the filename) independently of storage. The integration test places sample CSVs in the fixture folder and verifies they land in the bronze Delta entity correctly.

**As a developer, I want to add a pipe that takes multiple input entities**
so that fixture stubs are generated for all of them.

```bash
kindling app add pipe silver.clean_orders --inputs bronze.orders,bronze.products --app ./orders
```

Generated fixture stubs:
```
tests/entities/bronze/orders.csv
tests/entities/bronze/products.csv
tests/unit/test_clean_orders.py
tests/integration/test_clean_orders.py
```

---

## 6. Entity and Data Source Configuration

These stories cover wiring entities to their data sources — distinguishing between CSV files on cloud storage, Delta tables, and local test fixtures.

**As a developer, I want to configure a bronze entity to read from CSV files on ABFSS storage**
so that my ingestion pipe reads raw source files rather than a Delta table.

The entity provider is set in `kindling.yaml` per environment:

```yaml
environments:
  dev:
    entities:
      bronze.fawkes_raw:
        provider: csv
        path: "abfss://raw@{account}.dfs.core.windows.net/fawkes/"
  prod:
    entities:
      bronze.fawkes_raw:
        provider: csv
        path: "abfss://raw@{account}.dfs.core.windows.net/fawkes/"
```

The `tests/entities/` convention takes over automatically when running without `--env` or with `--env local`, so the same entity uses local CSV fixtures during unit and integration tests without any config change.

**As a developer, I want to verify what provider and storage path each entity resolves to for a given environment**
so that I can confirm the right data source is active before running a pipeline.

```bash
kindling app inspect fawkes --entities --env dev
kindling app inspect fawkes --entities --env prod
```

Expected output: table of entity ID, provider type, resolved storage path, and whether a `tests/entities/` fixture override is active.

**As a developer, I want to configure silver and gold entities to write to Delta tables on ABFSS**
so that each layer persists its output to the correct storage location.

```yaml
environments:
  dev:
    entities:
      silver.fawkes:
        provider: delta
        path: "abfss://silver@{account}.dfs.core.windows.net/fawkes/"
      gold.fawkes:
        provider: delta
        path: "abfss://gold@{account}.dfs.core.windows.net/fawkes/"
```

---

## 7. Packaging and Deployment

**As a developer, I want to package my app into a `.kda` archive**
so that I have a versioned artifact I can deploy or share.

```bash
kindling app package --local-folder ./orders
kindling app package --local-folder ./orders --output dist/orders.kda
```

**As a developer, I want to deploy my local app directory to a remote platform**
so that the runner has access to the latest app code and config.

```bash
kindling app deploy --local-folder ./orders --platform synapse
kindling app deploy --local-folder ./orders --platform fabric --env prod
```

**As a developer, I want to deploy a pre-built `.kda` archive to a remote platform**
so that I can promote a pinned artifact without re-packaging from source.

```bash
kindling app deploy --kda-package dist/orders.kda --platform synapse --env prod
```

**As a developer, I want to see what app version is currently deployed**
so that I know whether my local changes are reflected in the remote environment.

```bash
kindling app status orders --deployed
```

**As a developer, I want to clean up deployed app artifacts for an old app version**
so that I do not accumulate stale packages in storage.

```bash
kindling app cleanup orders
```

---

## 8. Remote Execution

**As a developer, I want to run a deployed app on a remote platform and stream its logs**
so that I can observe the run without separate log polling.

```bash
kindling app run orders --env dev
```

**As a developer, I want to deploy my local app and run it in one command**
so that I do not have to run `deploy` and `run` separately during rapid iteration.

```bash
kindling app run orders --deploy --local-folder ./orders --platform synapse
```

**As a developer, I want to run a specific pipeline within an app**
so that I can execute a single stage without triggering the full app.

```bash
kindling pipeline run bronze.ingest_orders --app orders --env dev
```

**As a developer, I want to run all pipelines in dependency order in a single command**
so that I can execute the full medallion chain (bronze → silver → gold) without manually sequencing each layer.

```bash
kindling app run fawkes --env dev
```

When no `--pipeline` is specified, `app run` executes all registered pipes in topological order based on their declared input/output entity dependencies. Bronze ingestion runs first, its output feeds silver staging, silver output feeds gold curation.

**As a developer, I want to pass runtime parameters to an app run**
so that I can vary inputs like date ranges or batch sizes without changing config files.

```bash
kindling app run orders --parameters params.yaml
kindling app run orders --param start_date=2026-01-01 --param batch_size=500
```

**As a developer, I want to start a run and get the run id immediately without waiting**
so that I can submit multiple runs in parallel from a script.

```bash
kindling app run orders --no-wait --json
```

**As a developer, I want to check the status of a run by its run id**
so that I can poll or inspect a run I submitted earlier.

```bash
kindling app status <run-id>
```

**As a developer, I want to stream logs for a run that is already in progress**
so that I can attach to a run I started with `--no-wait`.

```bash
kindling app logs <run-id> --stream
```

**As a developer, I want to cancel a run that is taking too long or running against the wrong data**
so that I can stop it without waiting for timeout.

```bash
kindling app cancel <run-id>
```

---

## 9. Runner Management

**As a developer setting up a new workspace, I want to install the Kindling runner**
so that remote app execution is available for my team.

```bash
kindling runner ensure --platform synapse
```

**As a developer, I want to check the status of the runner**
so that I can confirm it is installed, healthy, and at the expected version before running jobs.

```bash
kindling runner status
```

**As a developer, I want to repair the runner after a config or credential change**
so that the runner picks up new settings without requiring a full reinstall.

```bash
kindling runner repair
```

**As an admin, I want to delete the runner from a workspace**
so that I can tear down a deprecated environment cleanly.

```bash
kindling runner delete
```

---

## 10. CI/CD Integration

**As a CI pipeline, I want to deploy an app and run it non-interactively, capturing structured output**
so that I can parse run results and fail the build on error.

```bash
kindling app run orders --deploy --kda-package dist/orders.kda --no-wait --json > run.json
RUN_ID=$(jq -r '.run_id' run.json)
kindling app status $RUN_ID --poll --json
```

**As a CI pipeline, I want a single command that deploys and runs, waits for completion, and exits non-zero on failure**
so that CI job configuration stays simple.

```bash
kindling app run orders --deploy --kda-package dist/orders.kda --fail-on-error --platform synapse
```

**As a CI pipeline, I want to run unit and component tests and produce a report**
so that PR checks include test results without requiring platform credentials.

```bash
kindling test run --suite unit --suite component --report junit
```

**As a CI pipeline, I want to run system tests only on release, not on every push**
so that expensive platform tests are gated appropriately.

```bash
kindling test run --suite system --platform synapse --platform fabric
```

**As a CI pipeline, I want to package the app and upload the artifact to storage for downstream jobs**
so that deploy and run steps can reference a pinned artifact rather than re-packaging.

```bash
kindling app package --local-folder ./orders --output dist/orders.kda
kindling app deploy --kda-package dist/orders.kda --platform synapse
```

**As a CI pipeline, I want to clean up deployed app artifacts after a test run**
so that CI does not leave stale data in shared dev storage.

```bash
kindling app cleanup orders --env ci
```

**As a CI pipeline, I want to ensure the runner is installed before running jobs**
so that a missing runner does not cause an obscure failure inside `app run`.

```bash
kindling runner ensure --platform synapse
kindling app run orders --no-wait --json
```

---

## 11. Debugging and Observability

**As a developer, I want to fetch the logs for a completed run**
so that I can diagnose why a run failed after the fact.

```bash
kindling app logs <run-id>
```

**As a developer, I want to inspect the full runner state, including version and config path**
so that I can tell whether an old runner definition is causing unexpected behavior.

```bash
kindling runner status --verbose
```

**As a developer, I want to invoke the runner with a raw parameters file**
so that I can reproduce a failure using the exact parameters a CI job used.

```bash
kindling runner invoke --params params.yaml
```

**As a developer, I want to inspect the current contents of an intermediate entity**
so that I can verify what bronze or silver produced before running the next layer.

```bash
kindling entity show bronze.fawkes_raw --env dev --limit 20
kindling entity show silver.fawkes --env dev --count
```

Expected behavior: reads the entity from its configured provider for the given env and prints a sample of rows or a row count. Useful for confirming an upstream layer produced expected output before running a downstream transform.

**As a developer, I want to check data quality after a pipeline run**
so that I can detect zero-row output, schema drift, or failed unit conversions without reading raw logs.

```bash
kindling entity validate gold.fawkes --env dev
```

Expected checks: row count > 0, no unexpected nulls in key columns, schema matches registered entity definition. Reports violations as warnings or errors depending on severity.

**As a developer, I want to check what entities and their storage paths are registered for my app**
so that I can confirm the data layer is pointing at the right locations before running.

```bash
kindling app inspect orders --entities
```

---

## 12. App Lifecycle and Versioning

**As a developer, I want to see a history of recent runs for an app**
so that I can compare results across runs and spot regressions.

```bash
kindling app history orders
```

**As a developer, I want to promote an app artifact from a dev environment to prod**
so that I do not have to re-package from source for a production deploy.

```bash
kindling app deploy --kda-package dist/orders.kda --platform synapse --env prod
```

**As a developer, I want to know what the last successfully deployed version of an app is**
so that I can roll back to it if the current version is broken.

```bash
kindling app status orders --env prod --deployed
```
