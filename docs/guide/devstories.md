# Kindling Developer Stories

Developer-oriented use cases for the Kindling CLI, organized by phase of the development lifecycle. These stories describe the intended devex and can be used to evaluate CLI design proposals against real workflows.

---

## 1. First-Time Setup

**As a developer, I want to initialize a new Kindling app from a template**
so that I get a working project structure without reading docs first.

```bash
kindling app init orders --package orders --repo-root .
```

This creates `apps/orders/` containing `app.py`, `app.yaml`, `lake-reqs.txt`, `settings.yaml`, `settings.local.yaml`, and `QUICKSTART.md`.

**As a developer, I want to generate an initial config file for my app**
so that I have a `settings.yaml` stub to fill in with storage account, platform, and environment details.

```bash
kindling config init
```

Expected behavior: generates a `settings.yaml` with Kindling telemetry, bootstrap, and extension stubs. The developer fills in storage account, container, secrets, and platform-specific settings.

Example output structure:
```yaml
name: myproject
version: "0.1.0"
description: "Kindling data app"

kindling:
  telemetry:
    logging:
      level: INFO
      print: true
  bootstrap:
    load_lake: true
    load_local: false
  spark_configs: {}
  required_packages: []
  extensions: []
```

**As a developer, I want to check whether my local environment is configured correctly**
so that I know if I can run pipelines locally before writing any code.

```bash
kindling env check
```

**As a developer, I want to check whether my local Spark stack is ready**
so that I can confirm Java, PySpark, delta-spark, and the hadoop-azure JARs are all present.

```bash
kindling env check --local
```

**As a developer, I want to check whether my platform credentials are configured correctly**
so that I can confirm credentials and workspace settings before attempting a remote run.

```bash
kindling env check --platform synapse
kindling env check --platform fabric
kindling env check --platform databricks
```

**As a developer, I want to download all JARs required for local ABFSS development**
so that I can run pipelines locally against Azure Data Lake Storage.

```bash
kindling env ensure
```

---

## 2. Local Development Loop

**As a developer, I want to run a single pipe locally against my dev data**
so that I can iterate quickly without deploying to a remote platform.

```bash
kindling pipeline run bronze.ingest_orders --env dev
```

**As a developer, I want to run a pipe locally with a specific config directory**
so that I can test different parameter sets without changing my defaults.

```bash
kindling pipeline run bronze.ingest_orders --config ./local-test-config/
```

**As a developer, I want to see what pipes are registered in my app**
so that I can verify the registration wiring is correct before running anything.

```bash
kindling pipeline list --app apps/orders/app.py
```

**As a developer, I want to run a single layer using an already-populated upstream entity as input**
so that I can iterate on silver or gold transforms without re-running bronze every time.

```bash
kindling pipeline run silver.stage_myproject --env dev
```

The silver pipe reads from the `bronze.myproject` Delta entity that already exists in dev storage. No bronze re-execution needed. This is the primary iteration loop when developing mid-pipeline stages — the upstream entity is already there; only the downstream transform changes.

**As a developer, I want to run a pipe with watermarking disabled**
so that I can do a full backfill during development without resetting watermark state manually.

```bash
kindling pipeline run bronze.ingest_orders --no-watermark
```

**As a developer, I want to validate my app's entity and pipe registrations without starting Spark**
so that I can catch wiring errors before running anything that touches data.

```bash
kindling app validate
```

---

## 3. Testing

The test suites map to two execution environments:

- **Local execution** (unit, component, integration) — Spark runs on the dev machine or CI runner. No platform credentials required for unit and component. Integration may use `tests/entities/` CSVs (local, no credentials) or cloud storage (credentials required).
- **Remote execution** (system) — code runs inside the platform Spark pool. Requires platform credentials and real environment config. This is `kindling app run` with assertions.

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

System tests submit to the platform and execute remotely — the same path as `kindling app run`. Requires platform credentials configured.

```bash
kindling test run --suite system --platform synapse
kindling test run --suite system --platform fabric
```

**As a developer, I want to write an end-to-end medallion integration test that seeds one CSV and asserts on gold output**
so that a single test covers the full bronze → silver → gold chain with controlled data.

The integration test seeds a CSV into `tests/entities/bronze/myproject_raw/`, runs the full pipeline locally, and asserts on the gold entity output. `kindling package add pipe` scaffolds the per-layer tests, but this story is about the cross-layer test that validates the chain as a whole.

```bash
kindling test run --suite integration --test test_myproject_medallion
```

The scaffolded skeleton for this test is generated by `kindling package add pipe gold.curate_myproject`, which produces an additional `tests/integration/test_curate_myproject.py` covering the full flow end to end.

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

## 4. Scaffolding

These commands add new building blocks to an existing package and generate the corresponding test scaffolding so that the new code has a place to be tested immediately.

**As a developer, I want to add a new entity to a package**
so that the entity is registered and a sample CSV fixture is created for use in unit and integration tests.

```bash
kindling package add entity bronze.orders --package packages/orders/src/orders
```

Generated:
```
packages/orders/src/orders/entities.py     # entity definition appended or created
tests/entities/bronze/orders.csv           # empty CSV with header row matching entity schema
```

The generated CSV is a stub — column headers only — so unit and integration tests have a file to populate rather than having to create it from scratch.

**As a developer, I want to add a new data pipe to a package**
so that the pipe is registered, a transform function skeleton exists, and all three test tiers have scaffolding.

```bash
kindling package add pipe bronze.ingest_orders \
    --inputs bronze.raw_orders \
    --package packages/orders/src/orders
```

Generated:
```
packages/orders/src/orders/bronze/ingest_orders.py    # pipe + transform function skeleton
tests/unit/test_ingest_orders.py                       # unit test: calls transform directly with sample data
tests/integration/test_ingest_orders.py                # integration test: runs full pipeline, asserts on output entity
tests/entities/bronze/raw_orders.csv                  # fixture stub if not already present for input entities
```

The unit test skeleton reads from `tests/entities/` directly and calls the transform function. The integration test skeleton runs the full pipeline via the Kindling runtime and reads the output entity to assert on.

**As a developer, I want scaffolded tests to appear in batch test runs immediately**
so that I can see which tests still need to be implemented without having to track them manually.

Scaffolded tests are generated as `pytest.mark.skip` stubs, not empty `pass` bodies. They show up in `kindling test run` output from the moment the pipe is added, with a message indicating what needs to be done:

```python
@pytest.mark.skip(reason="scaffolded — implement transform assertions")
def test_ingest_orders_transform():
    ...

@pytest.mark.skip(reason="scaffolded — populate tests/entities/bronze/raw_orders.csv before running")
def test_ingest_orders_pipeline():
    ...
```

Since generated files follow pytest naming conventions (`test_*.py` in `tests/unit/` and `tests/integration/`), no registration is needed — `kindling test run --suite unit` and `--suite integration` pick them up automatically.

**As a developer, I want to add a file ingestion pipe that reads CSVs from a storage path by filename pattern**
so that I have the correct ingestion scaffolding for a bronze CSV source without writing the file scanning boilerplate.

```bash
# Option A: provide a full filename regex (named groups become columns automatically)
kindling package add ingestion bronze.myproject_raw \
  --source-pattern 'myproject_(?P<test_name>[^_]+)_(?P<frequency>\d+)hz\.csv' \
  --package packages/myproject/src/myproject

# Option B: let --filename-metadata generate a default single-group pattern
kindling package add ingestion bronze.myproject_raw \
  --filename-metadata frequency \
  --package packages/myproject/src/myproject
```

The base storage path (ABFSS URL) is set in `settings.yaml` per environment and passed to
`process_path()` at runtime — it is not part of the filename pattern.

Generated:
```
packages/myproject/src/myproject/bronze/myproject_raw_ingestion.py    # FileIngestionEntries entry with filename regex
packages/myproject/src/myproject/entities.py                          # bronze.myproject_raw entity definition with CSV provider
tests/unit/test_myproject_raw_ingestion.py                            # unit test: filename pattern matching, metadata extraction
tests/integration/test_myproject_raw_ingestion.py                     # integration test: reads from tests/entities/bronze/myproject_raw/
tests/entities/bronze/myproject_raw/                                  # folder for sample CSV files matching the ingestion pattern
```

The unit test covers filename parsing and metadata extraction (e.g. deriving `frequency` from the filename) independently of storage. The integration test places sample CSVs in the fixture folder and verifies they land in the bronze Delta entity correctly.

**As a developer, I want to add a pipe that takes multiple input entities**
so that fixture stubs are generated for all of them.

```bash
kindling package add pipe silver.clean_orders \
    --inputs bronze.orders,bronze.products \
    --package packages/orders/src/orders
```

Generated fixture stubs:
```
tests/entities/bronze/orders.csv
tests/entities/bronze/products.csv
tests/unit/test_clean_orders.py
tests/integration/test_clean_orders.py
```

---

## 5. Entity and Data Source Configuration

These stories cover wiring entities to their data sources — distinguishing between CSV files on cloud storage, Delta tables, and local test fixtures.

**As a developer, I want to configure a bronze entity to read from CSV files on ABFSS storage**
so that my ingestion pipe reads raw source files rather than a Delta table.

Entity provider settings are configured in `settings.yaml` (and environment-specific overlays such as `settings.dev.yaml`):

```yaml
# settings.dev.yaml
kindling:
  spark_configs:
    spark.hadoop.fs.azure.account.auth.type: OAuth
```

The entity provider type is set via entity tags in the Python registration code:
```python
DataEntities.entity(
    entityid="bronze.myproject_raw",
    tags={
        "provider_type": "csv",
        "provider.path": "abfss://raw@myaccount.dfs.core.windows.net/myproject/",
    },
    ...
)
```

The `tests/entities/` convention takes over automatically when running without `--env` or with `--env local`, so the same entity uses local CSV fixtures during unit and integration tests without any config change.

**As a developer, I want to verify what provider and storage path each entity resolves to for a given environment**
so that I can confirm the right data source is active before running a pipeline.

```bash
kindling app inspect myproject --entities --env dev
kindling app inspect myproject --entities --env prod
```

Expected output: table of entity ID, provider type, resolved storage path, and whether a `tests/entities/` fixture override is active.

**As a developer, I want to inspect the current contents of an intermediate entity**
so that I can verify what bronze or silver produced before running the next layer.

```bash
kindling entity show bronze.myproject_raw --env dev --limit 20
kindling entity show silver.myproject --env dev --count
```

Expected behavior: reads the entity from its configured provider for the given env and prints a sample of rows or a row count. Useful for confirming an upstream layer produced expected output before running a downstream transform.

**As a developer, I want to check data quality after a pipeline run**
so that I can detect zero-row output, schema drift, or failed unit conversions without reading raw logs.

```bash
kindling entity validate gold.myproject --env dev
```

Expected checks: row count > 0, no unexpected nulls in key columns, schema matches registered entity definition. Reports violations as warnings or errors depending on severity.

---

## 6. Packaging and Deployment

**As a developer, I want to package my app into a `.kda` archive**
so that I have a versioned artifact I can deploy or share.

```bash
kindling app package orders
kindling app package orders --output dist/orders.kda
```

**As a developer, I want to deploy my local app directory to a remote platform**
so that the platform has access to the latest app code and config.

```bash
kindling app deploy orders --platform synapse
kindling app deploy orders --platform fabric --env prod
```

**As a developer, I want to deploy a pre-built `.kda` archive to a remote platform**
so that I can promote a pinned artifact without re-packaging from source.

```bash
kindling app deploy orders --kda-package dist/orders.kda --platform synapse --env prod
```

**As a developer, I want to see what entities and their storage paths are registered for my app**
so that I can confirm the data layer is pointing at the right locations before running.

```bash
kindling app inspect orders --entities
```

**As a developer, I want to clean up deployed app artifacts for an old app version**
so that I do not accumulate stale packages in storage.

```bash
kindling app cleanup orders
```

---

## 7. Remote Execution

**As a developer, I want to run a deployed app on a remote platform and stream its logs**
so that I can observe the run without separate log polling.

```bash
kindling app run orders --platform synapse --env dev
```

**As a developer, I want to deploy my local app and run it in one command**
so that I do not have to run `deploy` and `run` separately during rapid iteration.

For standalone (local) runs, the app is run directly without a separate deploy step:

```bash
kindling app run orders --platform standalone
```

For remote platforms, deploy first then run:

```bash
kindling app deploy orders --platform synapse
kindling app run orders --platform synapse
```

**As a developer, I want to run all pipelines in dependency order in a single command**
so that I can execute the full medallion chain (bronze → silver → gold) without manually sequencing each layer.

```bash
kindling app run orders --platform standalone --env dev
```

When no `--pipeline` is specified, `app run` executes all registered pipes in topological order based on their declared input/output entity dependencies. Bronze ingestion runs first, its output feeds silver staging, silver output feeds gold curation.

**As a developer, I want to pass runtime parameters to an app run**
so that I can vary inputs like date ranges or batch sizes without changing config files.

```bash
kindling app run orders --platform synapse --parameters params.yaml
kindling app run orders --platform synapse --param start_date=2026-01-01 --param batch_size=500
```

**As a developer, I want to start a run and get the run id immediately without waiting**
so that I can submit multiple runs in parallel from a script.

```bash
kindling app run orders --platform synapse --no-wait --json
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

## 8. Runner Registration

The `runner` group manages persistent job definitions that external orchestrators (Synapse Pipeline, Databricks Workflows, Fabric Pipeline) can trigger by name. Most developers only need `runner register` and `runner status`. For ad-hoc runs, use `kindling app run` directly — no job definition is needed.

**As a developer, I want to register my app as a named job definition**
so that an external orchestrator can trigger it by name without needing the Kindling CLI.

```bash
kindling runner register --app orders --platform synapse
```

**As a developer, I want to check the status of registered job definitions**
so that I can confirm which apps have definitions registered on the platform.

```bash
kindling runner status
kindling runner status --app orders --platform synapse
```

**As a developer, I want to delete a registered job definition**
so that I can clean up a deprecated environment.

```bash
kindling runner delete --app orders --platform synapse --yes
```

**As a developer, I want to invoke the runner with a raw parameters file**
so that I can reproduce a failure using the exact parameters a CI job used.

```bash
kindling runner invoke --params params.yaml
```

---

## 9. CI/CD Integration

**As a CI pipeline, I want to deploy an app and run it non-interactively, capturing structured output**
so that I can parse run results and fail the build on error.

```bash
kindling app run orders --platform synapse --no-wait --json > run.json
RUN_ID=$(jq -r '.run_id' run.json)
kindling app status $RUN_ID --platform synapse
```

**As a CI pipeline, I want a single command that deploys and runs, waits for completion, and exits non-zero on failure**
so that CI job configuration stays simple.

```bash
kindling app run orders --fail-on-error --platform synapse
```

**As a CI pipeline, I want to run unit and component tests and produce a report**
so that PR checks include test results without requiring platform credentials.

```bash
kindling test run --suite unit --ci
kindling test run --suite component --ci
```

**As a CI pipeline, I want to run system tests only on release, not on every push**
so that expensive platform tests are gated appropriately.

```bash
kindling test run --suite system --platform synapse
kindling test run --suite system --platform fabric
```

**As a CI pipeline, I want to package the app and upload the artifact to storage for downstream jobs**
so that deploy and run steps can reference a pinned artifact rather than re-packaging.

```bash
kindling app package orders --output dist/orders.kda
kindling app deploy orders --kda-package dist/orders.kda --platform synapse
```

**As a CI pipeline, I want to clean up deployed app artifacts after a test run**
so that CI does not leave stale data in shared dev storage.

```bash
kindling app cleanup orders --platform synapse
```

---

## 10. Debugging and Observability

**As a developer, I want to fetch the logs for a completed run**
so that I can diagnose why a run failed after the fact.

```bash
kindling app logs <run-id>
```

**As a developer, I want to inspect the full runner registration state**
so that I can tell whether a job definition is registered and at what id.

```bash
kindling runner status --app orders --platform synapse
```

**As a developer, I want to check what entities and their storage paths are registered for my app**
so that I can confirm the data layer is pointing at the right locations before running.

```bash
kindling app inspect orders --entities
```

**As a developer, I want to inspect the current contents of an intermediate entity**
so that I can verify what bronze or silver produced before running the next layer.

```bash
kindling entity show bronze.myproject_raw --env dev --limit 20
kindling entity show silver.myproject --env dev --count
```

**As a developer, I want to check data quality after a pipeline run**
so that I can detect zero-row output, schema drift, or failed unit conversions without reading raw logs.

```bash
kindling entity validate gold.myproject --env dev
```

---

## 11. App Lifecycle and Versioning

**As a developer, I want to promote an app artifact from a dev environment to prod**
so that I do not have to re-package from source for a production deploy.

```bash
kindling app deploy orders --kda-package dist/orders.kda --platform synapse --env prod
```

**As a developer, I want to know what entities are registered for a deployed app and what version of config it uses**
so that I can verify the deployment before running it in production.

```bash
kindling app inspect orders --entities --env prod
```
