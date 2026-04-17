# Local Python-First Development with Kindling

This guide covers how to build, test, and package a Kindling data pipeline project entirely in Python — no notebooks required during development.

**Background:** see `docs/proposals/local_code_first_development.md` for the design rationale.

---

## Quick start

```bash
# Scaffold a new project
kindling new my-pipeline

cd my_pipeline

# Install deps (kindling separately — see "Installing Kindling" below)
poetry install
pip install kindling  # or: pip install -e /path/to/kindling-source

# Copy and fill in credentials
cp .env.example .env
# edit .env

# Run unit and component tests (no Azure needed)
source .env
poetry run pytest tests/unit tests/component

# Run integration tests (requires Azure credentials in .env)
poetry run pytest tests/integration
```

---

## Project structure

`kindling new` generates a project with this layout — the same regardless of `--layers` or `--auth`:

```
my_pipeline/
  src/my_pipeline/
    app.py              initialize() + register_all()
    entities/           DataEntities.entity() registrations
    pipes/              @DataPipes.pipe decorators
    transforms/         pure-PySpark functions (unit-testable)
  config/
    settings.yaml       base Kindling config
    env.local.yaml      ABFSS entity paths from env vars
  .env.example          all required env vars (auth-specific)
  .gitignore
  tests/
    conftest.py         Spark fixtures (auth-flavored)
    unit/               pure Python, no Spark catalog, no Azure
    component/          DI wiring, no Azure
    integration/        live ABFSS reads/writes
  pyproject.toml
```

### `kindling new` options

| Option | Values | Effect |
|--------|--------|--------|
| `--auth` | `oauth` (default), `key`, `cli` | ABFSS auth method in conftest.py |
| `--layers` | `medallion` (default), `minimal` | Template content (bronze/silver vs. raw) |
| `--no-integration` | flag | Skip `tests/integration/` |
| `--output-dir` | path | Parent directory (default: `.`) |

All combinations produce the same directory structure and wheel layout.

---

## Installing Kindling locally

Kindling is not pinned to a path in `pyproject.toml` by design — the environment controls which version is installed:

```bash
# Option A: use the kindling-local wheel (all platforms, full runtime deps)
poetry run poe build          # from the kindling repo
pip install dist/kindling_local-*.whl

# Option B: editable install from source (fastest inner loop)
pip install -e /path/to/kindling

# Option C: install a released version
pip install "kindling>=0.8.14"
```

---

## Setting up the local Spark stack

Unit tests run with a plain `SparkSession` and need no special setup. Component tests need Kindling's DI system but not Spark at all. Integration tests need the full stack:

### 1. Java

PySpark requires Java 11 or later on `PATH`:

```bash
# Ubuntu / Debian
sudo apt-get install -y openjdk-17-jdk
java -version
```

### 2. PySpark and delta-spark

These are declared as `[tool.poetry.dependencies]` in your `pyproject.toml`:

```bash
poetry install   # installs pyspark and delta-spark automatically
```

### 3. Hadoop-Azure JARs

Maven resolution is unreliable in dev containers. Pre-download the JARs and the generated `conftest.py` will reference them via `spark.jars`:

```bash
mkdir -p /tmp/hadoop-jars
cd /tmp/hadoop-jars

curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar
curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.3.4/hadoop-azure-datalake-3.3.4.jar
curl -LO https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar
curl -LO https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar
curl -LO https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.51.v20230217/jetty-util-ajax-9.4.51.v20230217.jar
```

### Verify the full local stack

```bash
kindling env check --local --config config/settings.yaml
```

Expected output when everything is ready:

```
[PASS] python_version: 3.11.x (>=3.10)
[PASS] config_file_exists: config/settings.yaml
[PASS] kindling_section_present: root.kindling is a mapping
[PASS] java: openjdk version "17.0.x" ...
[PASS] pyspark: 3.5.x
[PASS] delta_spark: 3.x.x
[PASS] hadoop_azure_jars: /tmp/hadoop-jars
Environment check passed.
```

---

## Configuration

### `config/settings.yaml` — base config

```yaml
kindling:
  delta:
    access_mode: storage     # use explicit provider.path, not Spark catalog
    optimize_write: false

  telemetry:
    logging:
      level: INFO
      print: true

  bootstrap:
    load_local: false        # consumer code is imported explicitly
```

### `config/env.local.yaml` — local overlay

Entity paths are resolved from environment variables using `@secret:` references:

```yaml
# IMPORTANT: entity_tags must be at the TOP LEVEL — no 'default:' wrapper.
# Dynaconf's get_entity_tags() looks for a top-level key.
entity_tags:
  bronze.records:
    provider.path: "@secret:ABFSS_BRONZE_PATH"
  silver.records:
    provider.path: "@secret:ABFSS_SILVER_PATH"
```

`StandaloneService` resolves `@secret:VAR_NAME` from environment variables. Set them in `.env`:

```bash
ABFSS_BRONZE_PATH=abfss://kindling-dev@myaccount.dfs.core.windows.net/bronze/records
ABFSS_SILVER_PATH=abfss://kindling-dev@myaccount.dfs.core.windows.net/silver/records
```

---

## The three test layers

### Unit tests — pure Python

```python
# tests/unit/test_transforms.py
def test_drop_null_keys(spark_local):
    from my_pipeline.transforms.quality import drop_null_keys
    df = spark_local.createDataFrame([Row(id="a"), Row(id=None)])
    assert drop_null_keys(df).count() == 1
```

- Uses the `spark_local` fixture: plain `SparkSession`, no Delta catalog, no Azure
- No credentials needed
- Fast — Spark starts in seconds without Delta or Azure JARs

### Component tests — DI wiring

```python
# tests/component/test_registration.py
@pytest.fixture(scope="module", autouse=True)
def initialize_my_pipeline():
    from my_pipeline.app import initialize
    initialize(env="local")   # reads config/env.local.yaml

def test_pipe_is_registered():
    pipe = GlobalInjector.get(DataPipesRegistry).get_pipe_definition("bronze_to_silver")
    assert pipe.output_entity_id == "silver.records"
```

- Calls `initialize(env="local")` — bootstraps Kindling's DI with the standalone platform
- No Spark, no Azure — `@secret:` references are left unresolved (that's fine for metadata tests)
- Validates entity/pipe registration, metadata, and DI graph correctness

### Integration tests — live ABFSS

```python
# tests/integration/test_pipeline.py
@pytest.mark.integration
@pytest.mark.requires_azure
class TestPipeline:
    @pytest.fixture(autouse=True)
    def _setup(self, spark_abfss):
        from my_pipeline.app import initialize
        self.spark = spark_abfss
        self.svc = initialize(env="local")
```

- Uses `spark_abfss` fixture: Delta-enabled session with Azure auth
- Skipped automatically when Azure credentials are absent
- Reads and writes real ABFSS Delta tables

---

## Key implementation notes

### Initialization order matters

```python
# app.py — this order is required
svc = initialize_framework({"platform": "standalone", ...})
register_all()   # AFTER — @DataPipes.pipe resolves DataPipesRegistry from DI at import time
```

### Schema and tags are required

`DataEntities.entity()` requires `schema=` (a `StructType`). `@DataPipes.pipe` requires `tags=`. Both are included in the `kindling new` templates.

### `spark_local` must not configure Delta catalog

Without Delta JARs on the classpath, Spark fails on _any_ query if the Delta catalog extension is configured. Unit test fixtures omit Delta configuration entirely; only `spark_abfss` enables it.

### YAML format for `entity_tags`

Dynaconf's `get_entity_tags()` looks for a **top-level** `entity_tags` key. Do not nest it under `default:`:

```yaml
# CORRECT
entity_tags:
  bronze.records:
    provider.path: "@secret:ABFSS_BRONZE_PATH"

# WRONG — get_entity_tags() returns {}
default:
  entity_tags:
    bronze.records:
      provider.path: "@secret:ABFSS_BRONZE_PATH"
```

---

## Building and distributing the wheel

Your project is a standard Python package. Build it with Poetry:

```bash
poetry build
# produces dist/my-pipeline-0.1.0-py3-none-any.whl
```

The wheel can be installed in any environment that has Kindling available — cloud notebooks, CI pipelines, or other local environments. The build output is identical regardless of which `kindling new` options were used.

For the Kindling framework itself, use `poetry run poe build` to produce the `kindling-local` wheel (includes all platforms and full runtime deps):

```bash
# from the kindling repo root
poetry run poe build
pip install dist/kindling_local-*.whl
```
