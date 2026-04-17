"""Scaffold templates and generator for `kindling new <project>`."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass
class ScaffoldConfig:
    name: str  # raw name supplied by user, e.g. "my-project" or "my_project"
    layers: str = "medallion"  # "medallion" | "minimal"
    auth: str = "oauth"  # "oauth" | "key" | "cli"
    integration: bool = True
    output_dir: Path = field(default_factory=Path.cwd)

    @property
    def snake_name(self) -> str:
        return _to_snake(self.name)

    @property
    def kebab_name(self) -> str:
        return self.snake_name.replace("_", "-")


def _to_snake(name: str) -> str:
    """Normalize a project name to a valid Python identifier (snake_case)."""
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    if not s or s[0].isdigit():
        raise ValueError(f"Project name {name!r} cannot be converted to a valid Python identifier.")
    return s


def validate_name(name: str) -> str:
    """Return the snake_case form of *name* or raise ValueError."""
    return _to_snake(name)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_project(cfg: ScaffoldConfig) -> List[Path]:
    """Write scaffolded project to *cfg.output_dir / cfg.snake_name*.

    Returns the list of files created.
    """
    root = cfg.output_dir / cfg.snake_name
    root.mkdir(parents=True, exist_ok=False)

    pkg = cfg.snake_name
    files: List[Path] = []

    def _write(rel: str, content: str) -> Path:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        files.append(p)
        return p

    # -- Package source -------------------------------------------------------
    _write("src/__init__.py", "")
    _write(f"src/{pkg}/__init__.py", "")
    _write(f"src/{pkg}/app.py", _app(cfg))

    _write(f"src/{pkg}/entities/__init__.py", "")
    for fname, content in _entity_files(cfg):
        _write(f"src/{pkg}/entities/{fname}", content)

    _write(f"src/{pkg}/pipes/__init__.py", "")
    for fname, content in _pipe_files(cfg):
        _write(f"src/{pkg}/pipes/{fname}", content)

    _write(f"src/{pkg}/transforms/__init__.py", "")
    for fname, content in _transform_files(cfg):
        _write(f"src/{pkg}/transforms/{fname}", content)

    # -- Config ---------------------------------------------------------------
    _write("config/settings.yaml", _settings_yaml(cfg))
    _write("config/env.local.yaml", _env_local_yaml(cfg))

    # -- Tests ----------------------------------------------------------------
    _write("tests/__init__.py", "")
    _write("tests/conftest.py", _conftest(cfg))

    _write("tests/unit/__init__.py", "")
    _write("tests/unit/test_transforms.py", _unit_tests(cfg))

    _write("tests/component/__init__.py", "")
    _write("tests/component/test_registration.py", _component_tests(cfg))

    if cfg.integration:
        _write("tests/integration/__init__.py", "")
        _write("tests/integration/test_pipeline.py", _integration_tests(cfg))

    # -- Project metadata -----------------------------------------------------
    _write("pyproject.toml", _pyproject(cfg))
    _write(".env.example", _env_example(cfg))
    _write(".gitignore", _gitignore())
    _write(".devcontainer/Dockerfile", _devcontainer_dockerfile(cfg))
    _write(".devcontainer/devcontainer.json", _devcontainer_json(cfg))
    _write(".devcontainer/docker-compose.yml", _devcontainer_compose(cfg))

    return files


# ---------------------------------------------------------------------------
# Per-layer source templates
# ---------------------------------------------------------------------------


def _entity_files(cfg: ScaffoldConfig) -> List[tuple[str, str]]:
    if cfg.layers == "medallion":
        return _medallion_entities(cfg)
    return _minimal_entities(cfg)


def _pipe_files(cfg: ScaffoldConfig) -> List[tuple[str, str]]:
    if cfg.layers == "medallion":
        return _medallion_pipes(cfg)
    return _minimal_pipes(cfg)


def _transform_files(cfg: ScaffoldConfig) -> List[tuple[str, str]]:
    if cfg.layers == "medallion":
        return _medallion_transforms(cfg)
    return _minimal_transforms(cfg)


# -- medallion ---------------------------------------------------------------


def _medallion_entities(cfg: ScaffoldConfig) -> List[tuple[str, str]]:
    pkg = cfg.snake_name
    content = f'''"""Entity registrations for {cfg.kebab_name}.

Each DataEntities.entity() call fires at import time as a side effect.
These are imported by {pkg}.app.register_all().
"""

from kindling.data_entities import DataEntities
from pyspark.sql.types import StringType, StructField, StructType

_bronze_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("date", StringType(), True),
        StructField("value", StringType(), True),
    ]
)

_silver_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("date", StringType(), False),
        StructField("value", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)

DataEntities.entity(
    entityid="bronze.records",
    name="bronze_records",
    schema=_bronze_schema,
    merge_columns=["id"],
    partition_columns=["date"],
    tags={{"layer": "bronze"}},
)

DataEntities.entity(
    entityid="silver.records",
    name="silver_records",
    schema=_silver_schema,
    merge_columns=["id"],
    partition_columns=["date"],
    tags={{"layer": "silver"}},
)
'''
    return [("records.py", content)]


def _medallion_pipes(cfg: ScaffoldConfig) -> List[tuple[str, str]]:
    pkg = cfg.snake_name
    content = f'''"""Pipe definitions for {cfg.kebab_name}.

Each @DataPipes.pipe decorator fires at import time.
Import order matters: initialize_framework() must run before this module
is imported (see {pkg}.app.register_all).
"""

from kindling.data_pipes import DataPipes


@DataPipes.pipe(
    pipeid="bronze_to_silver",
    name="Bronze to Silver",
    tags={{"layer": "silver"}},
    input_entity_ids=["bronze.records"],
    output_entity_id="silver.records",
    output_type="table",
)
def bronze_to_silver(bronze_df, spark):
    ""\"Promote bronze records to the silver layer.\"\"\"
    from {pkg}.transforms.quality import clean_records

    return clean_records(bronze_df)
'''
    return [("bronze_to_silver.py", content)]


def _medallion_transforms(cfg: ScaffoldConfig) -> List[tuple[str, str]]:
    content = '''"""Pure-PySpark transformation functions.

No Kindling imports — these can be unit-tested with a plain local SparkSession.
"""

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def drop_null_keys(df: DataFrame) -> DataFrame:
    """Drop rows where the primary key column is null."""
    return df.filter(F.col("id").isNotNull())


def add_ingested_at(df: DataFrame) -> DataFrame:
    """Stamp the current date into an ingested_at column."""
    return df.withColumn("ingested_at", F.lit(str(date.today())))


def clean_records(df: DataFrame) -> DataFrame:
    """Apply all quality transforms in order."""
    df = drop_null_keys(df)
    df = add_ingested_at(df)
    return df
'''
    return [("quality.py", content)]


# -- minimal -----------------------------------------------------------------


def _minimal_entities(cfg: ScaffoldConfig) -> List[tuple[str, str]]:
    pkg = cfg.snake_name
    content = f'''"""Entity registrations for {cfg.kebab_name}."""

from kindling.data_entities import DataEntities
from pyspark.sql.types import StringType, StructField, StructType

_raw_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("value", StringType(), True),
    ]
)

DataEntities.entity(
    entityid="raw.records",
    name="raw_records",
    schema=_raw_schema,
    merge_columns=["id"],
    tags={{}},
)
'''
    return [("records.py", content)]


def _minimal_pipes(cfg: ScaffoldConfig) -> List[tuple[str, str]]:
    pkg = cfg.snake_name
    content = f'''"""Pipe definitions for {cfg.kebab_name}."""

from kindling.data_pipes import DataPipes


@DataPipes.pipe(
    pipeid="process_records",
    name="Process Records",
    tags={{}},
    input_entity_ids=["raw.records"],
    output_entity_id="raw.records",
    output_type="table",
)
def process_records(raw_df, spark):
    from {pkg}.transforms.quality import clean_records

    return clean_records(raw_df)
'''
    return [("process.py", content)]


def _minimal_transforms(cfg: ScaffoldConfig) -> List[tuple[str, str]]:
    content = '''"""Pure-PySpark transformation functions."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def drop_null_keys(df: DataFrame) -> DataFrame:
    return df.filter(F.col("id").isNotNull())


def clean_records(df: DataFrame) -> DataFrame:
    return drop_null_keys(df)
'''
    return [("quality.py", content)]


# ---------------------------------------------------------------------------
# Always-generated files
# ---------------------------------------------------------------------------


def _app(cfg: ScaffoldConfig) -> str:
    pkg = cfg.snake_name
    kebab = cfg.kebab_name

    if cfg.layers == "medallion":
        imports = f"""\
    import {pkg}.entities.records  # noqa: F401
    import {pkg}.pipes.bronze_to_silver  # noqa: F401"""
    else:
        imports = f"""\
    import {pkg}.entities.records  # noqa: F401
    import {pkg}.pipes.process  # noqa: F401"""

    return f'''"""Application entrypoint for {kebab}.

Responsibilities:
  - Locate config files relative to this package
  - Import entity/pipe modules to trigger registration side-effects
  - Expose initialize() for use by tests, notebooks, and deployed jobs

Local usage:
    from {pkg}.app import initialize
    svc = initialize()   # reads config/env.local.yaml by default

Notebook/job usage (after installing the wheel):
    from {pkg}.app import initialize
    svc = initialize(env="prod")
"""

import os
from pathlib import Path


def _config_dir() -> Path:
    return Path(__file__).parent.parent.parent / "config"


def _config_files(env: str) -> list[str]:
    config_dir = _config_dir()
    files = [str(config_dir / "settings.yaml")]
    env_file = config_dir / f"env.{{env}}.yaml"
    if env_file.exists():
        files.append(str(env_file))
    return files


def register_all() -> None:
    """Import all entity and pipe modules to trigger @DataEntities / @DataPipes registration."""
{imports}


def initialize(env: str | None = None):
    """Initialize the Kindling framework for standalone (local) execution.

    Args:
        env: Environment overlay name. Defaults to the KINDLING_ENV env var,
             falling back to "local".

    Returns:
        The initialized StandaloneService instance.
    """
    from kindling.bootstrap import initialize_framework

    if env is None:
        env = os.environ.get("KINDLING_ENV", "local")

    # initialize_framework must come first: it wires ConfigService into the DI
    # container. The @DataPipes.pipe decorator (fired on module import inside
    # register_all) resolves DataPipesManager from the injector, which
    # transitively needs ConfigService to be live.
    svc = initialize_framework(
        {{
            "platform": "standalone",
            "environment": env,
            "config_files": _config_files(env),
        }}
    )

    register_all()

    return svc
'''


def _settings_yaml(cfg: ScaffoldConfig) -> str:
    return f"""\
kindling:
  delta:
    # storage mode: use explicit provider.path tags rather than Spark catalog
    access_mode: storage
    optimize_write: false

  telemetry:
    logging:
      level: INFO
      print: true

  # Disable workspace package scanning — consumer code is imported explicitly
  bootstrap:
    load_local: false
"""


def _env_local_yaml(cfg: ScaffoldConfig) -> str:
    """entity_tags must be at the top level (no 'default:' wrapper) for Dynaconf."""
    if cfg.layers == "medallion":
        entity_block = """\
entity_tags:
  bronze.records:
    provider.path: "@secret:ABFSS_BRONZE_PATH"
  silver.records:
    provider.path: "@secret:ABFSS_SILVER_PATH"
"""
        env_vars = ("ABFSS_BRONZE_PATH", "ABFSS_SILVER_PATH")
    else:
        entity_block = """\
entity_tags:
  raw.records:
    provider.path: "@secret:ABFSS_RAW_PATH"
"""
        env_vars = ("ABFSS_RAW_PATH",)

    cred_comment = _auth_env_var_comment(cfg.auth)
    path_vars = "\n".join(
        f"#   {v}=abfss://<container>@<account>.dfs.core.windows.net/<path>" for v in env_vars
    )

    return f"""\
# Local development overlay — ABFSS paths resolved from environment variables.
#
# Credential env vars ({cfg.auth} auth):
{cred_comment}
#
# Path env vars:
{path_vars}

{entity_block}"""


def _auth_env_var_comment(auth: str) -> str:
    if auth == "oauth":
        return """\
#   AZURE_STORAGE_ACCOUNT   storage account name
#   AZURE_TENANT_ID          AAD tenant ID
#   AZURE_CLIENT_ID          service principal app ID
#   AZURE_CLIENT_SECRET      service principal secret"""
    if auth == "key":
        return """\
#   AZURE_STORAGE_ACCOUNT   storage account name
#   AZURE_STORAGE_KEY        storage account key"""
    # cli
    return """\
#   AZURE_STORAGE_ACCOUNT   storage account name
#   (run `az login` before running tests)"""


def _pyproject(cfg: ScaffoldConfig) -> str:
    pkg = cfg.snake_name
    kebab = cfg.kebab_name

    marker_lines = [
        '    "unit: pure Python, no Spark",',
        '    "component: DI wiring, no ABFSS",',
    ]
    if cfg.integration:
        marker_lines += [
            '    "integration: requires Spark and ABFSS credentials",',
            '    "requires_azure: skipped when Azure credentials are not set",',
        ]

    markers = "\n".join(marker_lines)

    return f"""\
[build-system]
requires = ["poetry-core>=1.0.0"]
build-backend = "poetry.core.masonry.api"

[tool.poetry]
name = "{kebab}"
version = "0.1.0"
description = "Kindling data pipeline project — {kebab}"
packages = [{{include = "{pkg}", from = "src"}}]

[tool.poetry.dependencies]
python = "^3.10"
# kindling is installed separately — either from the kindling-local wheel
# or via `pip install -e /path/to/kindling` for source iteration.
# The manifest does not pin the install path so both workflows are identical.
kindling = ">=0.8.14"
pyspark = ">=3.4.0"
delta-spark = ">=2.4.0"

[tool.poetry.group.dev.dependencies]
pytest = ">=7.0.0"
pytest-cov = ">=4.0.0"

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
markers = [
{markers}
]
"""


# ---------------------------------------------------------------------------
# Test templates
# ---------------------------------------------------------------------------


def _conftest(cfg: ScaffoldConfig) -> str:
    pkg = cfg.snake_name
    creds_fn, spark_abfss_fixture = _auth_fixtures(cfg)

    integration_fixtures = (
        f"""

{spark_abfss_fixture}


# ---------------------------------------------------------------------------
# Kindling registry isolation
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_kindling():
    \"\"\"Reset the Kindling DI graph between integration tests.

    initialize_framework() is idempotent (it returns early if already
    initialized). Use this fixture when a test needs a fresh bootstrap.
    \"\"\"
    import sys

    from kindling.data_entities import DataEntities
    from kindling.data_pipes import DataPipes
    from kindling.injection import GlobalInjector

    _pkg_modules = [k for k in sys.modules if k.startswith("{pkg}")]

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
    _reset()"""
        if cfg.integration
        else ""
    )

    requires_azure_skip = (
        """

def pytest_collection_modifyitems(config, items):
    for item in items:
        if "requires_azure" in item.keywords and not _azure_creds_available():
            item.add_marker(pytest.mark.skip(reason="Azure credentials not set"))"""
        if cfg.integration
        else ""
    )

    return f"""\
\"\"\"Shared test fixtures for {cfg.kebab_name}.\"\"\"

import os
import sys
from pathlib import Path

import pytest

# Make {pkg} importable without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


{creds_fn}


def pytest_configure(config):
    config.addinivalue_line("markers", "unit: pure Python, no Spark")
    config.addinivalue_line("markers", "component: DI wiring tests, no ABFSS")
    config.addinivalue_line("markers", "integration: requires live Spark and ABFSS")
    config.addinivalue_line("markers", "requires_azure: skipped when Azure creds are absent"){requires_azure_skip}


# ---------------------------------------------------------------------------
# Spark fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark_local():
    \"\"\"Plain local SparkSession — no Delta catalog, no Azure.

    Unit tests only need standard DataFrame operations. Configuring the Delta
    catalog extension without Delta JARs causes Spark to fail on any query,
    so we omit it here. Use spark_abfss for tests that need Delta reads/writes.
    \"\"\"
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("{cfg.kebab_name}UnitTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
{integration_fixtures}
"""


def _auth_fixtures(cfg: ScaffoldConfig) -> tuple[str, str]:
    """Return (creds_check_function_source, spark_abfss_fixture_source)."""
    if cfg.auth == "oauth":
        return _oauth_fixtures(cfg)
    if cfg.auth == "key":
        return _key_fixtures(cfg)
    return _cli_fixtures(cfg)


def _oauth_fixtures(cfg: ScaffoldConfig) -> tuple[str, str]:
    creds_fn = """\
def _azure_creds_available() -> bool:
    \"\"\"True when the Azure SP creds needed for ABFSS access are all present.\"\"\"
    return all(
        os.environ.get(v)
        for v in (
            "AZURE_STORAGE_ACCOUNT",
            "AZURE_TENANT_ID",
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
        )
    )"""

    spark_fixture = """\
@pytest.fixture(scope="session")
def spark_abfss():
    \"\"\"Delta-enabled SparkSession authenticated via Azure service principal (OAuth).

    Required env vars:
      AZURE_STORAGE_ACCOUNT   storage account name
      AZURE_TENANT_ID          AAD tenant ID
      AZURE_CLIENT_ID          service principal app ID
      AZURE_CLIENT_SECRET      service principal secret
    \"\"\"
    if not _azure_creds_available():
        pytest.skip("Azure SP creds not set")

    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    tenant = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]
    endpoint = f"{account}.dfs.core.windows.net"
    token_endpoint = f"https://login.microsoftonline.com/{tenant}/oauth2/token"

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    # hadoop-azure JARs are not on the classpath by default.
    # Pre-download them and reference via spark.jars to avoid Maven resolution issues.
    # See README for download instructions.
    jar_dir = "/tmp/hadoop-jars"
    extra_jars = ",".join(
        [
            f"{jar_dir}/hadoop-azure-3.3.4.jar",
            f"{jar_dir}/hadoop-azure-datalake-3.3.4.jar",
            f"{jar_dir}/azure-storage-8.6.6.jar",
            f"{jar_dir}/wildfly-openssl-1.1.3.Final.jar",
            f"{jar_dir}/jetty-util-ajax-9.4.51.v20230217.jar",
        ]
    )

    builder = (
        SparkSession.builder.appName("{cfg.kebab_name}ABFSSTests")
        .master("local[2]")
        .config("spark.jars", extra_jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(f"fs.azure.account.auth.type.{endpoint}", "OAuth")
        .config(
            f"fs.azure.account.oauth.provider.type.{endpoint}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        .config(f"fs.azure.account.oauth2.client.id.{endpoint}", client_id)
        .config(f"fs.azure.account.oauth2.client.secret.{endpoint}", client_secret)
        .config(f"fs.azure.account.oauth2.client.endpoint.{endpoint}", token_endpoint)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()"""

    return creds_fn, spark_fixture


def _key_fixtures(cfg: ScaffoldConfig) -> tuple[str, str]:
    creds_fn = """\
def _azure_creds_available() -> bool:
    \"\"\"True when the storage account name and key are both present.\"\"\"
    return bool(os.environ.get("AZURE_STORAGE_ACCOUNT") and os.environ.get("AZURE_STORAGE_KEY"))"""

    spark_fixture = """\
@pytest.fixture(scope="session")
def spark_abfss():
    \"\"\"Delta-enabled SparkSession authenticated via storage account key.

    Required env vars:
      AZURE_STORAGE_ACCOUNT   storage account name
      AZURE_STORAGE_KEY        storage account key
    \"\"\"
    if not _azure_creds_available():
        pytest.skip("Azure storage key creds not set")

    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    key = os.environ["AZURE_STORAGE_KEY"]
    endpoint = f"{account}.dfs.core.windows.net"

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    jar_dir = "/tmp/hadoop-jars"
    extra_jars = ",".join(
        [
            f"{jar_dir}/hadoop-azure-3.3.4.jar",
            f"{jar_dir}/hadoop-azure-datalake-3.3.4.jar",
            f"{jar_dir}/azure-storage-8.6.6.jar",
            f"{jar_dir}/wildfly-openssl-1.1.3.Final.jar",
            f"{jar_dir}/jetty-util-ajax-9.4.51.v20230217.jar",
        ]
    )

    builder = (
        SparkSession.builder.appName("{cfg.kebab_name}ABFSSTests")
        .master("local[2]")
        .config("spark.jars", extra_jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(f"fs.azure.account.key.{endpoint}", key)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()"""

    return creds_fn, spark_fixture


def _cli_fixtures(cfg: ScaffoldConfig) -> tuple[str, str]:
    creds_fn = """\
def _azure_creds_available() -> bool:
    \"\"\"True when a storage account name is set (assumes `az login` was run).\"\"\"
    return bool(os.environ.get("AZURE_STORAGE_ACCOUNT"))"""

    spark_fixture = """\
@pytest.fixture(scope="session")
def spark_abfss():
    \"\"\"Delta-enabled SparkSession authenticated via DefaultAzureCredential (az login).

    Required env vars:
      AZURE_STORAGE_ACCOUNT   storage account name

    Run `az login` before executing integration tests.
    \"\"\"
    if not _azure_creds_available():
        pytest.skip("AZURE_STORAGE_ACCOUNT not set")

    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    endpoint = f"{account}.dfs.core.windows.net"

    from azure.identity import DefaultAzureCredential
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default").token

    jar_dir = "/tmp/hadoop-jars"
    extra_jars = ",".join(
        [
            f"{jar_dir}/hadoop-azure-3.3.4.jar",
            f"{jar_dir}/hadoop-azure-datalake-3.3.4.jar",
            f"{jar_dir}/azure-storage-8.6.6.jar",
            f"{jar_dir}/wildfly-openssl-1.1.3.Final.jar",
            f"{jar_dir}/jetty-util-ajax-9.4.51.v20230217.jar",
        ]
    )

    builder = (
        SparkSession.builder.appName("{cfg.kebab_name}ABFSSTests")
        .master("local[2]")
        .config("spark.jars", extra_jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(f"fs.azure.account.auth.type.{endpoint}", "OAuth")
        .config(
            f"fs.azure.account.oauth.provider.type.{endpoint}",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        )
        .config(f"fs.azure.account.oauth2.client.endpoint.{endpoint}", token)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()"""

    return creds_fn, spark_fixture


# ---------------------------------------------------------------------------
# Test content templates
# ---------------------------------------------------------------------------


def _unit_tests(cfg: ScaffoldConfig) -> str:
    pkg = cfg.snake_name
    if cfg.layers == "medallion":
        return f'''\
"""Unit tests for {cfg.kebab_name} transform functions.

These tests use a plain local SparkSession — no Delta catalog, no Azure.
"""

import pytest
from pyspark.sql import Row

pytestmark = pytest.mark.unit


def test_drop_null_keys_removes_null_rows(spark_local):
    from {pkg}.transforms.quality import drop_null_keys

    df = spark_local.createDataFrame([Row(id="a"), Row(id=None), Row(id="b")])
    result = drop_null_keys(df).collect()

    assert len(result) == 2
    assert all(r.id is not None for r in result)


def test_drop_null_keys_preserves_all_valid_rows(spark_local):
    from {pkg}.transforms.quality import drop_null_keys

    df = spark_local.createDataFrame([Row(id="x"), Row(id="y")])
    assert drop_null_keys(df).count() == 2


def test_add_ingested_at_adds_column(spark_local):
    from {pkg}.transforms.quality import add_ingested_at

    df = spark_local.createDataFrame([Row(id="a")])
    result = add_ingested_at(df)

    assert "ingested_at" in result.columns
    assert result.collect()[0]["ingested_at"] is not None


def test_clean_records_end_to_end(spark_local):
    from {pkg}.transforms.quality import clean_records

    df = spark_local.createDataFrame([Row(id="a"), Row(id=None)])
    result = clean_records(df)

    assert result.count() == 1
    assert "ingested_at" in result.columns
'''
    else:
        return f'''\
"""Unit tests for {cfg.kebab_name} transform functions."""

import pytest
from pyspark.sql import Row

pytestmark = pytest.mark.unit


def test_drop_null_keys_removes_null_rows(spark_local):
    from {pkg}.transforms.quality import drop_null_keys

    df = spark_local.createDataFrame([Row(id="a"), Row(id=None), Row(id="b")])
    result = drop_null_keys(df).collect()

    assert len(result) == 2
    assert all(r.id is not None for r in result)


def test_clean_records_returns_dataframe(spark_local):
    from {pkg}.transforms.quality import clean_records

    df = spark_local.createDataFrame([Row(id="x")])
    result = clean_records(df)

    assert result.count() == 1
'''


def _component_tests(cfg: ScaffoldConfig) -> str:
    pkg = cfg.snake_name

    if cfg.layers == "medallion":
        entity_ids = '"bronze.records", "silver.records"'
        pipe_id = '"bronze_to_silver"'
        pipe_inputs = '["bronze.records"]'
        pipe_output = '"silver.records"'
    else:
        entity_ids = '"raw.records"'
        pipe_id = '"process_records"'
        pipe_inputs = '["raw.records"]'
        pipe_output = '"raw.records"'

    return f'''\
"""Component tests — Kindling DI wiring and registry correctness.

Strategy: entity/pipe registration is a module-level side effect that runs once
per process. Tests share a single injector session and verify registry state
after importing {pkg} modules. No DataFrames are read or written.
"""

import pytest


def _entity_registry():
    from kindling.data_entities import DataEntityRegistry
    from kindling.injection import GlobalInjector

    return GlobalInjector.get(DataEntityRegistry)


def _pipe_registry():
    from kindling.data_pipes import DataPipesRegistry
    from kindling.injection import GlobalInjector

    return GlobalInjector.get(DataPipesRegistry)


@pytest.fixture(scope="module", autouse=True)
def initialize_{pkg}():
    \"\"\"Bootstrap the Kindling framework and register {pkg} modules.\"\"\"
    from {pkg}.app import initialize

    initialize(env="local")


@pytest.mark.component
class TestEntityRegistration:
    def test_entity_ids_populated(self):
        ids = set(_entity_registry().get_entity_ids())

        for eid in [{entity_ids}]:
            assert eid in ids

    def test_entity_has_merge_columns(self):
        entity = _entity_registry().get_entity_definition({entity_ids.split(",")[0].strip()})

        assert entity is not None
        assert entity.merge_columns


@pytest.mark.component
class TestPipeRegistration:
    def test_pipe_is_registered(self):
        pipe = _pipe_registry().get_pipe_definition({pipe_id})

        assert pipe is not None
        assert pipe.input_entity_ids == {pipe_inputs}
        assert pipe.output_entity_id == {pipe_output}
        assert pipe.output_type == "table"

    def test_pipe_execute_is_callable(self):
        pipe = _pipe_registry().get_pipe_definition({pipe_id})

        assert callable(pipe.execute)
'''


def _integration_tests(cfg: ScaffoldConfig) -> str:
    pkg = cfg.snake_name

    if cfg.layers == "medallion":
        bronze_env = "ABFSS_BRONZE_PATH"
        silver_env = "ABFSS_SILVER_PATH"
        read_entity = "bronze.records"
        write_entity = "silver.records"
        pipe_id = "bronze_to_silver"
        source_env_var = bronze_env
        sink_env_var = silver_env
    else:
        bronze_env = "ABFSS_RAW_PATH"
        silver_env = "ABFSS_RAW_PATH"
        read_entity = "raw.records"
        write_entity = "raw.records"
        pipe_id = "process_records"
        source_env_var = bronze_env
        sink_env_var = silver_env

    return f'''\
"""Integration tests — reads from and writes to live ABFSS storage.

Skipped automatically when Azure credentials are not set.
"""

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_azure]


def _path(env_var: str) -> str:
    val = os.environ.get(env_var)
    if not val:
        pytest.skip(f"{{env_var}} not set — skipping ABFSS test")
    return val


class TestPipelineViaKindling:
    """Run the pipeline through Kindling\'s entity/pipe machinery."""

    @pytest.fixture(autouse=True)
    def _setup(self, spark_abfss):
        from {pkg}.app import initialize

        self.spark = spark_abfss
        self.svc = initialize(env="local")

    def test_read_source_entity(self):
        from kindling.data_entities import DataEntityRegistry
        from kindling.injection import GlobalInjector

        registry = GlobalInjector.get(DataEntityRegistry)
        entity = registry.get_entity_definition("{read_entity}")

        assert entity is not None

        source_path = _path("{source_env_var}")
        df = self.spark.read.format("delta").load(source_path)
        assert df.count() >= 0  # table exists and is readable

    def test_write_sink_entity(self):
        sink_path = _path("{sink_env_var}")
        df = self.spark.createDataFrame(
            [{{"id": "integration-test-001"}}]
        )
        df.write.format("delta").mode("overwrite").save(sink_path)

        result = self.spark.read.format("delta").load(sink_path)
        assert result.count() >= 1
'''


# ---------------------------------------------------------------------------
# Supporting file templates
# ---------------------------------------------------------------------------


def _env_example(cfg: ScaffoldConfig) -> str:
    """Generate .env.example listing all required environment variables."""
    if cfg.auth == "oauth":
        cred_lines = """\
AZURE_STORAGE_ACCOUNT=<storage-account-name>
AZURE_TENANT_ID=<aad-tenant-id>
AZURE_CLIENT_ID=<service-principal-app-id>
AZURE_CLIENT_SECRET=<service-principal-secret>"""
    elif cfg.auth == "key":
        cred_lines = """\
AZURE_STORAGE_ACCOUNT=<storage-account-name>
AZURE_STORAGE_KEY=<storage-account-key>"""
    else:  # cli
        cred_lines = """\
AZURE_STORAGE_ACCOUNT=<storage-account-name>
# Run `az login` before executing integration tests — no secret needed here."""

    if cfg.layers == "medallion":
        path_lines = """\
ABFSS_BRONZE_PATH=abfss://<container>@<account>.dfs.core.windows.net/bronze/records
ABFSS_SILVER_PATH=abfss://<container>@<account>.dfs.core.windows.net/silver/records"""
    else:
        path_lines = """\
ABFSS_RAW_PATH=abfss://<container>@<account>.dfs.core.windows.net/raw/records"""

    return f"""\
# Copy this file to .env and fill in your values.
# .env is git-ignored — never commit real credentials.
#
# Load with: export $(grep -v '^#' .env | xargs)
# Or use a tool like direnv or python-dotenv.

# --- Credentials ({cfg.auth} auth) ---
{cred_lines}

# --- ABFSS entity paths ---
{path_lines}

# --- Optional ---
# KINDLING_ENV=local
"""


def _gitignore() -> str:
    return """\
.env
__pycache__/
*.py[cod]
*.egg-info/
dist/
.pytest_cache/
.coverage
htmlcov/
"""


# ---------------------------------------------------------------------------
# Dev container templates
# ---------------------------------------------------------------------------

_HADOOP_JAR_BLOCK = """\
# Pre-download hadoop-azure JARs for local ABFSS integration testing.
# Stored at /opt/hadoop-jars (stable image layer); /tmp/hadoop-jars is a
# symlink so conftest.py and `kindling env check --local` find them at the
# expected path without any manual setup.
RUN mkdir -p /opt/hadoop-jars \\
    && curl -fsSL -o /opt/hadoop-jars/hadoop-azure-3.3.4.jar \\
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar \\
    && curl -fsSL -o /opt/hadoop-jars/hadoop-azure-datalake-3.3.4.jar \\
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.3.4/hadoop-azure-datalake-3.3.4.jar \\
    && curl -fsSL -o /opt/hadoop-jars/azure-storage-8.6.6.jar \\
        https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar \\
    && curl -fsSL -o /opt/hadoop-jars/wildfly-openssl-1.1.3.Final.jar \\
        https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar \\
    && curl -fsSL -o /opt/hadoop-jars/jetty-util-ajax-9.4.51.v20230217.jar \\
        https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-util-ajax/9.4.51.v20230217/jetty-util-ajax-9.4.51.v20230217.jar \\
    && ln -s /opt/hadoop-jars /tmp/hadoop-jars"""


def _devcontainer_dockerfile(cfg: ScaffoldConfig) -> str:
    return f"""\
FROM mcr.microsoft.com/devcontainers/python:3.11

# System deps: Java 21 for PySpark 3.5, curl for JAR downloads
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \\
    && apt-get -y install --no-install-recommends \\
        openjdk-21-jdk-headless \\
        curl \\
    && apt-get clean \\
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${{JAVA_HOME}}/bin:${{PATH}}"

# Python packages: Spark stack + Azure auth for ABFSS integration tests
RUN pip install --no-cache-dir \\
    pyspark==3.5.5 \\
    delta-spark==3.3.2 \\
    pandas \\
    pyarrow \\
    azure-identity \\
    azure-storage-blob \\
    azure-storage-file-datalake \\
    poetry

{_HADOOP_JAR_BLOCK}

RUN mkdir -p /spark-warehouse && chown -R vscode:vscode /spark-warehouse

WORKDIR /workspaces/{cfg.snake_name}
"""


def _devcontainer_json(cfg: ScaffoldConfig) -> str:
    pkg = cfg.snake_name
    kebab = cfg.kebab_name
    post_create = (
        f"pip install 'kindling>=0.8.14' && poetry install && "
        f"bash -lc '"
        f'grep -q "# {kebab} .env" ~/.bashrc || '
        f'{{ echo "# {kebab} .env auto-load" >> ~/.bashrc; '
        f'echo "[ -f /workspaces/{pkg}/.env ] && set -a && source /workspaces/{pkg}/.env && set +a" >> ~/.bashrc; }}'
        f"'"
    )
    post_start = (
        f'bash -c \'[ -f "/workspaces/{pkg}/.env" ] && '
        f'echo "\\u2705 .env loaded" || '
        f'echo "\\u26a0\\ufe0f  No .env \\u2014 copy .env.example to .env and fill in credentials"'
        f"'"
    )
    return f"""\
{{
    "name": "{kebab}",
    "dockerComposeFile": "docker-compose.yml",
    "service": "devcontainer",
    "workspaceFolder": "/workspaces/{pkg}",
    "customizations": {{
        "vscode": {{
            "extensions": [
                "ms-python.python",
                "ms-python.vscode-pylance",
                "redhat.vscode-yaml"
            ],
            "settings": {{
                "python.defaultInterpreterPath": "/usr/local/bin/python",
                "editor.formatOnSave": true,
                "files.trimTrailingWhitespace": true,
                "files.insertFinalNewline": true
            }}
        }}
    }},
    "postCreateCommand": "{post_create}",
    "postStartCommand": "{post_start}",
    "forwardPorts": [4040],
    "portsAttributes": {{
        "4040": {{
            "label": "Spark UI",
            "onAutoForward": "notify"
        }}
    }},
    "remoteUser": "vscode"
}}
"""


def _devcontainer_compose(cfg: ScaffoldConfig) -> str:
    pkg = cfg.snake_name
    return f"""\
services:
  devcontainer:
    build:
      context: .
      dockerfile: Dockerfile
    volumes:
      - ..:/workspaces/{pkg}:cached
      - spark-warehouse:/spark-warehouse
    command: sleep infinity
    environment:
      - PYTHONPATH=/workspaces/{pkg}/src
      - SPARK_LOCAL_IP=127.0.0.1

volumes:
  spark-warehouse:
"""
