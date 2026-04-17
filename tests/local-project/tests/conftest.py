"""Shared fixtures for the sales-ops local project tests.

Fixture scope:
  spark_local    — plain local SparkSession, no Delta catalog, no Azure (unit/component)
  spark_abfss    — Delta-enabled SparkSession with Azure SP OAuth config (integration)

Integration tests are skipped automatically when AZURE_STORAGE_ACCOUNT,
AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET are not all set.
"""

import os
import sys
from pathlib import Path

import pytest

# Make sales_ops importable without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def _abfss_creds_available() -> bool:
    """True when the Azure SP creds needed for ABFSS access are all present."""
    return all(
        os.environ.get(v)
        for v in (
            "AZURE_STORAGE_ACCOUNT",
            "AZURE_TENANT_ID",
            "AZURE_CLIENT_ID",
            "AZURE_CLIENT_SECRET",
        )
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "unit: local Spark tests, no Azure or ABFSS")
    config.addinivalue_line("markers", "component: DI wiring tests, no ABFSS")
    config.addinivalue_line("markers", "integration: requires live Spark and ABFSS credentials")
    config.addinivalue_line(
        "markers",
        "requires_azure: skipped when AZURE_STORAGE_ACCOUNT / AZURE_TENANT_ID / "
        "AZURE_CLIENT_ID / AZURE_CLIENT_SECRET are not all set",
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "requires_azure" in item.keywords and not _abfss_creds_available():
            item.add_marker(pytest.mark.skip(reason="Azure SP creds not set — skipping Azure test"))


# ---------------------------------------------------------------------------
# Spark fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark_local():
    """Plain local SparkSession — no Delta catalog, no Azure.

    Unit tests only need standard DataFrame operations (filter, withColumn,
    collect). Configuring the Delta catalog extension without Delta JARs on
    the classpath causes Spark to fail on any query, so we omit it here.
    Use spark_abfss for tests that need Delta reads/writes.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("SalesOpsUnitTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def spark_abfss():
    """Delta-enabled SparkSession authenticated via Azure service principal (OAuth).

    Uses configure_spark_with_delta_pip() so Delta JARs are resolved via pip,
    and adds hadoop-azure JARs for ABFSS access. Authenticates with the same
    service principal creds present in the project .env.

    Required env vars:
      AZURE_STORAGE_ACCOUNT   storage account name
      AZURE_TENANT_ID          AAD tenant ID
      AZURE_CLIENT_ID          service principal app ID
      AZURE_CLIENT_SECRET      service principal secret
    """
    if not _abfss_creds_available():
        pytest.skip("Azure SP creds not set")

    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    tenant = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]
    client_secret = os.environ["AZURE_CLIENT_SECRET"]
    endpoint = f"{account}.dfs.core.windows.net"
    token_endpoint = f"https://login.microsoftonline.com/{tenant}/oauth2/token"

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    # hadoop-azure JARs are not on the classpath by default and Maven resolution
    # is unreliable inside this devcontainer, so we reference pre-downloaded JARs.
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
        SparkSession.builder.appName("SalesOpsABFSSTests")
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
    spark.stop()


# ---------------------------------------------------------------------------
# Kindling registry isolation
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_kindling_for_integration():
    """Full injector reset for integration tests that call initialize_framework().

    initialize_framework() checks is_framework_initialized() and bails out if
    already initialised, so each integration test that needs a fresh bootstrap
    must call this fixture to get a clean slate.

    Also evicts cached sales_ops modules from sys.modules so that their
    module-level DataEntities.entity() / @DataPipes.pipe() side effects fire
    again against the fresh injector on next import.

    NOT autouse — only opt in from integration tests.
    """
    import sys

    from kindling.data_entities import DataEntities
    from kindling.data_pipes import DataPipes
    from kindling.injection import GlobalInjector

    _sales_ops_modules = [k for k in sys.modules if k.startswith("sales_ops")]

    def _reset():
        from injector import singleton
        from kindling.spark_config import ConfigService, DynaconfConfig

        GlobalInjector.reset()
        DataEntities.deregistry = None
        DataPipes.dpregistry = None
        for mod in _sales_ops_modules:
            sys.modules.pop(mod, None)

        # Re-establish the ConfigService → DynaconfConfig binding lost by reset.
        # configure_injector_with_config() calls GlobalInjector.get(ConfigService)
        # directly; without this the injector tries to instantiate the abstract base.
        inj = GlobalInjector.get_injector()
        inj.binder.bind(ConfigService, to=DynaconfConfig, scope=singleton)
        inj.binder.bind(DynaconfConfig, to=DynaconfConfig, scope=singleton)

    _reset()
    yield
    _reset()
