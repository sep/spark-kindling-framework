"""Shared fixtures for the sales-ops local project tests.

Fixture scope:
  spark_local    — Delta-enabled local SparkSession, no Azure config (unit/component)
  spark_abfss    — Same session with fs.azure.account.key.* configured (integration)

Integration tests are skipped automatically when ABFSS_STORAGE_ACCOUNT is not set.
"""

import os
import sys
from pathlib import Path

import pytest

# Make sales_ops importable without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def _abfss_creds_available() -> bool:
    return bool(os.environ.get("ABFSS_STORAGE_ACCOUNT"))


def pytest_configure(config):
    config.addinivalue_line("markers", "unit: pure Python, no Spark")
    config.addinivalue_line("markers", "component: DI wiring tests, no ABFSS")
    config.addinivalue_line("markers", "integration: requires live Spark and ABFSS credentials")
    config.addinivalue_line(
        "markers", "requires_azure: skipped when ABFSS_STORAGE_ACCOUNT is not set"
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "requires_azure" in item.keywords and not _abfss_creds_available():
            item.add_marker(
                pytest.mark.skip(reason="ABFSS_STORAGE_ACCOUNT not set — skipping Azure test")
            )


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
    """Delta-enabled SparkSession with ABFSS credentials.

    Uses configure_spark_with_delta_pip() so that Delta JARs are resolved
    automatically via pip, and adds hadoop-azure JARs for ABFSS access.

    Reads credentials from env vars:
      ABFSS_STORAGE_ACCOUNT   storage account name
      ABFSS_STORAGE_KEY        storage account key
    """
    if not _abfss_creds_available():
        pytest.skip("ABFSS_STORAGE_ACCOUNT not set")

    account = os.environ["ABFSS_STORAGE_ACCOUNT"]
    key = os.environ["ABFSS_STORAGE_KEY"]

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("SalesOpsABFSSTests")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-azure:3.3.4,org.apache.hadoop:hadoop-azure-datalake:3.3.4",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
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

    NOT autouse — only opt in from integration tests.
    """
    from kindling.data_entities import DataEntities
    from kindling.data_pipes import DataPipes
    from kindling.injection import GlobalInjector

    GlobalInjector.reset()
    DataEntities.deregistry = None
    DataPipes.dpregistry = None
    yield
    GlobalInjector.reset()
    DataEntities.deregistry = None
    DataPipes.dpregistry = None
