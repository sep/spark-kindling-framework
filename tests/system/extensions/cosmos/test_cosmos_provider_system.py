"""System test: CosmosEntityProvider round-trip against a live Cosmos DB account.

Upserts uniquely marked documents into the configured container through the
provider (Cosmos Spark connector), reads them back with a Cosmos SQL query,
verifies upsert-by-id semantics by overwriting one document, then deletes the
test documents via the connector's ItemDelete strategy.

Target resources come from env vars (see .env.sep in local dev):
``COSMOS_TEST_ACCOUNT_ENDPOINT``, ``COSMOS_TEST_DATABASE``,
``COSMOS_TEST_CONTAINER``. The service principal defaults to the ambient
``AZURE_CLIENT_ID``/``AZURE_TENANT_ID``/``AZURE_CLIENT_SECRET`` (overridable
via ``COSMOS_TEST_CLIENT_ID`` etc.); the SP needs a Cosmos **data-plane**
RBAC role (e.g. Cosmos DB Built-in Data Contributor). The test skips — never
fails — when configuration or credentials are unavailable.

Requirements: Spark 3.5 / Scala 2.12, outbound HTTPS to the public Cosmos
endpoint, and the Cosmos Spark connector (downloaded via
``spark.jars.packages`` on first run).
"""

import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

import pytest

from kindling.data_entities import EntityMetadata

EXTENSION_PACKAGE_ROOT = Path(__file__).resolve().parents[4] / "packages" / "kindling_cosmos"

COSMOS_ENDPOINT = os.getenv("COSMOS_TEST_ACCOUNT_ENDPOINT", "").strip()
COSMOS_DATABASE = os.getenv("COSMOS_TEST_DATABASE", "").strip()
COSMOS_CONTAINER = os.getenv("COSMOS_TEST_CONTAINER", "").strip()
COSMOS_CLIENT_ID = os.getenv("COSMOS_TEST_CLIENT_ID") or os.getenv("AZURE_CLIENT_ID", "")
COSMOS_TENANT_ID = os.getenv("COSMOS_TEST_TENANT_ID") or os.getenv("AZURE_TENANT_ID", "")
COSMOS_SUBSCRIPTION_ID = os.getenv("COSMOS_TEST_SUBSCRIPTION_ID") or os.getenv(
    "AZURE_SUBSCRIPTION_ID", ""
)
COSMOS_RESOURCE_GROUP = os.getenv("COSMOS_TEST_RESOURCE_GROUP", "")
COSMOS_SPARK_PACKAGE = "com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.37.2"

# Single-region account with eventual consistency: reads usually see writes
# within seconds; poll defensively.
READ_TIMEOUT_SECONDS = int(os.getenv("COSMOS_TEST_READ_TIMEOUT", "120"))


def _import_provider_class():
    """Import CosmosEntityProvider without triggering DI registration.

    ``kindling_cosmos.__init__`` calls ``register_provider()`` on import,
    which resolves the real EntityProviderRegistry through GlobalInjector —
    that requires an initialized framework. This test drives the provider
    directly, so stub the injector during import (same pattern as the unit
    tests and the ADX system test).
    """
    from unittest.mock import MagicMock, patch

    for module_name in list(sys.modules):
        if module_name == "kindling_cosmos" or module_name.startswith("kindling_cosmos."):
            del sys.modules[module_name]

    with patch("kindling.injection.GlobalInjector.get", return_value=MagicMock()):
        from kindling_cosmos import CosmosEntityProvider

    return CosmosEntityProvider


class _LoggerProvider:
    def get_logger(self, name):
        return logging.getLogger(name)


def _resolve_client_secret() -> Optional[str]:
    explicit = (os.getenv("COSMOS_TEST_CLIENT_SECRET") or "").strip()
    if explicit:
        return explicit

    if (os.getenv("AZURE_CLIENT_ID") or "").strip().lower() == COSMOS_CLIENT_ID.lower():
        ambient = (os.getenv("AZURE_CLIENT_SECRET") or "").strip()
        if ambient:
            return ambient

    return None


@pytest.fixture(scope="module")
def cosmos_client_secret():
    missing = [
        name
        for name, value in {
            "COSMOS_TEST_ACCOUNT_ENDPOINT": COSMOS_ENDPOINT,
            "COSMOS_TEST_DATABASE": COSMOS_DATABASE,
            "COSMOS_TEST_CONTAINER": COSMOS_CONTAINER,
            "COSMOS_TEST_CLIENT_ID (or AZURE_CLIENT_ID)": COSMOS_CLIENT_ID,
            "COSMOS_TEST_TENANT_ID (or AZURE_TENANT_ID)": COSMOS_TENANT_ID,
            "COSMOS_TEST_SUBSCRIPTION_ID (or AZURE_SUBSCRIPTION_ID)": COSMOS_SUBSCRIPTION_ID,
            "COSMOS_TEST_RESOURCE_GROUP": COSMOS_RESOURCE_GROUP,
        }.items()
        if not value
    ]
    if missing:
        pytest.skip(f"Cosmos test resource not configured (see .env.sep): {', '.join(missing)}")

    secret = _resolve_client_secret()
    if not secret:
        pytest.skip(
            "Cosmos service principal secret not available. Set COSMOS_TEST_CLIENT_SECRET "
            "or AZURE_CLIENT_ID/AZURE_CLIENT_SECRET for the shared service principal."
        )
    return secret


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("pyspark not available")

    session = (
        SparkSession.builder.appName("kindling-cosmos-system-test")
        .master("local[2]")
        .config("spark.jars.packages", COSMOS_SPARK_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


def _entity(client_secret: str, extra_tags: Optional[dict] = None) -> EntityMetadata:
    tags = {
        "provider_type": "cosmos",
        "provider.auth": "service_principal",
        "provider.account_endpoint": COSMOS_ENDPOINT,
        "provider.database": COSMOS_DATABASE,
        "provider.container": COSMOS_CONTAINER,
        "provider.client_id": COSMOS_CLIENT_ID,
        "provider.tenant_id": COSMOS_TENANT_ID,
        "provider.subscription_id": COSMOS_SUBSCRIPTION_ID,
        "provider.resource_group": COSMOS_RESOURCE_GROUP,
        "provider.client_secret": client_secret,
        **(extra_tags or {}),
    }
    return EntityMetadata(
        entityid=f"systest.{COSMOS_CONTAINER}",
        name=COSMOS_CONTAINER,
        partition_columns=[],
        merge_columns=["id"],
        tags=tags,
        schema=None,
    )


def _query_entity(client_secret: str, run_id: str) -> EntityMetadata:
    return _entity(
        client_secret,
        {
            "provider.query": (
                "SELECT c.id, c.name, c.amount, c.run_id FROM c " f"WHERE c.run_id = '{run_id}'"
            )
        },
    )


def _poll_for_rows(provider, entity, expected_count: int):
    """Poll read_entity until the expected documents are visible."""
    deadline = time.time() + READ_TIMEOUT_SECONDS
    last_count = -1
    while time.time() < deadline:
        df = provider.read_entity(entity)
        last_count = df.count()
        if last_count >= expected_count:
            return df
        time.sleep(5)
    pytest.fail(
        f"Expected {expected_count} documents in Cosmos within {READ_TIMEOUT_SECONDS}s, "
        f"last saw {last_count}"
    )


@pytest.mark.system
@pytest.mark.azure
@pytest.mark.slow
def test_cosmos_write_read_upsert_roundtrip(spark, cosmos_client_secret):
    CosmosEntityProvider = _import_provider_class()

    run_id = f"kindling-systest-{uuid.uuid4().hex[:8]}"
    provider = CosmosEntityProvider(_LoggerProvider())

    columns = ["id", "name", "amount", "run_id"]
    rows = [
        (f"{run_id}-1", "alpha", 10.5, run_id),
        (f"{run_id}-2", "bravo", 20.0, run_id),
        (f"{run_id}-3", "charlie", 30.25, run_id),
    ]
    df = spark.createDataFrame(rows, columns)

    try:
        # Upsert documents through the provider.
        provider.write_to_entity(df, _entity(cosmos_client_secret))

        # Read back via Cosmos SQL query scoped to this run.
        result = _poll_for_rows(provider, _query_entity(cosmos_client_secret, run_id), 3)
        by_id = {row["id"]: row for row in result.collect()}
        assert set(by_id) == {f"{run_id}-1", f"{run_id}-2", f"{run_id}-3"}
        assert by_id[f"{run_id}-2"]["name"] == "bravo"
        assert by_id[f"{run_id}-3"]["amount"] == pytest.approx(30.25)

        # Upsert semantics: rewriting an existing id updates, never duplicates.
        updated = spark.createDataFrame([(f"{run_id}-2", "bravo", 99.75, run_id)], columns)
        provider.write_to_entity(updated, _entity(cosmos_client_secret))

        deadline = time.time() + READ_TIMEOUT_SECONDS
        while time.time() < deadline:
            rows_now = provider.read_entity(_query_entity(cosmos_client_secret, run_id)).collect()
            amounts = {row["id"]: row["amount"] for row in rows_now}
            if len(rows_now) == 3 and amounts.get(f"{run_id}-2") == pytest.approx(99.75):
                break
            time.sleep(5)
        else:
            pytest.fail(f"Upsert of {run_id}-2 not visible within {READ_TIMEOUT_SECONDS}s")
    finally:
        # Delete the test documents (partition key is /id, so id suffices).
        try:
            cleanup = spark.createDataFrame(rows, columns)
            provider.write_to_entity(
                cleanup,
                _entity(cosmos_client_secret, {"provider.write_strategy": "ItemDelete"}),
            )
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).warning(
                "Failed to delete Cosmos test documents for run %s — clean up manually", run_id
            )
