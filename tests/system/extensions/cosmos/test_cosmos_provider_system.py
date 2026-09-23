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

Requirements: a Spark line the connector is published for (3.4/3.5 on Scala
2.12, 4.0/4.1 on Scala 2.13 -- resolved from the installed pyspark), outbound
HTTPS to the public Cosmos endpoint, and the Cosmos Spark connector
(downloaded via ``spark.jars.packages`` on first run).
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

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[4] / "packages" / "extensions" / "kindling_ext_cosmos"
)


def _env(*names: str) -> str:
    """First non-blank env var among names, stripped."""
    for name in names:
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    return ""


COSMOS_ENDPOINT = _env("COSMOS_TEST_ACCOUNT_ENDPOINT")
COSMOS_DATABASE = _env("COSMOS_TEST_DATABASE")
COSMOS_CONTAINER = _env("COSMOS_TEST_CONTAINER")
COSMOS_CLIENT_ID = _env("COSMOS_TEST_CLIENT_ID", "AZURE_CLIENT_ID")
COSMOS_TENANT_ID = _env("COSMOS_TEST_TENANT_ID", "AZURE_TENANT_ID")
COSMOS_SUBSCRIPTION_ID = _env("COSMOS_TEST_SUBSCRIPTION_ID", "AZURE_SUBSCRIPTION_ID")
COSMOS_RESOURCE_GROUP = _env("COSMOS_TEST_RESOURCE_GROUP", "AZURE_RESOURCE_GROUP")


def _cosmos_spark_package() -> str:
    """Connector coordinate for the locally installed pyspark's Spark line."""
    import pyspark

    # Called from the module-scoped spark fixture, which runs before the
    # function-scoped autouse sys.path fixture below.
    if str(EXTENSION_PACKAGE_ROOT) not in sys.path:
        sys.path.insert(0, str(EXTENSION_PACKAGE_ROOT))

    # Import under the stubbed injector: the package registers its provider
    # at import time, which needs an initialized framework otherwise.
    _import_provider_class()
    from kindling_ext_cosmos.entity_provider_cosmos import (
        resolve_cosmos_spark_connector_coordinate,
    )

    return resolve_cosmos_spark_connector_coordinate(pyspark.__version__)


# Single-region account with eventual consistency: reads usually see writes
# within seconds; poll defensively.
READ_TIMEOUT_SECONDS = int(os.getenv("COSMOS_TEST_READ_TIMEOUT", "120"))


def _import_provider_class():
    """Import CosmosEntityProvider without triggering DI registration.

    ``kindling_ext_cosmos.__init__`` calls ``register_provider()`` on import,
    which resolves the real EntityProviderRegistry through GlobalInjector —
    that requires an initialized framework. This test drives the provider
    directly, so stub the injector during import (same pattern as the unit
    tests and the ADX system test).
    """
    from unittest.mock import MagicMock, patch

    for module_name in list(sys.modules):
        if module_name == "kindling_ext_cosmos" or module_name.startswith("kindling_ext_cosmos."):
            del sys.modules[module_name]

    with patch("kindling.injection.GlobalInjector.get", return_value=MagicMock()):
        from kindling_ext_cosmos import CosmosEntityProvider

    return CosmosEntityProvider


class _LoggerProvider:
    def get_logger(self, name):
        return logging.getLogger(name)


def _resolve_client_secret() -> Optional[str]:
    explicit = _env("COSMOS_TEST_CLIENT_SECRET")
    if explicit:
        return explicit

    if COSMOS_CLIENT_ID and _env("AZURE_CLIENT_ID").lower() == COSMOS_CLIENT_ID.lower():
        ambient = _env("AZURE_CLIENT_SECRET")
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

    builder = (
        SparkSession.builder.appName("kindling-cosmos-system-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
    )
    # tests/conftest.py sets KINDLING_SPARK_ENABLE_DELTA=true, so the
    # provider's get_or_create_spark_session() applies the Delta catalog
    # confs to this (already running) session. The Delta jars must therefore
    # be on the JVM classpath alongside the Cosmos connector, or every
    # DataFrame action fails with "Cannot find catalog plugin class ...
    # DeltaCatalog". configure_spark_with_delta_pip pins the Delta artifact
    # matching the installed delta-spark (and its Scala binary).
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder, extra_packages=[_cosmos_spark_package()])
    except ImportError:
        builder = builder.config("spark.jars.packages", _cosmos_spark_package())

    session = builder.getOrCreate()
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
def test_cosmos_write_read_upsert_roundtrip(cosmos_client_secret, spark):
    # Fixture order matters: cosmos_client_secret first, so an unconfigured
    # environment skips before the spark fixture downloads the connector.
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


def _streaming_entity(client_secret: str, run_id: str) -> EntityMetadata:
    # Start the change feed at "Now" so the stream only carries this run's
    # writes and does not replay the whole container on first trigger.
    return _entity(
        client_secret,
        {
            "provider.changefeed.mode": "latest_version",
            "provider.changefeed.start_from": "Now",
            "provider.query_name": f"cosmos-changefeed-{run_id}",
        },
    )


@pytest.mark.system
@pytest.mark.azure
@pytest.mark.slow
def test_cosmos_change_feed_streaming_read(cosmos_client_secret, spark, tmp_path):
    """Write documents, then observe them arrive through the change-feed stream.

    Exercises ``read_entity_as_stream`` end to end: the change-feed source
    (``cosmos.oltp.changeFeed``) is started from ``Now``, a batch is upserted
    through the provider, and the streamed micro-batches must contain every
    document of this run. Uses ``latest_version`` mode, which any container
    supports; ``full_fidelity`` needs a container provisioned for
    all-versions-and-deletes and is covered at the option level in the unit
    tests only.
    """
    CosmosEntityProvider = _import_provider_class()
    from kindling_ext_cosmos.entity_provider_cosmos import COSMOS_CHANGEFEED_FORMAT

    run_id = f"kindling-cfsystest-{uuid.uuid4().hex[:8]}"
    provider = CosmosEntityProvider(_LoggerProvider())

    columns = ["id", "name", "amount", "run_id"]
    rows = [
        (f"{run_id}-1", "delta", 1.5, run_id),
        (f"{run_id}-2", "echo", 2.5, run_id),
        (f"{run_id}-3", "foxtrot", 3.5, run_id),
    ]
    expected_ids = {row[0] for row in rows}

    stream_df = provider.read_entity_as_stream(_streaming_entity(cosmos_client_secret, run_id))
    assert stream_df.isStreaming

    # The change-feed schema is inferred from the container; only rows of
    # this run matter, and the run_id column is present on every test document.
    filtered = stream_df.filter(stream_df["run_id"] == run_id).select("id", "name", "amount")
    memory_table = f"cosmos_cf_{run_id.replace('-', '_')}"
    query = (
        filtered.writeStream.format("memory")
        .queryName(memory_table)
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .start()
    )

    try:
        # Let the source resolve its starting continuation before writing, so
        # "Now" is strictly before the upsert.
        query.processAllAvailable()

        provider.write_to_entity(
            spark.createDataFrame(rows, columns), _entity(cosmos_client_secret)
        )

        deadline = time.time() + READ_TIMEOUT_SECONDS
        seen = {}
        while time.time() < deadline:
            query.processAllAvailable()
            seen = {
                row["id"]: row
                for row in spark.sql(f"SELECT * FROM {memory_table}").collect()
                if row["id"] in expected_ids
            }
            if set(seen) == expected_ids:
                break
            time.sleep(5)
        else:
            pytest.fail(
                f"Change feed ({COSMOS_CHANGEFEED_FORMAT}) did not deliver "
                f"{sorted(expected_ids - set(seen))} within {READ_TIMEOUT_SECONDS}s"
            )

        assert seen[f"{run_id}-2"]["name"] == "echo"
        assert float(seen[f"{run_id}-3"]["amount"]) == pytest.approx(3.5)
    finally:
        try:
            query.stop()
        except Exception:  # noqa: BLE001
            pass
        try:
            provider.write_to_entity(
                spark.createDataFrame(rows, columns),
                _entity(cosmos_client_secret, {"provider.write_strategy": "ItemDelete"}),
            )
        except Exception:  # noqa: BLE001
            logging.getLogger(__name__).warning(
                "Failed to delete Cosmos change-feed test documents for run %s", run_id
            )
