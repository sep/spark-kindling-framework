"""System test: AdxEntityProvider round-trip against a live ADX cluster.

Creates a uniquely named table in the `kindling` database on the
sep-adx-dataint-dev cluster, writes rows through the provider (Kusto Spark
connector, queued ingestion), reads them back both by table and by KQL
query, then drops the table.

Credential resolution (service principal `sep-kindling-nonprod`), in order:

1. ``ADX_TEST_CLIENT_SECRET`` env var.
2. ``AZURE_CLIENT_SECRET`` env var, but only when ``AZURE_CLIENT_ID`` matches
   the ADX service principal's client id.
3. Key Vault: ``SYSTEM_TEST_KEY_VAULT_URL`` + secret named
   ``ADX_TEST_CLIENT_SECRET_NAME`` (default
   ``sep-kindling-nonprod-client-secret``), fetched with
   ``DefaultAzureCredential`` (az login works locally).

The test skips — never fails — when credentials or network access are
unavailable.

Requirements: Spark 3.x / Scala 2.12, outbound HTTPS to the public ADX
endpoint, and the Kusto Spark connector (downloaded via
``spark.jars.packages`` on first run).
"""

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Optional

import pytest
from kindling.data_entities import EntityMetadata


def _import_provider_class():
    """Import AdxEntityProvider without triggering DI registration.

    ``kindling_adx.__init__`` calls ``register_provider()`` on import, which
    resolves the real EntityProviderRegistry through GlobalInjector — that
    requires an initialized framework. This test drives the provider
    directly, so stub the injector during import (same pattern as the unit
    tests).
    """
    from unittest.mock import MagicMock, patch

    for module_name in list(sys.modules):
        if module_name == "kindling_adx" or module_name.startswith("kindling_adx."):
            del sys.modules[module_name]

    with patch("kindling.injection.GlobalInjector.get", return_value=MagicMock()):
        from kindling_adx import AdxEntityProvider

    return AdxEntityProvider


EXTENSION_PACKAGE_ROOT = Path(__file__).resolve().parents[4] / "packages" / "kindling_adx"

# Target resources come from env vars (see .env.sep in local dev). The
# service principal defaults to the ambient AZURE_CLIENT_ID/AZURE_TENANT_ID.
ADX_CLUSTER = os.getenv("ADX_TEST_CLUSTER", "").rstrip("/")
ADX_DATABASE = os.getenv("ADX_TEST_DATABASE", "")
ADX_CLIENT_ID = os.getenv("ADX_TEST_CLIENT_ID") or os.getenv("AZURE_CLIENT_ID", "")
ADX_TENANT_ID = os.getenv("ADX_TEST_TENANT_ID") or os.getenv("AZURE_TENANT_ID", "")
ADX_SECRET_NAME = os.getenv("ADX_TEST_CLIENT_SECRET_NAME", "sep-kindling-nonprod-client-secret")
KUSTO_SPARK_PACKAGE = "com.microsoft.azure.kusto:kusto-spark_3.0_2.12:7.0.6"

# Queued ingestion is asynchronous and the default batching policy holds rows
# for up to ~5 minutes; a full run takes ~6 minutes on the Dev(No SLA) cluster.
READ_TIMEOUT_SECONDS = int(os.getenv("ADX_TEST_READ_TIMEOUT", "420"))


class _LoggerProvider:
    def get_logger(self, name):
        return logging.getLogger(name)


def _fetch_secret_from_key_vault(vault_url: str, secret_name: str) -> Optional[str]:
    """Fetch a secret value via the Key Vault REST API using azure-identity."""
    try:
        from azure.identity import DefaultAzureCredential

        token = DefaultAzureCredential().get_token("https://vault.azure.net/.default").token
        url = f"{vault_url.rstrip('/')}/secrets/{urllib.parse.quote(secret_name)}?api-version=7.4"
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read())["value"]
    except Exception:
        return None


def _resolve_client_secret() -> Optional[str]:
    explicit = (os.getenv("ADX_TEST_CLIENT_SECRET") or "").strip()
    if explicit:
        return explicit

    if (os.getenv("AZURE_CLIENT_ID") or "").strip().lower() == ADX_CLIENT_ID.lower():
        ambient = (os.getenv("AZURE_CLIENT_SECRET") or "").strip()
        if ambient:
            return ambient

    vault_url = (os.getenv("SYSTEM_TEST_KEY_VAULT_URL") or "").strip()
    if vault_url:
        return _fetch_secret_from_key_vault(vault_url, ADX_SECRET_NAME)

    return None


def _adx_mgmt(csl: str, client_secret: str) -> None:
    """Run a Kusto management command via the cluster's REST endpoint."""
    from azure.identity import ClientSecretCredential

    credential = ClientSecretCredential(ADX_TENANT_ID, ADX_CLIENT_ID, client_secret)
    token = credential.get_token(f"{ADX_CLUSTER}/.default").token
    payload = json.dumps({"db": ADX_DATABASE, "csl": csl}).encode("utf-8")
    request = urllib.request.Request(
        f"{ADX_CLUSTER}/v1/rest/mgmt",
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=60):
        pass


@pytest.fixture(scope="module")
def adx_client_secret():
    missing = [
        name
        for name, value in {
            "ADX_TEST_CLUSTER": ADX_CLUSTER,
            "ADX_TEST_DATABASE": ADX_DATABASE,
            "ADX_TEST_CLIENT_ID (or AZURE_CLIENT_ID)": ADX_CLIENT_ID,
            "ADX_TEST_TENANT_ID (or AZURE_TENANT_ID)": ADX_TENANT_ID,
        }.items()
        if not value
    ]
    if missing:
        pytest.skip(f"ADX test resource not configured (see .env.sep): {', '.join(missing)}")

    secret = _resolve_client_secret()
    if not secret:
        pytest.skip(
            "ADX service principal secret not available. Set ADX_TEST_CLIENT_SECRET, "
            "or AZURE_CLIENT_ID/AZURE_CLIENT_SECRET for the sep-kindling-nonprod SP, "
            f"or store '{ADX_SECRET_NAME}' in SYSTEM_TEST_KEY_VAULT_URL."
        )
    return secret


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("pyspark not available")

    session = (
        SparkSession.builder.appName("kindling-adx-system-test")
        .master("local[2]")
        .config("spark.jars.packages", KUSTO_SPARK_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


def _entity(table: str, extra_tags: Optional[dict] = None) -> EntityMetadata:
    tags = {
        "provider_type": "adx",
        "provider.auth": "service_principal",
        "provider.cluster": ADX_CLUSTER,
        "provider.database": ADX_DATABASE,
        "provider.table": table,
        "provider.client_id": ADX_CLIENT_ID,
        "provider.tenant_id": ADX_TENANT_ID,
        # The connector must create the table itself: externally pre-created
        # tables fail queued ingestion (ingestion-service schema cache).
        "provider.table_create_options": "CreateIfNotExist",
        **(extra_tags or {}),
    }
    return EntityMetadata(
        entityid=f"systest.{table}",
        name=table,
        partition_columns=[],
        merge_columns=[],
        tags=tags,
        schema=None,
    )


def _recent_ingestion_failures(client_secret: str) -> str:
    """Best-effort: fetch recent ingestion failures for the failure message."""
    try:
        from azure.identity import ClientSecretCredential

        credential = ClientSecretCredential(ADX_TENANT_ID, ADX_CLIENT_ID, client_secret)
        token = credential.get_token(f"{ADX_CLUSTER}/.default").token
        payload = json.dumps(
            {"db": ADX_DATABASE, "csl": ".show ingestion failures | top 3 by FailedOn desc"}
        ).encode("utf-8")
        request = urllib.request.Request(
            f"{ADX_CLUSTER}/v1/rest/mgmt",
            data=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read().decode("utf-8")[:2000]
    except Exception as exc:  # noqa: BLE001
        return f"<could not fetch ingestion failures: {exc}>"


def _poll_for_rows(provider, entity, expected_count: int, client_secret: str):
    """Poll read_entity until queued ingestion lands or the timeout expires."""
    deadline = time.time() + READ_TIMEOUT_SECONDS
    last_count = -1
    while time.time() < deadline:
        df = provider.read_entity(entity)
        last_count = df.count()
        if last_count >= expected_count:
            return df
        time.sleep(15)
    pytest.fail(
        f"Expected {expected_count} rows in ADX within {READ_TIMEOUT_SECONDS}s, "
        f"last saw {last_count}. Recent ingestion failures: "
        f"{_recent_ingestion_failures(client_secret)}"
    )


@pytest.mark.system
@pytest.mark.azure
@pytest.mark.slow
def test_adx_create_write_read_roundtrip(spark, adx_client_secret):
    AdxEntityProvider = _import_provider_class()

    table = f"kindling_systest_{uuid.uuid4().hex[:8]}"
    provider = AdxEntityProvider(_LoggerProvider())
    secret_tags = {"provider.client_secret": adx_client_secret}

    try:
        rows = [(1, "alpha", 10.5), (2, "bravo", 20.0), (3, "charlie", 30.25)]
        df = spark.createDataFrame(rows, ["id", "name", "amount"])

        # Write through the provider. The connector creates the table itself
        # (tableCreateOptions=CreateIfNotExist from _entity) — pre-creating it
        # via the mgmt REST API makes queued ingestion fail with a permanent
        # "table could not be found" (ingestion-service schema cache does not
        # see externally created tables immediately). flushImmediately asks
        # the ingestion service to skip batch aggregation; observed runs on
        # the Dev SKU still take the ~5 minute batching window, hence the
        # generous read timeout.
        write_tags = {
            **secret_tags,
            "provider.option.sparkIngestionProperties": '{"flushImmediately":true}',
        }
        provider.write_to_entity(df, _entity(table, write_tags))

        # Read back by table.
        result = _poll_for_rows(
            provider, _entity(table, secret_tags), expected_count=3, client_secret=adx_client_secret
        )
        by_id = {row["id"]: row for row in result.collect()}
        assert set(by_id) == {1, 2, 3}
        assert by_id[2]["name"] == "bravo"
        assert by_id[3]["amount"] == pytest.approx(30.25)

        # Read back by KQL query (provider.query takes precedence over table).
        query_entity = _entity(
            table,
            {**secret_tags, "provider.query": f"{table} | where amount > 15 | project id"},
        )
        filtered_ids = {row["id"] for row in provider.read_entity(query_entity).collect()}
        assert filtered_ids == {2, 3}
    finally:
        try:
            _adx_mgmt(f".drop table {table} ifexists", adx_client_secret)
        except Exception:
            logging.getLogger(__name__).warning(
                "Failed to drop ADX test table %s — clean up manually", table
            )
