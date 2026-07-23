"""System test: core AdxApiEntityProvider round-trip against a live ADX cluster.

Drives the API-based provider (provider_type ``adx-api``, azure-kusto SDKs —
no Spark connector, no JVM library) against the same cluster and database as
the connector extension's system test: ensure_destination creates a uniquely
named table from the declared schema, rows are queued-ingested with
flush_immediately, read back (by table and by KQL query), then the table is
dropped.

Credential resolution matches test_adx_provider_system.py (service principal
``sep-kindling-nonprod``): ADX_TEST_CLIENT_SECRET, then ambient
AZURE_CLIENT_SECRET when AZURE_CLIENT_ID matches, then Key Vault.

The test skips — never fails — when configuration, credentials, the kusto
SDKs, or network access are unavailable. A stopped Dev cluster drops its DNS
record, which surfaces here as the unreachable-cluster skip.
"""

import json
import logging
import os
import socket
import time
import urllib.parse
import urllib.request
import uuid
from typing import Optional
from unittest.mock import MagicMock

import pytest

from kindling.data_entities import EntityMetadata

ADX_CLUSTER = os.getenv("ADX_TEST_CLUSTER", "").rstrip("/")
ADX_DATABASE = os.getenv("ADX_TEST_DATABASE", "")
ADX_CLIENT_ID = os.getenv("ADX_TEST_CLIENT_ID") or os.getenv("AZURE_CLIENT_ID", "")
ADX_TENANT_ID = os.getenv("ADX_TEST_TENANT_ID") or os.getenv("AZURE_TENANT_ID", "")
ADX_SECRET_NAME = os.getenv("ADX_TEST_CLIENT_SECRET_NAME", "sep-kindling-nonprod-client-secret")

# Queued ingestion is asynchronous; flush_immediately shortens but does not
# eliminate the wait on the Dev (No SLA) cluster.
READ_TIMEOUT_SECONDS = int(os.getenv("ADX_TEST_READ_TIMEOUT", "420"))


def _fetch_secret_from_key_vault(vault_url: str, secret_name: str) -> Optional[str]:
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

    pytest.importorskip("azure.kusto.data", reason="azure-kusto-data not installed ([adx] extra)")
    pytest.importorskip(
        "azure.kusto.ingest", reason="azure-kusto-ingest not installed ([adx] extra)"
    )

    host = ADX_CLUSTER.replace("https://", "").split("/")[0]
    try:
        socket.gethostbyname(host)
    except OSError:
        pytest.skip(
            f"ADX cluster '{host}' is unreachable (DNS does not resolve — "
            "a stopped Dev cluster drops its DNS record)."
        )

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
        SparkSession.builder.appName("kindling-adx-api-system-test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session


def _entity(table: str, secret: str, schema) -> EntityMetadata:
    return EntityMetadata(
        entityid="system.adx_api_smoke",
        name="adx_api_smoke",
        partition_columns=[],
        merge_columns=[],
        tags={
            "provider_type": "adx-api",
            "provider.cluster": ADX_CLUSTER,
            "provider.database": ADX_DATABASE,
            "provider.table": table,
            "provider.auth": "service_principal",
            "provider.app_id": ADX_CLIENT_ID,
            "provider.app_secret": secret,
            "provider.tenant_id": ADX_TENANT_ID,
            "provider.flush_immediately": "true",
        },
        schema=schema,
    )


def test_adx_api_ensure_write_read_roundtrip(spark, adx_client_secret):
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    from kindling.entity_provider_adx import AdxApiEntityProvider

    schema = StructType(
        [
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
        ]
    )
    table = f"kindling_api_smoke_{uuid.uuid4().hex[:8]}"

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = logging.getLogger("adx-api-system-test")
    provider = AdxApiEntityProvider(logger_provider)
    entity = _entity(table, adx_client_secret, schema)
    config = provider._get_provider_config(entity)

    try:
        # ensure_destination creates the table from the declared schema —
        # queued ingestion fails permanently for tables the ingestion
        # service has never seen, so this must come first.
        provider.ensure_destination(entity)
        assert provider.check_entity_exists(entity) is True

        df = spark.createDataFrame([(1, "alpha"), (2, "beta"), (3, "gamma")], schema=schema)
        provider.write_to_entity(df, entity)

        deadline = time.time() + READ_TIMEOUT_SECONDS
        count = 0
        result = None
        while time.time() < deadline:
            result = provider.read_entity(entity)
            count = result.count()
            if count >= 3:
                break
            time.sleep(20)
        assert count == 3, f"expected 3 ingested rows within {READ_TIMEOUT_SECONDS}s, got {count}"

        rows = {(r["id"], r["name"]) for r in result.collect()}
        assert rows == {(1, "alpha"), (2, "beta"), (3, "gamma")}

        # KQL read path
        entity.tags["provider.query"] = f"{table} | where name == 'beta'"
        filtered = provider.read_entity(entity)
        assert filtered.count() == 1
        assert filtered.collect()[0]["id"] == 2
        entity.tags.pop("provider.query")
    finally:
        try:
            client = provider._query_client(entity, config)
            client.execute_mgmt(ADX_DATABASE, f".drop table ['{table}'] ifexists")
        except Exception as cleanup_error:  # pragma: no cover - best effort
            logging.getLogger("adx-api-system-test").warning(
                f"Cleanup failed for table {table}: {cleanup_error}"
            )
