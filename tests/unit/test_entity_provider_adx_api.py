"""Unit tests for the core API-based ADX entity provider (provider_type adx-api).

The azure-kusto SDKs are not installed in the unit environment; fakes are
injected into sys.modules, which also pins the provider's lazy-import
behavior (module import and registration never require the SDKs).
"""

import sys
import types
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from kindling.data_entities import EntityMetadata
from kindling.entity_provider import can_ensure_destination, is_writable
from kindling.entity_provider_adx import AdxApiEntityProvider

BASE_TAGS = {
    "provider.cluster": "https://mycluster.westus.kusto.windows.net",
    "provider.database": "analytics",
    "provider.table": "events",
}


def _entity(tags, schema=None, entityid="boundary.events"):
    return EntityMetadata(
        entityid=entityid,
        name=entityid.split(".")[-1],
        partition_columns=[],
        merge_columns=[],
        tags={"provider_type": "adx-api", **tags},
        schema=schema,
    )


def _provider():
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    return AdxApiEntityProvider(logger_provider)


class _FakeKcsb:
    """Records which auth classmethod produced the connection string."""

    def __init__(self, mode, uri, **kwargs):
        self.mode = mode
        self.uri = uri
        self.kwargs = kwargs


class _FakeKustoClient:
    def __init__(self, kcsb):
        self.kcsb = kcsb
        self.executed = []
        self.mgmt = []
        self.query_result = None
        self.mgmt_rows = []

    def execute(self, database, query):
        self.executed.append((database, query))
        result = types.SimpleNamespace(primary_results=[self.query_result])
        return result

    def execute_mgmt(self, database, command):
        self.mgmt.append((database, command))
        return types.SimpleNamespace(primary_results=[types.SimpleNamespace(rows=self.mgmt_rows)])


class _FakeQueuedIngestClient:
    def __init__(self, kcsb):
        self.kcsb = kcsb
        self.ingested = []

    def ingest_from_dataframe(self, df, ingestion_properties=None):
        self.ingested.append((df, ingestion_properties))


class _FakeIngestionProperties:
    def __init__(self, database, table, flush_immediately=False, **kwargs):
        self.database = database
        self.table = table
        self.flush_immediately = flush_immediately


@pytest.fixture
def kusto(monkeypatch):
    """Install fake azure.kusto modules; returns a namespace with live handles."""
    state = types.SimpleNamespace(
        query_clients=[], ingest_clients=[], query_pdf=pd.DataFrame({"a": [1]})
    )

    class _Kcsb:
        @classmethod
        def with_aad_managed_service_identity_authentication(cls, uri, client_id=None):
            return _FakeKcsb("managed_identity", uri, client_id=client_id)

        @classmethod
        def with_aad_application_key_authentication(cls, uri, app_id, app_secret, tenant_id):
            return _FakeKcsb(
                "service_principal", uri, app_id=app_id, app_secret=app_secret, tenant_id=tenant_id
            )

        @classmethod
        def with_az_cli_authentication(cls, uri):
            return _FakeKcsb("azure_cli", uri)

        @classmethod
        def with_aad_application_token_authentication(cls, uri, token):
            return _FakeKcsb("access_token", uri, token=token)

    def _make_query_client(kcsb):
        client = _FakeKustoClient(kcsb)
        client.query_result = types.SimpleNamespace(name="primary")
        state.query_clients.append(client)
        return client

    def _make_ingest_client(kcsb):
        client = _FakeQueuedIngestClient(kcsb)
        state.ingest_clients.append(client)
        return client

    data_mod = types.ModuleType("azure.kusto.data")
    data_mod.KustoConnectionStringBuilder = _Kcsb
    data_mod.KustoClient = _make_query_client

    helpers_mod = types.ModuleType("azure.kusto.data.helpers")
    helpers_mod.dataframe_from_result_table = lambda result: state.query_pdf
    data_mod.helpers = helpers_mod

    ingest_mod = types.ModuleType("azure.kusto.ingest")
    ingest_mod.QueuedIngestClient = _make_ingest_client
    ingest_mod.IngestionProperties = _FakeIngestionProperties

    kusto_mod = types.ModuleType("azure.kusto")
    kusto_mod.data = data_mod
    kusto_mod.ingest = ingest_mod

    monkeypatch.setitem(sys.modules, "azure.kusto", kusto_mod)
    monkeypatch.setitem(sys.modules, "azure.kusto.data", data_mod)
    monkeypatch.setitem(sys.modules, "azure.kusto.data.helpers", helpers_mod)
    monkeypatch.setitem(sys.modules, "azure.kusto.ingest", ingest_mod)
    return state


class TestRegistration:
    def test_registered_as_builtin(self):
        from kindling.entity_provider_registry import EntityProviderRegistry

        with patch("kindling.entity_provider_registry.GlobalInjector"):
            logger_provider = MagicMock()
            logger_provider.get_logger.return_value = MagicMock()
            registry = EntityProviderRegistry(logger_provider)

        assert "adx-api" in registry.list_registered_providers()

    def test_capabilities(self):
        provider = _provider()
        assert is_writable(provider)
        assert can_ensure_destination(provider)

    def test_missing_sdk_raises_install_hint(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def _no_kusto(name, *args, **kwargs):
            if name.startswith("azure.kusto"):
                raise ImportError(name)
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _no_kusto)
        with pytest.raises(ImportError, match=r"spark-kindling\[adx\]"):
            _provider()._ingest(MagicMock(), _entity(BASE_TAGS))


class TestRead:
    def _read(self, kusto, tags, schema=None):
        provider = _provider()
        spark = MagicMock()
        with patch("kindling.entity_provider_adx.get_or_create_spark_session", return_value=spark):
            df = provider.read_entity(_entity(tags, schema=schema))
        return df, spark, kusto.query_clients[-1]

    def test_reads_table_as_kql(self, kusto):
        df, spark, client = self._read(kusto, BASE_TAGS)

        assert client.executed == [("analytics", "events")]
        spark.createDataFrame.assert_called_once_with(kusto.query_pdf)
        assert df is spark.createDataFrame.return_value

    def test_reads_explicit_query(self, kusto):
        tags = {**BASE_TAGS, "provider.query": "events | take 10"}
        _, _, client = self._read(kusto, tags)
        assert client.executed == [("analytics", "events | take 10")]

    def test_empty_result_uses_declared_schema(self, kusto):
        from pyspark.sql.types import LongType, StructField, StructType

        kusto.query_pdf = pd.DataFrame()
        schema = StructType([StructField("a", LongType(), True)])
        df, spark, _ = self._read(kusto, BASE_TAGS, schema=schema)

        spark.createDataFrame.assert_called_once_with([], schema=schema)

    def test_empty_result_without_schema_raises(self, kusto):
        kusto.query_pdf = pd.DataFrame()
        with pytest.raises(ValueError, match="declares no schema"):
            self._read(kusto, BASE_TAGS)

    def test_requires_database(self, kusto):
        with pytest.raises(ValueError, match="provider.database"):
            _provider().read_entity(_entity({"provider.cluster": "c", "provider.table": "t"}))

    def test_requires_table_or_query(self, kusto):
        with pytest.raises(ValueError, match="provider.table"):
            _provider().read_entity(_entity({"provider.cluster": "c", "provider.database": "db"}))


class TestWrite:
    def _df(self, pdf):
        df = MagicMock()
        df.toPandas.return_value = pdf
        return df

    def test_write_ingests_dataframe(self, kusto):
        provider = _provider()
        pdf = pd.DataFrame({"a": [1, 2]})
        provider.write_to_entity(self._df(pdf), _entity(BASE_TAGS))

        client = kusto.ingest_clients[-1]
        ((ingested_pdf, props),) = client.ingested
        assert ingested_pdf is pdf
        assert (props.database, props.table) == ("analytics", "events")
        assert props.flush_immediately is False

    def test_flush_immediately_tag(self, kusto):
        provider = _provider()
        tags = {**BASE_TAGS, "provider.flush_immediately": "true"}
        provider.append_to_entity(self._df(pd.DataFrame({"a": [1]})), _entity(tags))

        ((_, props),) = kusto.ingest_clients[-1].ingested
        assert props.flush_immediately is True

    def test_empty_dataframe_skips_ingestion(self, kusto):
        _provider().write_to_entity(self._df(pd.DataFrame()), _entity(BASE_TAGS))
        assert kusto.ingest_clients == []

    def test_ingest_uri_derived_from_cluster(self, kusto):
        _provider().write_to_entity(self._df(pd.DataFrame({"a": [1]})), _entity(BASE_TAGS))
        kcsb = kusto.ingest_clients[-1].kcsb
        assert kcsb.uri == "https://ingest-mycluster.westus.kusto.windows.net"

    def test_ingest_uri_override(self, kusto):
        tags = {**BASE_TAGS, "provider.ingest_uri": "https://custom-ingest.example.net"}
        _provider().write_to_entity(self._df(pd.DataFrame({"a": [1]})), _entity(tags))
        assert kusto.ingest_clients[-1].kcsb.uri == "https://custom-ingest.example.net"


class TestEnsureDestination:
    def test_creates_merge_table_from_schema(self, kusto):
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("score", DoubleType(), True),
                StructField("active", BooleanType(), True),
                StructField("at", TimestampType(), True),
                StructField("tags", ArrayType(StringType()), True),
            ]
        )
        _provider().ensure_destination(_entity(BASE_TAGS, schema=schema))

        client = kusto.query_clients[-1]
        ((database, command),) = client.mgmt
        assert database == "analytics"
        assert command == (
            ".create-merge table ['events'] (['id']: long, ['name']: string, "
            "['score']: real, ['active']: bool, ['at']: datetime, ['tags']: dynamic)"
        )

    def test_no_schema_is_a_noop(self, kusto):
        _provider().ensure_destination(_entity(BASE_TAGS))
        assert kusto.query_clients == []


class TestExists:
    def test_true_when_metadata_query_finds_table(self, kusto):
        provider = _provider()

        # First call creates the client with default empty rows → False
        assert provider.check_entity_exists(_entity(BASE_TAGS)) is False
        kusto.query_clients[-1].mgmt_rows = [("events",)]
        assert provider.check_entity_exists(_entity(BASE_TAGS)) is True

    def test_falls_back_to_assume_exists_on_error(self, kusto):
        provider = _provider()
        entity = _entity(BASE_TAGS)
        client = provider._query_client(entity, provider._get_provider_config(entity))
        client.execute_mgmt = MagicMock(side_effect=RuntimeError("forbidden"))

        assert provider.check_entity_exists(entity) is True

        entity2 = _entity({**BASE_TAGS, "provider.assume_exists": "false"}, entityid="e2")
        client2 = provider._query_client(entity2, provider._get_provider_config(entity2))
        client2.execute_mgmt = MagicMock(side_effect=RuntimeError("forbidden"))
        assert provider.check_entity_exists(entity2) is False


class TestAuth:
    def _kcsb(self, kusto, tags, monkeypatch=None, env=None):
        provider = _provider()
        entity = _entity(tags)
        return provider._connection_builder(
            entity, provider._get_provider_config(entity), "https://c.kusto.windows.net"
        )

    def test_default_is_managed_identity(self, kusto):
        kcsb = self._kcsb(kusto, BASE_TAGS)
        assert kcsb.mode == "managed_identity"
        assert kcsb.kwargs["client_id"] is None

    def test_managed_identity_with_client_id(self, kusto):
        tags = {**BASE_TAGS, "provider.managed_identity_client_id": "mi-123"}
        kcsb = self._kcsb(kusto, tags)
        assert kcsb.kwargs["client_id"] == "mi-123"

    def test_service_principal_from_tags(self, kusto):
        tags = {
            **BASE_TAGS,
            "provider.auth": "service_principal",
            "provider.app_id": "app",
            "provider.app_secret": "secret",
            "provider.tenant_id": "tenant",
        }
        kcsb = self._kcsb(kusto, tags)
        assert kcsb.mode == "service_principal"
        assert kcsb.kwargs == {"app_id": "app", "app_secret": "secret", "tenant_id": "tenant"}

    def test_service_principal_falls_back_to_env(self, kusto, monkeypatch):
        monkeypatch.setenv("AZURE_CLIENT_ID", "env-app")
        monkeypatch.setenv("AZURE_CLIENT_SECRET", "env-secret")
        monkeypatch.setenv("AZURE_TENANT_ID", "env-tenant")
        tags = {**BASE_TAGS, "provider.auth": "service_principal"}
        kcsb = self._kcsb(kusto, tags)
        assert kcsb.kwargs["app_id"] == "env-app"

    def test_service_principal_missing_values_raise(self, kusto, monkeypatch):
        for var in ("AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID"):
            monkeypatch.delenv(var, raising=False)
        tags = {**BASE_TAGS, "provider.auth": "service_principal"}
        with pytest.raises(ValueError, match="missing"):
            self._kcsb(kusto, tags)

    def test_azure_cli(self, kusto):
        kcsb = self._kcsb(kusto, {**BASE_TAGS, "provider.auth": "azure_cli"})
        assert kcsb.mode == "azure_cli"

    def test_access_token(self, kusto):
        tags = {**BASE_TAGS, "provider.auth": "access_token", "provider.access_token": "tok"}
        kcsb = self._kcsb(kusto, tags)
        assert kcsb.mode == "access_token"
        assert kcsb.kwargs["token"] == "tok"

    def test_access_token_missing_raises(self, kusto):
        tags = {**BASE_TAGS, "provider.auth": "access_token"}
        with pytest.raises(ValueError, match="access_token"):
            self._kcsb(kusto, tags)

    def test_unsupported_mode_raises(self, kusto):
        with pytest.raises(ValueError, match="Unsupported ADX auth mode"):
            self._kcsb(kusto, {**BASE_TAGS, "provider.auth": "device_code"})
