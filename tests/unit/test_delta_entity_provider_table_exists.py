"""
Regression coverage: catalog-mode table existence checks must not rely on
DeltaTable.forName()/get_delta_table() succeeding without raising.

On a Spark-Connect-backed session, DeltaTable.forName() builds an unresolved
relation client-side without validating it against the catalog -- it returns
successfully even when the table doesn't exist yet. The real
DELTA_MISSING_DELTA_TABLE error only surfaces later, when that relation is
actually executed (e.g. mid-merge). _check_table_exists() and
_check_catalog_table_exists() must use spark.catalog.tableExists(), an eager
catalog metadata RPC on both classic and Spark Connect sessions, as the
authoritative check for catalog ("for name") mode -- not a lazily
constructed DeltaTable/DataFrame handle.
"""

from unittest.mock import MagicMock

import pytest

from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import DeltaEntityProvider, DeltaTableReference
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider


@pytest.fixture
def provider_and_spark(monkeypatch):
    spark = MagicMock()
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)

    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: (
        "catalog" if key == "kindling.delta.access_mode" else default
    )

    entity_name_mapper = MagicMock(spec=EntityNameMapper)
    path_locator = MagicMock(spec=EntityPathLocator)

    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger_provider.get_logger.return_value = MagicMock()

    signal_provider = MagicMock(spec=SignalProvider)

    provider = DeltaEntityProvider(
        config=config,
        entity_name_mapper=entity_name_mapper,
        path_locator=path_locator,
        tp=logger_provider,
        signal_provider=signal_provider,
    )

    return provider, spark


def _table_ref(table_name="kindling.kindling.my_table"):
    return DeltaTableReference(table_name=table_name, table_path=None, access_mode="catalog")


class TestCheckTableExistsCatalogMode:
    def test_returns_false_when_tableExists_false_even_if_get_delta_table_is_lazy(
        self, provider_and_spark
    ):
        """The Spark-Connect laziness regression: get_delta_table() must not
        be trusted to raise for a nonexistent catalog table."""
        provider, spark = provider_and_spark
        spark.catalog.tableExists.return_value = False

        table_ref = _table_ref()
        # Simulate Spark Connect: constructing a DeltaTable handle for a
        # nonexistent table succeeds client-side (no exception) instead of
        # raising DELTA_MISSING_DELTA_TABLE like a classic session would.
        table_ref.get_delta_table = MagicMock(return_value=MagicMock())

        assert provider._check_table_exists(table_ref) is False
        spark.catalog.tableExists.assert_called_once_with(table_ref.table_name)

    def test_returns_true_when_tableExists_true(self, provider_and_spark):
        provider, spark = provider_and_spark
        spark.catalog.tableExists.return_value = True

        table_ref = _table_ref()
        assert provider._check_table_exists(table_ref) is True

    def test_returns_false_when_tableExists_raises(self, provider_and_spark):
        provider, spark = provider_and_spark
        spark.catalog.tableExists.side_effect = Exception("catalog unreachable")

        table_ref = _table_ref()
        assert provider._check_table_exists(table_ref) is False


class TestCheckCatalogTableExists:
    def test_uses_catalog_tableExists_not_read_table(self, provider_and_spark):
        provider, spark = provider_and_spark
        spark.catalog.tableExists.return_value = True

        table_ref = _table_ref()
        assert provider._check_catalog_table_exists(table_ref) is True
        spark.catalog.tableExists.assert_called_once_with(table_ref.table_name)
        spark.read.table.assert_not_called()

    def test_returns_false_without_table_name(self, provider_and_spark):
        provider, _spark = provider_and_spark
        table_ref = _table_ref(table_name=None)
        assert provider._check_catalog_table_exists(table_ref) is False


class TestMergeToEntityCreatesTableOnFirstRun:
    def test_merge_creates_table_when_catalog_reports_missing(
        self, provider_and_spark, monkeypatch
    ):
        """End-to-end: merge_to_entity() must call ensure_entity_table() (and
        therefore create the table) when the catalog genuinely has no table
        yet, regardless of what a lazily-constructed DeltaTable handle claims."""
        provider, spark = provider_and_spark
        spark.catalog.tableExists.return_value = False

        entity = MagicMock()
        entity.entityid = "staging.device_telemetry_abc123"
        entity.name = "device_telemetry_abc123"
        entity.tags = {}
        entity.merge_columns = ["device_id"]

        table_ref = _table_ref("kindling.kindling.staging_device_telemetry_abc123")
        monkeypatch.setattr(provider, "_get_table_reference", lambda e: table_ref)

        ensure_called = []
        monkeypatch.setattr(provider, "ensure_entity_table", lambda e: ensure_called.append(e))
        monkeypatch.setattr(provider, "_merge_to_delta_table", lambda df, e, ref: None)

        df = MagicMock()
        provider.merge_to_entity(df, entity)

        assert ensure_called == [entity]
