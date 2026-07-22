"""
Unit tests for ensure-on-write defaults: ensure only when there is a
schema to ensure FROM.

A schema-less entity has nothing to pre-create with. The old behavior
planted a zero-column Delta table in catalog mode (ensure and the data
write are separate transactions, so a crash in between leaves the trap
behind and retries take the merge path into a table with no key
columns). Schema-less entities are now write-driven-created on every
path, and creating a columnless managed table is refused outright.
"""

from unittest.mock import MagicMock, patch

import pytest
from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import DeltaEntityProvider
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from pyspark.sql.types import StringType, StructField, StructType


@pytest.fixture(autouse=True)
def mock_spark_session(monkeypatch):
    spark = MagicMock()
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)
    return spark


@pytest.fixture
def provider():
    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: (
        "catalog" if key == "kindling.delta.access_mode" else default
    )
    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger_provider.get_logger.return_value = MagicMock()
    return DeltaEntityProvider(
        config=config,
        entity_name_mapper=MagicMock(spec=EntityNameMapper),
        path_locator=MagicMock(spec=EntityPathLocator),
        tp=logger_provider,
        signal_provider=MagicMock(spec=SignalProvider),
    )


def _entity(schema=None, tags=None):
    return EntityMetadata(
        entityid="silver.orders",
        name="orders",
        merge_columns=["order_id"],
        tags=tags or {},
        schema=schema,
    )


_SCHEMA = StructType([StructField("order_id", StringType())])


class TestEnsureBeforeBatchWrite:
    def test_schema_less_entity_skips_ensure(self, provider):
        entity = _entity(schema=None)
        table_ref = MagicMock(access_mode="catalog", table_name="silver_orders")

        with patch.object(provider, "_ensure_table_exists") as ensure:
            provider._ensure_destination_for_write(entity, table_ref)

        ensure.assert_not_called()

    def test_declared_schema_still_ensures(self, provider):
        entity = _entity(schema=_SCHEMA)
        table_ref = MagicMock(access_mode="catalog", table_name="silver_orders")

        with patch.object(provider, "_ensure_table_exists") as ensure:
            provider._ensure_destination_for_write(entity, table_ref)

        ensure.assert_called_once_with(entity, table_ref)

    def test_ensure_on_write_false_still_skips(self, provider):
        provider.config.get.side_effect = lambda key, default=None: {
            "kindling.delta.access_mode": "catalog",
            "kindling.delta.ensure_on_write": "false",
        }.get(key, default)
        entity = _entity(schema=_SCHEMA)

        with patch.object(provider, "_ensure_table_exists") as ensure:
            provider._ensure_destination_for_write(entity, MagicMock())

        ensure.assert_not_called()


class TestEnsureDestinationStreaming:
    def test_schema_less_writable_entity_skips(self, provider):
        entity = _entity(schema=None)

        with patch.object(provider, "ensure_entity_table") as ensure:
            provider.ensure_destination(entity)

        ensure.assert_not_called()

    def test_declared_schema_ensures(self, provider):
        entity = _entity(schema=_SCHEMA)

        with patch.object(provider, "ensure_entity_table") as ensure:
            provider.ensure_destination(entity)

        ensure.assert_called_once_with(entity)

    def test_schema_less_read_only_still_ensures_registration(self, provider):
        # Read-only ensure only registers an existing external table — it
        # never creates, so schema-less is safe and registration is wanted.
        entity = _entity(schema=None, tags={"read_only": "true"})

        with patch.object(provider, "ensure_entity_table") as ensure:
            provider.ensure_destination(entity)

        ensure.assert_called_once_with(entity)


class TestZeroColumnGuard:
    def test_managed_create_without_schema_refuses(self, provider):
        entity = _entity(schema=None)
        table_ref = MagicMock(access_mode="catalog", table_name="silver_orders")

        with patch.object(provider, "_ensure_configured_table_schema_exists"):
            with pytest.raises(ValueError, match="without\\s+schema"):
                provider._create_managed_table(entity, table_ref)

    def test_managed_create_with_schema_bootstraps(self, provider, mock_spark_session):
        entity = _entity(schema=_SCHEMA)
        table_ref = MagicMock(access_mode="catalog", table_name="silver_orders")
        writer = mock_spark_session.createDataFrame.return_value.write
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer

        with (
            patch.object(provider, "_ensure_configured_table_schema_exists"),
            patch.object(provider, "_resolve_catalog_table_location", return_value=None),
        ):
            provider._create_managed_table(entity, table_ref)

        writer.saveAsTable.assert_called_once_with("silver_orders")
