from unittest.mock import MagicMock

import pytest

from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import DeltaEntityProvider, DeltaTableReference
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider


@pytest.fixture
def provider_and_mocks(monkeypatch):
    spark = MagicMock()
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)

    config = MagicMock(spec=ConfigService)

    def _get(key, default=None):
        if key == "kindling.delta.access_mode":
            return "catalog"
        if key == "kindling.features.delta.auto_clustering":
            return True
        # No explicit feature overrides in unit tests
        if key.startswith("kindling.features.") or key.startswith("kindling.runtime.features."):
            return default
        return default

    config.get.side_effect = _get

    entity_name_mapper = MagicMock(spec=EntityNameMapper)
    entity_name_mapper.get_table_name.return_value = "main.default.my_table"

    path_locator = MagicMock(spec=EntityPathLocator)
    path_locator.get_table_path.return_value = "Tables/my_table"

    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger = MagicMock()
    logger_provider.get_logger.return_value = logger

    signal_provider = MagicMock(spec=SignalProvider)

    provider = DeltaEntityProvider(
        config=config,
        entity_name_mapper=entity_name_mapper,
        path_locator=path_locator,
        tp=logger_provider,
        signal_provider=signal_provider,
    )

    return provider, spark, logger


def _mock_df_writer_chain(df: MagicMock) -> MagicMock:
    writer = MagicMock()
    df.write = MagicMock()
    df.write.format.return_value = writer

    # Support chained calls: .option(...).option(...).partitionBy(...).mode(...).saveAsTable(...)
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer
    writer.mode.return_value = writer
    return writer


def test_write_skips_partition_by_when_cluster_columns_present(provider_and_mocks):
    provider, _spark, logger = provider_and_mocks
    provider._resolve_catalog_table_location = MagicMock(return_value="abfss://resolved/location")

    entity = EntityMetadata(
        entityid="e1",
        name="E1",
        partition_columns=["date"],
        merge_columns=["id"],
        tags={},
        schema=None,
        cluster_columns=["id"],
    )

    df = MagicMock()
    writer = _mock_df_writer_chain(df)

    table_ref = DeltaTableReference(
        table_name="main.default.my_table", table_path="Tables/my_table", access_mode="catalog"
    )

    provider._write_to_delta_table(df, entity, table_ref)

    writer.partitionBy.assert_not_called()
    logger.warning.assert_called()


def test_write_partitions_when_no_cluster_columns(provider_and_mocks):
    provider, _spark, _logger = provider_and_mocks
    provider._resolve_catalog_table_location = MagicMock(return_value="abfss://resolved/location")

    entity = EntityMetadata(
        entityid="e1",
        name="E1",
        partition_columns=["date"],
        merge_columns=["id"],
        tags={},
        schema=None,
    )

    df = MagicMock()
    writer = _mock_df_writer_chain(df)

    table_ref = DeltaTableReference(
        table_name="main.default.my_table", table_path="Tables/my_table", access_mode="catalog"
    )

    provider._write_to_delta_table(df, entity, table_ref)

    writer.partitionBy.assert_called_once_with("date")


def test_ensure_clustering_runs_alter_table(provider_and_mocks):
    provider, spark, _logger = provider_and_mocks

    entity = EntityMetadata(
        entityid="e1",
        name="E1",
        partition_columns=[],
        merge_columns=[],
        tags={},
        schema=None,
        cluster_columns=["id", "date"],
    )

    table_ref = DeltaTableReference(
        table_name="main.default.my_table", table_path=None, access_mode="catalog"
    )

    provider._ensure_clustering(entity, table_ref)

    # One DESCRIBE DETAIL probe + one ALTER TABLE CLUSTER BY.
    assert spark.sql.call_count == 2
    sql = spark.sql.call_args_list[-1][0][0]
    assert "ALTER TABLE" in sql
    assert "CLUSTER BY" in sql


def test_ensure_clustering_auto_uses_cluster_by_auto(provider_and_mocks):
    provider, spark, _logger = provider_and_mocks

    entity = EntityMetadata(
        entityid="e1",
        name="E1",
        partition_columns=[],
        merge_columns=[],
        tags={},
        schema=None,
        cluster_columns=["auto"],
    )
    table_ref = DeltaTableReference(
        table_name="main.default.my_table", table_path=None, access_mode="catalog"
    )

    provider._ensure_clustering(entity, table_ref)

    sql = spark.sql.call_args_list[-1][0][0]
    assert "CLUSTER BY AUTO" in sql


def test_ensure_clustering_auto_rejects_for_path(provider_and_mocks):
    provider, _spark, _logger = provider_and_mocks

    entity = EntityMetadata(
        entityid="e1",
        name="E1",
        partition_columns=[],
        merge_columns=[],
        tags={},
        schema=None,
        cluster_columns="auto",
    )
    table_ref = DeltaTableReference(
        table_name="main.default.my_table", table_path="Tables/my_table", access_mode="storage"
    )

    with pytest.raises(ValueError):
        provider._ensure_clustering(entity, table_ref)
