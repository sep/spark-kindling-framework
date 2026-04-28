"""
Unit tests for SqlEntityProvider.
"""

from unittest.mock import MagicMock, patch

import pytest
from kindling.data_entities import EntityMetadata
from kindling.entity_provider_sql import SqlEntityProvider


def _sql_entity(entityid="reporting.recent_sales", name="recent_sales", sql="SELECT 1", tags=None):
    return EntityMetadata(
        entityid=entityid,
        name=name,
        merge_columns=[],
        tags={"provider_type": "view", **(tags or {})},
        schema=None,
        sql=sql,
    )


def _delta_entity():
    return EntityMetadata(
        entityid="bronze.orders",
        name="orders",
        merge_columns=["id"],
        tags={"provider_type": "delta"},
        schema=None,
    )


@pytest.fixture
def provider():
    tp = MagicMock()
    tp.get_logger.return_value = MagicMock()
    return SqlEntityProvider(tp)


class TestSqlEntityProviderViewName:
    def test_uses_table_name_tag_when_present(self, provider):
        entity = _sql_entity(tags={"provider.table_name": "catalog.schema.my_view"})
        assert provider._view_name(entity) == "catalog.schema.my_view"

    def test_falls_back_to_entity_name(self, provider):
        entity = _sql_entity(name="recent_sales")
        assert provider._view_name(entity) == "recent_sales"


class TestSqlEntityProviderReadEntity:
    def test_reads_view_as_dataframe(self, provider):
        entity = _sql_entity(name="recent_sales")
        mock_df = MagicMock()
        mock_spark = MagicMock()
        mock_spark.read.table.return_value = mock_df

        with patch(
            "kindling.entity_provider_sql.get_or_create_spark_session", return_value=mock_spark
        ):
            result = provider.read_entity(entity)

        assert result is mock_df
        mock_spark.read.table.assert_called_once_with("recent_sales")

    def test_uses_table_name_tag_for_read(self, provider):
        entity = _sql_entity(tags={"provider.table_name": "catalog.schema.my_view"})
        mock_spark = MagicMock()

        with patch(
            "kindling.entity_provider_sql.get_or_create_spark_session", return_value=mock_spark
        ):
            provider.read_entity(entity)

        mock_spark.read.table.assert_called_once_with("catalog.schema.my_view")

    def test_raises_for_non_sql_entity(self, provider):
        entity = _delta_entity()

        with pytest.raises(ValueError, match="SqlEntityProvider cannot read non-SQL entity"):
            provider.read_entity(entity)


class TestSqlEntityProviderCheckEntityExists:
    def test_returns_true_when_view_exists(self, provider):
        entity = _sql_entity(name="my_view")
        mock_spark = MagicMock()  # spark.sql() succeeds → view exists

        with patch(
            "kindling.entity_provider_sql.get_or_create_spark_session", return_value=mock_spark
        ):
            assert provider.check_entity_exists(entity) is True

        mock_spark.sql.assert_called_once_with("DESCRIBE my_view")

    def test_returns_false_when_view_missing(self, provider):
        entity = _sql_entity(name="my_view")
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")

        with patch(
            "kindling.entity_provider_sql.get_or_create_spark_session", return_value=mock_spark
        ):
            assert provider.check_entity_exists(entity) is False


class TestSqlEntityProviderEnsureDestination:
    def test_issues_create_or_replace_view(self, provider):
        entity = _sql_entity(name="recent_sales", sql="SELECT a, b FROM source")
        mock_spark = MagicMock()

        with patch(
            "kindling.entity_provider_sql.get_or_create_spark_session", return_value=mock_spark
        ):
            provider.ensure_destination(entity)

        mock_spark.sql.assert_called_once_with(
            "CREATE OR REPLACE VIEW recent_sales AS SELECT a, b FROM source"
        )

    def test_uses_table_name_tag_in_ddl(self, provider):
        entity = _sql_entity(
            sql="SELECT 1",
            tags={"provider.table_name": "catalog.schema.v"},
        )
        mock_spark = MagicMock()

        with patch(
            "kindling.entity_provider_sql.get_or_create_spark_session", return_value=mock_spark
        ):
            provider.ensure_destination(entity)

        calls = [c.args[0] for c in mock_spark.sql.call_args_list]
        assert "CREATE SCHEMA IF NOT EXISTS catalog.schema" in calls
        assert "CREATE OR REPLACE VIEW catalog.schema.v AS SELECT 1" in calls

    def test_raises_for_non_sql_entity(self, provider):
        entity = _delta_entity()

        with pytest.raises(ValueError, match="ensure_destination called on non-SQL entity"):
            provider.ensure_destination(entity)
