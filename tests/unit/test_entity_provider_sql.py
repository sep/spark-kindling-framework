"""
Unit tests for SqlEntityProvider (provider_type "view").
"""

from unittest.mock import MagicMock, patch

import pytest
from kindling.data_entities import EntityMetadata
from kindling.entity_provider import (
    can_ensure_destination,
    is_replace_writable,
    is_stream_writable,
    is_streamable,
    is_writable,
)
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


def _patched_spark(mock_spark):
    return patch(
        "kindling.entity_provider_sql.get_or_create_spark_session", return_value=mock_spark
    )


class TestSqlEntityProviderReadEntity:
    def test_evaluates_declared_sql(self, provider):
        entity = _sql_entity(sql="SELECT a FROM sales.tx WHERE a > 1")
        mock_df = MagicMock()
        mock_spark = MagicMock()
        mock_spark.sql.return_value = mock_df

        with _patched_spark(mock_spark):
            result = provider.read_entity(entity)

        assert result is mock_df
        mock_spark.sql.assert_called_once_with("SELECT a FROM sales.tx WHERE a > 1")
        mock_spark.read.table.assert_not_called()

    def test_does_not_read_or_create_the_catalog_view(self, provider):
        """Reads must not depend on, or create, the migrate-managed view."""
        entity = _sql_entity(sql="SELECT 1", tags={"provider.table_name": "cat.sch.v"})
        mock_spark = MagicMock()

        with _patched_spark(mock_spark):
            provider.read_entity(entity)

        mock_spark.sql.assert_called_once_with("SELECT 1")
        mock_spark.read.table.assert_not_called()

    def test_raises_for_non_sql_entity(self, provider):
        with pytest.raises(ValueError, match="SqlEntityProvider cannot read non-SQL entity"):
            provider.read_entity(_delta_entity())


class TestSqlEntityProviderCheckEntityExists:
    def test_true_when_sql_resolves(self, provider):
        entity = _sql_entity(sql="SELECT * FROM t")
        mock_spark = MagicMock()

        with _patched_spark(mock_spark):
            assert provider.check_entity_exists(entity) is True

        mock_spark.sql.assert_called_once_with("SELECT * FROM t")

    def test_false_when_sql_does_not_resolve(self, provider):
        entity = _sql_entity(sql="SELECT * FROM missing")
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")

        with _patched_spark(mock_spark):
            assert provider.check_entity_exists(entity) is False


class TestSqlEntityProviderCapabilities:
    def test_is_read_only(self, provider):
        assert not is_writable(provider)
        assert not is_stream_writable(provider)
        assert not is_replace_writable(provider)
        assert not hasattr(provider, "merge_to_entity")

    def test_does_not_ensure_destinations(self, provider):
        """The runner calls ensure_destination on pipe outputs; a SQL entity
        is never a valid output, and the view is migrate's to create."""
        assert not can_ensure_destination(provider)

    def test_is_not_streamable(self, provider):
        assert not is_streamable(provider)


class TestSqlEntityProviderRegistration:
    def test_registered_under_view(self):
        from kindling.entity_provider_registry import EntityProviderRegistry

        with patch("kindling.entity_provider_registry.GlobalInjector"):
            lp = MagicMock()
            lp.get_logger.return_value = MagicMock()
            registry = EntityProviderRegistry(lp)

        assert registry.get_provider_class("view") is SqlEntityProvider

    def test_sql_entity_decorator_tag_resolves_to_provider(self):
        """DataEntities.sql_entity tags entities provider_type 'view'; that
        tag must resolve through the registry rather than fail with
        "Unknown provider type: 'view'"."""
        from kindling.entity_provider_registry import EntityProviderRegistry

        lp = MagicMock()
        lp.get_logger.return_value = MagicMock()
        sql_provider = SqlEntityProvider(lp)
        with patch("kindling.entity_provider_registry.GlobalInjector") as injector:
            registry = EntityProviderRegistry(lp)
            injector.get.return_value = sql_provider
            assert registry.get_provider_for_entity(_sql_entity()) is sql_provider
            injector.get.assert_called_once_with(SqlEntityProvider)

    @pytest.mark.parametrize("provider_type", ["delta", "csv", "memory"])
    def test_sql_entity_ignores_writable_provider_tag(self, provider_type):
        """A provider_type override (e.g. from a config overlay) must not
        make a SQL entity writable: it still resolves to the view provider."""
        from kindling.entity_provider_registry import EntityProviderRegistry

        lp = MagicMock()
        lp.get_logger.return_value = MagicMock()
        sql_provider = SqlEntityProvider(lp)
        with patch("kindling.entity_provider_registry.GlobalInjector") as injector:
            registry = EntityProviderRegistry(lp)
            injector.get.return_value = sql_provider
            entity = _sql_entity(tags={"provider_type": provider_type})
            assert registry.get_provider_for_entity(entity) is sql_provider
            injector.get.assert_called_once_with(SqlEntityProvider)


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE IF EXISTS important_table",
        "select 1; drop table x",
        "with x as (select 1) insert into t select * from x",
        "INSERT INTO t VALUES (1)",
        "SET spark.sql.shuffle.partitions=1",
        "",
        # comment markers inside strings must not hide the statement
        "WITH x AS (SELECT '--' AS n) INSERT INTO target SELECT n FROM x",
        "WITH x AS (SELECT '/*' AS a) INSERT INTO target SELECT '*/' AS a FROM x",
        "FROM src INSERT INTO t1 SELECT a",
        "with x as (select 1) delete from t",
        "select 'unterminated",
        # Spark nests block comments; the inner */ must not end the comment
        "FROM src /* outer /* inner */ ( */ INSERT INTO dst SELECT id",
        "WITH x AS (SELECT 1) /* a /* b */ ( */ INSERT INTO dst SELECT * FROM x",
        "select 1 /* never closed /* nested */",
        # raw literals have no escapes: r'\' is a complete string
        "FROM (SELECT r'\\' AS n) src INSERT INTO dst SELECT '--' AS n",
        'FROM (SELECT R"\\" AS n) src INSERT INTO dst SELECT "--" AS n',
        "select r'unterminated",
        # Spark ends a -- comment at a carriage return as well as a line feed
        "FROM src -- note\rINSERT INTO dst SELECT id",
    ],
)
def test_non_query_sql_is_rejected_before_spark(sql, monkeypatch):
    """A SQL entity is read by running its SQL; a statement that is not a
    single read-only query must never reach spark.sql()."""
    import kindling.entity_provider_sql as module
    from kindling.entity_provider_sql import SqlEntityProvider

    spark = MagicMock()
    monkeypatch.setattr(module, "get_or_create_spark_session", lambda: spark)
    provider = SqlEntityProvider.__new__(SqlEntityProvider)
    provider._logger = MagicMock()
    entity = MagicMock(entityid="reporting.x", sql=sql, is_sql_entity=True)

    with pytest.raises(ValueError, match="single read-only query"):
        provider.read_entity(entity)
    with pytest.raises(ValueError, match="single read-only query"):
        provider.check_entity_exists(entity)
    spark.sql.assert_not_called()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM sales.orders",
        "with recent as (select * from s.o) select * from recent;",
        "select 'drop table x' as note, last_update from t -- insert later",
        "(SELECT 1)",
        "SELECT replace('abc', 'a', 'z') AS cleaned",  # function named like a command
        "select load, update_time from t",
        "with a as (select 1), b (c) as (select 2) select * from a join b",
        "select 'it''s' as q, 'semi;colon' as s",
        "select /* a /* nested */ comment */ id from t",
        "SELECT regexp_extract(s, r'\\d+') AS digits, r'C:\\dir\\' AS p FROM t",
        'select r, R"x" as y from t -- note\r\n',
    ],
)
def test_queries_are_accepted(sql):
    from kindling.entity_provider_sql import require_query_sql

    require_query_sql(sql, "reporting.x")
