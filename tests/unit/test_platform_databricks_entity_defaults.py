from unittest.mock import MagicMock, call, patch

from kindling.entity_resolution import (
    ConfigDrivenEntityNameMapper,
    ConfigDrivenEntityPathLocator,
)
from kindling.platform_databricks import _bind_default_entity_services
from kindling.spark_config import ConfigService


def test_bind_default_entity_services_defaults_to_forName_when_uc_enabled():
    logger = MagicMock()
    cs = MagicMock(spec=ConfigService)
    cs.get.return_value = None  # no tablerefmode set, no feature flags

    with patch("kindling.platform_databricks.GlobalInjector.get", return_value=cs):
        _bind_default_entity_services(logger)

    cs.set.assert_called_once_with("kindling.delta.tablerefmode", "forName")


def test_bind_default_entity_services_defaults_to_forPath_when_uc_disabled():
    logger = MagicMock()
    values = {
        "kindling.delta.tablerefmode": None,
        "kindling.features.databricks.uc_enabled": False,
    }
    cs = MagicMock(spec=ConfigService)
    cs.get.side_effect = lambda key, default=None: values.get(key, default)

    with patch("kindling.platform_databricks.GlobalInjector.get", return_value=cs):
        _bind_default_entity_services(logger)

    cs.set.assert_called_once_with("kindling.delta.tablerefmode", "forPath")


def test_bind_default_entity_services_does_not_override_explicit_setting():
    logger = MagicMock()
    cs = MagicMock(spec=ConfigService)
    cs.get.return_value = "forPath"

    with patch("kindling.platform_databricks.GlobalInjector.get", return_value=cs):
        _bind_default_entity_services(logger)

    cs.set.assert_not_called()


class TestGetCurrentNamespaceUCAwareness:
    """_get_current_namespace should skip the current_catalog() query when UC is disabled."""

    def test_skips_catalog_query_when_uc_disabled(self):
        from kindling.entity_resolution import _get_current_namespace

        fake_cs = MagicMock(spec=ConfigService)
        fake_cs.get.side_effect = lambda key, default=None: {
            "kindling.features.databricks.uc_enabled": False,
        }.get(key, default)

        mock_spark = MagicMock()
        # current_database() returns a Row-like
        db_row = MagicMock()
        db_row.__getitem__ = lambda self, k: "my_db" if k == "schema" else None
        mock_spark.sql.return_value.select.return_value.first.return_value = db_row

        with (
            patch("kindling.entity_resolution.GlobalInjector.get", return_value=fake_cs),
            patch("kindling.entity_resolution.get_or_create_spark_session", return_value=mock_spark),
        ):
            catalog, schema = _get_current_namespace()

        assert catalog is None
        assert schema == "my_db"
        # Should only have called current_database, never current_catalog
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert not any("current_catalog" in c for c in sql_calls)

    def test_queries_catalog_when_uc_enabled(self):
        from kindling.entity_resolution import _get_current_namespace

        fake_cs = MagicMock(spec=ConfigService)
        fake_cs.get.side_effect = lambda key, default=None: {
            "kindling.features.databricks.uc_enabled": True,
        }.get(key, default)

        mock_spark = MagicMock()
        catalog_row = MagicMock()
        catalog_row.__getitem__ = lambda self, k: {"catalog": "main", "schema": "analytics"}.get(k)
        mock_spark.sql.return_value.select.return_value.first.return_value = catalog_row

        with (
            patch("kindling.entity_resolution.GlobalInjector.get", return_value=fake_cs),
            patch("kindling.entity_resolution.get_or_create_spark_session", return_value=mock_spark),
        ):
            catalog, schema = _get_current_namespace()

        assert catalog == "main"
        assert schema == "analytics"

    def test_queries_catalog_when_flag_not_set(self):
        """When no UC flag is configured, default behavior queries catalog (backward compat)."""
        from kindling.entity_resolution import _get_current_namespace

        fake_cs = MagicMock(spec=ConfigService)
        fake_cs.get.return_value = None  # no feature flags set

        mock_spark = MagicMock()
        catalog_row = MagicMock()
        catalog_row.__getitem__ = lambda self, k: {"catalog": "spark_catalog", "schema": "default"}.get(k)
        mock_spark.sql.return_value.select.return_value.first.return_value = catalog_row

        with (
            patch("kindling.entity_resolution.GlobalInjector.get", return_value=fake_cs),
            patch("kindling.entity_resolution.get_or_create_spark_session", return_value=mock_spark),
        ):
            catalog, schema = _get_current_namespace()

        assert catalog == "spark_catalog"
        assert schema == "default"
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any("current_catalog" in c for c in sql_calls)


class TestConfigDrivenEntityNameMapperDatabricksFallbacks:
    def test_uses_provider_table_name_tag_when_present(self):
        config = MagicMock(spec=ConfigService)
        config.get.return_value = None

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        entity = MagicMock()
        entity.tags = {"provider.table_name": "main.analytics.orders"}
        entity.entityid = "stream.orders"

        mapper = ConfigDrivenEntityNameMapper(config, logger_provider)
        assert mapper.get_table_name(entity) == "main.analytics.orders"

    def test_uses_databricks_catalog_and_schema_when_storage_namespace_not_set(self):
        values = {
            "kindling.storage.table_catalog": None,
            "kindling.storage.table_schema": None,
            "kindling.databricks.catalog": "main",
            "kindling.databricks.schema": "analytics",
            "kindling.storage.table_name_prefix": "",
        }
        config = MagicMock(spec=ConfigService)
        config.get.side_effect = lambda key, default=None: values.get(key, default)

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        entity = MagicMock()
        entity.tags = {}
        entity.entityid = "stream.orders"

        mapper = ConfigDrivenEntityNameMapper(config, logger_provider)
        assert mapper.get_table_name(entity) == "main.analytics.stream_orders"


class TestConfigDrivenEntityPathLocatorDatabricksFallbacks:
    def test_returns_provider_path_tag_when_present(self):
        config = MagicMock(spec=ConfigService)
        config.get.return_value = None

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        entity = MagicMock()
        entity.tags = {"provider.path": "abfss://override/path"}
        entity.entityid = "sales.orders"

        locator = ConfigDrivenEntityPathLocator(config, logger_provider)
        assert locator.get_table_path(entity) == "abfss://override/path"

    def test_uses_databricks_table_root_when_storage_root_not_set(self):
        values = {
            "kindling.storage.table_root": None,
            "kindling.databricks.table_root": "/tmp/tables",
        }
        config = MagicMock(spec=ConfigService)
        config.get.side_effect = lambda key, default=None: values.get(key, default)

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        entity = MagicMock()
        entity.tags = {}
        entity.entityid = "stream.orders"

        locator = ConfigDrivenEntityPathLocator(config, logger_provider)
        assert locator.get_table_path(entity) == "/tmp/tables/stream/orders"
