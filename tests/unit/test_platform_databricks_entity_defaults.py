from unittest.mock import MagicMock, patch

from kindling.entity_resolution import (
    ConfigDrivenEntityNameMapper,
    ConfigDrivenEntityPathLocator,
)
from kindling.platform_databricks import _bind_default_entity_services
from kindling.spark_config import ConfigService


def test_bind_default_entity_services_sets_delta_access_default_when_missing():
    logger = MagicMock()
    cs = MagicMock(spec=ConfigService)
    cs.get.return_value = None

    with patch("kindling.platform_databricks.GlobalInjector.get", return_value=cs):
        _bind_default_entity_services(logger)

    cs.set.assert_called_once_with("kindling.delta.tablerefmode", "forName")


def test_bind_default_entity_services_does_not_override_explicit_setting():
    logger = MagicMock()
    cs = MagicMock(spec=ConfigService)
    cs.get.return_value = "forPath"

    with patch("kindling.platform_databricks.GlobalInjector.get", return_value=cs):
        _bind_default_entity_services(logger)

    cs.set.assert_not_called()


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

