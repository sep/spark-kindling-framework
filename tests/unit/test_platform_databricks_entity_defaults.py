from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.platform_databricks import (
    DatabricksEntityNameMapper,
    DatabricksEntityPathLocator,
    _bind_default_entity_services,
)


def _build_injector(has_name_binding: bool, has_path_binding: bool):
    binder = MagicMock()

    def _has_explicit_binding_for(interface):
        if interface is EntityNameMapper:
            return has_name_binding
        if interface is EntityPathLocator:
            return has_path_binding
        return False

    binder.has_explicit_binding_for.side_effect = _has_explicit_binding_for

    injector = MagicMock()
    injector.binder = binder
    return injector, binder


def test_bind_default_entity_services_binds_databricks_defaults():
    logger = MagicMock()
    injector, binder = _build_injector(False, False)

    with patch("kindling.platform_databricks.GlobalInjector.get_injector", return_value=injector):
        _bind_default_entity_services(logger)

    assert binder.bind.call_count == 2
    first = binder.bind.call_args_list[0]
    second = binder.bind.call_args_list[1]

    assert first.args[0] is EntityNameMapper
    assert first.kwargs["to"] is DatabricksEntityNameMapper

    assert second.args[0] is EntityPathLocator
    assert second.kwargs["to"] is DatabricksEntityPathLocator


def test_bind_default_entity_services_does_not_override_explicit_bindings():
    logger = MagicMock()
    injector, binder = _build_injector(True, True)

    with patch("kindling.platform_databricks.GlobalInjector.get_injector", return_value=injector):
        _bind_default_entity_services(logger)

    binder.bind.assert_not_called()


class TestDatabricksEntityPathLocator:
    def test_returns_provider_path_tag_when_present(self):
        mapper = MagicMock(spec=EntityNameMapper)
        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()
        spark = MagicMock()

        entity = MagicMock()
        entity.tags = {"provider.path": "abfss://override/path"}
        entity.entityid = "sales.orders"

        with patch("kindling.platform_databricks.get_or_create_spark_session", return_value=spark):
            locator = DatabricksEntityPathLocator(mapper, logger_provider)

        result = locator.get_table_path(entity)
        assert result == "abfss://override/path"
        spark.sql.assert_not_called()

    def test_resolves_location_from_catalog(self):
        mapper = MagicMock(spec=EntityNameMapper)
        mapper.get_table_name.return_value = "main.sales.orders"

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        spark = MagicMock()
        spark.sql.return_value.select.return_value.first.return_value = {
            "location": "abfss://catalog-managed/main/sales/orders"
        }

        entity = MagicMock()
        entity.tags = {}
        entity.entityid = "sales.orders"

        with patch("kindling.platform_databricks.get_or_create_spark_session", return_value=spark):
            locator = DatabricksEntityPathLocator(mapper, logger_provider)

        result = locator.get_table_path(entity)
        assert result == "abfss://catalog-managed/main/sales/orders"
        spark.sql.assert_called_once_with("DESCRIBE DETAIL `main`.`sales`.`orders`")

    def test_raises_when_catalog_lookup_fails(self):
        mapper = MagicMock(spec=EntityNameMapper)
        mapper.get_table_name.return_value = "main.sales.missing_table"

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        spark = MagicMock()
        spark.sql.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND")

        entity = MagicMock()
        entity.tags = {}
        entity.entityid = "sales.missing_table"

        with patch("kindling.platform_databricks.get_or_create_spark_session", return_value=spark):
            locator = DatabricksEntityPathLocator(mapper, logger_provider)

        with pytest.raises(ValueError) as exc_info:
            locator.get_table_path(entity)

        assert "provider.path" in str(exc_info.value)


class TestDatabricksEntityNameMapper:
    def test_uses_provider_table_name_tag_when_present(self):
        config = MagicMock()
        config.get.return_value = None

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        spark = MagicMock()

        entity = MagicMock()
        entity.tags = {"provider.table_name": "main.analytics.orders"}
        entity.entityid = "stream.orders"

        with patch("kindling.platform_databricks.get_or_create_spark_session", return_value=spark):
            mapper = DatabricksEntityNameMapper(config, logger_provider)

        result = mapper.get_table_name(entity)
        assert result == "main.analytics.orders"
        spark.sql.assert_not_called()

    def test_uses_configured_catalog_and_schema(self):
        values = {
            "kindling.databricks.catalog": "main",
            "kindling.databricks.schema": "analytics",
        }
        config = MagicMock()
        config.get.side_effect = lambda key: values.get(key)

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        spark = MagicMock()

        entity = MagicMock()
        entity.tags = {}
        entity.entityid = "stream.orders"

        with patch("kindling.platform_databricks.get_or_create_spark_session", return_value=spark):
            mapper = DatabricksEntityNameMapper(config, logger_provider)

        result = mapper.get_table_name(entity)
        assert result == "main.analytics.stream_orders"
        spark.sql.assert_not_called()

    def test_falls_back_to_current_namespace(self):
        config = MagicMock()
        config.get.return_value = None

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        spark = MagicMock()
        spark.sql.return_value.select.return_value.first.return_value = {
            "catalog_name": "hive_metastore",
            "schema_name": "default",
        }

        entity = MagicMock()
        entity.tags = {}
        entity.entityid = "stream.orders"

        with patch("kindling.platform_databricks.get_or_create_spark_session", return_value=spark):
            mapper = DatabricksEntityNameMapper(config, logger_provider)

        result = mapper.get_table_name(entity)
        assert result == "hive_metastore.default.stream_orders"
