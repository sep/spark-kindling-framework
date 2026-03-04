from unittest.mock import MagicMock, patch

from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.platform_fabric import (
    FabricEntityNameMapper,
    FabricEntityPathLocator,
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


def test_bind_default_entity_services_binds_fabric_defaults():
    logger = MagicMock()
    injector, binder = _build_injector(False, False)

    with patch("kindling.platform_fabric.GlobalInjector.get_injector", return_value=injector):
        _bind_default_entity_services(logger)

    assert binder.bind.call_count == 2
    first = binder.bind.call_args_list[0]
    second = binder.bind.call_args_list[1]

    assert first.args[0] is EntityNameMapper
    assert first.kwargs["to"] is FabricEntityNameMapper

    assert second.args[0] is EntityPathLocator
    assert second.kwargs["to"] is FabricEntityPathLocator


def test_bind_default_entity_services_does_not_override_explicit_bindings():
    logger = MagicMock()
    injector, binder = _build_injector(True, True)

    with patch("kindling.platform_fabric.GlobalInjector.get_injector", return_value=injector):
        _bind_default_entity_services(logger)

    binder.bind.assert_not_called()


class TestFabricEntityNameMapper:
    def test_uses_provider_table_name_tag_when_present(self):
        config = MagicMock()
        config.get.return_value = None

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        entity = MagicMock()
        entity.tags = {"provider.table_name": "sales.orders"}
        entity.entityid = "stream.orders"

        mapper = FabricEntityNameMapper(config, logger_provider)
        result = mapper.get_table_name(entity)
        assert result == "sales.orders"

    def test_uses_configured_catalog_and_schema(self):
        values = {"kindling.fabric.catalog": "lake", "kindling.fabric.schema": "silver"}
        config = MagicMock()
        config.get.side_effect = lambda key: values.get(key)

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        entity = MagicMock()
        entity.tags = {}
        entity.entityid = "stream.orders"

        mapper = FabricEntityNameMapper(config, logger_provider)
        result = mapper.get_table_name(entity)
        assert result == "lake.silver.stream_orders"

    def test_falls_back_to_normalized_entity_id(self):
        config = MagicMock()
        config.get.return_value = None

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        entity = MagicMock()
        entity.tags = {}
        entity.entityid = "stream.orders"

        mapper = FabricEntityNameMapper(config, logger_provider)
        result = mapper.get_table_name(entity)
        assert result == "stream_orders"


class TestFabricEntityPathLocator:
    def test_returns_provider_path_tag_when_present(self):
        config = MagicMock()
        config.get.return_value = None

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        entity = MagicMock()
        entity.tags = {"provider.path": "Tables/custom/orders"}
        entity.entityid = "stream.orders"

        locator = FabricEntityPathLocator(config, logger_provider)
        result = locator.get_table_path(entity)
        assert result == "Tables/custom/orders"

    def test_uses_configured_table_root(self):
        values = {"kindling.fabric.table_root": "Tables/base"}
        config = MagicMock()
        config.get.side_effect = lambda key: values.get(key)

        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()

        entity = MagicMock()
        entity.tags = {}
        entity.entityid = "stream.orders"

        locator = FabricEntityPathLocator(config, logger_provider)
        result = locator.get_table_path(entity)
        assert result == "Tables/base/stream/orders"
