from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_current_view import CurrentViewEntityProvider


class Expr:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return Expr(f"{self.value} = {other}")

    def __str__(self):
        return self.value


@pytest.fixture(autouse=True)
def patch_spark_sql_functions(monkeypatch):
    monkeypatch.setattr(
        "kindling.entity_provider_current_view.col", lambda name: Expr(f"col({name})")
    )
    monkeypatch.setattr(
        "kindling.entity_provider_current_view.lit", lambda value: Expr(f"lit({value})")
    )


def _entity(entityid, tags=None):
    return EntityMetadata(
        entityid=entityid,
        name=entityid,
        merge_columns=["id"],
        tags=tags or {},
        schema=StructType([StructField("id", StringType(), False)]),
    )


def _provider():
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    return CurrentViewEntityProvider(logger_provider)


def _wire_provider(monkeypatch, companion, base=None, base_provider=None):
    base = base or _entity("orders", {"scd.type": "2"})
    base_provider = base_provider or MagicMock()
    base_df = MagicMock()
    filtered_df = MagicMock()
    base_provider.read_entity.return_value = base_df
    base_df.filter.return_value = filtered_df

    entity_manager = MagicMock()
    entity_manager.get_entity_definition.return_value = base
    provider_registry = MagicMock()
    provider_registry.get_provider_for_entity.return_value = base_provider

    def fake_get(cls):
        if cls.__name__ == "DataEntityManager":
            return entity_manager
        if cls.__name__ == "EntityProviderRegistry":
            return provider_registry
        raise AssertionError(f"Unexpected injector lookup for {cls}")

    monkeypatch.setattr("kindling.entity_provider_current_view.GlobalInjector.get", fake_get)
    return base, base_provider, base_df, filtered_df, entity_manager, provider_registry


def test_read_entity_filters_to_is_current_true(monkeypatch):
    companion = _entity("orders.current", {"scd.companion_of": "orders"})
    _, _, base_df, filtered_df, _, _ = _wire_provider(monkeypatch, companion)

    result = _provider().read_entity(companion)

    assert result is filtered_df
    base_df.filter.assert_called_once()
    assert "__is_current" in str(base_df.filter.call_args.args[0])


def test_read_entity_uses_is_current_column_from_scd_config(monkeypatch):
    companion = _entity("orders.current", {"scd.companion_of": "orders"})
    base = _entity("orders", {"scd.type": "2", "scd.current_col": "current_flag"})
    _, _, base_df, _, _, _ = _wire_provider(monkeypatch, companion, base=base)

    _provider().read_entity(companion)

    assert "current_flag" in str(base_df.filter.call_args.args[0])


def test_read_entity_delegates_to_base_delta_provider(monkeypatch):
    companion = _entity("orders.current", {"scd.companion_of": "orders"})
    base, base_provider, _, _, entity_manager, provider_registry = _wire_provider(
        monkeypatch, companion
    )

    _provider().read_entity(companion)

    entity_manager.get_entity_definition.assert_called_once_with("orders")
    provider_registry.get_provider_for_entity.assert_called_once_with(base)
    base_provider.read_entity.assert_called_once_with(base)


def test_check_entity_exists_delegates_to_base_provider(monkeypatch):
    companion = _entity("orders.current", {"scd.companion_of": "orders"})
    base, base_provider, _, _, _, _ = _wire_provider(monkeypatch, companion)
    base_provider.check_entity_exists.return_value = True

    assert _provider().check_entity_exists(companion) is True
    base_provider.check_entity_exists.assert_called_once_with(base)


def test_merge_to_entity_raises_not_implemented_error():
    with pytest.raises(NotImplementedError, match="read-only"):
        _provider().merge_to_entity(MagicMock(), _entity("orders.current"))


def test_write_to_entity_raises_not_implemented_error():
    with pytest.raises(NotImplementedError, match="read-only"):
        _provider().write_to_entity(MagicMock(), _entity("orders.current"))


def test_append_to_entity_raises_not_implemented_error():
    with pytest.raises(NotImplementedError, match="read-only"):
        _provider().append_to_entity(MagicMock(), _entity("orders.current"))


def test_read_entity_missing_companion_of_tag_raises_value_error():
    with pytest.raises(ValueError, match="scd.companion_of"):
        _provider().read_entity(_entity("orders.current"))
