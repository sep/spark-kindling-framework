from unittest.mock import MagicMock

import pytest

from kindling.entity_provider_delta import (
    DeltaMergeStrategies,
    DeltaMergeStrategy,
    SCD1MergeStrategy,
)


class ExampleMergeStrategy(DeltaMergeStrategy):
    def apply(self, delta_table, df, entity, merge_condition):
        return None


def test_delta_merge_strategies_register_stores_class():
    name = "unit_test_strategy"
    original = dict(DeltaMergeStrategies._registry)
    DeltaMergeStrategies._registry.pop(name, None)

    try:
        registered = DeltaMergeStrategies.register(name)(ExampleMergeStrategy)

        assert registered is ExampleMergeStrategy
        assert DeltaMergeStrategies._registry[name] is ExampleMergeStrategy
    finally:
        DeltaMergeStrategies._registry = original


def test_delta_merge_strategies_register_duplicate_raises_value_error():
    with pytest.raises(ValueError, match="already registered"):
        DeltaMergeStrategies.register("scd1")(ExampleMergeStrategy)


def test_delta_merge_strategies_get_returns_fresh_instance_each_call():
    first = DeltaMergeStrategies.get("scd1")
    second = DeltaMergeStrategies.get("scd1")

    assert isinstance(first, SCD1MergeStrategy)
    assert isinstance(second, SCD1MergeStrategy)
    assert first is not second


def test_delta_merge_strategies_get_unknown_name_raises_value_error():
    with pytest.raises(ValueError, match="Unknown merge strategy"):
        DeltaMergeStrategies.get("missing")


def _apply_scd1_strategy():
    delta_table = MagicMock()
    aliased_delta = delta_table.alias.return_value
    merge_builder = aliased_delta.merge.return_value
    df = MagicMock()
    entity = MagicMock()

    SCD1MergeStrategy().apply(delta_table, df, entity, "old.`id` = new.`id`")

    return delta_table, df, merge_builder


def test_scd1_strategy_apply_calls_when_matched_update_all():
    _, _, merge_builder = _apply_scd1_strategy()

    merge_builder.whenMatchedUpdateAll.assert_called_once_with()


def test_scd1_strategy_apply_calls_when_not_matched_insert_all():
    _, _, merge_builder = _apply_scd1_strategy()

    merge_builder.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.assert_called_once_with()


def test_scd1_strategy_apply_calls_execute():
    _, _, merge_builder = _apply_scd1_strategy()

    merge_builder.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.assert_called_once_with()


def test_scd1_strategy_apply_uses_provided_merge_condition():
    delta_table, df, _ = _apply_scd1_strategy()

    delta_table.alias.assert_called_once_with("old")
    df.alias.assert_called_once_with("new")
    delta_table.alias.return_value.merge.assert_called_once_with(
        source=df.alias.return_value,
        condition="old.`id` = new.`id`",
    )
