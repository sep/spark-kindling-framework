from types import SimpleNamespace
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


def _plain_entity(tags=None):
    """An entity without scd.untracked: the SCD1 merge stays unconditional."""
    return SimpleNamespace(entityid="silver.plain", merge_columns=["id"], tags=tags or {})


def _apply_scd1_strategy():
    delta_table = MagicMock()
    aliased_delta = delta_table.alias.return_value
    merge_builder = aliased_delta.merge.return_value
    df = MagicMock()
    entity = _plain_entity()

    SCD1MergeStrategy().apply(delta_table, df, entity, "old.`id` = new.`id`")

    return delta_table, df, merge_builder


def test_scd1_strategy_apply_enables_schema_evolution():
    _, _, merge_builder = _apply_scd1_strategy()

    merge_builder.withSchemaEvolution.assert_called_once_with()


def test_scd1_strategy_apply_calls_when_matched_update_all():
    _, _, merge_builder = _apply_scd1_strategy()

    merge_builder.withSchemaEvolution.return_value.whenMatchedUpdateAll.assert_called_once_with()


def test_scd1_strategy_apply_calls_when_not_matched_insert_all():
    _, _, merge_builder = _apply_scd1_strategy()

    merge_builder.withSchemaEvolution.return_value.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.assert_called_once_with()


def test_scd1_strategy_apply_calls_execute():
    _, _, merge_builder = _apply_scd1_strategy()

    merge_builder.withSchemaEvolution.return_value.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.assert_called_once_with()


def test_scd1_strategy_apply_uses_provided_merge_condition():
    delta_table, df, _ = _apply_scd1_strategy()

    delta_table.alias.assert_called_once_with("old")
    df.alias.assert_called_once_with("new")
    delta_table.alias.return_value.merge.assert_called_once_with(
        source=df.alias.return_value,
        condition="old.`id` = new.`id`",
    )


def _apply_scd1_strategy_legacy_delta(existing_conf_value=None):
    """Apply SCD1 against a merge builder WITHOUT withSchemaEvolution
    (Delta < 3.2), returning the builder and the session conf mock."""
    delta_table = MagicMock()
    aliased_delta = delta_table.alias.return_value
    merge_builder = aliased_delta.merge.return_value
    del merge_builder.withSchemaEvolution  # hasattr() -> False

    df = MagicMock()
    conf = df.sparkSession.conf
    conf.get.return_value = existing_conf_value
    entity = _plain_entity()

    SCD1MergeStrategy().apply(delta_table, df, entity, "old.`id` = new.`id`")

    return merge_builder, conf


def test_scd1_strategy_legacy_delta_executes_merge_chain():
    merge_builder, _ = _apply_scd1_strategy_legacy_delta()

    merge_builder.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.assert_called_once_with()


def test_scd1_strategy_legacy_delta_enables_automerge_conf():
    _, conf = _apply_scd1_strategy_legacy_delta()

    conf.set.assert_called_once_with("spark.databricks.delta.schema.autoMerge.enabled", "true")


def test_scd1_strategy_legacy_delta_unsets_conf_when_previously_absent():
    _, conf = _apply_scd1_strategy_legacy_delta(existing_conf_value=None)

    conf.unset.assert_called_once_with("spark.databricks.delta.schema.autoMerge.enabled")


def test_scd1_strategy_legacy_delta_restores_previous_conf_value():
    _, conf = _apply_scd1_strategy_legacy_delta(existing_conf_value="false")

    conf.unset.assert_not_called()
    assert conf.set.call_args_list[-1].args == (
        "spark.databricks.delta.schema.autoMerge.enabled",
        "false",
    )


def test_scd1_strategy_legacy_delta_restores_conf_even_when_merge_fails():
    delta_table = MagicMock()
    merge_builder = delta_table.alias.return_value.merge.return_value
    del merge_builder.withSchemaEvolution
    merge_builder.whenMatchedUpdateAll.return_value.whenNotMatchedInsertAll.return_value.execute.side_effect = RuntimeError(
        "merge failed"
    )

    df = MagicMock()
    conf = df.sparkSession.conf
    conf.get.return_value = None

    with pytest.raises(RuntimeError, match="merge failed"):
        SCD1MergeStrategy().apply(delta_table, df, _plain_entity(), "old.`id` = new.`id`")

    conf.unset.assert_called_once_with("spark.databricks.delta.schema.autoMerge.enabled")


# ── scd.untracked change condition ────────────────────────────────────────────


def _apply_scd1_untracked(target_columns, legacy=False):
    delta_table = MagicMock()
    delta_table.toDF.return_value.columns = target_columns
    merge_builder = delta_table.alias.return_value.merge.return_value
    if legacy:
        del merge_builder.withSchemaEvolution
    df = MagicMock()
    df.columns = ["id", "name", "ingested_at"]
    df.schema = None
    df.sparkSession.conf.get.return_value = None
    entity = _plain_entity({"scd.untracked": "ingested_at"})

    SCD1MergeStrategy().apply(delta_table, df, entity, "old.`id` = new.`id`")
    if legacy:
        return merge_builder
    return merge_builder.withSchemaEvolution.return_value


def test_scd1_untracked_updates_matched_rows_only_on_tracked_change():
    builder = _apply_scd1_untracked(["id", "name", "ingested_at"])

    condition = builder.whenMatchedUpdateAll.call_args.kwargs["condition"]
    assert condition == "(NOT (new.`name` <=> old.`name`))"


def test_scd1_untracked_condition_applies_on_legacy_delta_too():
    builder = _apply_scd1_untracked(["id", "name", "ingested_at"], legacy=True)

    condition = builder.whenMatchedUpdateAll.call_args.kwargs["condition"]
    assert "new.`name`" in condition
    assert "ingested_at" not in condition


def test_scd1_untracked_tracked_column_new_to_target_drops_condition():
    """A tracked column the table lacks must materialize on every matched row."""
    builder = _apply_scd1_untracked(["id", "ingested_at"])

    builder.whenMatchedUpdateAll.assert_called_once_with()
