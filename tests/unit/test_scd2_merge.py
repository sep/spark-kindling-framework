"""Unit tests for the Delta SCD2 staged MERGE (_execute_scd2_merge).

DataFrames are MagicMocks, so these tests assert on the MERGE that gets
built — clauses, conditions, staged actions — not on data. The data
behaviour is covered by tests/integration (test_scd2_declared_flow.py,
test_scd2_provider_parity.py, test_scd_untracked_parity.py).
"""

from unittest.mock import MagicMock

import pytest
from kindling.data_entities import EntityMetadata
from kindling.entity_provider_delta import _execute_scd2_merge
from pyspark.sql.types import StringType, StructField, StructType


class Expr:
    def __init__(self, value):
        self.value = value

    def cast(self, data_type):
        return Expr(f"CAST({self.value} AS {data_type})")

    def isNull(self):
        return Expr(f"{self.value} IS NULL")

    def __str__(self):
        return self.value


@pytest.fixture(autouse=True)
def patch_spark_sql_functions(monkeypatch):
    monkeypatch.setattr("kindling.entity_provider_delta.col", lambda name: Expr(f"col({name})"))
    monkeypatch.setattr("kindling.entity_provider_delta.lit", lambda value: Expr(f"lit({value})"))
    monkeypatch.setattr(
        "kindling.entity_provider_delta.concat_ws",
        lambda separator, *cols: Expr(
            f"concat_ws({separator}, {', '.join(str(col) for col in cols)})"
        ),
    )
    monkeypatch.setattr(
        "kindling.entity_provider_delta.struct",
        lambda *cols: Expr(f"struct({', '.join(str(col) for col in cols)})"),
    )
    monkeypatch.setattr(
        "kindling.entity_provider_delta.to_json", lambda expr: Expr(f"to_json({expr})")
    )
    monkeypatch.setattr(
        "kindling.entity_provider_delta.sha2",
        lambda expr, bits: Expr(f"sha2({expr}, {bits})"),
    )
    monkeypatch.setattr("kindling.entity_provider_delta.expr", lambda value: Expr(f"expr({value})"))
    monkeypatch.setattr("kindling.entity_provider_delta._capture_batch_micros", lambda spark: 1234)
    monkeypatch.setattr(
        "kindling.entity_provider_delta.coalesce",
        lambda *cols: Expr(f"COALESCE({', '.join(str(c) for c in cols)})"),
    )


COLUMNS = ["customer_id", "name", "status", "updated_at"]


def _entity(tags=None):
    return EntityMetadata(
        entityid="customer_dim",
        name="customer_dim",
        merge_columns=["customer_id"],
        tags={"scd.type": "2", **(tags or {})},
        schema=StructType([StructField(name, StringType(), True) for name in COLUMNS]),
    )


TEMPORAL = ["__effective_from", "__effective_to", "__is_current"]


def _mock_delta_table(target_columns=None):
    delta_table = MagicMock(name="delta_table")
    merge_builder = MagicMock(name="merge_builder")
    delta_table.toDF.return_value.columns = (
        list(COLUMNS) + TEMPORAL if target_columns is None else target_columns
    )
    delta_table.alias.return_value = delta_table
    merge_builder.withSchemaEvolution.return_value = merge_builder
    delta_table.merge.return_value = merge_builder
    merge_builder.whenMatchedUpdate.return_value = merge_builder
    merge_builder.whenNotMatchedInsert.return_value = merge_builder
    merge_builder.whenNotMatchedBySourceUpdate.return_value = merge_builder
    return delta_table, merge_builder


def _mock_source_df():
    df = MagicMock(name="source_df")
    df.columns = list(COLUMNS)
    df.schema = None
    # The null-sequence preflight finds no nulls.
    df.filter.return_value.head.return_value = []
    return df


def _execute(tags=None, target_columns=None):
    delta_table, merge_builder = _mock_delta_table(target_columns)
    df = _mock_source_df()
    _execute_scd2_merge(delta_table, df, _entity(tags))
    return delta_table, merge_builder, df


def _where_conditions(df):
    return [c.args[0] for c in df.mock_calls if c[0].endswith(".where")]


def _with_column_values(df, column):
    return [
        str(c.args[1]) for c in df.mock_calls if c[0].endswith("withColumn") and c.args[0] == column
    ]


def _matched_update(merge_builder, action):
    for call in merge_builder.whenMatchedUpdate.call_args_list:
        if call.kwargs["condition"] == f"staged.__scd2_action = '{action}'":
            return call.kwargs["set"]
    return None


# ── core MERGE shape ──────────────────────────────────────────────────────────


def test_execute_scd2_merge_calls_delta_table_merge_with_staged_source():
    delta_table, _, df = _execute()

    delta_table.merge.assert_called_once()
    assert any(c[0].endswith(".alias") and c.args == ("staged",) for c in df.mock_calls)


def test_every_when_matched_update_is_conditional():
    _, merge_builder, _ = _execute()

    for call in merge_builder.whenMatchedUpdate.call_args_list:
        assert call.kwargs.get("condition"), "every matched clause must be conditional"


def test_close_clause_closes_current_row_on_close_action():
    _, merge_builder, _ = _execute()

    assert _matched_update(merge_builder, "close") == {
        "`__effective_to`": "staged.`__scd2_effective_from`",
        "`__is_current`": "false",
    }


def test_insert_clause_is_conditional_on_insert_action():
    _, merge_builder, _ = _execute()

    kwargs = merge_builder.whenNotMatchedInsert.call_args.kwargs
    assert kwargs["condition"] == "staged.__scd2_action = 'insert'"


def test_insert_sets_is_current_true():
    _, merge_builder, _ = _execute()

    values = merge_builder.whenNotMatchedInsert.call_args.kwargs["values"]
    assert values["`__is_current`"] == "true"


def test_insert_sets_effective_from_to_the_staged_batch_timestamp():
    """Without sequence_by, effective_from is one processing-time instant
    captured on the driver and staged as a column, so insert, close and a
    surrogate version key all read the same value."""
    _, merge_builder, df = _execute()

    values = merge_builder.whenNotMatchedInsert.call_args.kwargs["values"]
    assert values["`__effective_from`"] == "staged.`__scd2_effective_from`"
    assert "expr(timestamp_micros(1234))" in _with_column_values(df, "__scd2_effective_from")


def test_insert_sets_effective_to_null():
    _, merge_builder, _ = _execute()

    values = merge_builder.whenNotMatchedInsert.call_args.kwargs["values"]
    assert values["`__effective_to`"] == "NULL"


def test_tracked_columns_default_excludes_keys_and_temporal():
    _, _, df = _execute()

    close_where = _where_conditions(df)[0]
    assert "source.`name`" in close_where
    assert "source.`status`" in close_where
    assert "source.`customer_id`" not in close_where
    assert "source.`__effective_from`" not in close_where


def test_without_untracked_columns_there_is_no_touch_clause():
    """No scd.untracked, no scd.tracked, no sequence_by: every non-key
    column is tracked, so nothing can be updated in place."""
    _, merge_builder, df = _execute()

    assert _matched_update(merge_builder, "touch") is None
    assert merge_builder.whenMatchedUpdate.call_count == 1
    assert "lit(touch)" not in _with_column_values(df, "__scd2_action")


def test_routing_key_hash_method_used_by_default():
    delta_table, _, df = _execute()

    merge_condition = delta_table.merge.call_args.kwargs["condition"]
    assert merge_condition.startswith("sha2(to_json(named_struct(")
    assert "target.`customer_id`" in merge_condition
    assert any("sha2" in value for value in _with_column_values(df, "__merge_key"))


def test_routing_key_concat_method_used_when_configured():
    delta_table, _, df = _execute({"scd.routing_key": "concat"})

    merge_condition = delta_table.merge.call_args.kwargs["condition"]
    assert merge_condition.startswith(
        "concat_ws('||', COALESCE(CAST(target.`customer_id` AS STRING), '__null__'))"
    )
    assert any("concat_ws" in value for value in _with_column_values(df, "__merge_key"))


def test_routing_key_concat_column_expr_uses_null_sentinel():
    _, _, df = _execute({"scd.routing_key": "concat"})

    keyed = [v for v in _with_column_values(df, "__merge_key") if "concat_ws" in v]
    assert keyed and all("COALESCE" in v and "__null__" in v for v in keyed)


# ── untracked columns: in-place updates ───────────────────────────────────────


def test_untracked_tag_adds_touch_clause_setting_untracked_columns():
    _, merge_builder, df = _execute({"scd.untracked": "updated_at"})

    assert _matched_update(merge_builder, "touch") == {"`updated_at`": "staged.`updated_at`"}
    assert "lit(touch)" in _with_column_values(df, "__scd2_action")


def test_untracked_columns_are_not_tracked():
    _, _, df = _execute({"scd.untracked": "updated_at"})

    close_where = _where_conditions(df)[0]
    assert "updated_at" not in close_where
    assert "source.`name`" in close_where


def test_touch_requires_no_tracked_change_and_an_untracked_change():
    _, _, df = _execute({"scd.untracked": "updated_at"})

    touch_where = _where_conditions(df)[1]
    assert touch_where.startswith("NOT (")
    assert "source.`updated_at`" in touch_where


def test_tracked_allow_list_updates_other_columns_in_place():
    """scd.tracked switches to in-place: columns outside the allow-list are
    set on the current version instead of being ignored."""
    _, merge_builder, _ = _execute({"scd.tracked": "status"})

    assert _matched_update(merge_builder, "touch") == {
        "`name`": "staged.`name`",
        "`updated_at`": "staged.`updated_at`",
    }


def test_sequence_by_is_updated_in_place():
    _, merge_builder, _ = _execute({"scd.sequence_by": "updated_at"})

    assert _matched_update(merge_builder, "touch") == {"`updated_at`": "staged.`updated_at`"}


def test_sequence_guard_compares_against_target_sequence_column():
    _, _, df = _execute({"scd.sequence_by": "updated_at", "scd.untracked": "name"})

    for condition in _where_conditions(df):
        assert "source.`updated_at` > target.`updated_at`" in condition
        assert "target.`__effective_from`" not in condition


def test_sequence_close_stamps_effective_to_from_staged_sequence():
    _, merge_builder, df = _execute({"scd.sequence_by": "updated_at"})

    assert (
        _matched_update(merge_builder, "close")["`__effective_to`"]
        == "staged.`__scd2_effective_from`"
    )
    assert "col(`updated_at`)" in _with_column_values(df, "__scd2_effective_from")


# ── close_on_missing (snapshot) ───────────────────────────────────────────────


def _execute_com(extra_tags=None):
    return _execute({"scd.close_on_missing": "true", **(extra_tags or {})})


def test_close_on_missing_calls_whenNotMatchedBySourceUpdate():
    _, merge_builder, _ = _execute_com()

    merge_builder.whenNotMatchedBySourceUpdate.assert_called_once()


def test_close_on_missing_whenNotMatchedBySourceUpdate_condition_targets_current_rows():
    _, merge_builder, _ = _execute_com()

    kwargs = merge_builder.whenNotMatchedBySourceUpdate.call_args.kwargs
    assert "__is_current" in kwargs["condition"]


def test_close_on_missing_whenNotMatchedBySourceUpdate_set_closes_row():
    _, merge_builder, _ = _execute_com()

    kwargs = merge_builder.whenNotMatchedBySourceUpdate.call_args.kwargs
    assert kwargs["set"]["`__effective_to`"] == "current_timestamp()"
    assert kwargs["set"]["`__is_current`"] == "false"


def test_close_on_missing_stages_keep_sentinels_for_unchanged_keys():
    _, merge_builder, df = _execute_com()

    assert "lit(keep)" in _with_column_values(df, "__scd2_action")
    # A KEEP row matches but takes no clause.
    assert _matched_update(merge_builder, "keep") is None


def test_close_on_missing_sentinels_exclude_touched_rows():
    _, merge_builder, df = _execute_com({"scd.untracked": "updated_at"})

    sentinel_where = _where_conditions(df)[2]
    assert sentinel_where.startswith("NOT (")
    assert "source.`updated_at`" in sentinel_where
    assert _matched_update(merge_builder, "touch") is not None


def test_close_on_missing_false_does_not_call_whenNotMatchedBySourceUpdate():
    _, merge_builder, _ = _execute()

    merge_builder.whenNotMatchedBySourceUpdate.assert_not_called()


# ── optimize_unchanged ────────────────────────────────────────────────────────


def test_optimize_unchanged_adds_src_hash_column_to_df():
    _, _, df = _execute({"scd.optimize_unchanged": "true"})

    col_names = [call.args[0] for call in df.withColumn.call_args_list]
    assert "__scd2_src_hash" in col_names


def test_optimize_unchanged_false_does_not_add_src_hash_column():
    _, _, df = _execute()

    col_names = [call.args[0] for call in df.withColumn.call_args_list]
    assert "__scd2_src_hash" not in col_names


def test_optimize_unchanged_hash_expr_uses_sha2():
    _, _, df = _execute({"scd.optimize_unchanged": "true"})

    hash_call = next(c for c in df.withColumn.call_args_list if c.args[0] == "__scd2_src_hash")
    assert "sha2" in str(hash_call.args[1])


def test_optimize_unchanged_classifies_tracked_change_by_hash():
    _, _, df = _execute({"scd.optimize_unchanged": "true", "scd.close_on_missing": "true"})

    conditions = _where_conditions(df)
    assert "source.__scd2_src_hash != sha2(" in conditions[0]
    # The sentinel is NOT(changed) on the same hash comparison.
    assert "source.__scd2_src_hash" in conditions[-1]
    assert conditions[-1].startswith("NOT (")


def test_optimize_unchanged_with_untracked_touch_uses_hash_for_tracked_part():
    _, merge_builder, df = _execute(
        {"scd.optimize_unchanged": "true", "scd.untracked": "updated_at"}
    )

    touch_where = _where_conditions(df)[1]
    assert touch_where.startswith("NOT (source.__scd2_src_hash != ")
    assert _matched_update(merge_builder, "touch") == {"`updated_at`": "staged.`updated_at`"}


# ── reserved staging columns and additive schema ─────────────────────────────


@pytest.mark.parametrize("column", ["__merge_key", "__scd2_action", "__scd2_src_hash"])
def test_incoming_reserved_staging_column_is_rejected(column):
    delta_table, _ = _mock_delta_table()
    df = _mock_source_df()
    df.columns = list(COLUMNS) + [column]

    with pytest.raises(ValueError, match="reserved"):
        _execute_scd2_merge(delta_table, df, _entity())
    delta_table.merge.assert_not_called()


def test_without_new_columns_the_merge_does_not_evolve_the_schema():
    _, merge_builder, _ = _execute()

    merge_builder.withSchemaEvolution.assert_not_called()


def test_column_new_to_the_table_evolves_the_schema_and_compares_to_typed_null():
    target = ["customer_id", "name", "status"] + TEMPORAL  # no updated_at yet
    delta_table, merge_builder = _mock_delta_table(target)
    df = _mock_source_df()
    df.schema = StructType([StructField(name, StringType(), True) for name in COLUMNS])

    _execute_scd2_merge(delta_table, df, _entity({"scd.untracked": "updated_at"}))

    merge_builder.withSchemaEvolution.assert_called_once_with()
    target_df = delta_table.toDF.return_value
    target_df.withColumn.assert_called_once()
    name, value = target_df.withColumn.call_args.args
    assert name == "updated_at" and str(value) == "CAST(lit(None) AS StringType())"
    assert _matched_update(merge_builder, "touch") == {"`updated_at`": "staged.`updated_at`"}
