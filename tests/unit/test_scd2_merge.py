from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_delta import _execute_scd2_merge


class Expr:
    def __init__(self, value):
        self.value = value

    def cast(self, data_type):
        return Expr(f"CAST({self.value} AS {data_type})")

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
    monkeypatch.setattr(
        "kindling.entity_provider_delta.coalesce",
        lambda *cols: Expr(f"COALESCE({', '.join(str(c) for c in cols)})"),
    )


def _entity(tags=None):
    return EntityMetadata(
        entityid="customer_dim",
        name="customer_dim",
        merge_columns=["customer_id"],
        tags={"scd.type": "2", **(tags or {})},
        schema=StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("status", StringType(), True),
            ]
        ),
    )


def _mock_source_df():
    df = MagicMock(name="source_df")
    df.columns = ["customer_id", "name", "status"]

    joined_changed = MagicMock(name="joined_changed")
    filtered_changed = MagicMock(name="filtered_changed")
    changed_rows = MagicMock(name="changed_rows")
    new_rows = MagicMock(name="new_rows")
    rows_to_close_or_insert = MagicMock(name="rows_to_close_or_insert")
    insert_rows = MagicMock(name="insert_rows")
    keyed_rows = MagicMock(name="keyed_rows")
    staged = MagicMock(name="staged")

    df.alias.return_value = df
    df.join.side_effect = [joined_changed, new_rows]
    joined_changed.where.return_value = filtered_changed
    filtered_changed.select.return_value = changed_rows
    changed_rows.unionByName.return_value = rows_to_close_or_insert
    changed_rows.withColumn.return_value = insert_rows
    rows_to_close_or_insert.withColumn.return_value = keyed_rows
    insert_rows.unionByName.return_value = staged
    staged.alias.return_value = staged

    return df, {
        "changed_rows": changed_rows,
        "new_rows": new_rows,
        "rows_to_close_or_insert": rows_to_close_or_insert,
        "insert_rows": insert_rows,
        "keyed_rows": keyed_rows,
        "staged": staged,
        "joined_changed": joined_changed,
    }


def _mock_delta_table():
    delta_table = MagicMock(name="delta_table")
    target_df = MagicMock(name="target_df")
    filtered_target = MagicMock(name="filtered_target")
    merge_builder = MagicMock(name="merge_builder")

    delta_table.toDF.return_value = target_df
    target_df.filter.return_value = filtered_target
    filtered_target.alias.return_value = filtered_target
    delta_table.alias.return_value = delta_table
    delta_table.merge.return_value = merge_builder
    merge_builder.whenMatchedUpdate.return_value = merge_builder
    merge_builder.whenNotMatchedInsert.return_value = merge_builder
    merge_builder.whenNotMatchedBySourceUpdate.return_value = merge_builder

    return delta_table, merge_builder


def _execute(tags=None):
    delta_table, merge_builder = _mock_delta_table()
    df, frames = _mock_source_df()
    _execute_scd2_merge(delta_table, df, _entity(tags))
    return delta_table, merge_builder, df, frames


def test_execute_scd2_merge_calls_delta_table_merge():
    delta_table, _, _, frames = _execute()

    delta_table.merge.assert_called_once()
    assert delta_table.merge.call_args.kwargs["source"] is frames["staged"]


def test_execute_scd2_merge_when_matched_update_closes_current_row():
    _, merge_builder, _, _ = _execute()

    merge_builder.whenMatchedUpdate.assert_called_once_with(
        set={"`__effective_to`": "current_timestamp()", "`__is_current`": "false"}
    )


def test_execute_scd2_merge_when_not_matched_insert_sets_is_current_true():
    _, merge_builder, _, _ = _execute()

    values = merge_builder.whenNotMatchedInsert.call_args.kwargs["values"]
    assert values["`__is_current`"] == "true"


def test_execute_scd2_merge_when_not_matched_insert_sets_effective_from_to_current_timestamp():
    _, merge_builder, _, _ = _execute()

    values = merge_builder.whenNotMatchedInsert.call_args.kwargs["values"]
    assert values["`__effective_from`"] == "current_timestamp()"


def test_execute_scd2_merge_when_not_matched_insert_sets_effective_to_null():
    _, merge_builder, _, _ = _execute()

    values = merge_builder.whenNotMatchedInsert.call_args.kwargs["values"]
    assert values["`__effective_to`"] == "NULL"


def test_execute_scd2_merge_tracked_columns_default_excludes_keys_and_temporal():
    _, _, _, frames = _execute()

    where_condition = frames["joined_changed"].where.call_args.args[0]
    assert "source.`name`" in where_condition
    assert "source.`status`" in where_condition
    assert "source.`customer_id`" not in where_condition
    assert "source.`__effective_from`" not in where_condition


def test_execute_scd2_merge_routing_key_hash_method_used_by_default():
    delta_table, _, _, frames = _execute()

    merge_condition = delta_table.merge.call_args.kwargs["condition"]
    assert merge_condition.startswith("sha2(to_json(named_struct(")
    assert "target.`customer_id`" in merge_condition
    routing_expr = frames["rows_to_close_or_insert"].withColumn.call_args.args[1]
    assert "sha2" in str(routing_expr)


def test_execute_scd2_merge_routing_key_concat_method_used_when_configured():
    delta_table, _, _, frames = _execute({"scd.routing_key": "concat"})

    merge_condition = delta_table.merge.call_args.kwargs["condition"]
    assert merge_condition.startswith(
        "concat_ws('||', COALESCE(CAST(target.`customer_id` AS STRING), '__null__'))"
    )
    routing_expr = frames["rows_to_close_or_insert"].withColumn.call_args.args[1]
    assert "concat_ws" in str(routing_expr)


def test_execute_scd2_merge_routing_key_concat_column_expr_uses_null_sentinel():
    _, _, _, frames = _execute({"scd.routing_key": "concat"})

    routing_expr = frames["rows_to_close_or_insert"].withColumn.call_args.args[1]
    assert "COALESCE" in str(routing_expr)
    assert "__null__" in str(routing_expr)


# ── close_on_missing tests ────────────────────────────────────────────────────


def _mock_source_df_close_on_missing():
    """Mock source DataFrame for close_on_missing=True (3 joins: changed, new, unchanged)."""
    df = MagicMock(name="source_df")
    df.columns = ["customer_id", "name", "status"]

    join_changed = MagicMock(name="join_changed")
    new_rows = MagicMock(name="new_rows")
    join_unchanged = MagicMock(name="join_unchanged")
    changed_rows = MagicMock(name="changed_rows")
    unchanged_rows = MagicMock(name="unchanged_rows")

    df.alias.return_value = df
    df.join.side_effect = [join_changed, new_rows, join_unchanged]
    join_changed.where.return_value.select.return_value = changed_rows
    join_unchanged.where.return_value.select.return_value = unchanged_rows

    rows_to_close_or_insert = MagicMock(name="rows_to_close_or_insert")
    changed_rows.unionByName.return_value = rows_to_close_or_insert

    insert_rows_wk = MagicMock(name="insert_rows_wk")
    insert_rows = MagicMock(name="insert_rows")
    keyed_rows_wk = MagicMock(name="keyed_rows_wk")
    keyed_rows = MagicMock(name="keyed_rows")
    sentinels_wk = MagicMock(name="sentinels_wk")
    sentinels = MagicMock(name="sentinels")

    changed_rows.withColumn.return_value = insert_rows_wk
    insert_rows_wk.withColumn.return_value = insert_rows
    rows_to_close_or_insert.withColumn.return_value = keyed_rows_wk
    keyed_rows_wk.withColumn.return_value = keyed_rows
    unchanged_rows.withColumn.return_value = sentinels_wk
    sentinels_wk.withColumn.return_value = sentinels

    keyed_plus_sentinels = MagicMock(name="keyed_plus_sentinels")
    staged = MagicMock(name="staged")
    keyed_rows.unionByName.return_value = keyed_plus_sentinels
    insert_rows.unionByName.return_value = staged
    staged.alias.return_value = staged

    return df


def _execute_com(extra_tags=None):
    """Run _execute_scd2_merge with close_on_missing=True."""
    delta_table, merge_builder = _mock_delta_table()
    df = _mock_source_df_close_on_missing()
    tags = {"scd.close_on_missing": "true", **(extra_tags or {})}
    _execute_scd2_merge(delta_table, df, _entity(tags))
    return delta_table, merge_builder, df


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


def test_close_on_missing_whenMatchedUpdate_has_changed_column_condition():
    _, merge_builder, _ = _execute_com()

    kwargs = merge_builder.whenMatchedUpdate.call_args.kwargs
    assert "__scd2_changed" in kwargs["condition"]


def test_close_on_missing_false_does_not_call_whenNotMatchedBySourceUpdate():
    _, merge_builder, _, _ = _execute()

    merge_builder.whenNotMatchedBySourceUpdate.assert_not_called()


def test_close_on_missing_false_whenMatchedUpdate_has_no_condition():
    _, merge_builder, _, _ = _execute()

    merge_builder.whenMatchedUpdate.assert_called_once_with(
        set={"`__effective_to`": "current_timestamp()", "`__is_current`": "false"}
    )


# ── optimize_unchanged tests ──────────────────────────────────────────────────


def _mock_source_df_optimize_unchanged():
    """Mock source DataFrame for optimize_unchanged=True (hash-based changed_rows)."""
    df = MagicMock(name="source_df")
    df.columns = ["customer_id", "name", "status"]

    source_hashed = MagicMock(name="source_hashed")
    new_rows = MagicMock(name="new_rows")
    changed_rows = MagicMock(name="changed_rows")
    rows_to_close_or_insert = MagicMock(name="rows_to_close_or_insert")
    insert_rows = MagicMock(name="insert_rows")
    keyed_rows = MagicMock(name="keyed_rows")
    staged = MagicMock(name="staged")

    df.withColumn.return_value = source_hashed
    df.join.return_value = new_rows

    source_hashed.alias.return_value = source_hashed
    source_hashed.join.return_value.where.return_value.select.return_value = changed_rows

    changed_rows.unionByName.return_value = rows_to_close_or_insert
    changed_rows.withColumn.return_value = insert_rows
    rows_to_close_or_insert.withColumn.return_value = keyed_rows
    insert_rows.unionByName.return_value = staged
    staged.alias.return_value = staged

    return df


def _execute_ou(extra_tags=None):
    """Run _execute_scd2_merge with optimize_unchanged=True."""
    delta_table, merge_builder = _mock_delta_table()
    df = _mock_source_df_optimize_unchanged()
    tags = {"scd.optimize_unchanged": "true", **(extra_tags or {})}
    _execute_scd2_merge(delta_table, df, _entity(tags))
    return delta_table, merge_builder, df


def test_optimize_unchanged_adds_src_hash_column_to_df():
    _, _, df = _execute_ou()

    col_names = [call.args[0] for call in df.withColumn.call_args_list]
    assert "__scd2_src_hash" in col_names


def test_optimize_unchanged_false_does_not_add_src_hash_column():
    _, _, df, _ = _execute()

    col_names = [call.args[0] for call in df.withColumn.call_args_list]
    assert "__scd2_src_hash" not in col_names


def test_optimize_unchanged_hash_expr_uses_sha2():
    _, _, df = _execute_ou()

    hash_call = next(c for c in df.withColumn.call_args_list if c.args[0] == "__scd2_src_hash")
    assert "sha2" in str(hash_call.args[1])


def test_optimize_unchanged_does_not_call_whenNotMatchedBySourceUpdate():
    _, merge_builder, _ = _execute_ou()

    merge_builder.whenNotMatchedBySourceUpdate.assert_not_called()


def test_optimize_unchanged_whenMatchedUpdate_has_no_condition():
    _, merge_builder, _ = _execute_ou()

    merge_builder.whenMatchedUpdate.assert_called_once_with(
        set={"`__effective_to`": "current_timestamp()", "`__is_current`": "false"}
    )


# ── both flags together ───────────────────────────────────────────────────────


def _mock_source_df_both_flags():
    """Mock for optimize_unchanged=True + close_on_missing=True.

    source_hashed.join is called twice:
      call 1 → hash != tgt_hash → changed_rows (where clause differs)
      call 2 → hash == tgt_hash → unchanged_rows
    """
    df = MagicMock(name="source_df")
    df.columns = ["customer_id", "name", "status"]

    source_hashed = MagicMock(name="source_hashed")
    new_rows = MagicMock(name="new_rows")
    changed_rows = MagicMock(name="changed_rows")
    unchanged_rows = MagicMock(name="unchanged_rows")

    df.withColumn.return_value = source_hashed
    df.join.return_value = new_rows

    join_changed = MagicMock(name="join_changed")
    join_unchanged = MagicMock(name="join_unchanged")
    source_hashed.alias.return_value = source_hashed
    source_hashed.join.side_effect = [join_changed, join_unchanged]
    join_changed.where.return_value.select.return_value = changed_rows
    join_unchanged.where.return_value.select.return_value = unchanged_rows

    rows_to_close_or_insert = MagicMock(name="rows_to_close_or_insert")
    changed_rows.unionByName.return_value = rows_to_close_or_insert

    insert_rows_wk = MagicMock(name="insert_rows_wk")
    insert_rows = MagicMock(name="insert_rows")
    keyed_rows_wk = MagicMock(name="keyed_rows_wk")
    keyed_rows = MagicMock(name="keyed_rows")
    sentinels_wk = MagicMock(name="sentinels_wk")
    sentinels = MagicMock(name="sentinels")

    changed_rows.withColumn.return_value = insert_rows_wk
    insert_rows_wk.withColumn.return_value = insert_rows
    rows_to_close_or_insert.withColumn.return_value = keyed_rows_wk
    keyed_rows_wk.withColumn.return_value = keyed_rows
    unchanged_rows.withColumn.return_value = sentinels_wk
    sentinels_wk.withColumn.return_value = sentinels

    keyed_plus_sentinels = MagicMock(name="keyed_plus_sentinels")
    staged = MagicMock(name="staged")
    keyed_rows.unionByName.return_value = keyed_plus_sentinels
    insert_rows.unionByName.return_value = staged
    staged.alias.return_value = staged

    return df


def _execute_both():
    delta_table, merge_builder = _mock_delta_table()
    df = _mock_source_df_both_flags()
    tags = {"scd.close_on_missing": "true", "scd.optimize_unchanged": "true"}
    _execute_scd2_merge(delta_table, df, _entity(tags))
    return delta_table, merge_builder, df


def test_both_flags_calls_whenNotMatchedBySourceUpdate():
    _, merge_builder, _ = _execute_both()

    merge_builder.whenNotMatchedBySourceUpdate.assert_called_once()


def test_both_flags_uses_hash_for_changed_rows():
    _, _, df = _execute_both()

    col_names = [call.args[0] for call in df.withColumn.call_args_list]
    assert "__scd2_src_hash" in col_names


def test_both_flags_sentinel_uses_second_hash_join_not_change_condition():
    """Unchanged rows come from hash comparison, not _build_null_safe_change_condition."""
    _, _, df = _execute_both()

    source_hashed = df.withColumn.return_value
    assert source_hashed.join.call_count == 2
