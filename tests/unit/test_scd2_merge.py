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
    assert merge_condition.startswith("concat_ws('||', CAST(target.`customer_id` AS STRING))")
    routing_expr = frames["rows_to_close_or_insert"].withColumn.call_args.args[1]
    assert "concat_ws" in str(routing_expr)


def test_execute_scd2_merge_does_not_implement_close_on_missing_in_phase_1():
    _, merge_builder, _, _ = _execute({"scd.close_on_missing": "true"})

    merge_builder.whenNotMatchedBySourceUpdate.assert_not_called()
