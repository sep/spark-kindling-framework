from unittest.mock import MagicMock

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_delta import DeltaEntityProvider


class Expr:
    def __init__(self, value):
        self.value = value

    def cast(self, dtype):
        return Expr(f"CAST({self.value} AS {dtype})")

    def __le__(self, other):
        return Expr(f"{self.value} <= {other}")

    def __gt__(self, other):
        return Expr(f"{self.value} > {other}")

    def __and__(self, other):
        return Expr(f"({self.value} AND {other.value})")

    def __or__(self, other):
        return Expr(f"({self.value} OR {other.value})")

    def isNull(self):
        return Expr(f"{self.value} IS NULL")

    def __str__(self):
        return self.value


@pytest.fixture()
def patch_functions(monkeypatch):
    monkeypatch.setattr("kindling.entity_provider_delta.col", lambda name: Expr(f"col({name})"))
    monkeypatch.setattr("kindling.entity_provider_delta.lit", lambda value: Expr(f"lit({value!r})"))


def _scd2_entity():
    return EntityMetadata(
        entityid="silver.customer",
        name="Customer",
        merge_columns=["customer_id"],
        tags={"scd.type": "2"},
        schema=StructType([StructField("customer_id", StringType(), False)]),
    )


def _make_provider(monkeypatch):
    provider = object.__new__(DeltaEntityProvider)
    provider.config = MagicMock()
    provider.config.get.return_value = "catalog"
    provider.access_mode = "catalog"
    provider.epl = MagicMock()
    provider.enm = MagicMock()
    provider.spark = MagicMock()
    provider.logger = MagicMock()
    provider._signal_emitter = None

    base_df = MagicMock()
    base_df.filter.return_value = base_df
    provider.read_entity = MagicMock(return_value=base_df)

    return provider, base_df


def test_read_entity_as_of_scd2_uses_lit_cast_timestamp(patch_functions, monkeypatch):
    provider, base_df = _make_provider(monkeypatch)
    entity = _scd2_entity()

    provider.read_entity_as_of(entity, "2024-01-15 00:00:00")

    assert base_df.filter.called
    filter_arg = str(base_df.filter.call_args.args[0])
    assert "lit('2024-01-15 00:00:00')" in filter_arg
    assert "CAST" in filter_arg
    assert "timestamp" in filter_arg


def test_read_entity_as_of_non_scd2_uses_delta_time_travel(monkeypatch):
    provider, _ = _make_provider(monkeypatch)
    entity = EntityMetadata(
        entityid="silver.customer",
        name="Customer",
        merge_columns=[],
        tags={},
        schema=StructType([StructField("customer_id", StringType(), False)]),
    )
    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    provider.spark.read = mock_reader
    provider._get_table_reference = MagicMock(return_value=MagicMock())
    provider._get_table_reference.return_value.get_read_path.return_value = "/some/path"

    provider.read_entity_as_of(entity, "2024-01-15 00:00:00")

    mock_reader.option.assert_called_once_with("timestampAsOf", "2024-01-15 00:00:00")
