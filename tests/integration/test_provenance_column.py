"""Provenance stamping: the column expression on a real (local) Spark session."""

from datetime import datetime

import pytest
from kindling.data_entities import EntityMetadata
from kindling.provenance import (
    ProvenanceStamp,
    drop_provenance,
    provenance_column_expr,
    provenance_struct_type,
    stamp_provenance,
)
from pyspark.sql.types import StringType, StructField, StructType

SCHEMA = StructType([StructField("id", StringType(), False)])


def _entity(tags=None, entityid="silver.orders"):
    return EntityMetadata(entityid, "orders", ["id"], dict(tags or {}), SCHEMA)


@pytest.fixture
def df(spark_session):
    return spark_session.createDataFrame([("a",), ("b",)], SCHEMA)


def test_column_has_the_contract_type_and_values(spark_session, df):
    stamp = ProvenanceStamp(
        pipe_id="p",
        pipe_name="Pipe",
        execution_id="e-1",
        persist_id="w-1",
        processed_at=datetime(2026, 10, 21, 12, 0, 0),
        mode="batch",
        input_entity_ids=("bronze.a", "bronze.b"),
        tags=(("owner", "sales"),),
    )
    out = df.withColumn("__provenance", provenance_column_expr(stamp))
    assert out.schema["__provenance"].dataType == provenance_struct_type()
    row = out.orderBy("id").first()["__provenance"]
    assert row["pipe_id"] == "p"
    assert row["pipe_name"] == "Pipe"
    assert row["run_id"] is None
    assert row["execution_id"] == "e-1"
    assert row["persist_id"] == "w-1"
    assert row["processed_at"] == datetime(2026, 10, 21, 12, 0, 0)
    assert row["mode"] == "batch"
    assert row["input_entity_ids"] == ["bronze.a", "bronze.b"]
    assert row["input_versions"] is None
    assert row["tags"] == {"owner": "sales"}
    assert row["schema_version"] == 1


def test_empty_stamp_is_typed_nulls(spark_session, df):
    out = df.withColumn("__provenance", provenance_column_expr(ProvenanceStamp()))
    assert out.schema["__provenance"].dataType == provenance_struct_type()
    row = out.first()["__provenance"]
    assert row["pipe_id"] is None
    assert row["processed_at"] is not None  # current_timestamp() at write
    assert row["input_entity_ids"] == []
    assert row["tags"] == {}


def test_stamp_only_when_enabled(df):
    assert stamp_provenance(df, _entity(), lambda: pytest.fail("not called")) is df
    stamped = stamp_provenance(df, _entity({"provenance.enabled": "true"}), ProvenanceStamp)
    assert stamped.columns == ["id", "__provenance"]


def test_stamp_refuses_to_replace_an_existing_column(df):
    entity = _entity({"provenance.enabled": "true"})
    upstream = df.withColumn("__PROVENANCE", provenance_column_expr(ProvenanceStamp()))
    with pytest.raises(ValueError, match="already has a column '__PROVENANCE'"):
        stamp_provenance(upstream, entity, ProvenanceStamp)


def test_drop_on_read_with_keep_inputs_opt_out(df):
    from kindling.data_pipes import PipeMetadata

    source = _entity({"provenance.enabled": "true"}, entityid="bronze.a")
    stamped = stamp_provenance(df, source, ProvenanceStamp)

    def pipe(tags):
        return PipeMetadata(
            pipeid="p",
            name="p",
            execute=lambda df: df,
            tags=tags,
            input_entity_ids=["bronze.a"],
            output_entity_id="silver.x",
            output_type="delta",
        )

    assert drop_provenance(stamped, source, pipe({})).columns == ["id"]
    assert drop_provenance(stamped, source).columns == ["id"]
    kept = drop_provenance(stamped, source, pipe({"provenance.keep_inputs": "bronze.a"}))
    assert kept.columns == ["id", "__provenance"]
    # An unstamped entity's reads are untouched.
    assert drop_provenance(stamped, _entity(entityid="bronze.a")) is stamped
    assert drop_provenance(None, source) is None
