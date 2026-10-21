"""Provenance stamping in the SDP dataset builder, evaluated on local Spark.

The OSS engine declares the dataset through a recording ``dp`` fake; the
recorded dataset function is then evaluated against a real session:

- the declared schema gains the provenance column;
- an input's stamp is dropped, and the output carries the SDP stamp
  (``mode='sdp'``, no run, execution or persist id);
- every value is bound at declaration time, ``processed_at`` included, so
  re-evaluating the function gives the same stamp.
"""

from kindling.data_entities import EntityMetadata
from kindling.data_pipes import PipeMetadata
from kindling.provenance import (
    ProvenanceStamp,
    provenance_struct_type,
    stamp_provenance,
)
from kindling_ext_sdp import OssSdpEngine
from pyspark.sql.types import StringType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("status", StringType(), True),
    ]
)


class _Registry:
    def __init__(self, items, key):
        self.registry = {getattr(item, key): item for item in items}

    def get_entity_ids(self):
        return self.registry.keys()

    get_pipe_ids = get_entity_ids

    def get_entity_definition(self, name):
        return self.registry.get(name)

    get_pipe_definition = get_entity_definition


class _RecordingDp:
    def __init__(self):
        self.declared = {}

    def materialized_view(self, **kwargs):
        def decorator(fn):
            self.declared[kwargs["name"]] = (kwargs, fn)
            return fn

        return decorator


def _declare(spark, pipe_tags=None):
    source = EntityMetadata(
        "bronze.orders", "orders", ["id"], {"provenance.enabled": "true"}, SCHEMA
    )
    output = EntityMetadata(
        "silver.orders",
        "orders",
        ["id"],
        {"provenance.enabled": "true", "provenance.pipe_tags": "owner"},
        SCHEMA,
    )
    pipe = PipeMetadata(
        pipeid="silver.build_orders",
        name="Build orders",
        execute=lambda bronze_orders: bronze_orders,
        tags={"owner": "sales", **(pipe_tags or {})},
        input_entity_ids=["bronze.orders"],
        output_entity_id="silver.orders",
        output_type="delta",
    )
    upstream = stamp_provenance(
        spark.createDataFrame([("o1", "new")], SCHEMA),
        source,
        lambda: ProvenanceStamp(pipe_id="bronze.ingest", mode="batch"),
    )
    dp = _RecordingDp()
    engine = OssSdpEngine(
        _Registry([source, output], "entityid"),
        _Registry([pipe], "pipeid"),
        dp_module=dp,
        session_provider=lambda: spark,
        external_read_resolver=lambda _spark, entity_id: upstream,
    )
    engine.declare_pipeline(engine.build_plan())
    return dp.declared["silver_orders"]


def test_declared_schema_gains_the_column(spark_session):
    kwargs, _ = _declare(spark_session)
    schema = kwargs["schema"]
    assert schema.fieldNames() == ["id", "status", "__provenance"]
    assert schema["__provenance"].dataType == provenance_struct_type()


def test_dataset_function_drops_the_input_stamp_and_stamps_the_output(spark_session):
    _, dataset_function = _declare(spark_session)

    result = dataset_function()
    assert result.columns == ["id", "status", "__provenance"]
    assert result.schema["__provenance"].dataType == provenance_struct_type()
    stamp = result.first()["__provenance"]
    assert stamp["pipe_id"] == "silver.build_orders", "the input's stamp was dropped"
    assert stamp["pipe_name"] == "Build orders"
    assert stamp["mode"] == "sdp"
    assert stamp["run_id"] is None
    assert stamp["execution_id"] is None
    assert stamp["persist_id"] is None
    assert stamp["input_entity_ids"] == ["bronze.orders"]
    assert stamp["tags"] == {"owner": "sales"}
    assert stamp["processed_at"] is not None

    again = dataset_function().first()["__provenance"]
    assert again == stamp, "declaration-time literals: the same stamp on every evaluation"


def test_kept_input_stamp_is_replaced_by_the_output_stamp(spark_session):
    """SDP never inspects a dataset's schema while declaring it, so a kept
    input stamp is replaced (withColumn) rather than rejected."""
    _, dataset_function = _declare(
        spark_session, pipe_tags={"provenance.keep_inputs": "bronze.orders"}
    )

    stamp = dataset_function().first()["__provenance"]
    assert stamp["pipe_id"] == "silver.build_orders"


def test_ddl_schema_string_gains_a_parseable_column(spark_session):
    from kindling.provenance import augment_schema_for_provenance

    entity = EntityMetadata("silver.orders", "orders", ["id"], {"provenance.enabled": "true"}, None)
    ddl = augment_schema_for_provenance("id STRING, status STRING", entity)
    parsed = spark_session.createDataFrame([], ddl).schema
    assert parsed.fieldNames() == ["id", "status", "__provenance"]
    assert parsed["__provenance"].dataType == provenance_struct_type()
