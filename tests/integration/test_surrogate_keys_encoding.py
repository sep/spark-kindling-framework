"""The ``v1`` surrogate key encoding, pinned byte for byte.

Golden vectors fix the canonical string (and its sha256) for null
placement, embedded separators, empty vs NULL, unicode, int vs long,
decimals (zero, negative, tiny values Spark's cast renders in scientific
notation, maximum precision), timestamps across session timezones and the
namespace. ``uuid5`` is cross-checked against Python's ``uuid.uuid5``. Also
covers KeyContext, ``assert_generator_deterministic`` and the non-SCD2
DataFrame placement (batch and streaming).

Local Spark only; built-in functions only, so the same vectors hold on
Spark 4 and Spark Connect.
"""

import hashlib
import uuid
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from delta import configure_spark_with_delta_pip
from kindling.surrogate_keys import (
    EntityView,
    KeyContext,
    SurrogateKeyGenerators,
    _typed_input,
    apply_surrogate_keys,
    assert_generator_deterministic,
    canonical_v1,
    hash_v1,
    uuid5_v1,
)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
)

from tests.spark_test_helper import _teardown_non_delta_jvm, set_spark_log_level


@pytest.fixture(scope="module")
def spark():
    _teardown_non_delta_jvm()
    builder = (
        SparkSession.builder.appName("SurrogateKeyEncoding")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    set_spark_log_level(spark, "ERROR")
    yield spark
    spark.stop()


def _canonical(spark, rows, schema, namespace=None):
    """Canonical strings for every row (inputs = all columns, in order)."""
    df = spark.createDataFrame(rows, schema)
    inputs = [_typed_input(schema, name, None, False, name) for name in schema.names]
    return [r["c"] for r in df.select(canonical_v1(inputs, namespace).alias("c")).collect()]


def _strings(*names):
    return StructType([StructField(name, StringType(), True) for name in names])


def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ── golden vectors ───────────────────────────────────────────────────────────


def test_null_placement_is_part_of_the_key(spark):
    got = _canonical(
        spark, [("a", None, "b"), ("a", "b", None), (None, "a", "b")], _strings(*"xyz")
    )
    assert got == ["S1:aNS1:b", "S1:aS1:bN", "NS1:aS1:b"]
    assert len({sha256(text) for text in got}) == 3


def test_embedded_separators_cannot_collide(spark):
    got = _canonical(
        spark, [("a|b", "c"), ("a", "b|c"), ("S1:a", ""), ("", "S1:a")], _strings("x", "y")
    )
    assert got == ["S3:a|bS1:c", "S1:aS3:b|c", "S4:S1:aS0:", "S0:S4:S1:a"]


def test_empty_string_and_null_differ(spark):
    assert _canonical(spark, [("",), (None,)], _strings("x")) == ["S0:", "N"]


def test_unicode_length_counts_characters(spark):
    got = _canonical(spark, [("é😀",), ("日本語",)], _strings("x"))
    assert got == ["S2:é😀", "S3:日本語"]


def test_integral_types_encode_identically(spark):
    schema = StructType(
        [
            StructField("b", ByteType()),
            StructField("s", ShortType()),
            StructField("i", IntegerType()),
            StructField("l", LongType()),
        ]
    )
    assert _canonical(spark, [(42, 42, 42, 42), (-7, -7, -7, -7)], schema) == [
        "S2:42S2:42S2:42S2:42",
        "S2:-7S2:-7S2:-7S2:-7",
    ]
    assert _canonical(
        spark, [(9223372036854775807,)], StructType([StructField("l", LongType())])
    ) == ["S19:9223372036854775807"]


@pytest.mark.parametrize(
    "precision, scale, value, expected",
    [
        (38, 18, "0", "S20:0.000000000000000000"),
        (38, 18, "0.0000001", "S20:0.000000100000000000"),  # cast gives 1.00000000000E-7
        (38, 18, "-0.00000000000000001", "S21:-0.000000000000000010"),  # cast gives -1.0E-17
        (38, 18, "0.000000000000000001", "S20:0.000000000000000001"),
        (
            38,
            18,
            "99999999999999999999.999999999999999999",
            "S39:99999999999999999999.999999999999999999",
        ),
        (
            38,
            18,
            "-99999999999999999999.999999999999999999",
            "S40:-99999999999999999999.999999999999999999",
        ),
        (18, 2, "10000000000.00", "S14:10000000000.00"),
        (10, 2, "-12.5", "S6:-12.50"),
        (10, 2, "-0.01", "S5:-0.01"),
        (38, 0, "9" * 38, "S38:" + "9" * 38),
        (5, 0, "0", "S1:0"),
        (5, 0, "-3", "S2:-3"),
        (38, 36, "1.5", "S38:1." + "5" + "0" * 35),
        (36, 36, "-0." + "0" * 35 + "1", "S39:-0." + "0" * 35 + "1"),
        (38, 36, "-0." + "9" * 36, "S39:-0." + "9" * 36),
        (38, 0, "-" + "9" * 38, "S39:-" + "9" * 38),
        (10, 3, "-5", "S6:-5.000"),
        (10, 3, "-0.5", "S6:-0.500"),
    ],
)
def test_decimals_are_plain_at_declared_scale(spark, precision, scale, value, expected):
    # Built with a SQL cast: Spark 4.0's Python-to-JVM conversion cannot
    # take some maximum-precision negative Decimals.
    df = spark.sql(f"SELECT CAST('{value}' AS DECIMAL({precision},{scale})) AS d")
    inputs = [_typed_input(df.schema, "d", None, False, "d")]
    assert [r[0] for r in df.select(canonical_v1(inputs)).collect()] == [expected]


def test_decimal_scale_is_part_of_the_key(spark):
    a = _canonical(spark, [(Decimal("1.5"),)], StructType([StructField("d", DecimalType(5, 1))]))
    b = _canonical(spark, [(Decimal("1.5"),)], StructType([StructField("d", DecimalType(5, 2))]))
    assert a == ["S3:1.5"] and b == ["S4:1.50"]


def test_boolean_date_binary(spark):
    schema = StructType(
        [
            StructField("b", BooleanType()),
            StructField("d", DateType()),
            StructField("x", BinaryType()),
        ]
    )
    got = _canonical(
        spark,
        [(True, date(2024, 2, 29), bytes([0, 255, 16])), (False, date(1, 1, 1), b"")],
        schema,
    )
    assert got == ["S1:TS10:2024-02-29S6:00ff10", "S1:FS10:0001-01-01S0:"]


@pytest.mark.parametrize("timezone", ["UTC", "America/New_York", "Asia/Kolkata", "Pacific/Chatham"])
def test_timestamps_are_session_timezone_independent(spark, timezone):
    previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", timezone)
    try:
        df = spark.sql(
            "SELECT timestamp_micros(1700000000123456) AS t, "
            "timestamp_micros(-1) AS before_epoch, "
            "CAST('2026-07-01T12:00:00.000001Z' AS TIMESTAMP) AS parsed"
        )
        inputs = [_typed_input(df.schema, name, None, False, name) for name in df.columns]
        row = df.select(
            *[canonical_v1([value]).alias(value.label.strip("'")) for value in inputs]
        ).first()
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)
    assert row["t"] == "S16:1700000000123456"
    assert row["before_epoch"] == "S2:-1"
    assert row["parsed"] == "S16:1782907200000001"


def test_namespace_is_encoded_first(spark):
    assert _canonical(spark, [("a",)], _strings("x"), namespace="crm") == ["S3:crmS1:a"]
    assert _canonical(spark, [("a",)], _strings("x"), namespace=None) == ["S1:a"]


def test_hash_is_lowercase_sha256_hex_of_the_canonical_string(spark):
    df = spark.createDataFrame([("c1", "crm")], _strings("id", "src"))
    inputs = [_typed_input(df.schema, name, None, False, name) for name in df.columns]
    key = df.select(hash_v1(canonical_v1(inputs)).alias("k")).first()["k"]
    # Pinned: v1 must never change.
    assert key == "3198fe60e2826b3a19febd2be36e5bb452df075cc65f8858711ebac4978a4739"
    assert key == sha256("S2:c1S3:crm")


def test_column_names_are_not_part_of_the_key(spark):
    a = spark.createDataFrame([("c1",)], _strings("customer_id"))
    b = spark.createDataFrame([("c1",)], _strings("cust_no"))
    key = lambda df: df.select(  # noqa: E731
        hash_v1(canonical_v1([_typed_input(df.schema, df.columns[0], None, False, "")]))
    ).first()[0]
    assert key(a) == key(b) == sha256("S2:c1")


# ── uuid5 ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "namespace", [uuid.NAMESPACE_URL, uuid.NAMESPACE_DNS, uuid.UUID(int=0), uuid.uuid4()]
)
def test_uuid5_matches_python(spark, namespace):
    values = ["hello", "", None, "é😀", "a|b", "x" * 300]
    df = spark.createDataFrame([(v,) for v in values], _strings("x"))
    canonical = canonical_v1([_typed_input(df.schema, "x", None, False, "x")])
    rows = df.select(canonical.alias("c"), uuid5_v1(canonical, str(namespace)).alias("u")).collect()
    for row in rows:
        assert row["u"] == str(uuid.uuid5(namespace, row["c"])), row["c"]


def test_uuid5_reproduces_the_documented_vector(spark):
    df = spark.createDataFrame([("hello",)], _strings("x"))
    canonical = canonical_v1([_typed_input(df.schema, "x", None, False, "x")])
    got = df.select(uuid5_v1(canonical, str(uuid.NAMESPACE_URL))).first()[0]
    assert got == str(uuid.uuid5(uuid.NAMESPACE_URL, "S5:hello"))


# ── KeyContext and the determinism helper ────────────────────────────────────


def _ctx(df, **overrides):
    params = dict(
        schema=df.schema,
        entity=EntityView("silver.e", "e", ("id",), (("surrogate.generator.prefix", "C"),)),
        kind="key",
        key_columns=("id",),
        options={"prefix": "C"},
        scd2=False,
    )
    params.update(overrides)
    return KeyContext(**params)


def test_key_context_helpers_use_typed_forms(spark):
    df = spark.sql("SELECT 'c1' AS id, 42 AS n, timestamp_micros(5) AS ef")
    ctx = _ctx(df, kind="version", scd2=True, effective_from_column="ef")
    row = df.select(
        ctx.canonical("id", ctx.col("n"), ctx.effective_from).alias("c"),
        ctx.canonical(F.lit(7)).alias("untyped"),
        ctx.hash("id").alias("h"),
        ctx.effective_to.alias("to"),
        ctx.is_current.alias("cur"),
    ).first()
    assert row["c"] == "S2:c1S2:42S1:5"
    assert row["untyped"] == "S1:7"
    assert row["h"] == sha256("S2:c1")
    assert row["to"] is None and row["cur"] is True
    assert ctx.columns == ("id", "n")  # effective_from is not a data column
    assert ctx.options["prefix"] == "C" and ctx.kind == "version"
    with pytest.raises(TypeError):
        ctx.options["prefix"] = "X"
    with pytest.raises(ValueError, match="not a data column"):
        ctx.col("nope")


def test_key_context_non_scd2_and_durable_have_no_effective_from(spark):
    df = spark.sql("SELECT 'c1' AS id, timestamp_micros(5) AS ef")
    assert _ctx(df).effective_from is None and _ctx(df).effective_to is None
    durable = _ctx(df, kind="durable", scd2=True, effective_from_column="ef")
    assert durable.effective_from is None
    assert durable.is_current is not None


def test_determinism_helper_accepts_a_deterministic_generator(spark):
    df = spark.range(200).select(F.col("id").cast("string").alias("id"))
    assert_generator_deterministic(lambda ctx: ctx.hash("id"), df, key_columns=["id"])


@pytest.mark.parametrize(
    "generator",
    [
        lambda ctx: F.expr("uuid()"),
        lambda ctx: F.rand().cast("string"),
        lambda ctx: F.monotonically_increasing_id().cast("string"),
    ],
    ids=["uuid", "rand", "monotonic_id"],
)
def test_determinism_helper_rejects_non_deterministic_generators(spark, generator):
    df = spark.range(200).select(F.col("id").cast("string").alias("id"))
    with pytest.raises(AssertionError, match="not deterministic"):
        assert_generator_deterministic(generator, df)


def test_determinism_helper_resolves_registered_names(spark):
    def stable(ctx):
        return ctx.hash(*ctx.key_columns)

    SurrogateKeyGenerators.register("enc_test_stable", deterministic=True)(stable)
    try:
        df = spark.range(20).select(F.col("id").cast("string").alias("id"))
        assert_generator_deterministic("enc_test_stable", df, key_columns=["id"])
    finally:
        SurrogateKeyGenerators.unregister("enc_test_stable")


# ── non-SCD2 placement ───────────────────────────────────────────────────────


def _plain_entity(tags):
    return SimpleNamespace(
        entityid="silver.dim_product",
        name="dim_product",
        merge_columns=["product_id"],
        tags=dict(tags),
        schema=None,
    )


def test_apply_adds_the_key_and_replaces_an_incoming_one(spark):
    df = spark.createDataFrame(
        [("p1", "bogus"), (None, None)], _strings("product_id", "Product_SK")
    )
    out = apply_surrogate_keys(df, _plain_entity({"surrogate.column": "product_sk"}))
    assert out.columns == ["product_id", "product_sk"]
    assert [r["product_sk"] for r in out.orderBy("product_id").collect()] == [
        sha256("N"),
        sha256("S2:p1"),
    ]


def test_apply_uuid5_and_namespace(spark):
    df = spark.createDataFrame([("p1",)], _strings("product_id"))
    entity = _plain_entity(
        {
            "surrogate.column": "product_uuid",
            "surrogate.strategy": "uuid5",
            "surrogate.uuid.namespace": str(uuid.NAMESPACE_OID),
            "surrogate.namespace": "erp",
        }
    )
    got = apply_surrogate_keys(df, entity).first()["product_uuid"]
    assert got == str(uuid.uuid5(uuid.NAMESPACE_OID, "S3:erpS2:p1"))


def test_apply_leaves_scd2_and_keyless_entities_alone(spark):
    df = spark.createDataFrame([("p1",)], _strings("product_id"))
    assert apply_surrogate_keys(df, _plain_entity({})) is df
    scd2 = _plain_entity({"scd.type": "2", "surrogate.column": "sk"})
    assert apply_surrogate_keys(df, scd2) is df


def test_apply_works_on_a_streaming_dataframe(spark, tmp_path):
    source, sink = str(tmp_path / "src"), str(tmp_path / "sink")
    spark.createDataFrame([("p1",), ("p2",)], _strings("product_id")).write.format("delta").save(
        source
    )
    stream = spark.readStream.format("delta").load(source)
    keyed = apply_surrogate_keys(stream, _plain_entity({"surrogate.column": "product_sk"}))
    query = (
        keyed.writeStream.format("delta")
        .option("checkpointLocation", str(tmp_path / "chk"))
        .trigger(availableNow=True)
        .start(sink)
    )
    query.awaitTermination()
    rows = spark.read.format("delta").load(sink).collect()
    assert {r["product_id"]: r["product_sk"] for r in rows} == {
        "p1": sha256("S2:p1"),
        "p2": sha256("S2:p2"),
    }


# ── the guide's custom generator example runs for every kind ─────────────────


def _guide_python_blocks():
    import re
    from pathlib import Path

    guide = Path(__file__).resolve().parents[2] / "docs" / "guide" / "data_entities.md"
    text = guide.read_text(encoding="utf-8")
    section = text[text.index("### Custom generators") :]
    section = section[: section.index("\n### ", 1)]
    return re.findall(r"```python\n(.*?)```", section, flags=re.S)


@pytest.mark.parametrize("kind", ["key", "version", "durable"])
def test_documented_generator_runs_for_every_kind(spark, kind):
    """docs/guide/data_entities.md 'Custom generators': the example generator
    and the determinism check must run for non-SCD2 keys ("key", where
    effective_from is None), SCD2 version keys and durable keys."""
    blocks = _guide_python_blocks()
    generator_block = next(b for b in blocks if "SurrogateKeyGenerators.register" in b)
    determinism_block = next(b for b in blocks if "assert_generator_deterministic(" in b)
    namespace = {}
    SurrogateKeyGenerators.unregister("acme_customer_key")
    try:
        exec(generator_block, namespace)  # noqa: S102 - the documented example
        sample_df = spark.sql(
            "SELECT 'c1' AS customer_id, 'crm' AS source_system, "
            "timestamp_micros(1700000000000000) AS ef"
        )
        ctx = KeyContext(
            schema=sample_df.schema,
            entity=EntityView("silver.dim_customer", "dim", ("customer_id",), ()),
            kind=kind,
            key_columns=("customer_id",),
            options={"prefix": "CUST"},
            scd2=kind != "key",
            effective_from_column="ef" if kind != "key" else None,
        )
        key = sample_df.select(namespace["acme_customer_key"](ctx)).first()[0]
        assert key.startswith("CUST-crm-00000000c1")
        assert (len(key.split("-")) == 4) == (kind == "version")
        if kind == "key":
            exec(determinism_block, {**namespace, "sample_df": sample_df})  # noqa: S102
    finally:
        SurrogateKeyGenerators.unregister("acme_customer_key")
