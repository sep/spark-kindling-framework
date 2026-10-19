"""Surrogate keys in the merges: Delta vs. Memory parity.

Every scenario runs against DeltaEntityProvider (staged MERGE INTO) and
MemoryEntityProvider (DataFrame joins/unions):

- SCD2 version keys = H(surrogate.from, effective_from) and durable keys =
  H(surrogate.from), minted on inserted rows only, from the stored
  effective_from; replays with sequence_by recreate the same keys; stale
  rows and snapshot closes mint nothing; closing a version and in-place
  updates (scd.untracked and scd.tracked) leave keys unchanged; key columns
  never open a version.
- SCD1: a tracked change leaves the stored key unchanged (insert-only).
- Custom generators: kind, options, effective_from equal to the stored
  value; the streaming merge uses the driver-captured generator function.
- Delta ensure_destination: generator resolution, schema augmentation and
  the ``kindling.surrogate.encoding`` table property.
- Batch placement: the key is computed in the persist step after every
  before_persist transform, from the stored values.

Local Spark + local Delta only; fixtures mirror test_scd_untracked_parity.py.
"""

import hashlib
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from delta import configure_spark_with_delta_pip
from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import DeltaEntityProvider
from kindling.entity_provider_memory import MemoryEntityProvider
from kindling.signaling import BlinkerSignalProvider
from kindling.simple_read_persist_strategy import SimpleReadPersistStrategy
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.surrogate_keys import (
    SurrogateKeyGenerators,
    apply_surrogate_keys,
    resolve_surrogate_keys,
)
from pyspark import cloudpickle
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from tests.spark_test_helper import _teardown_non_delta_jvm, set_spark_log_level


@pytest.fixture(scope="module")
def spark():
    _teardown_non_delta_jvm()
    builder = (
        SparkSession.builder.appName("SurrogateKeyMerge")
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


@pytest.fixture
def temp_dir():
    temp_path = tempfile.mkdtemp(prefix="kindling-surrogate-test-")
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def delta_provider(spark, temp_dir, monkeypatch):
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)
    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: (
        "storage" if key == "kindling.delta.access_mode" else default
    )
    name_mapper = MagicMock(spec=EntityNameMapper)
    name_mapper.get_table_name.side_effect = lambda entity: entity.entityid.replace(".", "_")
    path_locator = MagicMock(spec=EntityPathLocator)
    path_locator.get_table_path.side_effect = lambda entity: str(
        Path(str(temp_dir)) / entity.entityid
    )
    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger_provider.get_logger.return_value = MagicMock()
    return DeltaEntityProvider(
        config=config,
        entity_name_mapper=name_mapper,
        path_locator=path_locator,
        tp=logger_provider,
        signal_provider=None,
    )


@pytest.fixture
def memory_provider(spark, monkeypatch):
    monkeypatch.setattr(
        "kindling.entity_provider_memory.get_or_create_spark_session", lambda: MagicMock()
    )
    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger_provider.get_logger.return_value = MagicMock()
    provider = MemoryEntityProvider(logger_provider)
    provider.spark = spark
    return provider


@pytest.fixture(params=["delta", "memory"])
def provider(request, delta_provider, memory_provider):
    return {"delta": delta_provider, "memory": memory_provider}[request.param]


@pytest.fixture(autouse=True)
def _clean_generators():
    before = dict(SurrogateKeyGenerators._registry)
    yield
    SurrogateKeyGenerators._registry.clear()
    SurrogateKeyGenerators._registry.update(before)


BUSINESS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("source_system", StringType(), True),
        StructField("status", StringType(), True),
        StructField("load_id", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

TS = lambda day: datetime(2026, 7, day, 0, 0, 0)  # noqa: E731

KEY_TAGS = {
    "surrogate.column": "customer_version_sk",
    "surrogate.durable_column": "customer_dk",
}


def _suffix(request):
    callspec = getattr(request.node, "callspec", None)
    return "".join(ch if ch.isalnum() else "_" for ch in (callspec.id if callspec else "x"))


def _entity(request, name, tags, scd2=True, schema=BUSINESS_SCHEMA):
    base = {"scd.type": "2"} if scd2 else {}
    return SimpleNamespace(
        entityid=f"silver.{name}_{_suffix(request)}",
        name=name,
        partition_columns=[],
        cluster_columns=None,
        merge_columns=["customer_id", "source_system"],
        tags={**base, **tags},
        schema=schema,
    )


def _merge(spark, provider, entity, rows, schema=BUSINESS_SCHEMA):
    provider.merge_to_entity(spark.createDataFrame(rows, schema), entity)


def _rows(provider, entity, key="c1"):
    df = provider.read_entity(entity).withColumn(
        "__ef_micros", F.expr("unix_micros(__effective_from)")
    )
    rows = [r for r in df.collect() if r["customer_id"] == key]
    current = [r for r in rows if r["__is_current"]]
    closed = sorted((r for r in rows if not r["__is_current"]), key=lambda r: r["__effective_from"])
    return current, closed


def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def enc(text):
    return "N" if text is None else f"S{len(text)}:{text}"


def version_key(row):
    return sha256(
        enc(row["customer_id"]) + enc(row["source_system"]) + enc(str(row["__ef_micros"]))
    )


def durable_key(row):
    return sha256(enc(row["customer_id"]) + enc(row["source_system"]))


def _assert_keys_match_stored_values(row):
    assert row["customer_version_sk"] == version_key(row)
    assert row["customer_dk"] == durable_key(row)


SEQ = {"scd.sequence_by": "updated_at", **KEY_TAGS}


# ── SCD2 ──────────────────────────────────────────────────────────────────────


def test_scd2_mints_version_and_durable_keys_on_insert(spark, provider, request):
    entity = _entity(request, "keys_insert", SEQ)
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(1))])
    (current,), closed = _rows(provider, entity)
    assert closed == []
    _assert_keys_match_stored_values(current)


def test_scd2_tracked_change_mints_new_version_key_and_keeps_old_ones(spark, provider, request):
    entity = _entity(request, "keys_close", SEQ)
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(1))])
    (first,), _ = _rows(provider, entity)

    _merge(spark, provider, entity, [("c1", "crm", "silver", "L1", TS(2))])

    (current,), (closed,) = _rows(provider, entity)
    assert (
        closed["customer_version_sk"] == first["customer_version_sk"]
    ), "closing a version never touches its keys"
    assert closed["customer_dk"] == first["customer_dk"]
    _assert_keys_match_stored_values(current)
    assert current["customer_version_sk"] != closed["customer_version_sk"]
    assert current["customer_dk"] == closed["customer_dk"], "the durable key is stable"


def test_scd2_replay_with_sequence_by_recreates_the_same_keys(spark, provider, request):
    batches = [
        [("c1", "crm", "bronze", "L1", TS(1)), ("c2", "erp", "gold", "L1", TS(1))],
        [("c1", "crm", "silver", "L2", TS(3))],
        [("c1", "crm", "gold", "L3", TS(5)), ("c2", "erp", "gold", "L3", TS(6))],
    ]

    def keys(name):
        entity = _entity(request, name, SEQ)
        for batch in batches:
            _merge(spark, provider, entity, batch)
        return sorted(
            (r["customer_id"], r["customer_version_sk"], r["customer_dk"])
            for r in provider.read_entity(entity).collect()
        )

    first_build = keys("replay_a")
    assert keys("replay_b") == first_build, "a rebuild recreates the same version keys"
    assert len(first_build) == 5


def test_scd2_stale_and_duplicate_rows_mint_no_keys(spark, provider, request):
    entity = _entity(request, "keys_stale", SEQ)
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(5))])
    before = sorted(provider.read_entity(entity).collect())

    _merge(spark, provider, entity, [("c1", "crm", "silver", "L2", TS(3))])  # stale
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(5))])  # replay

    assert sorted(provider.read_entity(entity).collect()) == before


def test_scd2_snapshot_close_mints_no_keys(spark, provider, request):
    entity = _entity(request, "keys_snapshot", {**SEQ, "scd.source_kind": "snapshot"})
    _merge(
        spark,
        provider,
        entity,
        [("c1", "crm", "bronze", "L1", TS(1)), ("c2", "crm", "gold", "L1", TS(1))],
    )
    (c2_before,), _ = _rows(provider, entity, "c2")

    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(1))])  # c2 missing

    current, (closed,) = _rows(provider, entity, "c2")
    assert current == []
    assert closed["customer_version_sk"] == c2_before["customer_version_sk"]
    assert closed["customer_dk"] == c2_before["customer_dk"]
    assert provider.read_entity(entity).count() == 2, "a close inserts nothing"


@pytest.mark.parametrize(
    "form",
    [
        {"scd.untracked": "load_id"},
        {"scd.tracked": "status"},  # every other column (incl. load_id) is untracked
    ],
    ids=["untracked", "tracked"],
)
def test_scd2_in_place_update_keeps_keys(spark, provider, request, form):
    entity = _entity(request, "keys_touch", {**SEQ, **form})
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(1))])
    (opened,), _ = _rows(provider, entity)

    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L2", TS(2))])

    (current,), closed = _rows(provider, entity)
    assert closed == [], "an untracked-only change updates in place"
    assert current["load_id"] == "L2"
    assert current["customer_version_sk"] == opened["customer_version_sk"]
    assert current["customer_dk"] == opened["customer_dk"]
    assert current["customer_version_sk"] is not None, "a touch must not null the key"


def test_scd2_incoming_key_columns_never_open_a_version(spark, provider, request):
    entity = _entity(request, "keys_ignored", {**SEQ, "scd.tracked": "status"})
    schema = StructType(BUSINESS_SCHEMA.fields + [StructField("CUSTOMER_VERSION_SK", StringType())])
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(1), "upstream-1")], schema)
    (opened,), _ = _rows(provider, entity)
    _assert_keys_match_stored_values(opened)

    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(2), "upstream-2")], schema)

    (current,), closed = _rows(provider, entity)
    assert closed == [], "a key column is never tracked"
    assert current["customer_version_sk"] == opened["customer_version_sk"]


def test_scd2_without_sequence_by_keys_the_stored_effective_from(spark, provider, request):
    entity = _entity(request, "keys_no_seq", dict(KEY_TAGS))
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(1))])
    _merge(spark, provider, entity, [("c1", "crm", "silver", "L1", TS(1))])

    (current,), (closed,) = _rows(provider, entity)
    _assert_keys_match_stored_values(current)
    _assert_keys_match_stored_values(closed)
    assert closed["__effective_to"] == current["__effective_from"]


def test_scd2_delete_closes_without_minting(spark, provider, request):
    entity = _entity(request, "keys_delete", {**SEQ, "scd.delete_when": "status = 'deleted'"})
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(1))])
    (opened,), _ = _rows(provider, entity)
    _merge(spark, provider, entity, [("c1", "crm", "deleted", "L1", TS(2))])
    current, (closed,) = _rows(provider, entity)
    assert current == []
    assert closed["customer_version_sk"] == opened["customer_version_sk"]


def test_provider_parity_identical_keys(spark, delta_provider, memory_provider, request):
    batches = [
        [("c1", "crm", "bronze", "L1", TS(1)), ("c2", "erp", "gold", "L1", TS(1))],
        [("c1", "crm", "silver", "L2", TS(2)), ("c2", "erp", "gold", "L2", TS(3))],
    ]
    results = []
    for label, provider in (("d", delta_provider), ("m", memory_provider)):
        entity = SimpleNamespace(
            entityid=f"silver.parity_{label}",
            name="parity",
            partition_columns=[],
            cluster_columns=None,
            merge_columns=["customer_id", "source_system"],
            tags={"scd.type": "2", "scd.untracked": "load_id", **SEQ},
            schema=BUSINESS_SCHEMA,
        )
        for batch in batches:
            _merge(spark, provider, entity, batch)
        results.append(
            sorted(
                (
                    r["customer_id"],
                    r["__is_current"],
                    r["customer_version_sk"],
                    r["customer_dk"],
                    r["load_id"],
                )
                for r in provider.read_entity(entity).collect()
            )
        )
    assert results[0] == results[1]
    assert len(results[0]) == 3


# ── SCD1 ──────────────────────────────────────────────────────────────────────

SCD1_SCHEMA = StructType(BUSINESS_SCHEMA.fields[:4] + [StructField("product_sk", StringType())])


def test_scd1_tracked_change_leaves_the_key_unchanged(spark, provider, request):
    entity = _entity(request, "scd1_keys", {"surrogate.column": "product_sk"}, scd2=False)
    entity.schema = SCD1_SCHEMA
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", "first")], SCD1_SCHEMA)
    # A tracked change arriving with a different key (e.g. one minted by a
    # non-deterministic upstream): the row updates, the key does not.
    _merge(
        spark,
        provider,
        entity,
        [("c1", "crm", "silver", "L2", "second"), ("c2", "crm", "gold", "L1", "new")],
        SCD1_SCHEMA,
    )
    rows = {r["customer_id"]: r for r in provider.read_entity(entity).collect()}
    assert rows["c1"]["status"] == "silver"
    assert rows["c1"]["product_sk"] == "first"
    assert rows["c2"]["product_sk"] == "new", "a new business key inserts its key"


def test_scd1_with_keys_still_evolves_new_columns(spark, provider, request):
    entity = _entity(request, "scd1_evolve", {"surrogate.column": "product_sk"}, scd2=False)
    entity.schema = SCD1_SCHEMA
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", "k1")], SCD1_SCHEMA)
    wider = StructType(SCD1_SCHEMA.fields + [StructField("tier", StringType())])
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", "k2", "vip")], wider)
    (row,) = provider.read_entity(entity).collect()
    assert row["tier"] == "vip" and row["product_sk"] == "k1"


def test_scd1_aspect_keys_survive_merges(spark, provider, request):
    entity = _entity(request, "scd1_aspect", {"surrogate.column": "product_sk"}, scd2=False)
    entity.schema = StructType(BUSINESS_SCHEMA.fields[:4])
    df = spark.createDataFrame([("c1", "crm", "bronze", "L1")], entity.schema)
    provider.merge_to_entity(apply_surrogate_keys(df, entity), entity)
    df2 = spark.createDataFrame([("c1", "crm", "silver", "L1")], entity.schema)
    provider.merge_to_entity(apply_surrogate_keys(df2, entity), entity)
    (row,) = provider.read_entity(entity).collect()
    assert row["status"] == "silver"
    assert row["product_sk"] == sha256("S2:c1S3:crm")


# ── custom generators ────────────────────────────────────────────────────────


def acme_key(ctx):
    """Module-level, so it pickles (as a streaming merge requires)."""
    # Data columns only: no staging, temporal or key columns, on both providers.
    assert set(ctx.columns) == {
        "customer_id",
        "source_system",
        "status",
        "load_id",
        "updated_at",
    }, ctx.columns
    if ctx.kind == "durable":
        return F.concat_ws("-", F.lit(ctx.options["prefix"]), ctx.col("customer_id"))
    assert ctx.kind == "version", ctx.kind
    return F.concat_ws(
        "|",
        F.lit(ctx.options["prefix"]),
        F.lit(ctx.entity.entityid.split(".")[0]),
        ctx.col("customer_id"),
        F.expr("1").cast("string"),
        (
            F.unix_micros(ctx.effective_from).cast("string")
            if hasattr(F, "unix_micros")
            else ctx.canonical(ctx.effective_from)
        ),
        ctx.is_current.cast("string"),
    )


CUSTOM = {
    "surrogate.strategy": "custom",
    "surrogate.generator": "acme_key",
    "surrogate.generator.prefix": "CUST",
    **KEY_TAGS,
}


def test_custom_generator_sees_kind_options_and_stored_effective_from(spark, provider, request):
    SurrogateKeyGenerators.register("acme_key", deterministic=True)(acme_key)
    entity = _entity(request, "custom_keys", CUSTOM)  # no sequence_by: processing time
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(1))])

    (row,), _ = _rows(provider, entity)
    assert row["customer_dk"] == "CUST-c1"
    assert row["customer_version_sk"] == f"CUST|silver|c1|1|{row['__ef_micros']}|true"


def test_custom_generator_names_resolve_at_ensure_destination(spark, delta_provider, request):
    entity = _entity(request, "custom_missing", {**CUSTOM, "surrogate.generator": "acme_typo"})
    SurrogateKeyGenerators.register("acme_key", deterministic=True)(acme_key)
    with pytest.raises(ValueError, match=r"Unknown surrogate key generator 'acme_typo'.*acme_key"):
        delta_provider.ensure_destination(entity)


def test_streaming_merge_uses_the_driver_captured_generator(
    spark, delta_provider, temp_dir, request, monkeypatch
):
    """Under Spark Connect the foreachBatch worker never ran bootstrap and
    has an empty generator registry. Simulate that: the registry is cleared
    right after the driver resolves the plan, so a by-name lookup inside the
    per-batch merge would fail; the captured function object must be used."""
    SurrogateKeyGenerators.register("acme_key", deterministic=True)(acme_key)
    import kindling.surrogate_keys as surrogate_keys

    entity = _entity(request, "custom_stream", {**CUSTOM, "scd.sequence_by": "updated_at"})
    delta_provider.ensure_destination(entity)  # resolves while the registry is populated
    real_resolve = surrogate_keys.resolve_surrogate_keys

    def resolve_then_empty_registry(entity):
        plan = real_resolve(entity)
        SurrogateKeyGenerators._registry.clear()
        return plan

    monkeypatch.setattr(surrogate_keys, "resolve_surrogate_keys", resolve_then_empty_registry)

    # The plan crosses into foreachBatch by cloudpickle, function included.
    plan = cloudpickle.loads(cloudpickle.dumps(real_resolve(entity)))
    assert plan.generator.func is acme_key

    source = str(temp_dir / "custom_stream_source")
    spark.createDataFrame([("c1", "crm", "bronze", "L1", TS(1))], BUSINESS_SCHEMA).write.format(
        "delta"
    ).save(source)
    query = delta_provider.merge_as_stream(
        spark.readStream.format("delta").load(source),
        entity,
        str(temp_dir / "custom_stream_chk"),
        options={"trigger": {"availableNow": True}},
    )
    query.awaitTermination()
    assert SurrogateKeyGenerators.names() == [], "the worker-side registry was empty"

    (row,), _ = _rows(delta_provider, entity)
    assert row["customer_dk"] == "CUST-c1"
    assert row["customer_version_sk"] == f"CUST|silver|c1|1|{row['__ef_micros']}|true"


# ── Delta destination ────────────────────────────────────────────────────────


def _table_properties(spark, delta_provider, entity):
    path = delta_provider._get_table_reference(entity).table_path
    return spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()["properties"]


def test_ensure_destination_augments_schema_and_records_the_encoding(
    spark, delta_provider, request
):
    entity = _entity(request, "ensure_keys", SEQ)
    delta_provider.ensure_destination(entity)

    schema = delta_provider.read_entity(entity).schema
    for name in ("customer_version_sk", "customer_dk"):
        assert schema[name].dataType == StringType()
        assert schema[name].metadata.get("kindling.surrogate") == "hash"
    assert _table_properties(spark, delta_provider, entity)["kindling.surrogate.encoding"] == "v1"


def test_ensure_destination_refuses_a_table_with_another_encoding(spark, delta_provider, request):
    entity = _entity(request, "ensure_mismatch", SEQ)
    delta_provider.ensure_destination(entity)
    path = delta_provider._get_table_reference(entity).table_path
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES ('kindling.surrogate.encoding' = 'v2')"
    )
    with pytest.raises(ValueError, match="must not mix key encodings"):
        delta_provider.ensure_destination(entity)


def test_keys_declared_on_an_existing_table_evolve_it(spark, delta_provider, request):
    entity = _entity(request, "late_keys", {"scd.sequence_by": "updated_at"})
    _merge(spark, delta_provider, entity, [("c1", "crm", "bronze", "L1", TS(1))])
    entity.tags.update(KEY_TAGS)
    _merge(spark, delta_provider, entity, [("c1", "crm", "silver", "L1", TS(2))])
    (current,), (closed,) = _rows(delta_provider, entity)
    _assert_keys_match_stored_values(current)
    assert closed["customer_version_sk"] is None, "keys are never backfilled by a close"


# ── batch placement: keys are computed after every before_persist transform ─


class _NamedEntityRegistry:
    def __init__(self, entities):
        self.entities = entities

    def get_entity_definition(self, entity_id):
        return self.entities[entity_id]


@pytest.mark.parametrize("trim_first", [True, False], ids=["trim_first", "audit_first"])
def test_key_is_computed_from_the_stored_values_after_before_persist_handlers(
    spark, provider, request, trim_first
):
    """A before_persist handler that normalizes a surrogate.from column (here
    it trims customer_id): the key is computed from the stored, trimmed
    value, whatever order the handlers were connected in. The key is a
    framework step after every handler, not a signal receiver racing them."""
    output = _entity(request, "persist_keys", {"surrogate.column": "product_sk"}, scd2=False)
    output.schema = None
    entities = {
        "bronze.src": SimpleNamespace(entityid="bronze.src", tags={}),
        output.entityid: output,
    }
    signals = BlinkerSignalProvider()
    lp = MagicMock(spec=PythonLoggerProvider)
    lp.get_logger.return_value = MagicMock()

    def trim_ids(sender, df, **kwargs):
        return df.withColumn("customer_id", F.trim(F.col("customer_id")))

    def audit(sender, df, **kwargs):
        return df.withColumn("audit", F.lit("x"))

    signal = signals.create_signal("persist.before_persist")
    for handler in (trim_ids, audit) if trim_first else (audit, trim_ids):
        signal.connect(handler, weak=False)

    registry = Mock()
    registry.get_provider_for_entity.return_value = provider
    strategy = SimpleReadPersistStrategy(
        ep=Mock(),
        der=_NamedEntityRegistry(entities),
        tp=MagicMock(),
        lp=lp,
        provider_registry=registry,
        signal_provider=signals,
    )
    pipe = Mock(pipeid="p", input_entity_ids=["bronze.src"], output_entity_id=output.entityid)
    persist = strategy.create_pipe_persist_activator(pipe)
    for status in ("bronze", "silver"):  # first write, then a merge
        persist(
            spark.createDataFrame(
                [("  c1 ", "crm", status, "L1")], StructType(BUSINESS_SCHEMA.fields[:4])
            )
        )

    (row,) = provider.read_entity(output).collect()
    assert row["customer_id"] == "c1" and row["status"] == "silver"
    assert row["audit"] == "x"
    assert row["product_sk"] == sha256("S2:c1S3:crm"), "key of the stored (trimmed) value"


# ── first write through the persist path (no prior ensure_destination) ──────


def _persist_through_strategy(spark, provider, entity, rows, schema=BUSINESS_SCHEMA):
    """A sequential batch run's write: SimpleReadPersistStrategy's persist
    lambda straight to the provider, nothing ensured beforehand."""
    entities = {
        "bronze.src": SimpleNamespace(entityid="bronze.src", tags={}),
        entity.entityid: entity,
    }
    lp = MagicMock(spec=PythonLoggerProvider)
    lp.get_logger.return_value = MagicMock()
    registry = Mock()
    registry.get_provider_for_entity.return_value = provider
    strategy = SimpleReadPersistStrategy(
        ep=Mock(),
        der=_NamedEntityRegistry(entities),
        tp=MagicMock(),
        lp=lp,
        provider_registry=registry,
        signal_provider=BlinkerSignalProvider(),
    )
    pipe = Mock(pipeid="p", input_entity_ids=["bronze.src"], output_entity_id=entity.entityid)
    strategy.create_pipe_persist_activator(pipe)(spark.createDataFrame(rows, schema))


@pytest.mark.parametrize("declared_schema", [True, False], ids=["schema", "schemaless"])
@pytest.mark.parametrize("keys", [True, False], ids=["with_keys", "without_keys"])
def test_scd2_first_write_through_persist_runs_the_merge(
    spark, provider, request, keys, declared_schema
):
    """A sequential batch run writes a missing SCD2 table through the merge:
    the first load gets its temporal columns and keys, and the next one
    versions on top of it. (It used to be a plain write: NULL effective_from
    / is_current / keys on Delta, no temporal columns at all on memory.)"""
    tags = dict(SEQ) if keys else {"scd.sequence_by": "updated_at"}
    entity = _entity(
        request, "first_write", tags, schema=BUSINESS_SCHEMA if declared_schema else None
    )
    assert not provider.check_entity_exists(entity)

    _persist_through_strategy(spark, provider, entity, [("c1", "crm", "bronze", "L1", TS(1))])

    (first,), closed = _rows(provider, entity)
    assert closed == []
    assert first["__is_current"] is True
    assert first["__effective_from"] == TS(1)
    assert first["__effective_to"] is None
    if keys:
        _assert_keys_match_stored_values(first)

    _persist_through_strategy(spark, provider, entity, [("c1", "crm", "silver", "L1", TS(2))])

    (current,), (closed,) = _rows(provider, entity)
    assert current["status"] == "silver" and current["__effective_from"] == TS(2)
    assert closed["__effective_to"] == TS(2) and closed["__is_current"] is False
    if keys:
        _assert_keys_match_stored_values(current)
        assert closed["customer_version_sk"] == first["customer_version_sk"]


def test_scd1_keys_declared_on_an_existing_table_never_backfill_matched_rows(
    spark, provider, request
):
    """A keyed (SCD1) table written before the key was declared: matched rows
    keep NULL (keys are insert-only), only new business keys get a key."""
    base_schema = StructType(BUSINESS_SCHEMA.fields[:4])
    entity = _entity(request, "scd1_late_keys", {}, scd2=False, schema=base_schema)
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1")], base_schema)

    entity.tags["surrogate.column"] = "product_sk"
    batch = spark.createDataFrame(
        [("c1", "crm", "silver", "L2"), ("c2", "crm", "gold", "L1")], base_schema
    )
    provider.merge_to_entity(apply_surrogate_keys(batch, entity), entity)

    rows = {r["customer_id"]: r for r in provider.read_entity(entity).collect()}
    assert rows["c1"]["status"] == "silver"
    assert rows["c1"]["product_sk"] is None, "an update must not backfill a key"
    assert rows["c2"]["product_sk"] == sha256("S2:c2S3:crm")


def test_scd1_key_preservation_keeps_columns_named_like_its_helpers(spark, provider, request):
    """Columns that look like the memory merge's helper names are ordinary
    accepted columns: they keep their values and are never dropped."""
    schema = StructType(
        BUSINESS_SCHEMA.fields[:4]
        + [
            StructField("__sk_existing_present", StringType()),
            StructField("__SK_EXISTING", StringType()),
            StructField("__sk_existing_0", StringType()),
            StructField("product_sk", StringType()),
        ]
    )
    entity = _entity(
        request, "scd1_helper_names", {"surrogate.column": "product_sk"}, scd2=False, schema=schema
    )
    _merge(spark, provider, entity, [("c1", "crm", "bronze", "L1", "p1", "e1", "z1", "k1")], schema)
    _merge(
        spark,
        provider,
        entity,
        [
            ("c1", "crm", "silver", "L2", "p2", "e2", "z2", "k2"),
            ("c2", "crm", "gold", "L1", "p3", "e3", "z3", "k3"),
        ],
        schema,
    )
    rows = {r["customer_id"]: r.asDict() for r in provider.read_entity(entity).collect()}
    assert set(rows["c1"]) == {field.name for field in schema.fields}
    assert (rows["c1"]["__sk_existing_present"], rows["c1"]["__SK_EXISTING"]) == ("p2", "e2")
    assert rows["c1"]["__sk_existing_0"] == "z2"
    assert rows["c1"]["product_sk"] == "k1", "the matched row keeps its key"
    assert rows["c2"]["product_sk"] == "k3"
