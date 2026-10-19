"""Untracked columns (``scd.untracked`` / ``scd.tracked``): Delta vs. Memory parity.

Every scenario runs against both DeltaEntityProvider (staged MERGE INTO)
and MemoryEntityProvider (DataFrame joins/unions) and asserts the same
outcome:

- SCD2: a change only to untracked columns (``scd.untracked``, the columns
  outside ``scd.tracked``, and ``scd.sequence_by``) updates the current
  version in place; a tracked change closes it (keeping its untracked
  values) and opens a new one; no change is a no-op. The sequence guard
  compares against the current row's own sequence value, so an older event
  never overwrites a newer in-place value.
- SCD1 (no ``scd.type``): with ``scd.untracked``, a matched row is rewritten
  only when a tracked column differs; without the tag every matched row is
  rewritten, as always.

Local Spark + local Delta only; fixtures mirror test_scd2_provider_parity.py.
"""

import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from delta import configure_spark_with_delta_pip
from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import DeltaEntityProvider
from kindling.entity_provider_memory import MemoryEntityProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from tests.spark_test_helper import _teardown_non_delta_jvm, set_spark_log_level


@pytest.fixture(scope="module")
def spark():
    _teardown_non_delta_jvm()
    builder = (
        SparkSession.builder.appName("SCDUntrackedParity")
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
    temp_path = tempfile.mkdtemp(prefix="kindling-scd-untracked-test-")
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


BUSINESS_SCHEMA = [
    StructField("customer_id", StringType(), False),
    StructField("status", StringType(), True),
    StructField("load_id", StringType(), True),
    StructField("updated_at", TimestampType(), True),
]
TEMPORAL_SCHEMA = [
    StructField("__effective_from", TimestampType(), True),
    StructField("__effective_to", TimestampType(), True),
    StructField("__is_current", BooleanType(), True),
]
SOURCE_SCHEMA = StructType(BUSINESS_SCHEMA)

TS = lambda day: datetime(2026, 7, day, 0, 0, 0)  # noqa: E731

# The two forms that leave load_id as the only untracked column.
UNTRACKED_TAGS = {
    "scd.untracked": {"scd.untracked": "load_id"},
    "scd.tracked": {"scd.tracked": "status,updated_at"},
}


def _scd2_entity(request, name, tags):
    return SimpleNamespace(
        entityid=f"silver.{name}_{request.node.callspec.id.replace('.', '_').replace('-', '_')}",
        name=name,
        partition_columns=[],
        cluster_columns=None,
        merge_columns=["customer_id"],
        tags={"scd.type": "2", **tags},
        schema=StructType(BUSINESS_SCHEMA + TEMPORAL_SCHEMA),
    )


def _scd1_entity(request, name, tags):
    return SimpleNamespace(
        entityid=f"silver.{name}_{request.node.callspec.id.replace('.', '_').replace('-', '_')}",
        name=name,
        partition_columns=[],
        cluster_columns=None,
        merge_columns=["customer_id"],
        tags=dict(tags),
        schema=SOURCE_SCHEMA,
    )


def _merge(spark, provider, entity, rows, schema=SOURCE_SCHEMA):
    provider.merge_to_entity(spark.createDataFrame(rows, schema), entity)


def _versions(provider, entity, key="c1"):
    rows = [r for r in provider.read_entity(entity).collect() if r["customer_id"] == key]
    current = [r for r in rows if r["__is_current"]]
    closed = sorted((r for r in rows if not r["__is_current"]), key=lambda r: r["__effective_from"])
    return current, closed


# ── SCD2 ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("form", list(UNTRACKED_TAGS))
def test_scd2_untracked_only_change_updates_current_in_place(spark, provider, request, form):
    entity = _scd2_entity(request, "untracked_inplace", UNTRACKED_TAGS[form])

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    (opened,), _ = _versions(provider, entity)

    _merge(spark, provider, entity, [("c1", "bronze", "L2", TS(1))])

    current, closed = _versions(provider, entity)
    assert closed == [], "an untracked-only change must not open a new version"
    assert len(current) == 1
    assert current[0]["load_id"] == "L2", "the untracked column updates in place"
    assert current[0]["__effective_from"] == opened["__effective_from"]
    assert current[0]["__effective_to"] is None


@pytest.mark.parametrize("form", list(UNTRACKED_TAGS))
def test_scd2_tracked_change_closes_with_its_untracked_values(spark, provider, request, form):
    entity = _scd2_entity(request, "untracked_close", UNTRACKED_TAGS[form])

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    _merge(spark, provider, entity, [("c1", "bronze", "L2", TS(1))])  # in place
    _merge(spark, provider, entity, [("c1", "silver", "L3", TS(1))])  # tracked

    current, closed = _versions(provider, entity)
    assert len(current) == 1 and len(closed) == 1
    assert current[0]["status"] == "silver" and current[0]["load_id"] == "L3"
    assert closed[0]["status"] == "bronze"
    assert closed[0]["load_id"] == "L2", "history keeps the untracked value it had at close"
    assert closed[0]["__effective_to"] is not None


def test_scd2_no_change_is_a_no_op(spark, provider, request):
    entity = _scd2_entity(request, "untracked_noop", {"scd.untracked": "load_id"})

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1)), ("c2", None, None, TS(1))])
    before = sorted(provider.read_entity(entity).collect())
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1)), ("c2", None, None, TS(1))])

    assert sorted(provider.read_entity(entity).collect()) == before


def test_scd2_sequence_only_bump_updates_in_place(spark, provider, request):
    entity = _scd2_entity(request, "seq_bump", {"scd.sequence_by": "updated_at"})

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(2))])

    current, closed = _versions(provider, entity)
    assert closed == []
    assert current[0]["updated_at"] == TS(2), "a sequence-only bump updates in place"
    assert current[0]["__effective_from"] == TS(1)

    _merge(spark, provider, entity, [("c1", "silver", "L1", TS(3))])
    current, closed = _versions(provider, entity)
    assert current[0]["status"] == "silver" and current[0]["__effective_from"] == TS(3)
    assert closed[0]["updated_at"] == TS(2) and closed[0]["__effective_to"] == TS(3)


def test_scd2_sequence_guard_compares_against_current_sequence_value(spark, provider, request):
    """After an in-place update at TS(5), events at TS(3) are older than the
    current row even though they are later than its effective_from (TS(1)):
    neither an untracked nor a tracked change from TS(3) may apply."""
    entity = _scd2_entity(
        request,
        "seq_guard",
        {"scd.sequence_by": "updated_at", "scd.untracked": "load_id"},
    )

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    _merge(spark, provider, entity, [("c1", "bronze", "L5", TS(5))])  # in place

    _merge(spark, provider, entity, [("c1", "bronze", "L3", TS(3))])  # stale untracked
    _merge(spark, provider, entity, [("c1", "silver", "L3", TS(3))])  # stale tracked

    current, closed = _versions(provider, entity)
    assert closed == [], "a stale tracked change must not close the current version"
    assert current[0]["status"] == "bronze"
    assert current[0]["load_id"] == "L5", "an older event must not overwrite a newer value"
    assert current[0]["updated_at"] == TS(5)
    assert current[0]["__effective_from"] == TS(1)

    _merge(spark, provider, entity, [("c1", "silver", "L6", TS(6))])
    current, closed = _versions(provider, entity)
    assert current[0]["__effective_from"] == TS(6)
    assert closed[0]["__effective_to"] == TS(6) and closed[0]["load_id"] == "L5"


def test_scd2_stale_delete_is_ignored_after_in_place_update(spark, provider, request):
    entity = _scd2_entity(
        request,
        "seq_delete",
        {
            "scd.sequence_by": "updated_at",
            "scd.untracked": "load_id",
            "scd.delete_when": "status = 'DELETED'",
        },
    )

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    _merge(spark, provider, entity, [("c1", "bronze", "L5", TS(5))])  # in place
    _merge(spark, provider, entity, [("c1", "DELETED", "L3", TS(3))])  # stale

    current, closed = _versions(provider, entity)
    assert len(current) == 1 and closed == []

    _merge(spark, provider, entity, [("c1", "DELETED", "L7", TS(7))])
    current, closed = _versions(provider, entity)
    assert current == []
    assert closed[0]["status"] == "bronze" and closed[0]["load_id"] == "L5"
    assert closed[0]["__effective_to"] == TS(7)


def test_scd2_snapshot_untracked_in_place_and_missing_close(spark, provider, request):
    entity = _scd2_entity(
        request,
        "snapshot",
        {"scd.source_kind": "snapshot", "scd.untracked": "load_id"},
    )

    _merge(
        spark,
        provider,
        entity,
        [
            ("c1", "bronze", "L1", TS(1)),
            ("c2", "gold", "L1", TS(1)),
            ("c3", "lead", "L1", TS(1)),
            ("c4", None, "L1", TS(1)),
        ],
    )
    # c1 untracked-only, c2 unchanged, c3 missing, c4 unchanged with a null
    # tracked column.
    _merge(
        spark,
        provider,
        entity,
        [("c1", "bronze", "L2", TS(1)), ("c2", "gold", "L1", TS(1)), ("c4", None, "L1", TS(1))],
    )

    current, closed = _versions(provider, entity, "c1")
    assert closed == [] and current[0]["load_id"] == "L2"
    current, closed = _versions(provider, entity, "c2")
    assert closed == [] and len(current) == 1
    current, closed = _versions(provider, entity, "c3")
    assert current == [] and len(closed) == 1, "a missing key closes"
    current, closed = _versions(provider, entity, "c4")
    assert closed == [] and len(current) == 1, (
        "a present key whose tracked column is null on both sides is unchanged, " "not missing"
    )


def test_scd2_optimize_unchanged_with_untracked(spark, provider, request):
    entity = _scd2_entity(
        request,
        "opt_unchanged",
        {"scd.optimize_unchanged": "true", "scd.untracked": "load_id"},
    )

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    _merge(spark, provider, entity, [("c1", "bronze", "L2", TS(1))])
    current, closed = _versions(provider, entity)
    assert closed == [] and current[0]["load_id"] == "L2"

    _merge(spark, provider, entity, [("c1", "silver", "L3", TS(1))])
    current, closed = _versions(provider, entity)
    assert current[0]["status"] == "silver" and closed[0]["load_id"] == "L2"


# ── SCD1 (default keyed merge) ────────────────────────────────────────────────


def _scd1_rows(provider, entity):
    return {r["customer_id"]: r for r in provider.read_entity(entity).collect()}


def test_scd1_untracked_only_change_is_skipped(spark, provider, request):
    entity = _scd1_entity(request, "scd1_untracked", {"scd.untracked": "load_id,updated_at"})

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1)), ("c2", None, "L1", TS(1))])
    _merge(
        spark,
        provider,
        entity,
        [
            ("c1", "bronze", "L2", TS(2)),  # untracked-only: skipped
            ("c2", None, "L2", TS(2)),  # null == null: skipped
            ("c3", "new", "L2", TS(2)),  # new key: inserted
        ],
    )

    rows = _scd1_rows(provider, entity)
    assert rows["c1"]["load_id"] == "L1" and rows["c1"]["updated_at"] == TS(1)
    assert rows["c2"]["load_id"] == "L1"
    assert rows["c3"]["status"] == "new"

    _merge(spark, provider, entity, [("c1", "silver", "L3", TS(3)), ("c2", "x", "L3", TS(3))])
    rows = _scd1_rows(provider, entity)
    assert rows["c1"]["status"] == "silver" and rows["c1"]["load_id"] == "L3"
    assert rows["c2"]["status"] == "x" and rows["c2"]["load_id"] == "L3"
    assert len(rows) == 3


def test_scd1_without_untracked_tag_updates_every_matched_row(spark, provider, request):
    entity = _scd1_entity(request, "scd1_plain", {})

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    _merge(spark, provider, entity, [("c1", "bronze", "L2", TS(2))])

    rows = _scd1_rows(provider, entity)
    assert rows["c1"]["load_id"] == "L2" and rows["c1"]["updated_at"] == TS(2)


def test_scd1_tracked_column_new_to_table_materializes(spark, provider, request):
    # The declared schema predates updated_at; the table gains it by evolution.
    narrow = StructType([f for f in BUSINESS_SCHEMA if f.name != "updated_at"])
    entity = _scd1_entity(request, "scd1_evolve", {"scd.untracked": "load_id"})
    entity.schema = narrow

    _merge(spark, provider, entity, [("c1", "bronze", "L1")], schema=narrow)
    _merge(spark, provider, entity, [("c1", "bronze", "L2", TS(2))])

    rows = _scd1_rows(provider, entity)
    assert rows["c1"]["updated_at"] == TS(2), "a new tracked column must land on matched rows"
