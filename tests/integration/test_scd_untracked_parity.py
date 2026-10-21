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

from tests.spark_test_helper import (
    _teardown_non_delta_jvm,
    _worker_scoped_ivy_dir,
    set_spark_log_level,
)


@pytest.fixture(scope="module")
def spark():
    _teardown_non_delta_jvm()
    builder = (
        SparkSession.builder.appName("SCDUntrackedParity")
        .config("spark.jars.ivy", _worker_scoped_ivy_dir())
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


# ── Review fixes: reserved staging columns, additive columns, literal names ──


def _with_extra_column(name, data_type=StringType()):
    return StructType(BUSINESS_SCHEMA + [StructField(name, data_type, True)])


@pytest.mark.parametrize(
    "column",
    ["__scd2_action", "__scd2_source_present", "__scd2_source_delete", "__merge_key"],
)
def test_scd2_rejects_reserved_staging_columns(spark, provider, request, column):
    """A business column named like a staging helper would be overwritten
    by the merge (and stored on new versions); both providers refuse it."""
    entity = _scd2_entity(request, "reserved", {})
    schema = _with_extra_column(column, BooleanType())

    with pytest.raises(ValueError, match="reserved"):
        _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1), False)], schema=schema)


NARROW_SCHEMA = StructType([f for f in BUSINESS_SCHEMA if f.name != "load_id"])


@pytest.mark.parametrize("form", list(UNTRACKED_TAGS))
def test_scd2_untracked_column_new_to_table_updates_in_place(spark, provider, request, form):
    """An existing table on an older schema: the batch brings load_id,
    which is untracked, so the current version takes it in place (the
    table gains the column) instead of the merge failing analysis."""
    entity = _scd2_entity(request, "untracked_evolve", UNTRACKED_TAGS[form])
    entity.schema = StructType(list(NARROW_SCHEMA.fields) + TEMPORAL_SCHEMA)

    _merge(spark, provider, entity, [("c1", "bronze", TS(1))], schema=NARROW_SCHEMA)
    (opened,), _ = _versions(provider, entity)

    _merge(spark, provider, entity, [("c1", "bronze", "L2", TS(1))])

    current, closed = _versions(provider, entity)
    assert closed == [], "a new untracked column must not open a new version"
    assert current[0]["load_id"] == "L2"
    assert current[0]["__effective_from"] == opened["__effective_from"]


def test_scd2_tracked_column_new_to_table_opens_new_versions(spark, provider, request):
    entity = _scd2_entity(request, "tracked_evolve", {})
    entity.schema = StructType(list(NARROW_SCHEMA.fields) + TEMPORAL_SCHEMA)

    _merge(spark, provider, entity, [("c1", "bronze", TS(1))], schema=NARROW_SCHEMA)
    _merge(spark, provider, entity, [("c1", "bronze", "L2", TS(1))])

    current, closed = _versions(provider, entity)
    assert current[0]["load_id"] == "L2"
    assert len(closed) == 1 and closed[0]["load_id"] is None


DOTTED_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), True),
        StructField("address.city", StringType(), True),
    ]
)


@pytest.mark.parametrize(
    "tags", [{}, {"scd.untracked": "address.city"}], ids=["no_tags", "untracked"]
)
def test_scd2_literal_dotted_column_name(spark, provider, request, tags):
    """`address.city` is one column, not a field path into `address`."""
    entity = _scd2_entity(request, "dotted", tags)
    entity.schema = StructType(list(DOTTED_SCHEMA.fields) + TEMPORAL_SCHEMA)

    _merge(spark, provider, entity, [("c1", "bronze", "Oslo")], schema=DOTTED_SCHEMA)
    _merge(spark, provider, entity, [("c1", "bronze", "Bergen")], schema=DOTTED_SCHEMA)

    current, closed = _versions(provider, entity)
    assert current[0]["address.city"] == "Bergen"
    if tags:
        assert closed == [], "an untracked-only change updates in place"
    else:
        assert len(closed) == 1 and closed[0]["address.city"] == "Oslo"


def test_scd1_untracked_literal_dotted_business_key(spark, provider, request):
    """A business key named `source.id` is a literal column name."""
    schema = StructType(
        [
            StructField("source.id", StringType(), False),
            StructField("status", StringType(), True),
            StructField("load_id", StringType(), True),
        ]
    )
    entity = _scd1_entity(request, "scd1_dotted_key", {"scd.untracked": "load_id"})
    entity.merge_columns = ["source.id"]
    entity.schema = schema

    _merge(spark, provider, entity, [("k1", "bronze", "L1")], schema=schema)
    _merge(spark, provider, entity, [("k1", "bronze", "L2")], schema=schema)  # skipped
    _merge(spark, provider, entity, [("k1", "silver", "L3")], schema=schema)  # updated

    rows = provider.read_entity(entity).collect()
    assert len(rows) == 1
    assert rows[0]["status"] == "silver" and rows[0]["load_id"] == "L3"


# ── Case-only column-name differences (spark.sql.caseSensitive=false) ────────

UPPER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("STATUS", StringType(), True),
        StructField("LOAD_ID", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


@pytest.mark.parametrize("tags", [{}, {"scd.untracked": "load_id"}], ids=["no_tags", "untracked"])
def test_scd2_case_only_column_difference_is_the_same_column(spark, provider, request, tags):
    """`STATUS` in the batch is the table's `status` under Spark's default
    case-insensitive resolution: an unchanged value opens no version and
    the existing history keeps its values."""
    entity = _scd2_entity(request, "case_scd2", tags)
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    _merge(spark, provider, entity, [("c1", "silver", "L1", TS(1))])

    load_id = "L2" if tags else "L1"
    _merge(spark, provider, entity, [("c1", "silver", load_id, TS(1))], schema=UPPER_SCHEMA)

    current, closed = _versions(provider, entity)
    assert len(current) == 1 and len(closed) == 1, "an unchanged value opens no version"
    assert current[0]["status"] == "silver"
    assert current[0]["load_id"] == load_id
    assert closed[0]["status"] == "bronze", "history keeps its values"
    assert closed[0]["load_id"] == "L1"


def test_scd1_case_only_column_difference_is_the_same_column(spark, provider, request):
    entity = _scd1_entity(request, "case_scd1", {"scd.untracked": "load_id"})
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])

    _merge(spark, provider, entity, [("c1", "bronze", "L2", TS(1))], schema=UPPER_SCHEMA)

    rows = provider.read_entity(entity).collect()
    assert len(rows) == 1
    assert rows[0]["status"] == "bronze"
    assert rows[0]["load_id"] == "L1", "an untracked-only change is skipped"


# ── several rows for one business key in one batch ────────────────────────────


def test_scd2_several_stale_events_for_one_key_are_a_no_op(spark, provider, request):
    """Current version at sequence 5; the batch carries two older events for
    the key. Both fail the strictly-later guard: nothing changes (the current
    row must not be copied once per source row)."""
    entity = _scd2_entity(request, "dup_stale", {"scd.sequence_by": "updated_at"})
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(5))])
    before = sorted(provider.read_entity(entity).collect())

    _merge(spark, provider, entity, [("c1", "silver", "L3", TS(3)), ("c1", "gold", "L4", TS(4))])

    assert sorted(provider.read_entity(entity).collect()) == before


def test_scd2_several_newer_events_for_one_key_apply_the_latest(spark, provider, request):
    """With scd.sequence_by, a batch is collapsed to the latest row per key
    (the convention merge_as_stream applies per micro-batch): one new version
    from the latest event, closing the current version at its sequence."""
    entity = _scd2_entity(request, "dup_newer", {"scd.sequence_by": "updated_at"})
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])

    _merge(spark, provider, entity, [("c1", "silver", "L3", TS(3)), ("c1", "gold", "L4", TS(4))])

    current, closed = _versions(provider, entity)
    assert len(current) == 1 and len(closed) == 1
    assert current[0]["status"] == "gold" and current[0]["__effective_from"] == TS(4)
    assert closed[0]["status"] == "bronze" and closed[0]["__effective_to"] == TS(4)


def test_scd2_exact_duplicate_rows_without_sequence_by_collapse(spark, provider, request):
    """Without scd.sequence_by, rows identical in every column (full reloads,
    retried extracts) collapse to one on both providers: an unchanged key is
    a no-op and a new key gets a single version."""
    entity = _scd2_entity(request, "dup_exact_noseq", {})
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    before = sorted(provider.read_entity(entity).collect())

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1)), ("c1", "bronze", "L1", TS(1))])
    assert sorted(provider.read_entity(entity).collect()) == before

    _merge(spark, provider, entity, [("c2", "gold", "L2", TS(2)), ("c2", "gold", "L2", TS(2))])
    assert len([r for r in provider.read_entity(entity).collect() if r["customer_id"] == "c2"]) == 1


def test_scd2_nested_map_payload_without_sequence_by(spark, provider, request):
    """Without scd.sequence_by the batch is de-duplicated by whole-row
    identity; a struct or array containing a map can't be a Spark partition
    key, so it is compared through its JSON form. optimize_unchanged entities
    (whose hash comparison supports such payloads) keep working: a single
    row merges, exact duplicates collapse, and an unchanged row is a no-op."""
    from pyspark.sql.types import ArrayType, MapType

    payload = StructField(
        "payload",
        StructType(
            [
                StructField("attrs", MapType(StringType(), StringType()), True),
                StructField("tags", ArrayType(MapType(StringType(), StringType())), True),
            ]
        ),
        True,
    )
    source_schema = StructType(BUSINESS_SCHEMA + [payload])
    entity = _scd2_entity(request, "nested_map_noseq", {"scd.optimize_unchanged": "true"})
    entity.schema = StructType(BUSINESS_SCHEMA + [payload] + TEMPORAL_SCHEMA)
    row = ("c1", "bronze", "L1", TS(1), ({"a": "1"}, [{"k": "v"}]))

    _merge(spark, provider, entity, [row], schema=source_schema)
    _merge(spark, provider, entity, [row, row], schema=source_schema)

    current, closed = _versions(provider, entity)
    assert len(current) == 1 and closed == []
    assert current[0]["payload"]["attrs"] == {"a": "1"}


def _payload_field():
    from pyspark.sql.types import ArrayType, MapType

    return StructField(
        "payload",
        StructType(
            [
                StructField("attrs", MapType(StringType(), StringType()), True),
                StructField("tags", ArrayType(MapType(StringType(), StringType())), True),
            ]
        ),
        True,
    )


def test_scd2_untracked_nested_map_payload_updates_in_place(spark, provider, request):
    """Change detection compares a struct or array containing a map through
    its JSON form: with scd.tracked: status, an untracked payload change
    updates the current version in place, and an unchanged row is a no-op."""
    source_schema = StructType(BUSINESS_SCHEMA + [_payload_field()])
    entity = _scd2_entity(request, "scd2_nested_payload", {"scd.tracked": "status"})
    entity.schema = StructType(BUSINESS_SCHEMA + [_payload_field()] + TEMPORAL_SCHEMA)

    _merge(
        spark,
        provider,
        entity,
        [("c1", "bronze", "L1", TS(1), ({"a": "1"}, [{"k": "v"}]))],
        source_schema,
    )
    _merge(
        spark,
        provider,
        entity,
        [("c1", "bronze", "L1", TS(1), ({"a": "2"}, [{"k": "v"}]))],
        source_schema,
    )
    _merge(
        spark,
        provider,
        entity,
        [("c1", "bronze", "L1", TS(1), ({"a": "2"}, [{"k": "v"}]))],
        source_schema,
    )

    current, closed = _versions(provider, entity)
    assert closed == [] and len(current) == 1
    assert current[0]["payload"]["attrs"] == {"a": "2"}


def test_scd1_tracked_nested_map_payload(spark, provider, request):
    """SCD1 with scd.untracked: a tracked struct-with-map payload is compared
    through its JSON form; an unchanged payload is skipped, a changed one
    updates the row."""
    source_schema = StructType(BUSINESS_SCHEMA + [_payload_field()])
    entity = _scd1_entity(request, "scd1_nested_payload", {"scd.untracked": "load_id"})
    entity.schema = source_schema

    _merge(
        spark,
        provider,
        entity,
        [("c1", "bronze", "L1", TS(1), ({"a": "1"}, [{"k": "v"}]))],
        source_schema,
    )
    _merge(
        spark,
        provider,
        entity,
        [("c1", "bronze", "L2", TS(1), ({"a": "1"}, [{"k": "v"}]))],
        source_schema,
    )
    assert _scd1_rows(provider, entity)["c1"]["load_id"] == "L1"  # untracked-only: skipped

    _merge(
        spark,
        provider,
        entity,
        [("c1", "bronze", "L3", TS(1), ({"a": "9"}, [{"k": "v"}]))],
        source_schema,
    )
    row = _scd1_rows(provider, entity)["c1"]
    assert row["payload"]["attrs"] == {"a": "9"} and row["load_id"] == "L3"


def test_scd2_differing_duplicate_keys_without_sequence_by_are_rejected(spark, provider, request):
    """Without scd.sequence_by nothing orders two different rows for one key,
    so the batch is rejected on both providers and the table is unchanged."""
    entity = _scd2_entity(request, "dup_noseq", {})
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))])
    before = sorted(provider.read_entity(entity).collect())

    with pytest.raises(ValueError, match="differing rows"):
        _merge(
            spark, provider, entity, [("c1", "bronze", "L1", TS(1)), ("c1", "silver", "L1", TS(1))]
        )
    assert sorted(provider.read_entity(entity).collect()) == before
