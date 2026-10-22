"""Provenance column in the SCD1/SCD2 merges: Delta vs. Memory parity.

The provenance column is untracked and also left out of the untracked
comparison, and it is written with every row write ("overwrite
overwrites"). Every scenario runs against both DeltaEntityProvider and
MemoryEntityProvider:

- a provenance-only difference opens no SCD2 version, triggers no SCD2
  in-place update, and no SCD1 update under ``scd.untracked``;
- an SCD2 close keeps the closed version's stamp, the new version carries
  the write's; an SCD2 in-place update and an SCD1 update overwrite it;
- adding the column to an existing SCD2 table opens no versions;
- a custom surrogate key generator's ``ctx.columns`` leaves it out.

Each write is stamped here with a fixed stamp (``persist_id`` names the
write); the persist step that stamps pipe outputs is covered elsewhere.
Fixtures mirror test_scd_untracked_parity.py.
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
from kindling.provenance import (
    ProvenanceStamp,
    provenance_struct_type,
    stamp_provenance,
)
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.surrogate_keys import SurrogateKeyGenerators
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
        SparkSession.builder.appName("ProvenanceParity")
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
    temp_path = tempfile.mkdtemp(prefix="kindling-provenance-test-")
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
PROVENANCE = {"provenance.enabled": "true"}

TS = lambda day: datetime(2026, 7, day, 0, 0, 0)  # noqa: E731

# The ways an SCD2 entity splits its columns: the provenance column must
# stay out of the change tests under each (default: tracked by default;
# scd.untracked: an exclude-list; scd.tracked: an allow-list, where it would
# otherwise land in the untracked comparison; sequence_by: a sequence-only
# bump updates in place). Each test runs the forms that reach a different
# code path for it, to keep the single-process CI suite's JVM within budget.
SCD2_FORMS = {
    "default": {},
    "scd.untracked": {"scd.untracked": "load_id"},
    "scd.tracked": {"scd.tracked": "status,updated_at"},
    "sequence_by": {"scd.sequence_by": "updated_at"},
}
# A rewrite with an equal sequence value is also stopped by the sequence
# guard, so sequence_by can't pin the no-change rule; these three do.
NO_CHANGE_FORMS = ["default", "scd.untracked", "scd.tracked"]


def _suffix(request):
    return request.node.callspec.id.replace(".", "_").replace("-", "_")


def _scd2_entity(request, name, tags, provenance=True):
    return SimpleNamespace(
        entityid=f"silver.{name}_{_suffix(request)}",
        name=name,
        partition_columns=[],
        cluster_columns=None,
        merge_columns=["customer_id"],
        tags={"scd.type": "2", **tags, **(PROVENANCE if provenance else {})},
        schema=StructType(BUSINESS_SCHEMA + TEMPORAL_SCHEMA),
    )


def _scd1_entity(request, name, tags):
    return SimpleNamespace(
        entityid=f"silver.{name}_{_suffix(request)}",
        name=name,
        partition_columns=[],
        cluster_columns=None,
        merge_columns=["customer_id"],
        tags={**tags, **PROVENANCE},
        schema=SOURCE_SCHEMA,
    )


def _merge(spark, provider, entity, rows, write=None):
    """Merge ``rows``, stamped as write ``write`` (unstamped when None)."""
    df = spark.createDataFrame(rows, SOURCE_SCHEMA)
    if write is not None:
        df = stamp_provenance(
            df, entity, lambda: ProvenanceStamp(pipe_id="p", persist_id=write, mode="batch")
        )
    provider.merge_to_entity(df, entity)


def _stamp(row):
    stamp = row["__provenance"]
    return None if stamp is None else stamp["persist_id"]


def _versions(provider, entity, key="c1"):
    rows = [r for r in provider.read_entity(entity).collect() if r["customer_id"] == key]
    current = [r for r in rows if r["__is_current"]]
    closed = sorted((r for r in rows if not r["__is_current"]), key=lambda r: r["__effective_from"])
    return current, closed


def _rows(provider, entity):
    return {r["customer_id"]: r for r in provider.read_entity(entity).collect()}


# ── SCD2 ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("form", NO_CHANGE_FORMS)
def test_scd2_provenance_only_difference_is_no_change(spark, provider, request, form):
    entity = _scd2_entity(request, "prov_noop", SCD2_FORMS[form])

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))], write="w1")
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))], write="w2")

    current, closed = _versions(provider, entity)
    assert closed == [], "a provenance-only difference must not open a version"
    assert len(current) == 1
    assert _stamp(current[0]) == "w1", "nor update the current version in place"


@pytest.mark.parametrize("form", ["default", "scd.tracked"])
def test_scd2_close_keeps_the_closed_versions_stamp(spark, provider, request, form):
    entity = _scd2_entity(request, "prov_close", SCD2_FORMS[form])

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))], write="w1")
    _merge(spark, provider, entity, [("c1", "silver", "L1", TS(2))], write="w2")

    current, closed = _versions(provider, entity)
    assert len(current) == 1 and len(closed) == 1
    assert _stamp(closed[0]) == "w1", "the close sets only effective_to and is_current"
    assert _stamp(current[0]) == "w2", "the new version carries this write's stamp"


@pytest.mark.parametrize("form", ["scd.tracked", "sequence_by"])
def test_scd2_in_place_update_overwrites_the_stamp(spark, provider, request, form):
    entity = _scd2_entity(request, "prov_touch", SCD2_FORMS[form])

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))], write="w1")
    # An untracked-only change (load_id), or a sequence-only bump.
    changed = (
        ("c1", "bronze", "L2", TS(1)) if form != "sequence_by" else ("c1", "bronze", "L1", TS(2))
    )
    _merge(spark, provider, entity, [changed], write="w2")

    current, closed = _versions(provider, entity)
    assert closed == []
    assert _stamp(current[0]) == "w2", "overwrite overwrites: the in-place update restamps"


@pytest.mark.parametrize("form", ["default", "scd.tracked"])
def test_adding_the_column_to_an_existing_scd2_table_opens_no_versions(
    spark, provider, request, form
):
    before = _scd2_entity(request, "prov_added", SCD2_FORMS[form], provenance=False)
    after = SimpleNamespace(**{**vars(before), "tags": {**before.tags, **PROVENANCE}})

    _merge(
        spark,
        provider,
        before,
        [("c1", "bronze", "L1", TS(1)), ("c2", "bronze", "L1", TS(1))],
    )
    _merge(
        spark,
        provider,
        after,
        [("c1", "bronze", "L1", TS(1)), ("c2", "silver", "L1", TS(2))],
        write="w2",
    )

    current, closed = _versions(provider, after)
    assert closed == [], "the new column alone must not re-version the table"
    assert _stamp(current[0]) is None, "an unwritten row keeps a null stamp"
    current, closed = _versions(provider, after, key="c2")
    assert _stamp(closed[0]) is None and _stamp(current[0]) == "w2"


# ── SCD1 ──────────────────────────────────────────────────────────────────────


def test_scd1_provenance_only_difference_is_skipped_with_untracked(spark, provider, request):
    entity = _scd1_entity(request, "prov_scd1_skip", {"scd.untracked": "load_id"})

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))], write="w1")
    _merge(spark, provider, entity, [("c1", "bronze", "L2", TS(1))], write="w2")

    row = _rows(provider, entity)["c1"]
    assert row["load_id"] == "L1", "an untracked-only change is skipped"
    assert _stamp(row) == "w1", "so the stamp names the last writer that made a real change"


def test_scd1_update_overwrites_the_stamp(spark, provider, request):
    entity = _scd1_entity(request, "prov_scd1_update", {"scd.untracked": "load_id"})

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))], write="w1")
    _merge(spark, provider, entity, [("c1", "silver", "L1", TS(1))], write="w2")

    row = _rows(provider, entity)["c1"]
    assert row["status"] == "silver" and _stamp(row) == "w2"


def test_scd1_without_change_detection_restamps_every_matched_row(spark, provider, request):
    entity = _scd1_entity(request, "prov_scd1_all", {})

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))], write="w1")
    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))], write="w2")

    assert _stamp(_rows(provider, entity)["c1"]) == "w2"


# ── Surrogate keys ────────────────────────────────────────────────────────────

SEEN_COLUMNS = []


def recording_key(ctx):
    """Module-level custom generator recording the columns it is shown."""
    SEEN_COLUMNS.append(tuple(ctx.columns))
    return F.concat_ws("|", *[ctx.col(name).cast("string") for name in ctx.columns])


def test_custom_generator_columns_leave_out_the_provenance_column(spark, provider, request):
    SEEN_COLUMNS.clear()
    SurrogateKeyGenerators.register("recording_key", deterministic=True)(recording_key)
    entity = _scd2_entity(
        request,
        "prov_keys",
        {
            "surrogate.column": "customer_sk",
            "surrogate.strategy": "custom",
            "surrogate.generator": "recording_key",
        },
    )

    _merge(spark, provider, entity, [("c1", "bronze", "L1", TS(1))], write="w1")
    _merge(spark, provider, entity, [("c1", "silver", "L1", TS(2))], write="w2")

    assert SEEN_COLUMNS, "the generator ran"
    for columns in SEEN_COLUMNS:
        assert "__provenance" not in columns, columns
        assert set(columns) == {"customer_id", "status", "load_id", "updated_at"}, columns
    current, closed = _versions(provider, entity)
    assert current[0]["customer_sk"] != closed[0]["customer_sk"]


# ── Table creation ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("form", ["scd2"])
def test_delta_table_is_created_with_the_column(spark, delta_provider, request, form):
    entity = _scd2_entity(request, "prov_create", {})
    delta_provider.ensure_entity_table(entity)

    schema = delta_provider.read_entity(entity).schema
    assert schema["__provenance"].dataType == provenance_struct_type()
    assert schema.fieldNames()[-1] == "__provenance"
