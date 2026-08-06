"""Cross-provider SCD2 parity: DeltaEntityProvider vs. MemoryEntityProvider.

Both providers implement the same declared-flow SCD2 contract (SCDConfig /
scd_config_from_tags) — Delta via a single staged MERGE INTO, Memory via
plain DataFrame joins/unions (see entity_provider_memory.py). This file runs
the same scenarios (existing rows + incoming batch + tags) through both,
asserting equivalent current-row content and is_current/effective_to
structure. Local Spark + local Delta only, no Azure — same placement as
tests/integration/test_scd2_declared_flow.py, which characterizes Delta's
own behavior in far more depth; this file only asserts parity.

Effective_from/effective_to are stamped from current_timestamp() when
scd.sequence_by is unset, so wall-clock equality across providers is not
asserted for those scenarios — only structure (is_current, non-null-ness,
ordering). With sequence_by set, both providers stamp from the identical
incoming data value, so exact equality is asserted.
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


def _teardown_existing_spark_jvm():
    """See test_scd2_declared_flow.py — Delta jars are only on the classpath
    if this module's session launches the JVM."""
    from pyspark import SparkContext

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    if SparkContext._gateway is not None:
        try:
            SparkContext._gateway.shutdown()
        except Exception:
            pass
        SparkContext._gateway = None
        SparkContext._jvm = None


@pytest.fixture(scope="module")
def spark():
    _teardown_existing_spark_jvm()
    builder = (
        SparkSession.builder.appName("SCD2ProviderParity")
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
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    temp_path = tempfile.mkdtemp(prefix="kindling-scd2-parity-test-")
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
    StructField("updated_at", TimestampType(), True),
]

TEMPORAL_SCHEMA = [
    StructField("__effective_from", TimestampType(), True),
    StructField("__effective_to", TimestampType(), True),
    StructField("__is_current", BooleanType(), True),
]

FULL_SCHEMA = StructType(BUSINESS_SCHEMA + TEMPORAL_SCHEMA)
SOURCE_SCHEMA = StructType(BUSINESS_SCHEMA)


def _make_entity(entityid, tags):
    return SimpleNamespace(
        entityid=entityid,
        name=entityid.split(".")[-1],
        partition_columns=[],
        cluster_columns=None,
        merge_columns=["customer_id"],
        tags={"scd.type": "2", **tags},
        schema=FULL_SCHEMA,
    )


def _merge_batch(spark, provider, entity, rows):
    """Merge a batch, relying on the provider's own first-call bootstrap —
    both DeltaEntityProvider and MemoryEntityProvider self-create the table
    (augmented schema included) on an unseen entity."""
    df = spark.createDataFrame(rows, SOURCE_SCHEMA)
    provider.merge_to_entity(df, entity)


def _rows(provider, entity):
    return {
        (r["customer_id"], r["__is_current"]): r for r in provider.read_entity(entity).collect()
    }


TS = lambda day, hour=0: datetime(2026, 7, day, hour, 0, 0)  # noqa: E731


def test_plain_change_feed_upsert_parity(spark, provider, request):
    """Insert then change a tracked column: one closed + one current version,
    with equivalent content and structure regardless of provider."""
    entity = _make_entity(f"silver.parity_upsert_{request.node.callspec.id}", {})

    _merge_batch(spark, provider, entity, [("c1", "bronze", TS(1))])
    rows = _rows(provider, entity)
    assert ("c1", True) in rows
    assert rows[("c1", True)]["status"] == "bronze"
    assert rows[("c1", True)]["__effective_to"] is None

    _merge_batch(spark, provider, entity, [("c1", "silver", TS(2))])
    all_rows = provider.read_entity(entity).collect()
    current = [r for r in all_rows if r["__is_current"]]
    closed = [r for r in all_rows if not r["__is_current"]]
    assert len(current) == 1 and current[0]["status"] == "silver"
    assert current[0]["__effective_to"] is None
    assert len(closed) == 1 and closed[0]["status"] == "bronze"
    assert closed[0]["__effective_to"] is not None, "closed version must record when it closed"


def test_close_on_missing_snapshot_parity(spark, provider, request):
    """A key absent from a later snapshot batch is closed in both providers;
    a present, unchanged key stays current in both."""
    entity = _make_entity(
        f"silver.parity_snapshot_{request.node.callspec.id}",
        {"scd.close_on_missing": "true"},
    )

    _merge_batch(spark, provider, entity, [("c1", "bronze", TS(1)), ("c2", "gold", TS(1))])
    # c2 vanishes from the next snapshot; c1 is present and unchanged (same
    # updated_at too — scd.sequence_by is unset here, so updated_at is itself
    # a tracked column and must stay identical for c1 to count as unchanged).
    _merge_batch(spark, provider, entity, [("c1", "bronze", TS(1))])

    rows = _rows(provider, entity)
    assert ("c1", True) in rows, "present, unchanged key must stay current"
    assert ("c1", False) not in rows
    assert ("c2", False) in rows, "vanished key must be closed"
    assert ("c2", True) not in rows


def test_sequence_by_ordering_parity(spark, provider, request):
    """With scd.sequence_by set, both providers stamp effective_from/to from
    the identical incoming data value — wall-clock equality is assertable
    here (unlike the current_timestamp()-driven scenarios above)."""
    entity = _make_entity(
        f"silver.parity_seq_{request.node.callspec.id}", {"scd.sequence_by": "updated_at"}
    )

    _merge_batch(spark, provider, entity, [("c1", "bronze", TS(1))])
    _merge_batch(spark, provider, entity, [("c1", "silver", TS(5))])

    all_rows = provider.read_entity(entity).collect()
    current = next(r for r in all_rows if r["__is_current"])
    closed = next(r for r in all_rows if not r["__is_current"])
    assert current["status"] == "silver"
    assert current["__effective_from"] == TS(5)
    assert closed["__effective_to"] == TS(5), (
        "the closed version's validity must end exactly where the new "
        "version begins, identically for both providers"
    )

    # Out-of-order (older sequence value) must be ignored by both.
    _merge_batch(spark, provider, entity, [("c1", "bronze", TS(2))])
    all_rows = provider.read_entity(entity).collect()
    assert len(all_rows) == 2, "a stale out-of-order row must not be applied"
    current = next(r for r in all_rows if r["__is_current"])
    assert current["status"] == "silver"


def test_delete_when_parity(spark, provider, request):
    """A delete_when match closes the current version without inserting a
    new one; deletes for unknown keys are no-ops. Identical for both."""
    entity = _make_entity(
        f"silver.parity_delete_{request.node.callspec.id}",
        {"scd.delete_when": "status = 'DELETED'", "scd.sequence_by": "updated_at"},
    )

    _merge_batch(spark, provider, entity, [("c1", "bronze", TS(1))])
    _merge_batch(spark, provider, entity, [("c1", "DELETED", TS(3))])

    all_rows = provider.read_entity(entity).collect()
    assert len(all_rows) == 1, "a delete closes; it must not insert a new version"
    assert all_rows[0]["__is_current"] is False
    assert all_rows[0]["status"] == "bronze"
    assert all_rows[0]["__effective_to"] == TS(3)

    # Delete for an unknown key is a no-op.
    other_entity = _make_entity(
        f"silver.parity_delete_unknown_{request.node.callspec.id}",
        {"scd.delete_when": "status = 'DELETED'", "scd.sequence_by": "updated_at"},
    )
    _merge_batch(spark, provider, other_entity, [("zz", "DELETED", TS(3))])
    assert provider.read_entity(other_entity).count() == 0
