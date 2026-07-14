"""Integration tests for SCD2 declared-flow semantics against real Delta.

Characterization tests pin the behavior that must survive the refactor
(version chaining, close-on-missing snapshots). New-behavior tests cover
the declared-flow additions:

- ``scd.sequence_by`` — ordering authority comes from a declared data
  column instead of merge-time ``current_timestamp()``: effective_from of
  a new version and effective_to of the version it closes are the
  incoming row's sequence value, and stale (out-of-order) rows are
  ignored under strictly-later semantics.
- ``scd.source_kind`` — ``snapshot`` (absence closes, the explicit form
  of ``scd.close_on_missing``) vs ``change_feed`` (rows are change
  events; absence means nothing).
- ``scd.delete_when`` — change-feed delete markers close the current
  version without inserting a new one.
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
    """See test_watermark_incremental_correctness — Delta jars are only on
    the classpath if this module's session launches the JVM."""
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
        SparkSession.builder.appName("SCD2DeclaredFlow")
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
    temp_path = tempfile.mkdtemp(prefix="kindling-scd2-test-")
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
        tags={"provider_type": "delta", "scd.type": "2", **tags},
        schema=FULL_SCHEMA,
    )


def _seed_table(spark, provider, entity):
    """Create the SCD2 table with augmented schema (as registration-time
    ensure would)."""
    empty = spark.createDataFrame([], FULL_SCHEMA)
    provider.write_to_entity(empty, entity)


def _merge_batch(spark, provider, entity, rows):
    df = spark.createDataFrame(rows, SOURCE_SCHEMA)
    provider.merge_to_entity(df, entity)


def _rows(provider, entity):
    return {
        (r["customer_id"], r["__is_current"]): r for r in provider.read_entity(entity).collect()
    }


TS = lambda day, hour=0: datetime(2026, 7, day, hour, 0, 0)  # noqa: E731


class TestCharacterization:
    """Current SCD2 behavior that must survive the refactor."""

    def test_insert_then_change_chains_versions(self, spark, delta_provider):
        entity = _make_entity("silver.customers_char", {})
        _seed_table(spark, delta_provider, entity)

        _merge_batch(spark, delta_provider, entity, [("c1", "bronze", TS(1))])
        rows = _rows(delta_provider, entity)
        assert ("c1", True) in rows
        assert rows[("c1", True)]["__effective_to"] is None

        _merge_batch(spark, delta_provider, entity, [("c1", "silver", TS(2))])
        all_rows = delta_provider.read_entity(entity).collect()
        current = [r for r in all_rows if r["__is_current"]]
        closed = [r for r in all_rows if not r["__is_current"]]
        assert len(current) == 1 and current[0]["status"] == "silver"
        assert len(closed) == 1 and closed[0]["status"] == "bronze"
        assert closed[0]["__effective_to"] is not None

    def test_close_on_missing_closes_absent_keys(self, spark, delta_provider):
        entity = _make_entity("silver.customers_com", {"scd.close_on_missing": "true"})
        _seed_table(spark, delta_provider, entity)

        _merge_batch(
            spark,
            delta_provider,
            entity,
            [("c1", "bronze", TS(1)), ("c2", "gold", TS(1))],
        )
        # Next snapshot: c2 vanished.
        _merge_batch(spark, delta_provider, entity, [("c1", "bronze", TS(2))])

        rows = _rows(delta_provider, entity)
        assert ("c1", True) in rows, "present, unchanged key stays current"
        assert ("c2", False) in rows, "vanished key must be closed"
        assert ("c2", True) not in rows


class TestSequenceBy:
    """scd.sequence_by: ordering authority from data, not the wall clock."""

    def _entity(self, entityid, extra=None):
        return _make_entity(entityid, {"scd.sequence_by": "updated_at", **(extra or {})})

    def test_effective_columns_carry_sequence_values(self, spark, delta_provider):
        entity = self._entity("silver.customers_seq")
        _seed_table(spark, delta_provider, entity)

        _merge_batch(spark, delta_provider, entity, [("c1", "bronze", TS(1))])
        rows = _rows(delta_provider, entity)
        assert rows[("c1", True)]["__effective_from"] == TS(1)

        _merge_batch(spark, delta_provider, entity, [("c1", "silver", TS(5))])
        all_rows = delta_provider.read_entity(entity).collect()
        current = next(r for r in all_rows if r["__is_current"])
        closed = next(r for r in all_rows if not r["__is_current"])
        assert current["__effective_from"] == TS(5)
        assert closed["__effective_to"] == TS(5), (
            "the closed version's validity must end exactly where the new "
            "version begins — contiguous history in sequence time"
        )

    def test_out_of_order_stale_row_is_ignored(self, spark, delta_provider):
        entity = self._entity("silver.customers_ooo")
        _seed_table(spark, delta_provider, entity)

        _merge_batch(spark, delta_provider, entity, [("c1", "silver", TS(5))])
        # A late-arriving change with an OLDER sequence value must not
        # close the newer version (strictly-later semantics).
        _merge_batch(spark, delta_provider, entity, [("c1", "bronze", TS(2))])

        all_rows = delta_provider.read_entity(entity).collect()
        assert len(all_rows) == 1
        assert all_rows[0]["__is_current"] is True
        assert all_rows[0]["status"] == "silver"

    def test_missing_sequence_column_in_batch_raises(self, spark, delta_provider):
        entity = self._entity("silver.customers_seqmiss")
        _seed_table(spark, delta_provider, entity)
        bad = spark.createDataFrame(
            [("c1", "bronze")],
            StructType(
                [
                    StructField("customer_id", StringType(), False),
                    StructField("status", StringType(), True),
                ]
            ),
        )
        with pytest.raises(ValueError, match="sequence_by"):
            delta_provider.merge_to_entity(bad, entity)

    def test_null_sequence_value_in_batch_raises(self, spark, delta_provider):
        entity = self._entity("silver.customers_seqnull")
        _seed_table(spark, delta_provider, entity)
        bad = spark.createDataFrame([("c1", "bronze", None)], SOURCE_SCHEMA)
        with pytest.raises(ValueError, match="null values"):
            delta_provider.merge_to_entity(bad, entity)


class TestSourceKind:
    def test_snapshot_is_explicit_close_on_missing(self, spark, delta_provider):
        entity = _make_entity("silver.customers_snap", {"scd.source_kind": "snapshot"})
        _seed_table(spark, delta_provider, entity)

        _merge_batch(
            spark,
            delta_provider,
            entity,
            [("c1", "bronze", TS(1)), ("c2", "gold", TS(1))],
        )
        _merge_batch(spark, delta_provider, entity, [("c1", "bronze", TS(2))])

        rows = _rows(delta_provider, entity)
        assert ("c2", False) in rows and ("c2", True) not in rows

    def test_change_feed_absence_means_nothing(self, spark, delta_provider):
        entity = _make_entity("silver.customers_cf", {"scd.source_kind": "change_feed"})
        _seed_table(spark, delta_provider, entity)

        _merge_batch(
            spark,
            delta_provider,
            entity,
            [("c1", "bronze", TS(1)), ("c2", "gold", TS(1))],
        )
        _merge_batch(spark, delta_provider, entity, [("c1", "silver", TS(2))])

        rows = _rows(delta_provider, entity)
        assert ("c2", True) in rows, "absent key must remain current in change-feed mode"


class TestDeleteWhen:
    def _entity(self, entityid):
        return _make_entity(
            entityid,
            {
                "scd.source_kind": "change_feed",
                "scd.delete_when": "status = 'DELETED'",
                "scd.sequence_by": "updated_at",
            },
        )

    def test_delete_marker_closes_without_inserting(self, spark, delta_provider):
        entity = self._entity("silver.customers_del")
        _seed_table(spark, delta_provider, entity)

        _merge_batch(spark, delta_provider, entity, [("c1", "bronze", TS(1))])
        _merge_batch(spark, delta_provider, entity, [("c1", "DELETED", TS(3))])

        all_rows = delta_provider.read_entity(entity).collect()
        assert len(all_rows) == 1, "a delete closes; it must not insert a new version"
        assert all_rows[0]["__is_current"] is False
        assert all_rows[0]["status"] == "bronze"
        assert all_rows[0]["__effective_to"] == TS(3)

    def test_delete_for_unknown_key_does_nothing(self, spark, delta_provider):
        entity = self._entity("silver.customers_delx")
        _seed_table(spark, delta_provider, entity)

        _merge_batch(spark, delta_provider, entity, [("zz", "DELETED", TS(3))])

        assert delta_provider.read_entity(entity).count() == 0

    def test_stale_delete_is_ignored(self, spark, delta_provider):
        entity = self._entity("silver.customers_delstale")
        _seed_table(spark, delta_provider, entity)

        _merge_batch(spark, delta_provider, entity, [("c1", "silver", TS(5))])
        _merge_batch(spark, delta_provider, entity, [("c1", "DELETED", TS(2))])

        rows = _rows(delta_provider, entity)
        assert ("c1", True) in rows, "an out-of-order delete must not close a newer version"
