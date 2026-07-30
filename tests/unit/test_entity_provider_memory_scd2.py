"""
Unit tests for MemoryEntityProvider.merge_to_entity (SCD1, insert-only, SCD2).

Uses a real SparkSession — not a MagicMock — because this exercises genuine
DataFrame join/filter/union logic that a mock cannot meaningfully stand in
for. Deliberately does not depend on conftest.py's shared, Delta-configured
``spark_session`` fixture: that fixture's builder unconditionally sets
``spark.sql.catalog.spark_catalog`` to Delta's catalog class, and
``SparkSession.getOrCreate()`` applies that onto whatever SparkSession is
already active in the process, including a plain one some earlier,
unrelated unit test created without delta-spark on its classpath — which
then breaks every later query with "Cannot find catalog plugin class."
Memory needs no Delta capability at all, so a local, Delta-free fixture
sidesteps the conflict regardless of what else ran earlier in the suite.
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from kindling.data_entities import EntityMetadata
from kindling.entity_provider_memory import MemoryEntityProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from tests.conftest import _sockets_permitted

TS = lambda day, hour=0: datetime(2026, 7, day, hour, 0, 0)  # noqa: E731


@pytest.fixture(scope="module")
def spark_session():
    """Plain (non-Delta) SparkSession, module-scoped — see module docstring."""
    if not _sockets_permitted():
        pytest.skip(
            "Sockets are not permitted in this environment; cannot start a real SparkSession."
        )
    spark = SparkSession.builder.appName("MemorySCD2Tests").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark


BUSINESS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("status", StringType(), True),
        StructField("seq", IntegerType(), True),
    ]
)

SCD2_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("status", StringType(), True),
        StructField("category", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


def _make_provider(spark_session):
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    provider = MemoryEntityProvider(logger_provider)
    provider.spark = spark_session
    return provider


def _make_entity(entityid, tags=None, schema=BUSINESS_SCHEMA):
    return EntityMetadata(
        entityid=entityid,
        name=entityid.split(".")[-1],
        merge_columns=["id"],
        tags=tags or {},
        schema=schema,
    )


def _make_df(spark_session, rows, schema=BUSINESS_SCHEMA):
    return spark_session.createDataFrame(rows, schema)


def _rows_by_id(provider, entity):
    return {r["id"]: r for r in provider.read_entity(entity).collect()}


class TestMemoryProviderMergeSCD1:
    """merge_to_entity with no scd.*/write.mode tags: full-row SCD1 upsert."""

    def test_first_merge_writes_incoming_as_is(self, spark_session):
        provider = _make_provider(spark_session)
        entity = _make_entity("silver.scd1_first")

        provider.merge_to_entity(_make_df(spark_session, [("a", "new", 1)]), entity)

        assert _rows_by_id(provider, entity)["a"]["status"] == "new"

    def test_matched_key_is_fully_replaced(self, spark_session):
        provider = _make_provider(spark_session)
        entity = _make_entity("silver.scd1_replace")
        provider.merge_to_entity(_make_df(spark_session, [("a", "old", 1)]), entity)

        provider.merge_to_entity(_make_df(spark_session, [("a", "new", 2)]), entity)

        rows = _rows_by_id(provider, entity)
        assert len(rows) == 1
        assert rows["a"]["status"] == "new"
        assert rows["a"]["seq"] == 2

    def test_new_key_inserted_and_untouched_key_survives(self, spark_session):
        provider = _make_provider(spark_session)
        entity = _make_entity("silver.scd1_survive")
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "old", 1), ("b", "old", 1)]), entity
        )

        provider.merge_to_entity(
            _make_df(spark_session, [("a", "new", 2), ("c", "new", 1)]), entity
        )

        rows = _rows_by_id(provider, entity)
        assert set(rows) == {"a", "b", "c"}
        assert rows["a"]["status"] == "new"
        assert rows["b"]["status"] == "old", "untouched key must survive unchanged"
        assert rows["c"]["status"] == "new"


class TestMemoryProviderMergeInsertOnly:
    """merge_to_entity with write.mode=insert: existing keys are never touched."""

    def _entity(self, entityid):
        return _make_entity(entityid, tags={"write.mode": "insert"})

    def test_first_merge_writes_incoming_as_is(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.insert_first")

        provider.merge_to_entity(_make_df(spark_session, [("a", "new", 1)]), entity)

        assert _rows_by_id(provider, entity)["a"]["status"] == "new"

    def test_existing_key_untouched_even_when_incoming_differs(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.insert_untouched")
        provider.merge_to_entity(_make_df(spark_session, [("a", "original", 1)]), entity)

        provider.merge_to_entity(_make_df(spark_session, [("a", "changed", 2)]), entity)

        rows = _rows_by_id(provider, entity)
        assert len(rows) == 1
        assert rows["a"]["status"] == "original", "insert-only must never touch an existing key"

    def test_new_key_is_inserted(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.insert_new")
        provider.merge_to_entity(_make_df(spark_session, [("a", "original", 1)]), entity)

        provider.merge_to_entity(
            _make_df(spark_session, [("a", "changed", 2), ("b", "new", 1)]), entity
        )

        rows = _rows_by_id(provider, entity)
        assert set(rows) == {"a", "b"}
        assert rows["a"]["status"] == "original"
        assert rows["b"]["status"] == "new"


class TestMemoryProviderMergeSCD2:
    """merge_to_entity with scd.type=2: full declared-flow SCD2 semantics."""

    def _entity(self, entityid, **tags):
        return _make_entity(entityid, tags={"scd.type": "2", **tags}, schema=SCD2_SCHEMA)

    def test_first_merge_bootstraps_temporal_columns_on_missing_entity(self, spark_session):
        """entity.schema has no temporal columns; merge_to_entity must add them
        even though the entity does not exist yet."""
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_bootstrap")

        provider.merge_to_entity(
            _make_df(spark_session, [("a", "new", "x", TS(1))], SCD2_SCHEMA), entity
        )

        rows = provider.read_entity(entity).collect()
        assert len(rows) == 1
        assert rows[0]["__is_current"] is True
        assert rows[0]["__effective_to"] is None
        assert rows[0]["__effective_from"] is not None

    def test_unchanged_row_produces_no_new_version(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_unchanged")
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "same", "x", TS(1))], SCD2_SCHEMA), entity
        )
        first_effective_from = provider.read_entity(entity).collect()[0]["__effective_from"]

        provider.merge_to_entity(
            _make_df(spark_session, [("a", "same", "x", TS(1))], SCD2_SCHEMA), entity
        )

        rows = provider.read_entity(entity).collect()
        assert len(rows) == 1
        assert rows[0]["__is_current"] is True
        assert (
            rows[0]["__effective_from"] == first_effective_from
        ), "an unchanged tracked-column row must not open a new version"

    def test_changed_row_closes_old_version_and_opens_new(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_changed")
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(1))], SCD2_SCHEMA), entity
        )

        provider.merge_to_entity(
            _make_df(spark_session, [("a", "silver", "x", TS(1))], SCD2_SCHEMA), entity
        )

        rows = provider.read_entity(entity).collect()
        current = [r for r in rows if r["__is_current"]]
        closed = [r for r in rows if not r["__is_current"]]
        assert len(current) == 1 and current[0]["status"] == "silver"
        assert current[0]["__effective_to"] is None
        assert len(closed) == 1 and closed[0]["status"] == "bronze"
        assert closed[0]["__effective_to"] is not None

    def test_close_on_missing_closes_absent_key_but_not_unchanged_present_key(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_com", **{"scd.close_on_missing": "true"})
        provider.merge_to_entity(
            _make_df(
                spark_session,
                [("a", "bronze", "x", TS(1)), ("b", "gold", "x", TS(1))],
                SCD2_SCHEMA,
            ),
            entity,
        )

        # "b" vanishes from the next snapshot; "a" is present and unchanged.
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(1))], SCD2_SCHEMA), entity
        )

        rows = provider.read_entity(entity).collect()
        current_ids = {r["id"] for r in rows if r["__is_current"]}
        closed_ids = {r["id"] for r in rows if not r["__is_current"]}
        assert "a" in current_ids, "present, unchanged key must stay current"
        assert "a" not in closed_ids
        assert "b" in closed_ids, "vanished key must be closed"
        assert "b" not in current_ids

    def test_sequence_by_stale_row_is_ignored(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_seq_stale", **{"scd.sequence_by": "updated_at"})
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "silver", "x", TS(5))], SCD2_SCHEMA), entity
        )

        # Older sequence value than the current version -> must be ignored.
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(2))], SCD2_SCHEMA), entity
        )

        rows = provider.read_entity(entity).collect()
        assert len(rows) == 1
        assert rows[0]["status"] == "silver"
        assert rows[0]["__is_current"] is True

    def test_sequence_by_effective_columns_carry_sequence_values(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_seq_chain", **{"scd.sequence_by": "updated_at"})
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(1))], SCD2_SCHEMA), entity
        )

        provider.merge_to_entity(
            _make_df(spark_session, [("a", "silver", "x", TS(5))], SCD2_SCHEMA), entity
        )

        rows = provider.read_entity(entity).collect()
        current = next(r for r in rows if r["__is_current"])
        closed = next(r for r in rows if not r["__is_current"])
        assert current["__effective_from"] == TS(5)
        assert closed["__effective_to"] == TS(
            5
        ), "closed version's effective_to must equal the new version's effective_from"

    def test_sequence_by_missing_column_in_batch_raises(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_seq_missing_col", **{"scd.sequence_by": "updated_at"})
        no_seq_schema = StructType(
            [StructField("id", StringType(), False), StructField("status", StringType(), True)]
        )

        with pytest.raises(ValueError, match="sequence_by"):
            provider.merge_to_entity(
                _make_df(spark_session, [("a", "bronze")], no_seq_schema), entity
            )

    def test_sequence_by_null_value_in_batch_raises(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_seq_null", **{"scd.sequence_by": "updated_at"})

        with pytest.raises(ValueError, match="null values"):
            provider.merge_to_entity(
                _make_df(spark_session, [("a", "bronze", "x", None)], SCD2_SCHEMA), entity
            )

    def test_delete_when_closes_without_inserting(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_delete", **{"scd.delete_when": "status = 'DELETED'"})
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(1))], SCD2_SCHEMA), entity
        )

        provider.merge_to_entity(
            _make_df(spark_session, [("a", "DELETED", "x", TS(1))], SCD2_SCHEMA), entity
        )

        rows = provider.read_entity(entity).collect()
        assert len(rows) == 1, "a delete closes; it must not insert a new version"
        assert rows[0]["__is_current"] is False
        assert rows[0]["status"] == "bronze"
        assert rows[0]["__effective_to"] is not None

    def test_delete_when_unknown_key_is_a_no_op(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity(
            "silver.scd2_delete_unknown", **{"scd.delete_when": "status = 'DELETED'"}
        )

        provider.merge_to_entity(
            _make_df(spark_session, [("zz", "DELETED", "x", TS(1))], SCD2_SCHEMA), entity
        )

        assert provider.read_entity(entity).count() == 0

    def test_optimize_unchanged_matches_default_classification(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_opt", **{"scd.optimize_unchanged": "true"})
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(1))], SCD2_SCHEMA), entity
        )

        # Same content: no new version.
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(1))], SCD2_SCHEMA), entity
        )
        assert len(provider.read_entity(entity).collect()) == 1

        # Changed content: new version.
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "silver", "x", TS(1))], SCD2_SCHEMA), entity
        )
        rows = provider.read_entity(entity).collect()
        current = [r for r in rows if r["__is_current"]]
        closed = [r for r in rows if not r["__is_current"]]
        assert len(current) == 1 and current[0]["status"] == "silver"
        assert len(closed) == 1 and closed[0]["status"] == "bronze"

    def test_explicit_tracked_columns_ignore_untracked_changes(self, spark_session):
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_tracked_explicit", **{"scd.tracked": "status"})
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(1))], SCD2_SCHEMA), entity
        )

        # "category" changes but is not in scd.tracked -> must not open a new version.
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "y", TS(1))], SCD2_SCHEMA), entity
        )

        rows = provider.read_entity(entity).collect()
        assert len(rows) == 1
        assert rows[0]["category"] == "x", "untracked column change must not be picked up"

    def test_auto_derived_tracked_columns_excludes_business_and_sequence_columns(
        self, spark_session
    ):
        """Without scd.tracked, every non-key/non-temporal/non-sequence column is
        tracked automatically: a sequence-only bump must not open a new version,
        but a change to another business column must."""
        provider = _make_provider(spark_session)
        entity = self._entity("silver.scd2_tracked_auto", **{"scd.sequence_by": "updated_at"})
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(1))], SCD2_SCHEMA), entity
        )

        # updated_at alone bumps (sequence_by is excluded from auto-tracked columns).
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "x", TS(2))], SCD2_SCHEMA), entity
        )
        assert (
            len(provider.read_entity(entity).collect()) == 1
        ), "sequence_by must be excluded from auto-derived tracked columns"

        # category changes -> auto-tracked, must open a new version.
        provider.merge_to_entity(
            _make_df(spark_session, [("a", "bronze", "y", TS(3))], SCD2_SCHEMA), entity
        )
        rows = provider.read_entity(entity).collect()
        current = [r for r in rows if r["__is_current"]]
        assert len(current) == 1 and current[0]["category"] == "y"
        assert current[0]["__effective_from"] == TS(3)
