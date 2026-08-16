"""Unit tests for DeltaEntityProvider._transform_delta_feed_to_changes.

Regression coverage for the keyless-watermarking defect: an entity with
merge_columns=[] (append-only) consumed by a watermarked pipe used to lose
all but one arbitrary row per incremental CDF slice, because
Window.partitionBy() with no columns collapses to a single global
partition -- silently dropping valid append-only records and triggering
Spark's "No Partition Defined for Window operation" warning.
"""

from datetime import datetime

import pytest

from kindling.entity_provider_delta import DeltaEntityProvider


@pytest.fixture
def provider():
    # _transform_delta_feed_to_changes touches no instance state -- bypass
    # __init__ (DI/config/Spark-session wiring) entirely.
    return DeltaEntityProvider.__new__(DeltaEntityProvider)


def _cdf_df(spark_session, rows, extra_cols=("id", "value")):
    columns = [*extra_cols, "_change_type", "_commit_version", "_commit_timestamp"]
    return spark_session.createDataFrame(rows, columns)


class TestKeylessAppendOnly:
    """merge_columns=[] -- every non-delete row must survive, no window."""

    def test_all_non_delete_rows_retained_no_dedup(self, provider, spark_session):
        df = _cdf_df(
            spark_session,
            [
                (1, "a", "insert", 1, datetime(2026, 1, 1)),
                (2, "b", "insert", 1, datetime(2026, 1, 1)),
                (3, "c", "insert", 2, datetime(2026, 1, 2)),
            ],
        )

        result = provider._transform_delta_feed_to_changes(df, [])

        assert result.count() == 3
        ids = {row["id"] for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_delete_rows_excluded(self, provider, spark_session):
        df = _cdf_df(
            spark_session,
            [
                (1, "a", "insert", 1, datetime(2026, 1, 1)),
                (2, "b", "delete", 2, datetime(2026, 1, 2)),
                (3, "c", "insert", 2, datetime(2026, 1, 2)),
            ],
        )

        result = provider._transform_delta_feed_to_changes(df, [])

        ids = {row["id"] for row in result.collect()}
        assert ids == {1, 3}

    def test_output_has_source_version_and_timestamp_without_change_type(
        self, provider, spark_session
    ):
        df = _cdf_df(
            spark_session,
            [(1, "a", "insert", 5, datetime(2026, 1, 1))],
        )

        result = provider._transform_delta_feed_to_changes(df, [])
        row = result.collect()[0]

        assert "_change_type" not in result.columns
        assert "SourceVersion" in result.columns
        assert "SourceTimestamp" in result.columns
        assert row["SourceVersion"] == 5
        assert row["SourceTimestamp"] == datetime(2026, 1, 1)

    def test_duplicate_key_values_all_retained_when_keyless(self, provider, spark_session):
        """Multiple rows sharing the same 'id' value are NOT deduplicated
        when key_columns is empty -- id isn't a business key here, it's
        just a column; keyless means no dedup criterion exists at all."""
        df = _cdf_df(
            spark_session,
            [
                (1, "a", "insert", 1, datetime(2026, 1, 1)),
                (1, "b", "insert", 2, datetime(2026, 1, 2)),
            ],
        )

        result = provider._transform_delta_feed_to_changes(df, [])

        assert result.count() == 2


class TestKeyedLatestPerKey:
    """merge_columns set -- existing latest-per-key + delete-removal
    behavior must be unchanged."""

    def test_returns_only_latest_row_per_key(self, provider, spark_session):
        df = _cdf_df(
            spark_session,
            [
                (1, "old", "insert", 1, datetime(2026, 1, 1)),
                (1, "new", "update_postimage", 2, datetime(2026, 1, 2)),
                (2, "only", "insert", 1, datetime(2026, 1, 1)),
            ],
        )

        result = provider._transform_delta_feed_to_changes(df, ["id"])

        rows = {row["id"]: row["value"] for row in result.collect()}
        assert rows == {1: "new", 2: "only"}

    def test_delete_only_key_produces_no_row(self, provider, spark_session):
        """A key whose only event in this CDF slice is a delete produces
        no output row for it -- the delete-type row itself is always
        filtered, and no other row remains to rank as "latest"."""
        df = _cdf_df(
            spark_session,
            [
                (1, "a", "delete", 1, datetime(2026, 1, 1)),
                (2, "b", "insert", 1, datetime(2026, 1, 1)),
            ],
        )

        result = provider._transform_delta_feed_to_changes(df, ["id"])

        ids = {row["id"] for row in result.collect()}
        assert ids == {2}

    def test_output_has_source_version_and_timestamp_without_change_type(
        self, provider, spark_session
    ):
        df = _cdf_df(
            spark_session,
            [(1, "a", "insert", 7, datetime(2026, 1, 3))],
        )

        result = provider._transform_delta_feed_to_changes(df, ["id"])
        row = result.collect()[0]

        assert "_change_type" not in result.columns
        assert "SourceVersion" in result.columns
        assert "SourceTimestamp" in result.columns
        assert row["SourceVersion"] == 7
        assert row["SourceTimestamp"] == datetime(2026, 1, 3)

    def test_composite_key_partitions_independently(self, provider, spark_session):
        columns = ["tenant", "id", "value", "_change_type", "_commit_version", "_commit_timestamp"]
        df = spark_session.createDataFrame(
            [
                ("t1", 1, "old", "insert", 1, datetime(2026, 1, 1)),
                ("t1", 1, "new", "update_postimage", 2, datetime(2026, 1, 2)),
                ("t2", 1, "same-id-diff-tenant", "insert", 1, datetime(2026, 1, 1)),
            ],
            columns,
        )

        result = provider._transform_delta_feed_to_changes(df, ["tenant", "id"])

        rows = {(row["tenant"], row["id"]): row["value"] for row in result.collect()}
        assert rows == {("t1", 1): "new", ("t2", 1): "same-id-diff-tenant"}
