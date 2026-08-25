"""Unit tests for kindling.common_transforms.remove_duplicates.

Regression coverage: an append-only/keyless entity (keycolumns=[]) passed
into remove_duplicates -- from WatermarkManager._legacy_version_read's and
DeltaEntityProvider.read_entity_changes's initial-full-read paths -- built
Window.partitionBy() with no columns, which collapses to a single global
partition. That silently kept only one arbitrary row from the entire
dataset and triggered Spark's "No Partition Defined for Window operation"
warning. Mirrors the same fix already applied to
DeltaEntityProvider._transform_delta_feed_to_changes (see
test_delta_feed_to_changes.py).
"""

from datetime import datetime

from kindling.common_transforms import remove_duplicates


def _df(spark_session, rows, extra_cols=("id", "value")):
    columns = [*extra_cols, "SourceTimestamp"]
    return spark_session.createDataFrame(rows, columns)


class TestKeylessAppendOnly:
    """keycolumns=[] -- every row must survive, no window applied."""

    def test_all_rows_retained_no_dedup(self, spark_session):
        df = _df(
            spark_session,
            [
                (1, "a", datetime(2026, 1, 1)),
                (2, "b", datetime(2026, 1, 1)),
                (3, "c", datetime(2026, 1, 2)),
            ],
        )

        result = remove_duplicates(df, [])

        assert result.count() == 3

    def test_duplicate_key_values_all_retained_when_keyless(self, spark_session):
        df = _df(
            spark_session,
            [
                (1, "a", datetime(2026, 1, 1)),
                (1, "b", datetime(2026, 1, 2)),
            ],
        )

        result = remove_duplicates(df, [])

        assert result.count() == 2

    def test_returns_same_columns_no_row_num_leak(self, spark_session):
        df = _df(spark_session, [(1, "a", datetime(2026, 1, 1))])

        result = remove_duplicates(df, [])

        assert set(result.columns) == set(df.columns)


class TestKeyedLatestPerKey:
    """keycolumns set -- existing latest-per-key behavior unchanged."""

    def test_returns_only_latest_row_per_key(self, spark_session):
        df = _df(
            spark_session,
            [
                (1, "old", datetime(2026, 1, 1)),
                (1, "new", datetime(2026, 1, 2)),
                (2, "only", datetime(2026, 1, 1)),
            ],
        )

        result = remove_duplicates(df, ["id"])

        rows = {row["id"]: row["value"] for row in result.collect()}
        assert rows == {1: "new", 2: "only"}
