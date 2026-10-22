"""The per-module DeltaLog cache cleanup the single-process suite relies on.

Delta's static DeltaLog cache survives SparkSession.stop(); without the
tests/conftest.py fixture every Delta table created in a one-process run
stays cached and the 1 GB driver runs out of heap. This pins that the
helper really reaches the cache (a wrong py4j accessor fails silently).
"""

from tests.spark_test_helper import clear_delta_log_cache


def test_clear_delta_log_cache_empties_the_static_cache(spark_session, tmp_path):
    path = str(tmp_path / "t")
    spark_session.range(3).write.format("delta").save(path)
    assert spark_session.read.format("delta").load(path).count() == 3

    assert clear_delta_log_cache() >= 1, "the table's DeltaLog was cached"
    assert clear_delta_log_cache() == 0, "and the cache is now empty"
