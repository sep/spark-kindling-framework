"""JVM state the single-process suite releases after each module.

tests/conftest.py's module fixture clears Delta's DeltaLog cache and the
stopped SparkSessions pool threads keep in thread-locals; without both, a
one-process run of every module runs out of heap. These pin that the
helpers really reach that state (a wrong py4j/reflection path fails
silently, returning None).
"""

import pytest

from tests.spark_test_helper import (
    get_local_spark_session,
    release_stopped_sessions_pinned_by_threads,
    spark_connect_mode,
)


@pytest.mark.skipif(
    spark_connect_mode(),
    reason="classic JVM only: a Spark Connect client's stop() does not stop the "
    "server-side SparkContext this checks for",
)
def test_stopped_session_pinned_by_aqe_pool_threads_is_released():
    spark = get_local_spark_session("PinnedSessionRelease")
    left = spark.range(100000).withColumnRenamed("id", "k")
    right = spark.range(100000).withColumnRenamed("id", "k")
    # A join under AQE runs stage creation on the QueryStageCreator pool,
    # whose threads inherit this session as their active one.
    assert left.join(right, "k").count() == 100000
    spark.stop()

    released = release_stopped_sessions_pinned_by_threads()
    assert released is not None, "reflection on java.lang thread-locals failed"
    assert released >= 1, "no thread-local referenced the stopped session"
    assert release_stopped_sessions_pinned_by_threads() == 0
