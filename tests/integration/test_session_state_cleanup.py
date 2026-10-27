"""Per-test cleanup of a module's shared SparkSession (tests/conftest.py).

Temp views a test creates are dropped after it, so a module's session does
not accumulate every test's plans (memory-provider views chain their
merges). The ordering test only proves anything when both tests run on one
worker; the helper test is deterministic.
"""

from tests.spark_test_helper import active_temp_view_names, drop_temp_views

VIEW = "session_state_cleanup_probe_view"


def test_helpers_list_and_drop_the_tests_temp_views(spark_session):
    spark_session.range(1).createOrReplaceTempView(VIEW)
    assert VIEW in active_temp_view_names()
    assert drop_temp_views({VIEW}) == 1
    assert VIEW not in active_temp_view_names()


def test_a_test_leaves_a_temp_view_behind(spark_session):
    spark_session.range(1).createOrReplaceTempView(VIEW)
    assert VIEW in active_temp_view_names()


def test_the_next_test_does_not_see_it(spark_session):
    assert VIEW not in (active_temp_view_names() or set())
