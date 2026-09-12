"""Watermark tracing seams that need a live SparkContext.

Integration rather than unit: ``WatermarkManager.get_cursor()`` builds a real
``pyspark.sql.functions.col(...)`` predicate against its (mocked) DataFrame,
and constructing a Column requires an active SparkContext even though nothing
is ever executed. The rest of the watermark/tracing seam tests stay in
tests/unit/test_seam_tracing.py, which needs no JVM.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest
from kindling.test_framework import RecordingTraceProvider

from tests.conftest import _sockets_permitted


@pytest.fixture(scope="module")
def spark_session():
    """Module-scoped rather than conftest.py's session-scoped fixture:
    TestWatermarkSpansOnSpark only needs an active SparkContext for
    pyspark.sql.functions.col(...) to build a Column against a mocked
    DataFrame. Built via get_standalone_spark_session (see
    tests/integration/test_entity_provider_memory_scd2.py's module docstring)
    so it's always
    Delta-configured regardless of xdist worker test order.
    """
    if not _sockets_permitted():
        pytest.skip(
            "Sockets are not permitted in this environment; cannot start a real SparkSession."
        )
    from tests.spark_test_helper import get_standalone_spark_session

    spark = get_standalone_spark_session("SeamTracingTests")
    yield spark
    # Not spark.stop() here: this may be the same JVM-singleton session
    # other tests elsewhere in this xdist worker are still relying on.


class TestWatermarkSpansOnSpark:
    def _manager(self, tp):
        from kindling.watermarking import WatermarkManager

        lp = Mock()
        lp.get_logger.return_value = Mock()
        with patch("kindling.watermarking.get_or_create_spark_session", return_value=MagicMock()):
            manager = WatermarkManager(
                ep=Mock(),
                wef=Mock(),
                lp=lp,
                signal_provider=None,
                provider_registry=None,
                tp=tp,
                config=None,
            )
        return manager

    def test_get_cursor_emits_span_with_ids(self, spark_session):
        # get_cursor() builds a real pyspark.sql.functions.col(...) predicate
        # against the (mocked) DataFrame, which needs an active SparkContext
        # regardless of the DataFrame itself being a MagicMock. Request the
        # shared fixture explicitly rather than relying on some earlier,
        # unrelated test in the suite happening to leave a session active.
        tp = RecordingTraceProvider()
        manager = self._manager(tp)
        df = MagicMock()
        df.isEmpty.return_value = True
        manager.ep.read_entity.return_value.filter.return_value.limit.return_value = df

        cursor = manager.get_cursor("entity.src", "pipe1")

        assert cursor is None
        span = tp.find(component="kindling.watermark", operation="get_cursor")[0]
        assert span.details == {"source_entity_id": "entity.src", "reader_id": "pipe1"}
        assert span.closed

    def test_no_spans_when_tp_absent(self, spark_session):
        # Same real-SparkContext dependency as test_get_cursor_emits_span_with_ids.
        manager = self._manager(None)
        df = MagicMock()
        df.isEmpty.return_value = True
        manager.ep.read_entity.return_value.filter.return_value.limit.return_value = df

        assert manager.get_cursor("entity.src", "pipe1") is None
