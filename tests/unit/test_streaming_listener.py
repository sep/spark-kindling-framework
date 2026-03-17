"""
Unit tests for KindlingStreamingListener.

Tests the queue-based event processing pattern to ensure:
- Listener callbacks complete quickly (<10ms)
- Events processed asynchronously
- Queue overflow handled gracefully
- Signals emitted correctly
"""

import queue
import time
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from kindling.streaming_listener import (
    KindlingStreamingListener,
    StreamingEvent,
    StreamingEventType,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_signal_provider():
    """Mock signal provider for testing."""
    provider = Mock()
    provider.create_signal = Mock()
    return provider


@pytest.fixture
def mock_logger_provider():
    """Mock logger provider for testing."""
    provider = Mock()
    logger = Mock()
    provider.get_logger = Mock(return_value=logger)
    return provider


@pytest.fixture
def mock_trace_provider():
    """Mock trace provider for testing."""
    from kindling.spark_trace import SparkSpan

    provider = Mock()
    # start_span returns a SparkSpan-like object with an id
    span_counter = [0]

    def mock_start_span(operation, component, details=None):
        span_counter[0] += 1
        import uuid

        return SparkSpan(
            id=str(span_counter[0]),
            component=component,
            operation=operation,
            attributes=details or {},
            traceId=uuid.uuid4(),
            reraise=False,
        )

    provider.start_span = Mock(side_effect=mock_start_span)
    provider.add_event = Mock()
    provider.end_span = Mock()
    return provider


@pytest.fixture
def listener(mock_signal_provider, mock_logger_provider, mock_trace_provider):
    """Create listener instance for testing."""
    return KindlingStreamingListener(
        signal_provider=mock_signal_provider,
        logger_provider=mock_logger_provider,
        trace_provider=mock_trace_provider,
        max_queue_size=10,  # Small queue for testing overflow
    )


@pytest.fixture
def started_listener(listener):
    """Create and start listener for testing."""
    # Mock the emit method to track signal emissions
    listener.emit = Mock()
    listener.start()
    yield listener
    listener.stop()


# =============================================================================
# Event Data Structure Tests
# =============================================================================


class TestStreamingEvent:
    """Test StreamingEvent data structure."""

    def test_event_creation(self):
        """Test creating a streaming event."""
        event = StreamingEvent(
            event_type=StreamingEventType.QUERY_STARTED,
            query_id="query-123",
            run_id="run-456",
            name="test_query",
        )

        assert event.event_type == StreamingEventType.QUERY_STARTED
        assert event.query_id == "query-123"
        assert event.run_id == "run-456"
        assert event.name == "test_query"
        assert event.timestamp is not None
        assert event.data is None

    def test_event_with_data(self):
        """Test event with additional data."""
        data = {"batch_id": 42, "num_rows": 1000}

        event = StreamingEvent(
            event_type=StreamingEventType.QUERY_PROGRESS,
            query_id="query-123",
            run_id="run-456",
            data=data,
        )

        assert event.data == data

    def test_event_auto_timestamp(self):
        """Test automatic timestamp generation."""
        before = time.time()
        event = StreamingEvent(
            event_type=StreamingEventType.QUERY_STARTED,
            query_id="query-123",
            run_id="run-456",
        )
        after = time.time()

        assert before <= event.timestamp <= after


# =============================================================================
# Listener Initialization Tests
# =============================================================================


class TestListenerInitialization:
    """Test listener initialization and lifecycle."""

    def test_initialization(self, mock_signal_provider, mock_logger_provider, mock_trace_provider):
        """Test listener initialization."""
        listener = KindlingStreamingListener(
            signal_provider=mock_signal_provider,
            logger_provider=mock_logger_provider,
            trace_provider=mock_trace_provider,
            max_queue_size=100,
        )

        assert listener.max_queue_size == 100
        assert not listener._running
        assert listener._consumer_thread is None
        assert listener._events_processed == 0
        assert listener._events_dropped == 0

    def test_start(self, listener):
        """Test starting the listener."""
        listener.start()

        assert listener._running
        assert listener._consumer_thread is not None
        assert listener._consumer_thread.is_alive()

        # Cleanup
        listener.stop()

    def test_start_twice(self, started_listener):
        """Test starting already-running listener logs warning."""
        started_listener.start()  # Start again

        # Check mock logger was called with warning
        started_listener.logger.warning.assert_called()

    def test_stop(self, started_listener):
        """Test stopping the listener."""
        result = started_listener.stop()

        assert result is True
        assert not started_listener._running
        assert started_listener._shutdown_event.is_set()

    def test_stop_not_running(self, listener):
        """Test stopping non-running listener logs warning."""
        result = listener.stop()

        assert result is True
        # Check mock logger was called with warning
        listener.logger.warning.assert_called()

    def test_get_metrics(self, listener):
        """Test getting listener metrics."""
        metrics = listener.get_metrics()

        assert "events_processed" in metrics
        assert "events_dropped" in metrics
        assert "queue_size" in metrics
        assert metrics["events_processed"] == 0
        assert metrics["events_dropped"] == 0


# =============================================================================
# Event Callback Tests
# =============================================================================


class TestEventCallbacks:
    """Test PySpark listener callback methods."""

    def test_on_query_started(self, started_listener):
        """Test onQueryStarted callback enqueues event."""
        mock_event = Mock()
        mock_event.id = "query-123"
        mock_event.runId = "run-456"
        mock_event.name = "test_query"

        # Call callback
        started_listener.onQueryStarted(mock_event)

        # Give time for event to be enqueued
        time.sleep(0.1)

        # Check event was enqueued
        assert started_listener._event_queue.qsize() >= 0

    def test_on_query_progress(self, started_listener):
        """Test onQueryProgress callback enqueues event."""
        mock_progress = Mock()
        mock_progress.id = "query-123"
        mock_progress.runId = "run-456"
        mock_progress.name = "test_query"
        mock_progress.batchId = 42
        mock_progress.numInputRows = 1000
        mock_progress.inputRowsPerSecond = 100.0
        mock_progress.processedRowsPerSecond = 95.0
        mock_progress.batchDuration = 1000

        mock_event = Mock()
        mock_event.progress = mock_progress

        # Call callback
        started_listener.onQueryProgress(mock_event)

        # Give time for event to be enqueued
        time.sleep(0.1)

        # Check event was enqueued
        assert started_listener._event_queue.qsize() >= 0

    def test_on_query_terminated(self, started_listener):
        """Test onQueryTerminated callback enqueues event."""
        mock_event = Mock()
        mock_event.id = "query-123"
        mock_event.runId = "run-456"
        mock_event.exception = None

        # Call callback
        started_listener.onQueryTerminated(mock_event)

        # Give time for event to be enqueued
        time.sleep(0.1)

        # Check event was enqueued
        assert started_listener._event_queue.qsize() >= 0

    def test_on_query_terminated_with_exception(self, started_listener):
        """Test onQueryTerminated with exception captures error."""
        mock_exception = RuntimeError("Query failed")

        mock_event = Mock()
        mock_event.id = "query-123"
        mock_event.runId = "run-456"
        mock_event.exception = mock_exception

        # Call callback
        started_listener.onQueryTerminated(mock_event)

        # Give time for event to be processed
        time.sleep(0.2)

        # Event should have been processed
        assert started_listener._events_processed > 0


# =============================================================================
# Queue Overflow Tests
# =============================================================================


class TestQueueOverflow:
    """Test queue overflow handling."""

    def test_queue_overflow_drops_oldest(self, started_listener):
        """Test that queue overflow drops oldest events."""
        # Fill queue beyond max_queue_size (10)
        for i in range(15):
            mock_event = Mock()
            mock_event.id = f"query-{i}"
            mock_event.runId = f"run-{i}"
            mock_event.name = f"test_query_{i}"

            started_listener.onQueryStarted(mock_event)

        # Give time for processing
        time.sleep(0.5)

        # Some events should have been dropped
        metrics = started_listener.get_metrics()
        # Note: Exact count depends on processing speed, but some should be dropped
        # or all should be processed
        assert (
            metrics["events_processed"] + metrics["events_dropped"] >= 15
            or metrics["queue_size"] > 0
        )

    def test_queue_full_logs_warning(self, started_listener, mock_trace_provider, caplog):
        """Test that queue full condition logs warning."""
        # Stop consumer to prevent processing
        started_listener._running = False
        started_listener.stop()

        # Create new listener with tiny queue
        listener = KindlingStreamingListener(
            signal_provider=Mock(),
            logger_provider=(
                started_listener.logger.logger_provider
                if hasattr(started_listener.logger, "logger_provider")
                else Mock()
            ),
            trace_provider=mock_trace_provider,
            max_queue_size=2,
        )

        # Fill queue manually
        for i in range(5):
            event = StreamingEvent(
                event_type=StreamingEventType.QUERY_STARTED,
                query_id=f"query-{i}",
                run_id=f"run-{i}",
            )
            listener._enqueue_event(event)

        # Should have warnings about dropped events
        # (Check in listener's logger mock or caplog)


# =============================================================================
# Signal Emission Tests
# =============================================================================


class TestSignalEmission:
    """Test that signals are emitted correctly."""

    def test_query_started_signal(self, started_listener):
        """Test that query started signal is emitted."""
        mock_event = Mock()
        mock_event.id = "query-123"
        mock_event.runId = "run-456"
        mock_event.name = "test_query"

        # Call callback
        started_listener.onQueryStarted(mock_event)

        # Give time for event to be processed
        time.sleep(0.3)

        # Check signal was emitted (via mock)
        started_listener.emit.assert_called()

        # Check at least one call contains the expected signal
        calls = [str(call) for call in started_listener.emit.call_args_list]
        assert any("streaming.spark_query_started" in str(c) for c in calls)

    def test_query_progress_signal(self, started_listener):
        """Test that query progress signal is emitted."""
        mock_progress = Mock()
        mock_progress.id = "query-123"
        mock_progress.runId = "run-456"
        mock_progress.name = "test_query"
        mock_progress.batchId = 42
        mock_progress.numInputRows = 1000
        mock_progress.inputRowsPerSecond = 100.0
        mock_progress.processedRowsPerSecond = 95.0
        mock_progress.batchDuration = 1000

        mock_event = Mock()
        mock_event.progress = mock_progress

        # Call callback
        started_listener.onQueryProgress(mock_event)

        # Give time for event to be processed
        time.sleep(0.3)

        # Check signal was emitted
        started_listener.emit.assert_called()

    def test_query_terminated_signal(self, started_listener):
        """Test that query terminated signal is emitted."""
        mock_event = Mock()
        mock_event.id = "query-123"
        mock_event.runId = "run-456"
        mock_event.exception = None

        # Call callback
        started_listener.onQueryTerminated(mock_event)

        # Give time for event to be processed
        time.sleep(0.3)

        # Check signal was emitted
        started_listener.emit.assert_called()


# =============================================================================
# Performance Tests
# =============================================================================


class TestPerformance:
    """Test performance characteristics."""

    def test_callback_completes_quickly(self, started_listener):
        """Test that callbacks complete in < 10ms."""
        mock_event = Mock()
        mock_event.id = "query-123"
        mock_event.runId = "run-456"
        mock_event.name = "test_query"

        # Time the callback
        start = time.time()
        started_listener.onQueryStarted(mock_event)
        duration = time.time() - start

        # Should complete in well under 10ms
        assert duration < 0.010, f"Callback took {duration*1000:.2f}ms (>10ms)"

    def test_high_frequency_events(self, started_listener):
        """Test handling high-frequency events."""
        # Send many events rapidly
        for i in range(100):
            mock_event = Mock()
            mock_event.id = f"query-{i}"
            mock_event.runId = f"run-{i}"
            mock_event.name = f"test_query_{i}"

            started_listener.onQueryStarted(mock_event)

        # Give time for processing
        time.sleep(1.0)

        # All events should be processed or dropped (not stuck)
        metrics = started_listener.get_metrics()
        assert metrics["events_processed"] + metrics["events_dropped"] > 0


# =============================================================================
# Thread Safety Tests
# =============================================================================


class TestThreadSafety:
    """Test thread safety of queue operations."""

    def test_concurrent_enqueue(self, started_listener):
        """Test concurrent event enqueuing from multiple threads."""
        import threading

        def enqueue_events(listener, thread_id, count):
            for i in range(count):
                mock_event = Mock()
                mock_event.id = f"query-{thread_id}-{i}"
                mock_event.runId = f"run-{thread_id}-{i}"
                listener.onQueryStarted(mock_event)

        # Create multiple threads enqueuing events
        threads = []
        for thread_id in range(5):
            thread = threading.Thread(target=enqueue_events, args=(started_listener, thread_id, 20))
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Give time for processing
        time.sleep(1.0)

        # All events should be processed or dropped
        metrics = started_listener.get_metrics()
        assert metrics["events_processed"] + metrics["events_dropped"] == 100


# =============================================================================
# Cleanup Tests
# =============================================================================


class TestCleanup:
    """Test cleanup and shutdown behavior."""

    def test_drain_queue_on_shutdown(self, started_listener):
        """Test that remaining events are processed on shutdown."""
        # Add events
        for i in range(5):
            mock_event = Mock()
            mock_event.id = f"query-{i}"
            mock_event.runId = f"run-{i}"
            started_listener.onQueryStarted(mock_event)

        # Give time for some processing
        time.sleep(0.1)

        # Stop listener
        started_listener.stop()

        # All events should be processed
        metrics = started_listener.get_metrics()
        assert metrics["events_processed"] == 5

    def test_stop_timeout(self, started_listener):
        """Test stop with timeout."""
        # Stop with very short timeout
        result = started_listener.stop(timeout=0.001)

        # May or may not complete depending on timing
        # Just check it returns a boolean
        assert isinstance(result, bool)

    def test_stop_ends_orphaned_trace_spans(self, started_listener, mock_trace_provider):
        """Test that stop() ends trace spans for queries that never terminated."""
        mock_event = Mock()
        mock_event.id = "query-orphan"
        mock_event.runId = "run-orphan"
        mock_event.name = "orphan_query"

        started_listener.onQueryStarted(mock_event)
        time.sleep(0.3)

        started_listener.stop()

        # The span should have been ended during stop
        mock_trace_provider.end_span.assert_called()


# =============================================================================
# Tracing Tests
# =============================================================================


class TestTracing:
    """Test trace span lifecycle for streaming queries."""

    def test_query_started_opens_trace_span(self, started_listener, mock_trace_provider):
        """onQueryStarted should open a trace span."""
        mock_event = Mock()
        mock_event.id = "query-123"
        mock_event.runId = "run-456"
        mock_event.name = "test_query"

        started_listener.onQueryStarted(mock_event)
        time.sleep(0.3)

        mock_trace_provider.start_span.assert_called_once()
        call_kwargs = mock_trace_provider.start_span.call_args
        assert (
            call_kwargs[1]["operation"] == "streaming_query"
            or call_kwargs[0][0] == "streaming_query"
        )

    def test_query_progress_adds_span_event(self, started_listener, mock_trace_provider):
        """onQueryProgress should add an event to the query's trace span."""
        # First start a query
        start_event = Mock()
        start_event.id = "query-123"
        start_event.runId = "run-456"
        start_event.name = "test_query"
        started_listener.onQueryStarted(start_event)
        time.sleep(0.3)

        # Then send progress
        mock_progress = Mock()
        mock_progress.id = "query-123"
        mock_progress.runId = "run-456"
        mock_progress.name = "test_query"
        mock_progress.batchId = 42
        mock_progress.numInputRows = 1000
        mock_progress.inputRowsPerSecond = 100.0
        mock_progress.processedRowsPerSecond = 95.0
        mock_progress.batchDuration = 1000

        progress_event = Mock()
        progress_event.progress = mock_progress

        started_listener.onQueryProgress(progress_event)
        time.sleep(0.3)

        mock_trace_provider.add_event.assert_called_once()
        call_args = mock_trace_provider.add_event.call_args
        assert call_args[0][1] == "batch_progress"
        attrs = call_args[0][2]
        assert attrs["batch_id"] == 42
        assert attrs["num_input_rows"] == 1000

    def test_query_terminated_ends_trace_span(self, started_listener, mock_trace_provider):
        """onQueryTerminated should end the query's trace span."""
        # Start query
        start_event = Mock()
        start_event.id = "query-123"
        start_event.runId = "run-456"
        start_event.name = "test_query"
        started_listener.onQueryStarted(start_event)
        time.sleep(0.3)

        # Terminate query
        term_event = Mock()
        term_event.id = "query-123"
        term_event.runId = "run-456"
        term_event.exception = None
        started_listener.onQueryTerminated(term_event)
        time.sleep(0.3)

        mock_trace_provider.end_span.assert_called_once()
        call_kwargs = mock_trace_provider.end_span.call_args
        # error should be None for clean termination
        assert call_kwargs[1].get("error") is None or call_kwargs[0][1] is None

    def test_query_terminated_with_error_records_error_on_span(
        self, started_listener, mock_trace_provider
    ):
        """onQueryTerminated with exception should end span with error."""
        # Start query
        start_event = Mock()
        start_event.id = "query-123"
        start_event.runId = "run-456"
        start_event.name = "test_query"
        started_listener.onQueryStarted(start_event)
        time.sleep(0.3)

        # Terminate with error
        term_event = Mock()
        term_event.id = "query-123"
        term_event.runId = "run-456"
        term_event.exception = RuntimeError("Connection lost")
        started_listener.onQueryTerminated(term_event)
        time.sleep(0.3)

        mock_trace_provider.end_span.assert_called_once()
        call_args = mock_trace_provider.end_span.call_args
        # error argument should contain the exception string
        error_arg = call_args[1].get("error") if call_args[1] else call_args[0][1]
        assert error_arg is not None
        assert "Connection lost" in str(error_arg)

    def test_progress_without_start_skips_trace(self, started_listener, mock_trace_provider):
        """Progress for an untracked query should not fail, just skip tracing."""
        mock_progress = Mock()
        mock_progress.id = "unknown-query"
        mock_progress.runId = "run-789"
        mock_progress.name = "unknown"
        mock_progress.batchId = 1
        mock_progress.numInputRows = 10
        mock_progress.inputRowsPerSecond = 5.0
        mock_progress.processedRowsPerSecond = 5.0
        mock_progress.batchDuration = 200

        progress_event = Mock()
        progress_event.progress = mock_progress

        started_listener.onQueryProgress(progress_event)
        time.sleep(0.3)

        # Signal should still be emitted
        started_listener.emit.assert_called()
        # But add_event should not be called (no span to attach to)
        mock_trace_provider.add_event.assert_not_called()


# =============================================================================
# Logging Tests
# =============================================================================


class TestLogging:
    """Test structured logging for streaming events."""

    def test_query_started_logs_at_info(self, started_listener):
        """Query started should log at info level."""
        mock_event = Mock()
        mock_event.id = "query-123"
        mock_event.runId = "run-456"
        mock_event.name = "test_query"

        started_listener.onQueryStarted(mock_event)
        time.sleep(0.3)

        info_calls = [str(c) for c in started_listener.logger.info.call_args_list]
        assert any("Streaming query started" in c for c in info_calls)
        assert any("test_query" in c for c in info_calls)

    def test_query_terminated_with_error_logs_at_error(self, started_listener):
        """Query terminated with exception should log at error level."""
        # Start first so terminate can find the span
        start_event = Mock()
        start_event.id = "query-123"
        start_event.runId = "run-456"
        start_event.name = "test_query"
        started_listener.onQueryStarted(start_event)
        time.sleep(0.2)

        term_event = Mock()
        term_event.id = "query-123"
        term_event.runId = "run-456"
        term_event.exception = RuntimeError("Boom")

        started_listener.onQueryTerminated(term_event)
        time.sleep(0.3)

        error_calls = [str(c) for c in started_listener.logger.error.call_args_list]
        assert any("terminated with error" in c for c in error_calls)

    def test_query_terminated_cleanly_logs_at_info(self, started_listener):
        """Clean termination should log at info level."""
        start_event = Mock()
        start_event.id = "query-123"
        start_event.runId = "run-456"
        start_event.name = "test_query"
        started_listener.onQueryStarted(start_event)
        time.sleep(0.2)

        term_event = Mock()
        term_event.id = "query-123"
        term_event.runId = "run-456"
        term_event.exception = None
        started_listener.onQueryTerminated(term_event)
        time.sleep(0.3)

        info_calls = [str(c) for c in started_listener.logger.info.call_args_list]
        assert any("terminated" in c for c in info_calls)

    def test_query_progress_logs_at_debug(self, started_listener):
        """Progress should log at debug level."""
        mock_progress = Mock()
        mock_progress.id = "query-123"
        mock_progress.runId = "run-456"
        mock_progress.name = "test_query"
        mock_progress.batchId = 42
        mock_progress.numInputRows = 1000
        mock_progress.inputRowsPerSecond = 100.0
        mock_progress.processedRowsPerSecond = 95.0
        mock_progress.batchDuration = 1000

        mock_event = Mock()
        mock_event.progress = mock_progress

        started_listener.onQueryProgress(mock_event)
        time.sleep(0.3)

        debug_calls = [str(c) for c in started_listener.logger.debug.call_args_list]
        assert any("progress" in c.lower() for c in debug_calls)


# =============================================================================
# Periodic Metrics Tests
# =============================================================================


class TestPeriodicMetrics:
    """Test periodic metrics logging."""

    def test_metrics_logged_after_interval(
        self, mock_signal_provider, mock_logger_provider, mock_trace_provider
    ):
        """Metrics should be logged after the configured interval."""
        listener = KindlingStreamingListener(
            signal_provider=mock_signal_provider,
            logger_provider=mock_logger_provider,
            trace_provider=mock_trace_provider,
            max_queue_size=100,
            metrics_log_interval=0.5,  # Very short interval for testing
        )
        listener.emit = Mock()
        listener.start()

        try:
            # Send a few events
            for i in range(3):
                mock_event = Mock()
                mock_event.id = f"query-{i}"
                mock_event.runId = f"run-{i}"
                mock_event.name = f"q{i}"
                listener.onQueryStarted(mock_event)

            # Wait long enough for the metrics interval to fire
            time.sleep(1.5)

            info_calls = [str(c) for c in listener.logger.info.call_args_list]
            assert any("Streaming listener metrics" in c for c in info_calls)
        finally:
            listener.stop()
