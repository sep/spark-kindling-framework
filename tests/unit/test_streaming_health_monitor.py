"""
Unit tests for StreamingHealthMonitor.

Tests event-driven health monitoring, signal subscriptions,
stall detection, and health signal emissions.
"""

import time
from datetime import datetime, timedelta
from unittest.mock import Mock, call

import pytest
from kindling.streaming_health_monitor import (
    QueryHealthState,
    QueryHealthStatus,
    StreamingHealthMonitor,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_signal_provider():
    """Mock signal provider for testing."""
    provider = Mock()

    # Create mock signals that can connect/disconnect
    def create_signal(name):
        signal = Mock()
        signal.connect = Mock()
        signal.disconnect = Mock()
        signal.send = Mock()
        return signal

    provider.create_signal = Mock(side_effect=create_signal)
    return provider


@pytest.fixture
def mock_logger_provider():
    """Mock logger provider for testing."""
    provider = Mock()
    logger = Mock()
    provider.get_logger = Mock(return_value=logger)
    return provider


@pytest.fixture
def monitor(mock_signal_provider, mock_logger_provider):
    """Create monitor instance for testing."""
    mon = StreamingHealthMonitor(
        signal_provider=mock_signal_provider,
        logger_provider=mock_logger_provider,
        stall_threshold=300.0,
        stall_check_interval=1.0,  # Short interval for testing
    )
    # Mock emit for testing
    mon.emit = Mock()
    return mon


@pytest.fixture
def started_monitor(monitor):
    """Create and start monitor for testing."""
    monitor.start()
    yield monitor
    monitor.stop()


# =============================================================================
# Health State Tests
# =============================================================================


class TestQueryHealthState:
    """Test QueryHealthState dataclass."""

    def test_health_state_creation(self):
        """Test creating health state."""
        state = QueryHealthState(
            query_id="test_query",
            query_name="Test Query",
            stall_threshold=300.0,
        )

        assert state.query_id == "test_query"
        assert state.query_name == "Test Query"
        assert state.health_status == QueryHealthStatus.UNKNOWN
        assert state.last_progress_time is None
        assert state.stall_threshold == 300.0

    def test_update_from_progress(self):
        """Test updating state from progress."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)

        state.update_from_progress(
            batch_id=1, num_input_rows=100, input_rows_per_second=10.0, process_rate=9.0
        )

        assert state.last_progress_time is not None
        assert state.last_check_time is not None
        assert state.metrics["batch_id"] == 1
        assert state.exception_message is None

    def test_update_from_exception(self):
        """Test updating state from exception."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)

        state.update_from_exception("Test exception")

        assert state.exception_message == "Test exception"
        assert state.health_status == QueryHealthStatus.EXCEPTION

    def test_check_for_stall_no_progress(self):
        """Test stall check with no progress."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)

        assert state.check_for_stall() is False

    def test_check_for_stall_recent_progress(self):
        """Test stall check with recent progress."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)
        state.last_progress_time = datetime.now()

        assert state.check_for_stall() is False

    def test_check_for_stall_old_progress(self):
        """Test stall check with old progress."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)
        state.last_progress_time = datetime.now() - timedelta(seconds=400)

        assert state.check_for_stall() is True

    def test_evaluate_health_exception(self):
        """Test health evaluation with exception."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)
        state.update_from_exception("Test error")

        status = state.evaluate_health()

        assert status == QueryHealthStatus.EXCEPTION

    def test_evaluate_health_stalled(self):
        """Test health evaluation when stalled."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)
        state.last_progress_time = datetime.now() - timedelta(seconds=400)

        status = state.evaluate_health()

        assert status == QueryHealthStatus.STALLED

    def test_evaluate_health_unknown(self):
        """Test health evaluation with no data."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)

        status = state.evaluate_health()

        assert status == QueryHealthStatus.UNKNOWN

    def test_evaluate_health_unhealthy_slow_processing(self):
        """Test health evaluation with slow processing."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)
        state.last_progress_time = datetime.now()
        state.metrics = {"input_rows_per_second": 100.0, "process_rate": 30.0}  # 30% of input

        status = state.evaluate_health()

        assert status == QueryHealthStatus.UNHEALTHY

    def test_evaluate_health_healthy(self):
        """Test health evaluation when healthy."""
        state = QueryHealthState(query_id="test", query_name="test", stall_threshold=300.0)
        state.last_progress_time = datetime.now()
        state.metrics = {"input_rows_per_second": 100.0, "process_rate": 95.0}

        status = state.evaluate_health()

        assert status == QueryHealthStatus.HEALTHY


# =============================================================================
# Monitor Initialization Tests
# =============================================================================


class TestMonitorInitialization:
    """Test monitor initialization and lifecycle."""

    def test_initialization(self, mock_signal_provider, mock_logger_provider):
        """Test monitor initialization."""
        monitor = StreamingHealthMonitor(
            signal_provider=mock_signal_provider,
            logger_provider=mock_logger_provider,
            stall_threshold=300.0,
            stall_check_interval=60.0,
        )

        assert monitor.stall_threshold == 300.0
        assert monitor.stall_check_interval == 60.0
        assert not monitor._running
        assert len(monitor._health_states) == 0

    def test_start(self, monitor):
        """Test starting the monitor."""
        monitor.start()

        assert monitor._running
        assert monitor._stall_checker_thread is not None
        assert monitor._stall_checker_thread.is_alive()

        # Cleanup
        monitor.stop()

    def test_start_twice(self, started_monitor):
        """Test starting already-running monitor."""
        started_monitor.start()

        # Should log warning but not crash
        started_monitor.logger.warning.assert_called()

    def test_stop(self, started_monitor):
        """Test stopping the monitor."""
        result = started_monitor.stop()

        assert result is True
        assert not started_monitor._running

    def test_stop_not_running(self, monitor):
        """Test stopping non-running monitor."""
        result = monitor.stop()

        assert result is True
        monitor.logger.warning.assert_called()


# =============================================================================
# Signal Subscription Tests
# =============================================================================


class TestSignalSubscription:
    """Test signal subscription and handling."""

    def test_subscribes_to_signals_on_start(self, monitor):
        """Test that monitor subscribes to signals on start."""
        monitor.start()

        # Should have created signals for subscription
        calls = monitor.signal_provider.create_signal.call_args_list
        signal_names = [call[0][0] for call in calls]

        assert "streaming.spark_query_started" in signal_names
        assert "streaming.spark_query_progress" in signal_names
        assert "streaming.spark_query_terminated" in signal_names

        monitor.stop()

    def test_unsubscribes_on_stop(self, started_monitor):
        """Test that monitor unsubscribes on stop."""
        started_monitor.stop()

        # Should have disconnected from signals
        # Check that disconnect was called on signals
        for signal_name, handler in started_monitor._signal_connections:
            # Connections should be cleared
            pass
        assert len(started_monitor._signal_connections) == 0


# =============================================================================
# Signal Handler Tests
# =============================================================================


class TestSignalHandlers:
    """Test signal handler methods."""

    def test_on_query_started(self, started_monitor):
        """Test handling query started signal."""
        started_monitor._on_query_started(
            None, query_id="query-123", query_name="Test Query", name="Test Query"
        )

        # Should create health state
        state = started_monitor.get_health_status("query-123")
        assert state is not None
        assert state.query_id == "query-123"
        assert state.query_name == "Test Query"
        assert state.health_status == QueryHealthStatus.UNKNOWN

    def test_on_query_progress_new_query(self, started_monitor):
        """Test handling progress for new query."""
        started_monitor._on_query_progress(
            None,
            query_id="query-123",
            query_name="Test Query",
            batch_id=1,
            num_input_rows=100,
            input_rows_per_second=10.0,
            process_rate=9.0,
        )

        # Should create state and update with progress
        state = started_monitor.get_health_status("query-123")
        assert state is not None
        assert state.last_progress_time is not None
        assert state.metrics["batch_id"] == 1

    def test_on_query_progress_existing_query(self, started_monitor):
        """Test handling progress for existing query."""
        # Start tracking query
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        # Send progress
        started_monitor._on_query_progress(
            None,
            query_id="query-123",
            batch_id=2,
            num_input_rows=200,
            input_rows_per_second=20.0,
            process_rate=18.0,
        )

        state = started_monitor.get_health_status("query-123")
        assert state.metrics["batch_id"] == 2
        assert state.last_progress_time is not None

    def test_on_query_progress_emits_health_signal(self, started_monitor):
        """Test that progress triggers health signal."""
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        # Send progress with healthy metrics
        started_monitor._on_query_progress(
            None,
            query_id="query-123",
            batch_id=1,
            num_input_rows=100,
            input_rows_per_second=100.0,
            process_rate=95.0,
        )

        # Should emit healthy signal (state changed from UNKNOWN to HEALTHY)
        started_monitor.emit.assert_called()
        calls = [str(call) for call in started_monitor.emit.call_args_list]
        assert any("streaming.query_healthy" in str(c) for c in calls)

    def test_on_query_terminated_with_exception(self, started_monitor):
        """Test handling termination with exception."""
        # Start tracking query
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        # Terminate with exception
        started_monitor._on_query_terminated(None, query_id="query-123", exception="Test exception")

        # Should emit exception signal
        started_monitor.emit.assert_called()
        calls = [str(call) for call in started_monitor.emit.call_args_list]
        assert any("streaming.query_exception" in str(c) for c in calls)

        # Should remove from tracking
        assert started_monitor.get_health_status("query-123") is None

    def test_on_query_terminated_no_exception(self, started_monitor):
        """Test handling clean termination."""
        # Start tracking query
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        # Terminate without exception
        started_monitor._on_query_terminated(None, query_id="query-123")

        # Should remove from tracking without emitting exception signal
        assert started_monitor.get_health_status("query-123") is None


# =============================================================================
# Stall Detection Tests
# =============================================================================


class TestStallDetection:
    """Test stall detection functionality."""

    def test_detects_stalled_query(self, started_monitor):
        """Test detection of stalled query."""
        # Create query with old progress
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        state = started_monitor.get_health_status("query-123")
        state.last_progress_time = datetime.now() - timedelta(seconds=400)

        # Wait for stall checker to run
        time.sleep(1.5)

        # Should detect stall and emit signal
        started_monitor.emit.assert_called()
        calls = [str(call) for call in started_monitor.emit.call_args_list]
        assert any("streaming.query_stalled" in str(c) for c in calls)

    def test_no_stall_with_recent_progress(self, started_monitor):
        """Test no stall with recent progress."""
        # Send progress events
        started_monitor._on_query_progress(
            None,
            query_id="query-123",
            query_name="Test Query",
            batch_id=1,
            num_input_rows=100,
            input_rows_per_second=10.0,
            process_rate=9.0,
        )

        # Wait for stall checker
        time.sleep(1.5)

        # Should not detect stall (progress is recent)
        calls = [str(call) for call in started_monitor.emit.call_args_list]
        # Should emit healthy, not stalled
        assert not any("streaming.query_stalled" in str(c) for c in calls)


# =============================================================================
# Health Signal Emission Tests
# =============================================================================


class TestHealthSignalEmission:
    """Test health signal emission."""

    def test_emits_healthy_signal(self, started_monitor):
        """Test emission of healthy signal."""
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        # Send healthy progress
        started_monitor._on_query_progress(
            None,
            query_id="query-123",
            batch_id=1,
            num_input_rows=100,
            input_rows_per_second=100.0,
            process_rate=95.0,
        )

        # Should emit healthy signal
        calls = [str(call) for call in started_monitor.emit.call_args_list]
        assert any("streaming.query_healthy" in str(c) for c in calls)

    def test_emits_unhealthy_signal(self, started_monitor):
        """Test emission of unhealthy signal."""
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        # Send unhealthy progress (slow processing)
        started_monitor._on_query_progress(
            None,
            query_id="query-123",
            batch_id=1,
            num_input_rows=100,
            input_rows_per_second=100.0,
            process_rate=30.0,  # Only 30% of input rate
        )

        # Should emit unhealthy signal
        calls = [str(call) for call in started_monitor.emit.call_args_list]
        assert any("streaming.query_unhealthy" in str(c) for c in calls)

    def test_emits_exception_signal(self, started_monitor):
        """Test emission of exception signal."""
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        # Terminate with exception
        started_monitor._on_query_terminated(None, query_id="query-123", exception="Test exception")

        # Should emit exception signal
        calls = [str(call) for call in started_monitor.emit.call_args_list]
        assert any("streaming.query_exception" in str(c) for c in calls)


# =============================================================================
# State Management Tests
# =============================================================================


class TestStateManagement:
    """Test health state management."""

    def test_get_health_status(self, started_monitor):
        """Test getting health status."""
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        status = started_monitor.get_health_status("query-123")

        assert status is not None
        assert status.query_id == "query-123"

    def test_get_health_status_nonexistent(self, started_monitor):
        """Test getting status for nonexistent query."""
        status = started_monitor.get_health_status("nonexistent")

        assert status is None

    def test_get_all_health_statuses(self, started_monitor):
        """Test getting all health statuses."""
        started_monitor._on_query_started(None, query_id="query-1", query_name="Query 1")
        started_monitor._on_query_started(None, query_id="query-2", query_name="Query 2")

        statuses = started_monitor.get_all_health_statuses()

        assert len(statuses) == 2
        assert "query-1" in statuses
        assert "query-2" in statuses

    def test_get_metrics(self, started_monitor):
        """Test getting monitor metrics."""
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        metrics = started_monitor.get_metrics()

        assert "health_checks" in metrics
        assert "state_changes" in metrics
        assert "tracked_queries" in metrics
        assert metrics["tracked_queries"] == 1


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Test integration scenarios."""

    def test_full_query_lifecycle(self, started_monitor):
        """Test full query lifecycle monitoring."""
        # Query starts
        started_monitor._on_query_started(None, query_id="query-123", query_name="Test Query")

        # Progress events
        for i in range(3):
            started_monitor._on_query_progress(
                None,
                query_id="query-123",
                batch_id=i,
                num_input_rows=100,
                input_rows_per_second=10.0,
                process_rate=9.0,
            )

        # Query terminates
        started_monitor._on_query_terminated(None, query_id="query-123")

        # Should not be tracked anymore
        assert started_monitor.get_health_status("query-123") is None

    def test_multiple_queries(self, started_monitor):
        """Test monitoring multiple queries."""
        # Start multiple queries
        for i in range(3):
            started_monitor._on_query_started(None, query_id=f"query-{i}", query_name=f"Query {i}")

        statuses = started_monitor.get_all_health_statuses()
        assert len(statuses) == 3

        # Send progress to all
        for i in range(3):
            started_monitor._on_query_progress(
                None,
                query_id=f"query-{i}",
                batch_id=1,
                num_input_rows=100,
                input_rows_per_second=10.0,
                process_rate=9.0,
            )

        # All should have progress
        for i in range(3):
            state = started_monitor.get_health_status(f"query-{i}")
            assert state.last_progress_time is not None
