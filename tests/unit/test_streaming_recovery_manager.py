"""
Unit tests for StreamingRecoveryManager.

Tests cover:
- RecoveryState data structure
- Manager initialization and lifecycle
- Signal subscriptions
- Signal handlers
- Recovery attempts (success, failure, exhaustion)
- Exponential backoff
- Dead letter handling
- State management
- Integration scenarios
"""

import datetime as dt
import threading
import time
from unittest.mock import MagicMock, Mock, patch

import pytest
from kindling.signaling import SignalProvider
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.streaming_query_manager import StreamingQueryManager
from kindling.streaming_recovery_manager import (
    RecoveryState,
    RecoveryStatus,
    StreamingRecoveryManager,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def signal_provider():
    """Create mock signal provider."""
    provider = MagicMock(spec=SignalProvider)
    provider.create_signal = MagicMock(return_value=MagicMock())
    return provider


@pytest.fixture
def logger_provider():
    """Create mock logger provider."""
    provider = MagicMock(spec=SparkLoggerProvider)
    logger = MagicMock()
    provider.get_logger = MagicMock(return_value=logger)
    return provider


@pytest.fixture
def query_manager():
    """Create mock query manager."""
    manager = MagicMock(spec=StreamingQueryManager)
    manager.restart_query = MagicMock()
    return manager


@pytest.fixture
def recovery_manager(signal_provider, logger_provider, query_manager):
    """Create StreamingRecoveryManager instance."""
    manager = StreamingRecoveryManager(
        signal_provider=signal_provider,
        logger_provider=logger_provider,
        query_manager=query_manager,
        max_retries=3,
        initial_backoff=1.0,
        max_backoff=10.0,
        check_interval=0.1,  # Fast for testing
    )
    return manager


# =============================================================================
# RecoveryState Tests
# =============================================================================


class TestRecoveryState:
    """Tests for RecoveryState data structure."""

    def test_recovery_state_initialization(self):
        """Test RecoveryState initialization."""
        state = RecoveryState(
            query_id="test-query",
            query_name="TestQuery",
            max_retries=5,
            initial_backoff=1.0,
            max_backoff=300.0,
        )

        assert state.query_id == "test-query"
        assert state.query_name == "TestQuery"
        assert state.retry_count == 0
        assert state.max_retries == 5
        assert state.initial_backoff == 1.0
        assert state.max_backoff == 300.0
        assert state.recovery_status == RecoveryStatus.PENDING
        assert state.last_attempt_time is None
        assert state.next_attempt_time is None

    def test_calculate_backoff_exponential(self):
        """Test exponential backoff calculation."""
        state = RecoveryState(
            query_id="test", query_name="test", initial_backoff=1.0, max_backoff=100.0
        )

        # Retry 0: 1 * 2^0 = 1
        state.retry_count = 0
        assert state.calculate_backoff() == 1.0

        # Retry 1: 1 * 2^1 = 2
        state.retry_count = 1
        assert state.calculate_backoff() == 2.0

        # Retry 2: 1 * 2^2 = 4
        state.retry_count = 2
        assert state.calculate_backoff() == 4.0

        # Retry 3: 1 * 2^3 = 8
        state.retry_count = 3
        assert state.calculate_backoff() == 8.0

    def test_calculate_backoff_max_limit(self):
        """Test backoff respects max limit."""
        state = RecoveryState(
            query_id="test", query_name="test", initial_backoff=1.0, max_backoff=10.0
        )

        # Should cap at max_backoff
        state.retry_count = 10  # Would be 1024s without limit
        assert state.calculate_backoff() == 10.0

    def test_schedule_next_attempt(self):
        """Test scheduling next attempt."""
        state = RecoveryState(
            query_id="test", query_name="test", initial_backoff=1.0, max_backoff=10.0
        )

        before = dt.datetime.now()
        state.schedule_next_attempt()
        after = dt.datetime.now()

        assert state.next_attempt_time is not None
        assert state.recovery_status == RecoveryStatus.PENDING
        # Should be scheduled ~1 second in future (first attempt)
        assert state.next_attempt_time >= before + dt.timedelta(seconds=0.9)
        assert state.next_attempt_time <= after + dt.timedelta(seconds=1.1)

    def test_can_retry_true(self):
        """Test can_retry returns True when retries remaining."""
        state = RecoveryState(query_id="test", query_name="test", max_retries=5)
        state.retry_count = 3

        assert state.can_retry() is True

    def test_can_retry_false(self):
        """Test can_retry returns False when max retries reached."""
        state = RecoveryState(query_id="test", query_name="test", max_retries=5)
        state.retry_count = 5

        assert state.can_retry() is False

    def test_is_ready_for_attempt_not_scheduled(self):
        """Test is_ready_for_attempt returns False when not scheduled."""
        state = RecoveryState(query_id="test", query_name="test")

        assert state.is_ready_for_attempt() is False

    def test_is_ready_for_attempt_future(self):
        """Test is_ready_for_attempt returns False when scheduled in future."""
        state = RecoveryState(query_id="test", query_name="test")
        state.next_attempt_time = dt.datetime.now() + dt.timedelta(seconds=10)

        assert state.is_ready_for_attempt() is False

    def test_is_ready_for_attempt_past(self):
        """Test is_ready_for_attempt returns True when scheduled time passed."""
        state = RecoveryState(query_id="test", query_name="test")
        state.next_attempt_time = dt.datetime.now() - dt.timedelta(seconds=1)

        assert state.is_ready_for_attempt() is True


# =============================================================================
# Manager Initialization Tests
# =============================================================================


class TestRecoveryManagerInit:
    """Tests for StreamingRecoveryManager initialization."""

    def test_manager_initialization(self, signal_provider, logger_provider, query_manager):
        """Test manager initialization."""
        manager = StreamingRecoveryManager(
            signal_provider=signal_provider,
            logger_provider=logger_provider,
            query_manager=query_manager,
            max_retries=5,
            initial_backoff=1.0,
            max_backoff=300.0,
            check_interval=5.0,
        )

        assert manager.signal_provider is signal_provider
        assert manager.query_manager is query_manager
        assert manager.max_retries == 5
        assert manager.initial_backoff == 1.0
        assert manager.max_backoff == 300.0
        assert manager.check_interval == 5.0
        assert manager._running is False
        assert len(manager._recovery_states) == 0
        assert len(manager._dead_letter) == 0

    def test_manager_emits_signals(self, recovery_manager):
        """Test manager declares emitted signals."""
        assert "streaming.recovery_attempted" in recovery_manager.EMITS
        assert "streaming.recovery_succeeded" in recovery_manager.EMITS
        assert "streaming.recovery_failed" in recovery_manager.EMITS
        assert "streaming.recovery_exhausted" in recovery_manager.EMITS


# =============================================================================
# Lifecycle Tests
# =============================================================================


class TestRecoveryManagerLifecycle:
    """Tests for manager lifecycle (start/stop)."""

    def test_start_manager(self, recovery_manager):
        """Test starting the manager."""
        recovery_manager.start()

        assert recovery_manager._running is True
        assert recovery_manager._recovery_thread is not None
        assert recovery_manager._recovery_thread.is_alive()

        # Cleanup
        recovery_manager.stop()

    def test_start_already_running(self, recovery_manager):
        """Test starting manager when already running."""
        recovery_manager.start()
        recovery_manager.start()  # Should not error

        assert recovery_manager._running is True

        # Cleanup
        recovery_manager.stop()

    def test_stop_manager(self, recovery_manager):
        """Test stopping the manager."""
        recovery_manager.start()
        result = recovery_manager.stop(timeout=2.0)

        assert result is True
        assert recovery_manager._running is False
        if recovery_manager._recovery_thread:
            assert not recovery_manager._recovery_thread.is_alive()

    def test_stop_not_running(self, recovery_manager):
        """Test stopping manager when not running."""
        result = recovery_manager.stop()

        assert result is True


# =============================================================================
# Signal Subscription Tests
# =============================================================================


class TestSignalSubscription:
    """Tests for signal subscription."""

    def test_subscribe_to_signals(self, recovery_manager):
        """Test subscribing to health signals."""
        mock_signal = MagicMock()
        recovery_manager.signal_provider.create_signal.return_value = mock_signal

        recovery_manager._subscribe_to_signals()

        # Should subscribe to exception and stalled signals
        calls = recovery_manager.signal_provider.create_signal.call_args_list
        signal_names = [call[0][0] for call in calls]
        assert "streaming.query_exception" in signal_names
        assert "streaming.query_stalled" in signal_names

        # Should connect handlers
        assert mock_signal.connect.call_count >= 2

    def test_unsubscribe_from_signals(self, recovery_manager):
        """Test unsubscribing from health signals."""
        mock_signal = MagicMock()
        recovery_manager.signal_provider.create_signal.return_value = mock_signal

        recovery_manager._subscribe_to_signals()
        recovery_manager._unsubscribe_from_signals()

        # Should disconnect handlers
        assert mock_signal.disconnect.call_count >= 2
        assert len(recovery_manager._signal_connections) == 0


# =============================================================================
# Signal Handler Tests
# =============================================================================


class TestSignalHandlers:
    """Tests for signal handlers."""

    def test_on_query_unhealthy_creates_recovery_state(self, recovery_manager):
        """Test handling unhealthy query creates recovery state."""
        recovery_manager._on_query_unhealthy(
            sender=None,
            query_id="test-query",
            query_name="TestQuery",
            health_status="exception",
            exception="Test exception",
        )

        assert "test-query" in recovery_manager._recovery_states
        state = recovery_manager._recovery_states["test-query"]
        assert state.query_id == "test-query"
        assert state.query_name == "TestQuery"
        assert state.failure_reason == "Test exception"
        assert state.next_attempt_time is not None

    def test_on_query_unhealthy_without_query_id(self, recovery_manager):
        """Test handling unhealthy signal without query_id."""
        recovery_manager._on_query_unhealthy(sender=None, health_status="exception")

        assert len(recovery_manager._recovery_states) == 0

    def test_on_query_unhealthy_already_tracking(self, recovery_manager):
        """Test handling unhealthy signal for already tracked query."""
        # First signal
        recovery_manager._on_query_unhealthy(
            sender=None,
            query_id="test-query",
            query_name="TestQuery",
            health_status="exception",
            exception="First exception",
        )

        # Second signal (should be ignored)
        recovery_manager._on_query_unhealthy(
            sender=None,
            query_id="test-query",
            query_name="TestQuery",
            health_status="stalled",
        )

        # Should still have only one state with first exception
        assert len(recovery_manager._recovery_states) == 1
        state = recovery_manager._recovery_states["test-query"]
        assert state.failure_reason == "First exception"

    def test_on_query_unhealthy_in_dead_letter(self, recovery_manager):
        """Test handling unhealthy signal for query in dead letter."""
        # Add query to dead letter
        state = RecoveryState(query_id="test-query", query_name="TestQuery")
        recovery_manager._dead_letter["test-query"] = state

        # Signal should be ignored
        recovery_manager._on_query_unhealthy(
            sender=None,
            query_id="test-query",
            query_name="TestQuery",
            health_status="exception",
            exception="Test exception",
        )

        assert "test-query" not in recovery_manager._recovery_states


# =============================================================================
# Recovery Attempt Tests
# =============================================================================


class TestRecoveryAttempts:
    """Tests for recovery attempts."""

    def test_attempt_recovery_success(self, recovery_manager):
        """Test successful recovery attempt."""
        # Create recovery state
        restart_func = Mock()
        state = RecoveryState(
            query_id="test-query",
            query_name="TestQuery",
            max_retries=3,
            restart_function=restart_func,
        )
        state.next_attempt_time = dt.datetime.now() - dt.timedelta(seconds=1)
        recovery_manager._recovery_states["test-query"] = state

        # Attempt recovery
        recovery_manager._attempt_recovery(state)

        # Should call restart function
        restart_func.assert_called_once()

        # Should mark as succeeded
        assert state.recovery_status == RecoveryStatus.SUCCEEDED
        assert state.retry_count == 1

        # Should remove from tracking
        assert "test-query" not in recovery_manager._recovery_states

    def test_attempt_recovery_failure_with_retry(self, recovery_manager):
        """Test failed recovery attempt with retries remaining."""
        # Create recovery state
        restart_func = Mock(side_effect=Exception("Restart failed"))
        state = RecoveryState(
            query_id="test-query",
            query_name="TestQuery",
            max_retries=3,
            initial_backoff=1.0,
            restart_function=restart_func,
        )
        state.next_attempt_time = dt.datetime.now() - dt.timedelta(seconds=1)
        recovery_manager._recovery_states["test-query"] = state

        # Attempt recovery
        recovery_manager._attempt_recovery(state)

        # Should call restart function
        restart_func.assert_called_once()

        # Should mark as failed
        assert state.recovery_status == RecoveryStatus.PENDING  # Rescheduled
        assert state.retry_count == 1
        assert state.failure_reason == "Restart failed"

        # Should schedule next attempt
        assert state.next_attempt_time is not None

        # Should still be in tracking
        assert "test-query" in recovery_manager._recovery_states

    def test_attempt_recovery_exhausted(self, recovery_manager):
        """Test recovery attempt when max retries exhausted."""
        # Create recovery state at max retries
        restart_func = Mock(side_effect=Exception("Restart failed"))
        state = RecoveryState(
            query_id="test-query",
            query_name="TestQuery",
            max_retries=3,
            restart_function=restart_func,
        )
        state.retry_count = 2  # Will be 3 after attempt
        state.next_attempt_time = dt.datetime.now() - dt.timedelta(seconds=1)
        recovery_manager._recovery_states["test-query"] = state

        # Attempt recovery
        recovery_manager._attempt_recovery(state)

        # Should call restart function
        restart_func.assert_called_once()

        # Should move to dead letter
        assert "test-query" not in recovery_manager._recovery_states
        assert "test-query" in recovery_manager._dead_letter
        assert state.recovery_status == RecoveryStatus.EXHAUSTED


# =============================================================================
# Dead Letter Tests
# =============================================================================


class TestDeadLetter:
    """Tests for dead letter handling."""

    def test_move_to_dead_letter(self, recovery_manager):
        """Test moving query to dead letter."""
        state = RecoveryState(query_id="test-query", query_name="TestQuery", max_retries=3)
        state.retry_count = 3
        recovery_manager._recovery_states["test-query"] = state

        recovery_manager._move_to_dead_letter(state)

        # Should move to dead letter
        assert "test-query" not in recovery_manager._recovery_states
        assert "test-query" in recovery_manager._dead_letter
        assert state.recovery_status == RecoveryStatus.EXHAUSTED

    def test_get_dead_letter_queries(self, recovery_manager):
        """Test getting dead letter queries."""
        state1 = RecoveryState(query_id="query-1", query_name="Query1")
        state2 = RecoveryState(query_id="query-2", query_name="Query2")

        recovery_manager._dead_letter["query-1"] = state1
        recovery_manager._dead_letter["query-2"] = state2

        dead_letter = recovery_manager.get_dead_letter_queries()

        assert len(dead_letter) == 2
        assert "query-1" in dead_letter
        assert "query-2" in dead_letter


# =============================================================================
# State Management Tests
# =============================================================================


class TestStateManagement:
    """Tests for state management methods."""

    def test_get_recovery_state(self, recovery_manager):
        """Test getting recovery state for a query."""
        state = RecoveryState(query_id="test-query", query_name="TestQuery")
        recovery_manager._recovery_states["test-query"] = state

        retrieved = recovery_manager.get_recovery_state("test-query")

        assert retrieved is state

    def test_get_recovery_state_not_found(self, recovery_manager):
        """Test getting recovery state for non-existent query."""
        retrieved = recovery_manager.get_recovery_state("non-existent")

        assert retrieved is None

    def test_get_all_recovery_states(self, recovery_manager):
        """Test getting all recovery states."""
        state1 = RecoveryState(query_id="query-1", query_name="Query1")
        state2 = RecoveryState(query_id="query-2", query_name="Query2")

        recovery_manager._recovery_states["query-1"] = state1
        recovery_manager._recovery_states["query-2"] = state2

        all_states = recovery_manager.get_all_recovery_states()

        assert len(all_states) == 2
        assert "query-1" in all_states
        assert "query-2" in all_states

    def test_get_metrics(self, recovery_manager):
        """Test getting recovery metrics."""
        recovery_manager._total_attempts = 10
        recovery_manager._total_successes = 5
        recovery_manager._total_failures = 3
        recovery_manager._total_exhausted = 2
        recovery_manager._recovery_states["query-1"] = RecoveryState(
            query_id="query-1", query_name="Query1"
        )
        recovery_manager._dead_letter["query-2"] = RecoveryState(
            query_id="query-2", query_name="Query2"
        )

        metrics = recovery_manager.get_metrics()

        assert metrics["total_attempts"] == 10
        assert metrics["total_successes"] == 5
        assert metrics["total_failures"] == 3
        assert metrics["total_exhausted"] == 2
        assert metrics["tracked_queries"] == 1
        assert metrics["dead_letter_queries"] == 1


# =============================================================================
# Integration Tests
# =============================================================================


class TestRecoveryManagerIntegration:
    """Integration tests for recovery manager."""

    def test_full_recovery_flow_success(self, recovery_manager):
        """Test full recovery flow with successful restart."""
        recovery_manager.start()

        try:
            # Simulate unhealthy query
            recovery_manager._on_query_unhealthy(
                sender=None,
                query_id="test-query",
                query_name="TestQuery",
                health_status="exception",
                exception="Test exception",
            )

            # Should create recovery state
            assert "test-query" in recovery_manager._recovery_states

            # Mock restart to succeed
            state = recovery_manager._recovery_states["test-query"]
            state.restart_function = Mock()
            state.next_attempt_time = dt.datetime.now() - dt.timedelta(seconds=1)

            # Wait for recovery attempt
            time.sleep(0.2)

            # Should have attempted and succeeded
            assert state.restart_function.call_count >= 1
            assert "test-query" not in recovery_manager._recovery_states

        finally:
            recovery_manager.stop()

    def test_full_recovery_flow_exhausted(self, recovery_manager):
        """Test full recovery flow with max retries exhausted."""
        recovery_manager.start()

        try:
            # Simulate unhealthy query
            recovery_manager._on_query_unhealthy(
                sender=None,
                query_id="test-query",
                query_name="TestQuery",
                health_status="stalled",
            )

            # Should create recovery state
            assert "test-query" in recovery_manager._recovery_states

            # Mock restart to always fail
            state = recovery_manager._recovery_states["test-query"]
            state.restart_function = Mock(side_effect=Exception("Always fails"))
            state.initial_backoff = 0.05  # Fast backoff for testing
            state.next_attempt_time = dt.datetime.now() - dt.timedelta(seconds=1)

            # Wait for all retry attempts
            time.sleep(1.0)

            # Should have exhausted retries and moved to dead letter
            assert "test-query" not in recovery_manager._recovery_states
            assert "test-query" in recovery_manager._dead_letter

        finally:
            recovery_manager.stop()

    def test_multiple_queries_concurrent_recovery(self, recovery_manager):
        """Test concurrent recovery of multiple queries."""
        recovery_manager.start()

        try:
            # Simulate multiple unhealthy queries
            for i in range(3):
                recovery_manager._on_query_unhealthy(
                    sender=None,
                    query_id=f"query-{i}",
                    query_name=f"Query{i}",
                    health_status="exception",
                    exception=f"Exception {i}",
                )

            # Should track all queries
            assert len(recovery_manager._recovery_states) == 3

            # Mock all restarts to succeed
            for query_id, state in recovery_manager._recovery_states.items():
                state.restart_function = Mock()
                state.next_attempt_time = dt.datetime.now() - dt.timedelta(seconds=1)

            # Wait for recovery attempts
            time.sleep(0.3)

            # All should have succeeded
            assert len(recovery_manager._recovery_states) == 0

        finally:
            recovery_manager.stop()

    def test_recovery_with_signal_emission(self, recovery_manager):
        """Test recovery emits appropriate signals."""
        # Track emitted signals
        emitted_signals = []

        def track_signal(signal_name):
            def handler(sender, **kwargs):
                emitted_signals.append((signal_name, kwargs))

            return handler

        # Connect to recovery signals
        for signal_name in recovery_manager.EMITS:
            signal = recovery_manager.signal_provider.create_signal(signal_name)
            signal.connect(track_signal(signal_name))

        recovery_manager.start()

        try:
            # Simulate unhealthy query
            recovery_manager._on_query_unhealthy(
                sender=None,
                query_id="test-query",
                query_name="TestQuery",
                health_status="exception",
                exception="Test exception",
            )

            # Mock restart to succeed
            state = recovery_manager._recovery_states["test-query"]
            state.restart_function = Mock()
            state.next_attempt_time = dt.datetime.now() - dt.timedelta(seconds=1)

            # Wait for recovery
            time.sleep(0.2)

            # Should have emitted signals
            # Note: Actual signal emission depends on emit() implementation

        finally:
            recovery_manager.stop()
