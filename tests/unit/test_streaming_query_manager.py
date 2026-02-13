"""
Unit tests for StreamingQueryManager.

Tests lifecycle management, state tracking, and signal emission
for streaming queries.
"""

import threading
import time
from datetime import datetime
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from kindling.streaming_query_manager import (
    StreamingQueryInfo,
    StreamingQueryManager,
    StreamingQueryState,
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
def mock_spark():
    """Mock SparkSession for testing."""
    spark = Mock()
    spark.readStream = Mock()
    return spark


@pytest.fixture
def manager(mock_signal_provider, mock_logger_provider, mock_spark):
    """Create manager instance for testing."""
    with patch(
        "kindling.streaming_query_manager.get_or_create_spark_session", return_value=mock_spark
    ):
        mgr = StreamingQueryManager(
            signal_provider=mock_signal_provider,
            logger_provider=mock_logger_provider,
        )
    # Mock emit for testing
    mgr.emit = Mock()
    return mgr


@pytest.fixture
def mock_streaming_query():
    """Mock PySpark StreamingQuery."""
    query = Mock()
    query.id = "spark-query-123"
    query.runId = "spark-run-456"
    query.isActive = True
    query.lastProgress = {
        "batchId": 5,
        "numInputRows": 1000,
        "inputRowsPerSecond": 100.0,
        "processedRowsPerSecond": 95.0,
    }
    return query


def simple_builder(spark, config):
    """Simple query builder for testing."""
    query = Mock()
    query.id = "test-query-id"
    query.runId = "test-run-id"
    query.isActive = True
    return query


# =============================================================================
# State and Info Tests
# =============================================================================


class TestStreamingQueryState:
    """Test StreamingQueryState enum."""

    def test_state_values(self):
        """Test all state enum values exist."""
        assert StreamingQueryState.REGISTERED.value == "registered"
        assert StreamingQueryState.STARTING.value == "starting"
        assert StreamingQueryState.ACTIVE.value == "active"
        assert StreamingQueryState.STOPPING.value == "stopping"
        assert StreamingQueryState.STOPPED.value == "stopped"
        assert StreamingQueryState.FAILED.value == "failed"
        assert StreamingQueryState.RESTARTING.value == "restarting"


class TestStreamingQueryInfo:
    """Test StreamingQueryInfo dataclass."""

    def test_query_info_creation(self):
        """Test creating query info."""
        info = StreamingQueryInfo(
            query_id="test_query",
            query_name="Test Query",
            state=StreamingQueryState.REGISTERED,
        )

        assert info.query_id == "test_query"
        assert info.query_name == "Test Query"
        assert info.state == StreamingQueryState.REGISTERED
        assert info.spark_query is None
        assert info.restart_count == 0

    def test_is_active_property(self, mock_streaming_query):
        """Test is_active property."""
        info = StreamingQueryInfo(
            query_id="test",
            query_name="test",
            state=StreamingQueryState.ACTIVE,
            spark_query=mock_streaming_query,
        )

        assert info.is_active is True

    def test_is_active_false_without_spark_query(self):
        """Test is_active is False without spark query."""
        info = StreamingQueryInfo(
            query_id="test", query_name="test", state=StreamingQueryState.ACTIVE
        )

        assert info.is_active is False

    def test_spark_query_id_property(self, mock_streaming_query):
        """Test spark_query_id property."""
        info = StreamingQueryInfo(
            query_id="test",
            query_name="test",
            state=StreamingQueryState.ACTIVE,
            spark_query=mock_streaming_query,
        )

        assert info.spark_query_id == "spark-query-123"

    def test_spark_run_id_property(self, mock_streaming_query):
        """Test spark_run_id property."""
        info = StreamingQueryInfo(
            query_id="test",
            query_name="test",
            state=StreamingQueryState.ACTIVE,
            spark_query=mock_streaming_query,
        )

        assert info.spark_run_id == "spark-run-456"


# =============================================================================
# Manager Initialization Tests
# =============================================================================


class TestManagerInitialization:
    """Test manager initialization."""

    def test_initialization(self, mock_signal_provider, mock_logger_provider, mock_spark):
        """Test manager initialization."""
        with patch(
            "kindling.streaming_query_manager.get_or_create_spark_session", return_value=mock_spark
        ):
            manager = StreamingQueryManager(
                signal_provider=mock_signal_provider,
                logger_provider=mock_logger_provider,
            )

        assert manager.spark is mock_spark
        assert len(manager._queries) == 0
        assert len(manager._builders) == 0


# =============================================================================
# Query Registration Tests
# =============================================================================


class TestQueryRegistration:
    """Test query registration."""

    def test_register_query(self, manager):
        """Test registering a query."""
        config = {"path": "/data", "query_name": "My Query"}

        query_info = manager.register_query("test_query", simple_builder, config)

        assert query_info.query_id == "test_query"
        assert query_info.query_name == "My Query"
        assert query_info.state == StreamingQueryState.REGISTERED
        assert query_info.config == config

    def test_register_query_emits_signal(self, manager):
        """Test that registration emits signal."""
        manager.register_query("test_query", simple_builder, {"path": "/data"})

        manager.emit.assert_called()
        calls = [str(call) for call in manager.emit.call_args_list]
        assert any("streaming.query_registered" in str(c) for c in calls)

    def test_register_duplicate_query_raises(self, manager):
        """Test registering duplicate query raises ValueError."""
        manager.register_query("test_query", simple_builder, {})

        with pytest.raises(ValueError, match="already registered"):
            manager.register_query("test_query", simple_builder, {})

    def test_register_query_without_config(self, manager):
        """Test registering query without config."""
        query_info = manager.register_query("test_query", simple_builder)

        assert query_info.config == {}
        assert query_info.query_name == "test_query"  # Falls back to query_id


# =============================================================================
# Query Start Tests
# =============================================================================


class TestQueryStart:
    """Test starting queries."""

    def test_start_query(self, manager, mock_streaming_query):
        """Test starting a registered query."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        query_info = manager.start_query("test_query")

        assert query_info.state == StreamingQueryState.ACTIVE
        assert query_info.spark_query is mock_streaming_query
        assert query_info.start_time is not None

    def test_start_query_emits_signal(self, manager, mock_streaming_query):
        """Test that starting query emits signal."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")

        # Check signal was emitted
        calls = [str(call) for call in manager.emit.call_args_list]
        assert any("streaming.query_started" in str(c) for c in calls)

    def test_start_unregistered_query_raises(self, manager):
        """Test starting unregistered query raises ValueError."""
        with pytest.raises(ValueError, match="not registered"):
            manager.start_query("nonexistent_query")

    def test_start_already_active_query_raises(self, manager, mock_streaming_query):
        """Test starting already active query raises ValueError."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")

        with pytest.raises(ValueError, match="already active"):
            manager.start_query("test_query")

    def test_start_query_builder_failure(self, manager):
        """Test starting query with failing builder."""

        def failing_builder(spark, config):
            raise RuntimeError("Builder failed")

        manager.register_query("test_query", failing_builder, {})

        with pytest.raises(RuntimeError, match="Failed to start"):
            manager.start_query("test_query")

        # Query should be in FAILED state
        query_info = manager.get_query_status("test_query")
        assert query_info.state == StreamingQueryState.FAILED
        assert "Builder failed" in query_info.last_error

    def test_start_query_invalid_return_type(self, manager):
        """Test starting query with builder returning wrong type."""

        def bad_builder(spark, config):
            return "not a streaming query"

        manager.register_query("test_query", bad_builder, {})

        with pytest.raises(RuntimeError, match="did not return a valid StreamingQuery"):
            manager.start_query("test_query")


# =============================================================================
# Query Stop Tests
# =============================================================================


class TestQueryStop:
    """Test stopping queries."""

    def test_stop_query(self, manager, mock_streaming_query):
        """Test stopping an active query."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")

        query_info = manager.stop_query("test_query", await_termination=False)

        assert query_info.state == StreamingQueryState.STOPPED
        assert query_info.stop_time is not None
        mock_streaming_query.stop.assert_called_once()

    def test_stop_query_emits_signal(self, manager, mock_streaming_query):
        """Test that stopping query emits signal."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")
        manager.stop_query("test_query", await_termination=False)

        # Check signal was emitted
        calls = [str(call) for call in manager.emit.call_args_list]
        assert any("streaming.query_stopped" in str(c) for c in calls)

    def test_stop_unregistered_query_raises(self, manager):
        """Test stopping unregistered query raises ValueError."""
        with pytest.raises(ValueError, match="not registered"):
            manager.stop_query("nonexistent_query")

    def test_stop_inactive_query_raises(self, manager):
        """Test stopping inactive query raises ValueError."""
        manager.register_query("test_query", simple_builder, {})

        with pytest.raises(ValueError, match="not active"):
            manager.stop_query("test_query")

    def test_stop_query_with_await(self, manager, mock_streaming_query):
        """Test stopping query with await_termination."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")
        manager.stop_query("test_query", await_termination=True)

        mock_streaming_query.awaitTermination.assert_called_once()


# =============================================================================
# Query Restart Tests
# =============================================================================


class TestQueryRestart:
    """Test restarting queries."""

    def test_restart_query(self, manager, mock_streaming_query):
        """Test restarting a query."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")

        query_info = manager.restart_query("test_query")

        assert query_info.state == StreamingQueryState.ACTIVE
        assert query_info.restart_count == 1

    def test_restart_query_emits_signal(self, manager, mock_streaming_query):
        """Test that restarting query emits signal."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")
        manager.restart_query("test_query")

        # Check signal was emitted
        calls = [str(call) for call in manager.emit.call_args_list]
        assert any("streaming.query_restarted" in str(c) for c in calls)

    def test_restart_unregistered_query_raises(self, manager):
        """Test restarting unregistered query raises ValueError."""
        with pytest.raises(ValueError, match="not registered"):
            manager.restart_query("nonexistent_query")

    def test_restart_increments_count(self, manager, mock_streaming_query):
        """Test that restart increments restart_count."""

        def builder(spark, config):
            # Return new mock each time to avoid state issues
            query = Mock()
            query.id = "test-id"
            query.runId = "test-run"
            query.isActive = True
            return query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")

        manager.restart_query("test_query")
        query_info = manager.get_query_status("test_query")
        assert query_info.restart_count == 1

        manager.restart_query("test_query")
        query_info = manager.get_query_status("test_query")
        assert query_info.restart_count == 2

    def test_restart_active_query_stops_before_start(self, manager):
        """Active query should be stopped before restart start call."""

        query = Mock()
        query.id = "spark-id-1"
        query.runId = "spark-run-1"
        query.isActive = True

        manager.register_query("test_query", lambda spark, config: query, {})
        manager.start_query("test_query")

        original_stop = manager.stop_query
        manager.stop_query = Mock(wraps=original_stop)

        manager.restart_query("test_query")

        manager.stop_query.assert_called_once_with("test_query", await_termination=True)

    def test_restart_accepts_spark_query_id(self, manager, mock_streaming_query):
        """Restart can resolve Spark runtime query id to registered query id."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")

        query_info = manager.restart_query("spark-query-123")

        assert query_info.state == StreamingQueryState.ACTIVE
        assert query_info.restart_count == 1


# =============================================================================
# Query Status Tests
# =============================================================================


class TestQueryStatus:
    """Test query status retrieval."""

    def test_get_query_status(self, manager):
        """Test getting query status."""
        manager.register_query("test_query", simple_builder, {})

        query_info = manager.get_query_status("test_query")

        assert query_info.query_id == "test_query"
        assert query_info.state == StreamingQueryState.REGISTERED

    def test_get_query_status_unregistered_raises(self, manager):
        """Test getting status of unregistered query raises ValueError."""
        with pytest.raises(ValueError, match="not registered"):
            manager.get_query_status("nonexistent_query")

    def test_get_query_status_updates_metrics(self, manager, mock_streaming_query):
        """Test that get_query_status updates metrics from Spark."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("test_query", builder, {})
        manager.start_query("test_query")

        query_info = manager.get_query_status("test_query")

        assert "batch_id" in query_info.metrics
        assert query_info.metrics["batch_id"] == 5
        assert query_info.metrics["num_input_rows"] == 1000


# =============================================================================
# Query Listing Tests
# =============================================================================


class TestQueryListing:
    """Test listing queries."""

    def test_list_active_queries_empty(self, manager):
        """Test listing active queries when none active."""
        queries = manager.list_active_queries()
        assert len(queries) == 0

    def test_list_active_queries(self, manager, mock_streaming_query):
        """Test listing active queries."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("query1", builder, {})
        manager.register_query("query2", builder, {})
        manager.register_query("query3", simple_builder, {})  # Not started

        manager.start_query("query1")
        manager.start_query("query2")

        active = manager.list_active_queries()

        assert len(active) == 2
        query_ids = [q.query_id for q in active]
        assert "query1" in query_ids
        assert "query2" in query_ids

    def test_list_all_queries(self, manager):
        """Test listing all queries."""
        manager.register_query("query1", simple_builder, {})
        manager.register_query("query2", simple_builder, {})
        manager.register_query("query3", simple_builder, {})

        all_queries = manager.list_all_queries()

        assert len(all_queries) == 3

    def test_get_query_count(self, manager, mock_streaming_query):
        """Test getting query counts by state."""

        def builder(spark, config):
            return mock_streaming_query

        manager.register_query("query1", builder, {})
        manager.register_query("query2", builder, {})
        manager.register_query("query3", simple_builder, {})

        manager.start_query("query1")
        manager.start_query("query2")

        counts = manager.get_query_count()

        assert counts["registered"] == 1  # query3
        assert counts["active"] == 2  # query1, query2


# =============================================================================
# Thread Safety Tests
# =============================================================================


class TestThreadSafety:
    """Test thread safety of manager operations."""

    def test_concurrent_registration(self, manager):
        """Test concurrent query registration."""

        def register_queries(mgr, start_id, count):
            for i in range(count):
                query_id = f"query-{start_id}-{i}"
                mgr.register_query(query_id, simple_builder, {})

        threads = []
        for thread_id in range(3):
            thread = threading.Thread(target=register_queries, args=(manager, thread_id, 10))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All queries should be registered
        all_queries = manager.list_all_queries()
        assert len(all_queries) == 30

    def test_concurrent_start_stop(self, manager, mock_streaming_query):
        """Test concurrent start/stop operations."""

        def builder(spark, config):
            # Create new mock for each call
            query = Mock()
            query.id = "test-id"
            query.runId = "test-run"
            query.isActive = True
            query.lastProgress = {}
            return query

        # Register queries
        for i in range(5):
            manager.register_query(f"query-{i}", builder, {})

        # Start all queries
        for i in range(5):
            manager.start_query(f"query-{i}")

        # Concurrent stops
        def stop_query(mgr, query_id):
            try:
                mgr.stop_query(query_id, await_termination=False)
            except Exception:
                pass

        threads = []
        for i in range(5):
            thread = threading.Thread(target=stop_query, args=(manager, f"query-{i}"))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # All queries should be stopped
        active = manager.list_active_queries()
        assert len(active) == 0


# =============================================================================
# Signal Emission Tests
# =============================================================================


class TestSignalEmission:
    """Test signal emission for query lifecycle events."""

    def test_all_lifecycle_signals(self, manager, mock_streaming_query):
        """Test that all lifecycle signals are emitted."""

        def builder(spark, config):
            return mock_streaming_query

        # Register
        manager.register_query("test_query", builder, {})

        # Start
        manager.start_query("test_query")

        # Restart
        manager.restart_query("test_query")

        # Stop
        manager.stop_query("test_query", await_termination=False)

        # Check all signals were emitted
        calls = [str(call) for call in manager.emit.call_args_list]

        assert any("streaming.query_registered" in str(c) for c in calls)
        assert any("streaming.query_started" in str(c) for c in calls)
        assert any("streaming.query_restarted" in str(c) for c in calls)
        assert any("streaming.query_stopped" in str(c) for c in calls)
