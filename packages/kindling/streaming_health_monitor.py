"""
Kindling Streaming Health Monitor

Event-driven health monitoring for streaming queries using signal-based
architecture. Responds to streaming events and emits health signals.

Key Components:
- StreamingHealthMonitor: Event-driven health tracker
- QueryHealthStatus: Health state enum
- QueryHealthState: Health state dataclass

Related:
- Issue #19: https://github.com/sep/spark-kindling-framework/issues/19
- Feature #21: KindlingStreamingListener (event source)
- Feature #18: StreamingQueryManager (integration)

Architecture:
The monitor subscribes to streaming signals from KindlingStreamingListener
and tracks query health state, emitting health signals when state changes.

Pattern:
1. Monitor subscribes to streaming signals
2. Receives query_started, query_progress, query_terminated events
3. Tracks health state per query
4. Detects stalls (time since last progress)
5. Emits health signals on state changes
6. Can respond to other signals (e.g., restart commands)

Example:
    >>> from kindling.streaming_health_monitor import StreamingHealthMonitor
    >>>
    >>> monitor = StreamingHealthMonitor(
    ...     signal_provider=signals,
    ...     logger_provider=logger,
    ...     stall_threshold=300.0
    ... )
    >>> monitor.start()
    >>> # Monitor reacts to streaming events
    >>> monitor.stop()
"""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Optional

from injector import inject
from kindling.injection import GlobalInjector
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log_provider import SparkLoggerProvider

# =============================================================================
# Data Structures
# =============================================================================


class QueryHealthStatus(Enum):
    """Health status for streaming queries."""

    HEALTHY = "healthy"  # Operating normally
    UNHEALTHY = "unhealthy"  # Issues detected but still running
    EXCEPTION = "exception"  # Query has exception
    STALLED = "stalled"  # No progress for threshold period
    UNKNOWN = "unknown"  # No data yet


@dataclass
class QueryHealthState:
    """
    Health state for a streaming query.

    Attributes:
        query_id: Query identifier
        query_name: Query name
        health_status: Current health status
        last_progress_time: Last time progress event received
        last_check_time: Last time health was evaluated
        exception_message: Exception message if any
        stall_threshold: Seconds without progress before stalled
        metrics: Latest progress metrics
    """

    query_id: str
    query_name: str
    health_status: QueryHealthStatus = QueryHealthStatus.UNKNOWN
    last_progress_time: Optional[datetime] = None
    last_check_time: Optional[datetime] = None
    exception_message: Optional[str] = None
    stall_threshold: float = 300.0
    metrics: Dict = field(default_factory=dict)

    def update_from_progress(self, **metrics):
        """Update state from progress event."""
        self.last_progress_time = datetime.now()
        self.last_check_time = datetime.now()
        self.metrics = metrics

        # Clear exception on successful progress
        if self.exception_message:
            self.exception_message = None

    def update_from_exception(self, exception: str):
        """Update state from exception."""
        self.exception_message = exception
        self.health_status = QueryHealthStatus.EXCEPTION
        self.last_check_time = datetime.now()

    def check_for_stall(self) -> bool:
        """
        Check if query is stalled.

        Returns:
            True if stalled, False otherwise
        """
        if not self.last_progress_time:
            return False

        time_since_progress = (datetime.now() - self.last_progress_time).total_seconds()
        return time_since_progress > self.stall_threshold

    def evaluate_health(self) -> QueryHealthStatus:
        """
        Evaluate current health status.

        Returns:
            QueryHealthStatus
        """
        self.last_check_time = datetime.now()

        # Check for exception
        if self.exception_message:
            self.health_status = QueryHealthStatus.EXCEPTION
            return self.health_status

        # Check for stall
        if self.check_for_stall():
            self.health_status = QueryHealthStatus.STALLED
            return self.health_status

        # Check if we have progress data
        if not self.last_progress_time:
            self.health_status = QueryHealthStatus.UNKNOWN
            return self.health_status

        # Check metrics for unhealthy indicators
        if self.metrics:
            # Example: Check if processing rate is too low
            input_rate = self.metrics.get("input_rows_per_second", 0)
            process_rate = self.metrics.get("process_rate", 0)

            if input_rate > 0 and process_rate < input_rate * 0.5:
                # Processing rate is less than 50% of input rate
                self.health_status = QueryHealthStatus.UNHEALTHY
                return self.health_status

        # Default to healthy
        self.health_status = QueryHealthStatus.HEALTHY
        return self.health_status


# =============================================================================
# Streaming Health Monitor
# =============================================================================


@GlobalInjector.singleton_autobind()
class StreamingHealthMonitor(SignalEmitter):
    """
    Event-driven health monitor for streaming queries.

    Subscribes to streaming signals and tracks query health state,
    emitting health signals when state changes or issues are detected.

    Signals Subscribed:
        - streaming.spark_query_started: Track new queries
        - streaming.spark_query_progress: Update health from progress
        - streaming.spark_query_terminated: Handle termination/exceptions

    Signals Emitted:
        - streaming.query_healthy: Query is healthy
        - streaming.query_unhealthy: Query has issues
        - streaming.query_exception: Query has exception
        - streaming.query_stalled: Query is stalled

    Example:
        >>> monitor = StreamingHealthMonitor(
        ...     signal_provider=signals,
        ...     logger_provider=logger
        ... )
        >>> monitor.start()
        >>> # Monitor reacts to streaming events
        >>> monitor.stop()

    Thread Safety:
        State access is thread-safe using locks.
    """

    EMITS = [
        "streaming.query_healthy",
        "streaming.query_unhealthy",
        "streaming.query_exception",
        "streaming.query_stalled",
    ]

    @inject
    def __init__(
        self,
        signal_provider: SignalProvider,
        logger_provider: SparkLoggerProvider,
        stall_threshold: float = 300.0,
        stall_check_interval: float = 60.0,
    ):
        """
        Initialize the streaming health monitor.

        Args:
            signal_provider: Provider for creating/emitting signals
            logger_provider: Provider for logging
            stall_threshold: Seconds without progress before stalled (default: 300)
            stall_check_interval: Seconds between stall checks (default: 60)
        """
        self._init_signal_emitter(signal_provider)

        self.logger = logger_provider.get_logger("StreamingHealthMonitor")
        self.signal_provider = signal_provider
        self.stall_threshold = stall_threshold
        self.stall_check_interval = stall_check_interval

        # Health state tracking: query_id -> QueryHealthState
        self._health_states: Dict[str, QueryHealthState] = {}
        self._state_lock = threading.RLock()

        # Stall check thread
        self._stall_checker_thread: Optional[threading.Thread] = None
        self._running = False
        self._shutdown_event = threading.Event()

        # Metrics
        self._health_checks = 0
        self._state_changes = 0

        # Signal connections
        self._signal_connections = []

        self.logger.info(
            f"StreamingHealthMonitor initialized "
            f"(stall_threshold={stall_threshold}s, check_interval={stall_check_interval}s)"
        )

    def start(self):
        """
        Start the health monitor.

        Subscribes to streaming signals and starts stall checker thread.
        """
        if self._running:
            self.logger.warning("Monitor already running")
            return

        self._running = True
        self._shutdown_event.clear()

        # Subscribe to streaming signals
        self._subscribe_to_signals()

        # Start stall checker thread
        self._stall_checker_thread = threading.Thread(
            target=self._stall_checker_loop, name="StallChecker", daemon=True
        )
        self._stall_checker_thread.start()

        self.logger.info("Health monitor started")

    def stop(self, timeout: float = 5.0) -> bool:
        """
        Stop the health monitor.

        Args:
            timeout: Maximum time to wait for thread to stop (default: 5 seconds)

        Returns:
            True if stopped cleanly, False if timeout occurred
        """
        if not self._running:
            self.logger.warning("Monitor not running")
            return True

        self.logger.info("Stopping monitor...")
        self._running = False
        self._shutdown_event.set()

        # Unsubscribe from signals
        self._unsubscribe_from_signals()

        # Wait for stall checker thread
        if self._stall_checker_thread and self._stall_checker_thread.is_alive():
            self._stall_checker_thread.join(timeout=timeout)

            if self._stall_checker_thread.is_alive():
                self.logger.error(f"Stall checker thread did not stop within {timeout}s")
                return False

        self.logger.info(
            f"Monitor stopped (checks={self._health_checks}, "
            f"state_changes={self._state_changes})"
        )
        return True

    def get_health_status(self, query_id: str) -> Optional[QueryHealthState]:
        """
        Get current health status for a query.

        Args:
            query_id: Query to check

        Returns:
            QueryHealthState if available, None otherwise
        """
        with self._state_lock:
            return self._health_states.get(query_id)

    def get_all_health_statuses(self) -> Dict[str, QueryHealthState]:
        """
        Get health status for all tracked queries.

        Returns:
            Dictionary mapping query_id to QueryHealthState
        """
        with self._state_lock:
            return dict(self._health_states)

    def get_metrics(self) -> Dict[str, int]:
        """
        Get monitor metrics.

        Returns:
            Dictionary with health_checks, state_changes, tracked_queries
        """
        return {
            "health_checks": self._health_checks,
            "state_changes": self._state_changes,
            "tracked_queries": len(self._health_states),
        }

    # =========================================================================
    # Signal Subscription
    # =========================================================================

    def _subscribe_to_signals(self):
        """Subscribe to streaming signals."""
        try:
            # Subscribe to query started
            started_signal = self.signal_provider.create_signal("streaming.spark_query_started")
            started_signal.connect(self._on_query_started)
            self._signal_connections.append(
                ("streaming.spark_query_started", self._on_query_started)
            )

            # Subscribe to query progress
            progress_signal = self.signal_provider.create_signal("streaming.spark_query_progress")
            progress_signal.connect(self._on_query_progress)
            self._signal_connections.append(
                ("streaming.spark_query_progress", self._on_query_progress)
            )

            # Subscribe to query terminated
            terminated_signal = self.signal_provider.create_signal(
                "streaming.spark_query_terminated"
            )
            terminated_signal.connect(self._on_query_terminated)
            self._signal_connections.append(
                ("streaming.spark_query_terminated", self._on_query_terminated)
            )

            self.logger.info("Subscribed to streaming signals")

        except Exception as e:
            self.logger.error(f"Error subscribing to signals: {e}", exc_info=True)

    def _unsubscribe_from_signals(self):
        """Unsubscribe from streaming signals."""
        try:
            for signal_name, handler in self._signal_connections:
                signal = self.signal_provider.create_signal(signal_name)
                signal.disconnect(handler)

            self._signal_connections.clear()
            self.logger.info("Unsubscribed from streaming signals")

        except Exception as e:
            self.logger.error(f"Error unsubscribing from signals: {e}", exc_info=True)

    # =========================================================================
    # Signal Handlers
    # =========================================================================

    def _on_query_started(self, sender, **kwargs):
        """
        Handle query started signal.

        Args:
            sender: Signal sender
            **kwargs: Signal data (query_id, query_name, etc.)
        """
        query_id = kwargs.get("query_id")
        query_name = kwargs.get("name") or kwargs.get("query_name", query_id)

        if not query_id:
            self.logger.warning("Received query_started signal without query_id")
            return

        with self._state_lock:
            # Create new health state for query
            self._health_states[query_id] = QueryHealthState(
                query_id=query_id,
                query_name=query_name,
                stall_threshold=self.stall_threshold,
            )

        self.logger.debug(f"Started tracking health for query '{query_id}'")

    def _on_query_progress(self, sender, **kwargs):
        """
        Handle query progress signal.

        Args:
            sender: Signal sender
            **kwargs: Signal data (query_id, metrics, etc.)
        """
        query_id = kwargs.get("query_id")

        if not query_id:
            self.logger.warning("Received query_progress signal without query_id")
            return

        with self._state_lock:
            state = self._health_states.get(query_id)

            if not state:
                # Query not tracked yet, create state
                query_name = kwargs.get("name") or kwargs.get("query_name", query_id)
                state = QueryHealthState(
                    query_id=query_id,
                    query_name=query_name,
                    stall_threshold=self.stall_threshold,
                )
                self._health_states[query_id] = state

            # Extract metrics
            metrics = {
                "batch_id": kwargs.get("batch_id"),
                "num_input_rows": kwargs.get("num_input_rows"),
                "input_rows_per_second": kwargs.get("input_rows_per_second"),
                "process_rate": kwargs.get("process_rate"),
                "batch_duration": kwargs.get("batch_duration"),
            }

            # Update state with progress
            prev_status = state.health_status
            state.update_from_progress(**metrics)

            # Evaluate health
            new_status = state.evaluate_health()
            self._health_checks += 1

            # Emit signal if status changed
            if prev_status != new_status:
                self._emit_health_signal(state)
                self._state_changes += 1

    def _on_query_terminated(self, sender, **kwargs):
        """
        Handle query terminated signal.

        Args:
            sender: Signal sender
            **kwargs: Signal data (query_id, exception, etc.)
        """
        query_id = kwargs.get("query_id")

        if not query_id:
            self.logger.warning("Received query_terminated signal without query_id")
            return

        exception = kwargs.get("exception")

        with self._state_lock:
            state = self._health_states.get(query_id)

            if not state:
                # Query not tracked, skip
                return

            # Update with exception if present
            if exception:
                prev_status = state.health_status
                state.update_from_exception(exception)

                if prev_status != state.health_status:
                    self._emit_health_signal(state)
                    self._state_changes += 1

            # Remove from tracking (query is terminated)
            del self._health_states[query_id]
            self.logger.debug(f"Stopped tracking health for query '{query_id}'")

    # =========================================================================
    # Stall Detection
    # =========================================================================

    def _stall_checker_loop(self):
        """
        Background thread that periodically checks for stalled queries.
        """
        self.logger.info("Stall checker thread started")

        while self._running:
            try:
                self._check_for_stalls()

                # Wait for next check interval or shutdown
                if self._shutdown_event.wait(timeout=self.stall_check_interval):
                    break

            except Exception as e:
                self.logger.error(f"Error in stall checker: {e}", exc_info=True)

        self.logger.info("Stall checker thread stopped")

    def _check_for_stalls(self):
        """Check all queries for stalls."""
        with self._state_lock:
            for query_id, state in list(self._health_states.items()):
                try:
                    prev_status = state.health_status
                    new_status = state.evaluate_health()
                    self._health_checks += 1

                    if prev_status != new_status:
                        self._emit_health_signal(state)
                        self._state_changes += 1

                except Exception as e:
                    self.logger.error(
                        f"Error checking stall for query '{query_id}': {e}", exc_info=True
                    )

    # =========================================================================
    # Health Signal Emission
    # =========================================================================

    def _emit_health_signal(self, state: QueryHealthState):
        """
        Emit appropriate health signal based on status.

        Args:
            state: Query health state
        """
        signal_data = {
            "query_id": state.query_id,
            "query_name": state.query_name,
            "health_status": state.health_status.value,
            "check_time": state.last_check_time,
        }

        if state.health_status == QueryHealthStatus.HEALTHY:
            self.emit("streaming.query_healthy", **signal_data)
            self.logger.debug(f"Query '{state.query_id}' is healthy")

        elif state.health_status == QueryHealthStatus.UNHEALTHY:
            signal_data["metrics"] = state.metrics
            self.emit("streaming.query_unhealthy", **signal_data)
            self.logger.warning(f"Query '{state.query_id}' is unhealthy: {state.metrics}")

        elif state.health_status == QueryHealthStatus.EXCEPTION:
            signal_data["exception"] = state.exception_message
            self.emit("streaming.query_exception", **signal_data)
            self.logger.error(f"Query '{state.query_id}' has exception: {state.exception_message}")

        elif state.health_status == QueryHealthStatus.STALLED:
            signal_data["last_progress_time"] = state.last_progress_time
            if state.last_progress_time:
                time_since_progress = (datetime.now() - state.last_progress_time).total_seconds()
                signal_data["seconds_since_progress"] = time_since_progress
                self.logger.warning(
                    f"Query '{state.query_id}' is stalled "
                    f"({time_since_progress:.0f}s since progress)"
                )
            self.emit("streaming.query_stalled", **signal_data)
