"""
Kindling Streaming Recovery Manager

Automatic recovery for failed streaming queries using exponential backoff
and event-driven architecture. Responds to health signals and orchestrates
query restarts.

Key Components:
- StreamingRecoveryManager: Event-driven auto-recovery orchestrator
- RecoveryState: Tracks retry attempts and backoff timing
- Exponential backoff: 1s, 2s, 4s, 8s, ... up to 300s max

Related:
- Issue #20: https://github.com/sep/spark-kindling-framework/issues/20
- Feature #18: StreamingQueryManager (restart operations)
- Feature #19: StreamingHealthMonitor (health signals)

Architecture:
The recovery manager subscribes to health signals from StreamingHealthMonitor
and automatically attempts to restart failed queries using exponential backoff.

Pattern:
1. Manager subscribes to health signals
2. Receives query_exception or query_stalled events
3. Schedules recovery attempt with exponential backoff
4. Uses StreamingQueryManager to restart query
5. Tracks retry attempts and state per query
6. Emits recovery signals on attempts, success, failure
7. Moves to dead letter after max retries exhausted

Example:
    >>> from kindling.streaming_recovery_manager import StreamingRecoveryManager
    >>>
    >>> recovery = StreamingRecoveryManager(
    ...     signal_provider=signals,
    ...     logger_provider=logger,
    ...     query_manager=query_manager,
    ...     max_retries=5,
    ...     initial_backoff=1.0,
    ...     max_backoff=300.0
    ... )
    >>> recovery.start()
    >>> # Manager reacts to health signals and attempts recovery
    >>> recovery.stop()
"""

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, Optional

from injector import inject
from kindling.injection import GlobalInjector
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.streaming_query_manager import StreamingQueryManager

# =============================================================================
# Data Structures
# =============================================================================


class RecoveryStatus(Enum):
    """Recovery status for streaming queries."""

    PENDING = "pending"  # Recovery scheduled but not started
    IN_PROGRESS = "in_progress"  # Recovery attempt in progress
    SUCCEEDED = "succeeded"  # Recovery succeeded
    FAILED = "failed"  # Recovery failed but can retry
    EXHAUSTED = "exhausted"  # Max retries exhausted


@dataclass
class RecoveryState:
    """
    Recovery state for a streaming query.

    Attributes:
        query_id: Query identifier
        query_name: Query name
        retry_count: Number of retry attempts
        max_retries: Maximum retry attempts allowed
        initial_backoff: Initial backoff in seconds
        max_backoff: Maximum backoff in seconds
        last_attempt_time: Last recovery attempt timestamp
        next_attempt_time: Next scheduled recovery timestamp
        recovery_status: Current recovery status
        failure_reason: Reason for failure
        restart_function: Function to call to restart query
    """

    query_id: str
    query_name: str
    retry_count: int = 0
    max_retries: int = 5
    initial_backoff: float = 1.0
    max_backoff: float = 300.0
    last_attempt_time: Optional[datetime] = None
    next_attempt_time: Optional[datetime] = None
    recovery_status: RecoveryStatus = RecoveryStatus.PENDING
    failure_reason: Optional[str] = None
    restart_function: Optional[Callable] = None

    def calculate_backoff(self) -> float:
        """
        Calculate exponential backoff delay.

        Returns:
            Backoff delay in seconds
        """
        backoff = self.initial_backoff * (2**self.retry_count)
        return min(backoff, self.max_backoff)

    def schedule_next_attempt(self):
        """Schedule next recovery attempt with exponential backoff."""
        backoff = self.calculate_backoff()
        self.next_attempt_time = datetime.now()
        # Add backoff seconds to next_attempt_time
        import datetime as dt

        self.next_attempt_time = datetime.now() + dt.timedelta(seconds=backoff)
        self.recovery_status = RecoveryStatus.PENDING

    def can_retry(self) -> bool:
        """
        Check if query can be retried.

        Returns:
            True if retries remaining, False otherwise
        """
        return self.retry_count < self.max_retries

    def is_ready_for_attempt(self) -> bool:
        """
        Check if ready for next recovery attempt.

        Returns:
            True if next_attempt_time has passed, False otherwise
        """
        if not self.next_attempt_time:
            return False
        return datetime.now() >= self.next_attempt_time


# =============================================================================
# Streaming Recovery Manager
# =============================================================================


@GlobalInjector.singleton_autobind()
class StreamingRecoveryManager(SignalEmitter):
    """
    Event-driven auto-recovery manager for streaming queries.

    Subscribes to health signals and automatically attempts to restart
    failed queries using exponential backoff strategy.

    Signals Subscribed:
        - streaming.query_exception: Query has exception
        - streaming.query_stalled: Query is stalled

    Signals Emitted:
        - streaming.recovery_attempted: Recovery attempt started
        - streaming.recovery_succeeded: Recovery succeeded
        - streaming.recovery_failed: Recovery failed (will retry)
        - streaming.recovery_exhausted: Max retries exhausted

    Example:
        >>> recovery = StreamingRecoveryManager(
        ...     signal_provider=signals,
        ...     logger_provider=logger,
        ...     query_manager=query_manager
        ... )
        >>> recovery.start()
        >>> # Manager reacts to health signals
        >>> recovery.stop()

    Thread Safety:
        State access is thread-safe using locks.
    """

    EMITS = [
        "streaming.recovery_attempted",
        "streaming.recovery_succeeded",
        "streaming.recovery_failed",
        "streaming.recovery_exhausted",
    ]

    @inject
    def __init__(
        self,
        signal_provider: SignalProvider,
        logger_provider: SparkLoggerProvider,
        query_manager: StreamingQueryManager,
        max_retries: Optional[int] = None,
        initial_backoff: Optional[float] = None,
        max_backoff: Optional[float] = None,
        check_interval: Optional[float] = None,
    ):
        """
        Initialize the streaming recovery manager.

        Args:
            signal_provider: Provider for creating/emitting signals
            logger_provider: Provider for logging
            query_manager: Manager for streaming query operations
            max_retries: Maximum retry attempts per query (default: 5)
            initial_backoff: Initial backoff in seconds (default: 1.0)
            max_backoff: Maximum backoff in seconds (default: 300.0)
            check_interval: Seconds between recovery checks (default: 5.0)
        """
        self._init_signal_emitter(signal_provider)

        self.logger = logger_provider.get_logger("StreamingRecoveryManager")
        self.signal_provider = signal_provider
        self.query_manager = query_manager
        # Use Optional types to prevent DI auto_bind from injecting 0/0.0
        self.max_retries = max_retries if max_retries is not None else 5
        self.initial_backoff = initial_backoff if initial_backoff is not None else 1.0
        self.max_backoff = max_backoff if max_backoff is not None else 300.0
        self.check_interval = check_interval if check_interval is not None else 5.0

        # Recovery state tracking: query_id -> RecoveryState
        self._recovery_states: Dict[str, RecoveryState] = {}
        self._dead_letter: Dict[str, RecoveryState] = {}
        self._state_lock = threading.RLock()

        # Recovery thread
        self._recovery_thread: Optional[threading.Thread] = None
        self._running = False
        self._shutdown_event = threading.Event()

        # Metrics
        self._total_attempts = 0
        self._total_successes = 0
        self._total_failures = 0
        self._total_exhausted = 0

        # Signal connections
        self._signal_connections = []

        self.logger.info(
            f"StreamingRecoveryManager initialized "
            f"(max_retries={self.max_retries}, initial_backoff={self.initial_backoff}s, "
            f"max_backoff={self.max_backoff}s, check_interval={self.check_interval}s)"
        )

    def start(self):
        """
        Start the recovery manager.

        Subscribes to health signals and starts recovery thread.
        """
        if self._running:
            self.logger.warning("Recovery manager already running")
            return

        self._running = True
        self._shutdown_event.clear()

        # Subscribe to health signals
        self._subscribe_to_signals()

        # Start recovery thread
        self._recovery_thread = threading.Thread(
            target=self._recovery_loop, name="RecoveryThread", daemon=True
        )
        self._recovery_thread.start()

        self.logger.info("Recovery manager started")

    def stop(self, timeout: float = 10.0) -> bool:
        """
        Stop the recovery manager.

        Args:
            timeout: Maximum time to wait for thread to stop (default: 10 seconds)

        Returns:
            True if stopped cleanly, False if timeout occurred
        """
        if not self._running:
            self.logger.warning("Recovery manager not running")
            return True

        self.logger.info("Stopping recovery manager...")
        self._running = False
        self._shutdown_event.set()

        # Unsubscribe from signals
        self._unsubscribe_from_signals()

        # Wait for recovery thread
        if self._recovery_thread and self._recovery_thread.is_alive():
            self._recovery_thread.join(timeout=timeout)

            if self._recovery_thread.is_alive():
                self.logger.error(f"Recovery thread did not stop within {timeout}s")
                return False

        self.logger.info(
            f"Recovery manager stopped (attempts={self._total_attempts}, "
            f"successes={self._total_successes}, failures={self._total_failures}, "
            f"exhausted={self._total_exhausted})"
        )
        return True

    def get_recovery_state(self, query_id: str) -> Optional[RecoveryState]:
        """
        Get recovery state for a query.

        Args:
            query_id: Query to check

        Returns:
            RecoveryState if available, None otherwise
        """
        with self._state_lock:
            return self._recovery_states.get(query_id)

    def get_all_recovery_states(self) -> Dict[str, RecoveryState]:
        """
        Get recovery state for all tracked queries.

        Returns:
            Dictionary mapping query_id to RecoveryState
        """
        with self._state_lock:
            return dict(self._recovery_states)

    def get_dead_letter_queries(self) -> Dict[str, RecoveryState]:
        """
        Get queries that exhausted max retries.

        Returns:
            Dictionary mapping query_id to RecoveryState
        """
        with self._state_lock:
            return dict(self._dead_letter)

    def get_metrics(self) -> Dict[str, int]:
        """
        Get recovery metrics.

        Returns:
            Dictionary with attempts, successes, failures, exhausted, tracked_queries
        """
        return {
            "total_attempts": self._total_attempts,
            "total_successes": self._total_successes,
            "total_failures": self._total_failures,
            "total_exhausted": self._total_exhausted,
            "tracked_queries": len(self._recovery_states),
            "dead_letter_queries": len(self._dead_letter),
        }

    # =========================================================================
    # Signal Subscription
    # =========================================================================

    def _subscribe_to_signals(self):
        """Subscribe to health signals."""
        try:
            # Subscribe to query exception
            exception_signal = self.signal_provider.create_signal("streaming.query_exception")
            exception_signal.connect(self._on_query_unhealthy)
            self._signal_connections.append(("streaming.query_exception", self._on_query_unhealthy))

            # Subscribe to query stalled
            stalled_signal = self.signal_provider.create_signal("streaming.query_stalled")
            stalled_signal.connect(self._on_query_unhealthy)
            self._signal_connections.append(("streaming.query_stalled", self._on_query_unhealthy))

            self.logger.info("Subscribed to health signals")

        except Exception as e:
            self.logger.error(f"Error subscribing to signals: {e}", include_traceback=True)

    def _unsubscribe_from_signals(self):
        """Unsubscribe from health signals."""
        try:
            for signal_name, handler in self._signal_connections:
                signal = self.signal_provider.create_signal(signal_name)
                signal.disconnect(handler)

            self._signal_connections.clear()
            self.logger.info("Unsubscribed from health signals")

        except Exception as e:
            self.logger.error(f"Error unsubscribing from signals: {e}", include_traceback=True)

    # =========================================================================
    # Signal Handlers
    # =========================================================================

    def _on_query_unhealthy(self, sender, **kwargs):
        """
        Handle query unhealthy signal (exception or stalled).

        Args:
            sender: Signal sender
            **kwargs: Signal data (query_id, query_name, health_status, etc.)
        """
        query_id = kwargs.get("query_id")
        query_name = kwargs.get("query_name", query_id)
        health_status = kwargs.get("health_status")
        failure_reason = kwargs.get("exception") or f"Query {health_status}"

        if not query_id:
            self.logger.warning("Received health signal without query_id")
            return

        with self._state_lock:
            # Check if already tracking this query
            if query_id in self._recovery_states:
                # Already in recovery, skip duplicate signal
                self.logger.debug(f"Query '{query_id}' already in recovery")
                return

            # Check if query is in dead letter
            if query_id in self._dead_letter:
                self.logger.debug(f"Query '{query_id}' already in dead letter")
                return

            # Get restart function for this query
            restart_function = self._get_restart_function(query_id)

            if not restart_function:
                self.logger.warning(
                    f"Cannot recover query '{query_id}': No restart function available"
                )
                return

            # Create new recovery state
            state = RecoveryState(
                query_id=query_id,
                query_name=query_name,
                max_retries=self.max_retries,
                initial_backoff=self.initial_backoff,
                max_backoff=self.max_backoff,
                failure_reason=failure_reason,
                restart_function=restart_function,
            )

            # Schedule first attempt
            state.schedule_next_attempt()
            self._recovery_states[query_id] = state

            self.logger.info(
                f"Scheduled recovery for query '{query_id}' "
                f"(reason: {failure_reason}, backoff: {state.calculate_backoff():.1f}s)"
            )

    # =========================================================================
    # Recovery Loop
    # =========================================================================

    def _recovery_loop(self):
        """
        Background thread that processes recovery attempts.
        """
        self.logger.info("Recovery thread started")

        while self._running:
            try:
                self._process_recovery_attempts()

                # Wait for next check interval or shutdown
                if self._shutdown_event.wait(timeout=self.check_interval):
                    break

            except Exception as e:
                self.logger.error(f"Error in recovery loop: {e}", include_traceback=True)

        self.logger.info("Recovery thread stopped")

    def _process_recovery_attempts(self):
        """Process all pending recovery attempts."""
        with self._state_lock:
            for query_id, state in list(self._recovery_states.items()):
                try:
                    # Check if ready for attempt
                    if not state.is_ready_for_attempt():
                        continue

                    # Check if can retry
                    if not state.can_retry():
                        self._move_to_dead_letter(state)
                        continue

                    # Attempt recovery
                    self._attempt_recovery(state)

                except Exception as e:
                    self.logger.error(
                        f"Error processing recovery for query '{query_id}': {e}",
                        include_traceback=True,
                    )

    def _attempt_recovery(self, state: RecoveryState):
        """
        Attempt to recover a query.

        Args:
            state: Recovery state
        """
        state.recovery_status = RecoveryStatus.IN_PROGRESS
        state.last_attempt_time = datetime.now()
        state.retry_count += 1
        self._total_attempts += 1

        self.logger.info(
            f"Attempting recovery for query '{state.query_id}' "
            f"(attempt {state.retry_count}/{state.max_retries})"
        )

        # Emit recovery attempted signal
        self.emit(
            "streaming.recovery_attempted",
            query_id=state.query_id,
            query_name=state.query_name,
            retry_count=state.retry_count,
            max_retries=state.max_retries,
            attempt_time=state.last_attempt_time,
        )

        try:
            # Call restart function
            if state.restart_function:
                state.restart_function()

            # Recovery succeeded
            state.recovery_status = RecoveryStatus.SUCCEEDED
            self._total_successes += 1

            self.logger.info(
                f"Recovery succeeded for query '{state.query_id}' "
                f"(attempts: {state.retry_count})"
            )

            # Emit recovery succeeded signal
            self.emit(
                "streaming.recovery_succeeded",
                query_id=state.query_id,
                query_name=state.query_name,
                retry_count=state.retry_count,
                success_time=datetime.now(),
            )

            # Remove from tracking (no longer needs recovery)
            del self._recovery_states[state.query_id]

        except Exception as e:
            # Recovery failed
            state.recovery_status = RecoveryStatus.FAILED
            state.failure_reason = str(e)
            self._total_failures += 1

            self.logger.warning(
                f"Recovery failed for query '{state.query_id}': {e} "
                f"(attempt {state.retry_count}/{state.max_retries})"
            )

            # Emit recovery failed signal
            self.emit(
                "streaming.recovery_failed",
                query_id=state.query_id,
                query_name=state.query_name,
                retry_count=state.retry_count,
                max_retries=state.max_retries,
                failure_reason=state.failure_reason,
                failure_time=datetime.now(),
            )

            # Check if can retry
            if state.can_retry():
                # Schedule next attempt
                state.schedule_next_attempt()
                backoff = state.calculate_backoff()
                self.logger.info(
                    f"Scheduled next recovery attempt for query '{state.query_id}' "
                    f"in {backoff:.1f}s"
                )
            else:
                # Max retries exhausted
                self._move_to_dead_letter(state)

    def _move_to_dead_letter(self, state: RecoveryState):
        """
        Move query to dead letter (max retries exhausted).

        Args:
            state: Recovery state
        """
        state.recovery_status = RecoveryStatus.EXHAUSTED
        self._total_exhausted += 1

        self.logger.error(
            f"Max retries exhausted for query '{state.query_id}' "
            f"(attempts: {state.retry_count})"
        )

        # Emit recovery exhausted signal
        self.emit(
            "streaming.recovery_exhausted",
            query_id=state.query_id,
            query_name=state.query_name,
            retry_count=state.retry_count,
            max_retries=state.max_retries,
            failure_reason=state.failure_reason,
            exhausted_time=datetime.now(),
        )

        # Move to dead letter
        self._dead_letter[state.query_id] = state
        del self._recovery_states[state.query_id]

    def _get_restart_function(self, query_id: str) -> Optional[Callable]:
        """
        Get restart function for a query.

        Args:
            query_id: Query identifier

        Returns:
            Restart function or None
        """

        # Create restart function that uses StreamingQueryManager
        def restart():
            self.query_manager.restart_query(query_id)

        return restart
