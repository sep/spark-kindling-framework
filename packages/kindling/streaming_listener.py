"""
Kindling Streaming Listener

Provides non-blocking event processing for PySpark StreamingQueryListener
to prevent listener callback timeouts.

Key Components:
- KindlingStreamingListener: Queue-based listener implementation
- StreamingEventType: Enum for event types
- StreamingEvent: Event data structure

Related:
- Issue #21: https://github.com/sep/spark-kindling-framework/issues/21
- Proposal: docs/proposals/signal_dag_streaming_proposal.md

Architecture:
The listener implements a queue-based event processing pattern to ensure
PySpark StreamingQueryListener callbacks complete quickly (<10ms) while
still emitting signals for all streaming events.

Pattern:
1. Listener callbacks enqueue events (non-blocking)
2. Background consumer thread processes queue
3. Consumer emits signals asynchronously
4. Queue overflow handled gracefully (drop oldest)

Example:
    >>> from pyspark.sql import SparkSession
    >>> from kindling.streaming_listener import KindlingStreamingListener
    >>>
    >>> spark = SparkSession.builder.getOrCreate()
    >>> listener = KindlingStreamingListener(signal_provider, logger_provider)
    >>> listener.start()
    >>> spark.streams.addListener(listener)
"""

import queue
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from injector import inject
from kindling.injection import GlobalInjector
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log_provider import SparkLoggerProvider
from pyspark.sql.streaming import StreamingQueryListener

# =============================================================================
# Event Data Structures
# =============================================================================


class StreamingEventType(Enum):
    """Types of streaming query events."""

    QUERY_STARTED = "query_started"
    QUERY_PROGRESS = "query_progress"
    QUERY_TERMINATED = "query_terminated"


@dataclass
class StreamingEvent:
    """
    Container for streaming query events.

    Attributes:
        event_type: Type of event (started, progress, terminated)
        query_id: Unique identifier for the query
        run_id: Run identifier for the query
        name: Name of the query (if provided)
        timestamp: Event timestamp
        data: Additional event-specific data
    """

    event_type: StreamingEventType
    query_id: str
    run_id: str
    name: Optional[str] = None
    timestamp: Optional[float] = None
    data: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


# =============================================================================
# Kindling Streaming Listener
# =============================================================================


@GlobalInjector.singleton_autobind()
class KindlingStreamingListener(StreamingQueryListener, SignalEmitter):
    """
    Non-blocking streaming query listener with queue-based event processing.

    This listener ensures PySpark callbacks complete quickly by enqueuing
    events and processing them asynchronously in a background thread.

    Signals Emitted:
        - streaming.spark_query_started: When query starts
        - streaming.spark_query_progress: On query progress updates
        - streaming.spark_query_terminated: When query terminates

    Example:
        >>> listener = KindlingStreamingListener(signal_provider, logger_provider)
        >>> listener.start()
        >>> spark.streams.addListener(listener)
        >>>
        >>> # Later, when done:
        >>> listener.stop()

    Thread Safety:
        This class uses threading for background event processing.
        The queue operations are thread-safe.
    """

    EMITS = [
        "streaming.spark_query_started",
        "streaming.spark_query_progress",
        "streaming.spark_query_terminated",
    ]

    @inject
    def __init__(
        self,
        signal_provider: SignalProvider,
        logger_provider: SparkLoggerProvider,
        max_queue_size: Optional[int] = None,
    ):
        """
        Initialize the streaming listener.

        Args:
            signal_provider: Provider for creating/emitting signals
            logger_provider: Provider for logging
            max_queue_size: Maximum queue size before dropping events (default: 1000)
        """
        super().__init__()
        self._init_signal_emitter(signal_provider)

        self.logger = logger_provider.get_logger("KindlingStreamingListener")
        # Use Optional[int] to prevent DI auto_bind from injecting 0
        self.max_queue_size = max_queue_size if max_queue_size is not None else 1000

        # Event queue (thread-safe)
        self._event_queue: queue.Queue[StreamingEvent] = queue.Queue(maxsize=self.max_queue_size)

        # Consumer thread
        self._consumer_thread: Optional[threading.Thread] = None
        self._running = False
        self._shutdown_event = threading.Event()

        # Metrics
        self._events_processed = 0
        self._events_dropped = 0

        self.logger.info(
            f"KindlingStreamingListener initialized (max_queue_size={self.max_queue_size})"
        )

    def start(self):
        """
        Start the background event consumer thread.

        This must be called before the listener is registered with Spark.
        """
        if self._running:
            self.logger.warning("Listener already running")
            return

        self._running = True
        self._shutdown_event.clear()

        self._consumer_thread = threading.Thread(
            target=self._consume_events, name="StreamingEventConsumer", daemon=True
        )
        self._consumer_thread.start()

        self.logger.info("Background event consumer started")

    def stop(self, timeout: float = 5.0):
        """
        Stop the background event consumer thread.

        Args:
            timeout: Maximum time to wait for thread to stop (default: 5 seconds)

        Returns:
            True if stopped cleanly, False if timeout occurred
        """
        if not self._running:
            self.logger.warning("Listener not running")
            return True

        self.logger.info("Stopping event consumer...")
        self._running = False
        self._shutdown_event.set()

        if self._consumer_thread and self._consumer_thread.is_alive():
            self._consumer_thread.join(timeout=timeout)

            if self._consumer_thread.is_alive():
                self.logger.error(f"Consumer thread did not stop within {timeout}s timeout")
                return False

        self.logger.info(
            f"Event consumer stopped (processed={self._events_processed}, "
            f"dropped={self._events_dropped})"
        )
        return True

    def get_metrics(self) -> Dict[str, int]:
        """
        Get listener metrics.

        Returns:
            Dictionary with events_processed, events_dropped, queue_size
        """
        return {
            "events_processed": self._events_processed,
            "events_dropped": self._events_dropped,
            "queue_size": self._event_queue.qsize(),
        }

    # =========================================================================
    # PySpark StreamingQueryListener Callbacks
    # =========================================================================

    def onQueryStarted(self, event):
        """
        Called when a streaming query starts.

        This callback MUST complete quickly (<10ms) to avoid Spark timeouts.
        Events are enqueued for asynchronous processing.

        Args:
            event: QueryStartedEvent from PySpark
        """
        streaming_event = StreamingEvent(
            event_type=StreamingEventType.QUERY_STARTED,
            query_id=str(event.id),
            run_id=str(event.runId),
            name=event.name if hasattr(event, "name") else None,
        )

        self._enqueue_event(streaming_event)

    def onQueryProgress(self, event):
        """
        Called when a streaming query makes progress.

        This callback MUST complete quickly (<10ms) to avoid Spark timeouts.
        Events are enqueued for asynchronous processing.

        Args:
            event: QueryProgressEvent from PySpark
        """
        # Extract key metrics from progress
        progress = event.progress
        data = {
            "batch_id": progress.batchId,
            "num_input_rows": progress.numInputRows,
            "input_rows_per_second": progress.inputRowsPerSecond,
            "process_rate": progress.processedRowsPerSecond,
            "batch_duration": progress.batchDuration,
        }

        streaming_event = StreamingEvent(
            event_type=StreamingEventType.QUERY_PROGRESS,
            query_id=str(progress.id),
            run_id=str(progress.runId),
            name=progress.name if hasattr(progress, "name") else None,
            data=data,
        )

        self._enqueue_event(streaming_event)

    def onQueryTerminated(self, event):
        """
        Called when a streaming query terminates.

        This callback MUST complete quickly (<10ms) to avoid Spark timeouts.
        Events are enqueued for asynchronous processing.

        Args:
            event: QueryTerminatedEvent from PySpark
        """
        # Capture exception if query failed
        data = {}
        if hasattr(event, "exception") and event.exception:
            data["exception"] = str(event.exception)
            data["error_class"] = event.exception.__class__.__name__ if event.exception else None

        streaming_event = StreamingEvent(
            event_type=StreamingEventType.QUERY_TERMINATED,
            query_id=str(event.id),
            run_id=str(event.runId),
            data=data,
        )

        self._enqueue_event(streaming_event)

    # =========================================================================
    # Internal Event Processing
    # =========================================================================

    def _enqueue_event(self, event: StreamingEvent):
        """
        Enqueue event for asynchronous processing.

        If queue is full, drop oldest event to prevent blocking.

        Args:
            event: StreamingEvent to enqueue
        """
        try:
            # Non-blocking put with immediate timeout
            self._event_queue.put_nowait(event)

        except queue.Full:
            # Queue full - drop oldest event and add new one
            try:
                dropped_event = self._event_queue.get_nowait()
                self._events_dropped += 1
                self.logger.warning(
                    f"Event queue full ({self.max_queue_size}), "
                    f"dropped {dropped_event.event_type.value} event"
                )

                # Try to add new event
                self._event_queue.put_nowait(event)

            except (queue.Empty, queue.Full):
                # Race condition or still full - just drop new event
                self._events_dropped += 1
                self.logger.warning(f"Could not enqueue {event.event_type.value} event, dropped")

    def _consume_events(self):
        """
        Background thread that consumes events from queue and emits signals.

        Runs until stop() is called or thread is interrupted.
        """
        self.logger.info("Event consumer thread started")

        while self._running:
            try:
                # Block for up to 1 second waiting for events
                event = self._event_queue.get(timeout=1.0)

                # Process the event
                self._process_event(event)
                self._events_processed += 1

                # Mark task done
                self._event_queue.task_done()

            except queue.Empty:
                # No events in queue, check if we should shutdown
                if self._shutdown_event.is_set():
                    break
                continue

            except Exception as e:
                self.logger.error(f"Error processing event: {e}", include_traceback=True)

        # Drain remaining events before shutdown
        self._drain_queue()

        self.logger.info("Event consumer thread stopped")

    def _drain_queue(self):
        """Drain any remaining events in queue before shutdown."""
        drained = 0
        while True:
            try:
                event = self._event_queue.get_nowait()
                self._process_event(event)
                self._events_processed += 1
                drained += 1
                self._event_queue.task_done()

            except queue.Empty:
                break

            except Exception as e:
                self.logger.error(f"Error draining event: {e}", include_traceback=True)
                break

        if drained > 0:
            self.logger.info(f"Drained {drained} events from queue during shutdown")

    def _process_event(self, event: StreamingEvent):
        """
        Process a single streaming event by emitting appropriate signal.

        Args:
            event: StreamingEvent to process
        """
        try:
            if event.event_type == StreamingEventType.QUERY_STARTED:
                self.emit(
                    "streaming.spark_query_started",
                    query_id=event.query_id,
                    run_id=event.run_id,
                    name=event.name,
                    timestamp=event.timestamp,
                )

            elif event.event_type == StreamingEventType.QUERY_PROGRESS:
                self.emit(
                    "streaming.spark_query_progress",
                    query_id=event.query_id,
                    run_id=event.run_id,
                    name=event.name,
                    timestamp=event.timestamp,
                    **event.data,
                )

            elif event.event_type == StreamingEventType.QUERY_TERMINATED:
                self.emit(
                    "streaming.spark_query_terminated",
                    query_id=event.query_id,
                    run_id=event.run_id,
                    timestamp=event.timestamp,
                    **event.data,
                )

            else:
                self.logger.warning(f"Unknown event type: {event.event_type}")

        except Exception as e:
            self.logger.error(
                f"Error emitting signal for {event.event_type.value}: {e}",
                include_traceback=True,
            )
