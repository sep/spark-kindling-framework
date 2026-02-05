"""
Kindling Streaming Query Manager

Provides lifecycle management for PySpark streaming queries with registration,
monitoring, and state tracking.

Key Components:
- StreamingQueryManager: Central registry for streaming queries
- StreamingQueryInfo: Query metadata and state
- StreamingQueryState: Enum for query states

Related:
- Issue #18: https://github.com/sep/spark-kindling-framework/issues/18
- Feature #21: Queue-based event processing (KindlingStreamingListener)

Architecture:
The manager provides a high-level API for managing streaming queries:
1. Register query builder functions with configuration
2. Start/stop/restart queries with lifecycle tracking
3. Monitor query state and metrics
4. Emit signals for query lifecycle events

Pattern:
1. Register: manager.register_query(query_id, builder_fn, config)
2. Start: query_info = manager.start_query(query_id)
3. Monitor: status = manager.get_query_status(query_id)
4. Stop: manager.stop_query(query_id)

Example:
    >>> from kindling.streaming_query_manager import StreamingQueryManager
    >>>
    >>> def build_query(spark, config):
    ...     return spark.readStream.format("delta").load(config["path"])
    >>>
    >>> manager.register_query("my_query", build_query, {"path": "/data"})
    >>> query_info = manager.start_query("my_query")
    >>> print(f"Query {query_info.query_id} started at {query_info.start_time}")
"""

import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from injector import inject
from kindling.injection import GlobalInjector
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

# =============================================================================
# Data Structures
# =============================================================================


class StreamingQueryState(Enum):
    """States for streaming queries."""

    REGISTERED = "registered"  # Registered but not started
    STARTING = "starting"  # Start initiated, not yet active
    ACTIVE = "active"  # Running normally
    STOPPING = "stopping"  # Stop initiated, not yet terminated
    STOPPED = "stopped"  # Cleanly stopped
    FAILED = "failed"  # Terminated with error
    RESTARTING = "restarting"  # Restart in progress


@dataclass
class StreamingQueryInfo:
    """
    Container for streaming query metadata and state.

    Attributes:
        query_id: Unique identifier for the query
        query_name: Human-readable name
        state: Current query state
        spark_query: Reference to underlying PySpark StreamingQuery
        checkpoint_path: Location of query checkpoint
        start_time: When query was started
        stop_time: When query was stopped (if applicable)
        restart_count: Number of times query has been restarted
        last_error: Last error message (if any)
        metrics: Query metrics from progress reports
        config: Query configuration
    """

    query_id: str
    query_name: str
    state: StreamingQueryState
    spark_query: Optional[StreamingQuery] = None
    checkpoint_path: Optional[str] = None
    start_time: Optional[datetime] = None
    stop_time: Optional[datetime] = None
    restart_count: int = 0
    last_error: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_active(self) -> bool:
        """Check if query is actively running."""
        return self.state == StreamingQueryState.ACTIVE and self.spark_query is not None

    @property
    def spark_query_id(self) -> Optional[str]:
        """Get underlying Spark query ID."""
        return str(self.spark_query.id) if self.spark_query else None

    @property
    def spark_run_id(self) -> Optional[str]:
        """Get underlying Spark run ID."""
        return str(self.spark_query.runId) if self.spark_query else None


# =============================================================================
# Streaming Query Manager
# =============================================================================


@GlobalInjector.singleton_autobind()
class StreamingQueryManager(SignalEmitter):
    """
    Central registry and lifecycle manager for streaming queries.

    Manages registration, lifecycle (start/stop/restart), and monitoring
    of PySpark streaming queries.

    Signals Emitted:
        - streaming.query_registered: When query registered
        - streaming.query_started: When query starts
        - streaming.query_stopped: When query stops cleanly
        - streaming.query_failed: When query fails
        - streaming.query_restarted: When query restarts

    Example:
        >>> manager = StreamingQueryManager(signal_provider, logger_provider, spark)
        >>>
        >>> def build_query(spark, config):
        ...     return spark.readStream.format("delta").load(config["path"])
        >>>
        >>> manager.register_query("my_query", build_query, {"path": "/data"})
        >>> query_info = manager.start_query("my_query")
        >>> queries = manager.list_active_queries()

    Thread Safety:
        This class uses locking for thread-safe query state management.
    """

    EMITS = [
        "streaming.query_registered",
        "streaming.query_started",
        "streaming.query_stopped",
        "streaming.query_failed",
        "streaming.query_restarted",
    ]

    @inject
    def __init__(
        self,
        signal_provider: SignalProvider,
        logger_provider: SparkLoggerProvider,
    ):
        """
        Initialize the streaming query manager.

        Args:
            signal_provider: Provider for creating/emitting signals
            logger_provider: Provider for logging
        """
        self._init_signal_emitter(signal_provider)

        self.logger = logger_provider.get_logger("StreamingQueryManager")
        self.spark = get_or_create_spark_session()

        # Query registry: query_id -> StreamingQueryInfo
        self._queries: Dict[str, StreamingQueryInfo] = {}

        # Query builder registry: query_id -> (builder_fn, config)
        self._builders: Dict[str, tuple] = {}

        # Lock for thread-safe state management
        self._lock = threading.RLock()

        self.logger.info("StreamingQueryManager initialized")

    def register_query(
        self,
        query_id: str,
        builder_fn: Callable[[SparkSession, Dict[str, Any]], StreamingQuery],
        config: Optional[Dict[str, Any]] = None,
    ) -> StreamingQueryInfo:
        """
        Register a streaming query builder function.

        Args:
            query_id: Unique identifier for the query
            builder_fn: Function that builds and returns a StreamingQuery
                       Signature: (spark: SparkSession, config: Dict) -> StreamingQuery
            config: Optional configuration for the query

        Returns:
            StreamingQueryInfo for the registered query

        Raises:
            ValueError: If query_id already registered
        """
        with self._lock:
            if query_id in self._queries:
                raise ValueError(f"Query '{query_id}' already registered")

            config = config or {}
            query_name = config.get("query_name", query_id)
            checkpoint_path = config.get("checkpoint_path")

            # Create query info in REGISTERED state
            query_info = StreamingQueryInfo(
                query_id=query_id,
                query_name=query_name,
                state=StreamingQueryState.REGISTERED,
                checkpoint_path=checkpoint_path,
                config=config,
            )

            self._queries[query_id] = query_info
            self._builders[query_id] = (builder_fn, config)

            self.logger.info(f"Registered query '{query_id}' (name: {query_name})")

            self.emit(
                "streaming.query_registered",
                query_id=query_id,
                query_name=query_name,
                config=config,
            )

            return query_info

    def start_query(self, query_id: str) -> StreamingQueryInfo:
        """
        Start a registered streaming query.

        Args:
            query_id: ID of query to start

        Returns:
            StreamingQueryInfo with updated state

        Raises:
            ValueError: If query not registered or already active
            RuntimeError: If query fails to start
        """
        with self._lock:
            if query_id not in self._queries:
                raise ValueError(f"Query '{query_id}' not registered")

            query_info = self._queries[query_id]

            if query_info.is_active:
                raise ValueError(f"Query '{query_id}' already active")

            # Update state to STARTING
            query_info.state = StreamingQueryState.STARTING
            query_info.start_time = datetime.now()

            self.logger.info(f"Starting query '{query_id}'...")

        try:
            # Get builder function
            builder_fn, config = self._builders[query_id]

            # Execute builder to create StreamingQuery
            spark_query = builder_fn(self.spark, config)

            # Validate return value (duck typing for testability)
            if not hasattr(spark_query, "id") or not hasattr(spark_query, "runId"):
                raise RuntimeError(
                    f"Builder function for '{query_id}' did not return a valid StreamingQuery"
                )

            # Update query info with active query
            with self._lock:
                query_info.spark_query = spark_query
                query_info.state = StreamingQueryState.ACTIVE
                query_info.last_error = None

            self.logger.info(f"Query '{query_id}' started (spark_id: {query_info.spark_query_id})")

            self.emit(
                "streaming.query_started",
                query_id=query_id,
                query_name=query_info.query_name,
                spark_query_id=query_info.spark_query_id,
                spark_run_id=query_info.spark_run_id,
                start_time=query_info.start_time,
            )

            return query_info

        except Exception as e:
            # Mark as failed
            with self._lock:
                query_info.state = StreamingQueryState.FAILED
                query_info.last_error = str(e)

            self.logger.error(f"Failed to start query '{query_id}': {e}", include_traceback=True)

            self.emit(
                "streaming.query_failed",
                query_id=query_id,
                query_name=query_info.query_name,
                error=str(e),
            )

            raise RuntimeError(f"Failed to start query '{query_id}': {e}") from e

    def stop_query(self, query_id: str, await_termination: bool = True) -> StreamingQueryInfo:
        """
        Stop a running streaming query.

        Args:
            query_id: ID of query to stop
            await_termination: If True, wait for query to terminate

        Returns:
            StreamingQueryInfo with updated state

        Raises:
            ValueError: If query not registered or not active
        """
        with self._lock:
            if query_id not in self._queries:
                raise ValueError(f"Query '{query_id}' not registered")

            query_info = self._queries[query_id]

            if not query_info.is_active:
                raise ValueError(f"Query '{query_id}' not active")

            query_info.state = StreamingQueryState.STOPPING
            spark_query = query_info.spark_query

        try:
            self.logger.info(f"Stopping query '{query_id}'...")

            # Stop the Spark query
            spark_query.stop()

            if await_termination:
                spark_query.awaitTermination()

            # Update state
            with self._lock:
                query_info.state = StreamingQueryState.STOPPED
                query_info.stop_time = datetime.now()

            self.logger.info(f"Query '{query_id}' stopped")

            self.emit(
                "streaming.query_stopped",
                query_id=query_id,
                query_name=query_info.query_name,
                stop_time=query_info.stop_time,
                restart_count=query_info.restart_count,
            )

            return query_info

        except Exception as e:
            with self._lock:
                query_info.state = StreamingQueryState.FAILED
                query_info.last_error = str(e)

            self.logger.error(f"Error stopping query '{query_id}': {e}", include_traceback=True)

            self.emit(
                "streaming.query_failed",
                query_id=query_id,
                query_name=query_info.query_name,
                error=str(e),
            )

            raise RuntimeError(f"Failed to stop query '{query_id}': {e}") from e

    def restart_query(self, query_id: str) -> StreamingQueryInfo:
        """
        Restart a streaming query (stop then start).

        Args:
            query_id: ID of query to restart

        Returns:
            StreamingQueryInfo with updated state

        Raises:
            ValueError: If query not registered
        """
        with self._lock:
            if query_id not in self._queries:
                raise ValueError(f"Query '{query_id}' not registered")

            query_info = self._queries[query_id]
            query_info.state = StreamingQueryState.RESTARTING

        try:
            self.logger.info(f"Restarting query '{query_id}'...")

            # Stop if active
            if query_info.is_active:
                self.stop_query(query_id, await_termination=True)

            # Increment restart count
            with self._lock:
                query_info.restart_count += 1

            # Start again
            query_info = self.start_query(query_id)

            self.logger.info(
                f"Query '{query_id}' restarted (restart_count: {query_info.restart_count})"
            )

            self.emit(
                "streaming.query_restarted",
                query_id=query_id,
                query_name=query_info.query_name,
                restart_count=query_info.restart_count,
            )

            return query_info

        except Exception as e:
            self.logger.error(f"Failed to restart query '{query_id}': {e}", include_traceback=True)
            raise

    def get_query_status(self, query_id: str) -> StreamingQueryInfo:
        """
        Get current status of a query.

        Args:
            query_id: ID of query

        Returns:
            StreamingQueryInfo with current state

        Raises:
            ValueError: If query not registered
        """
        with self._lock:
            if query_id not in self._queries:
                raise ValueError(f"Query '{query_id}' not registered")

            query_info = self._queries[query_id]

            # Update metrics from Spark query if active
            if query_info.is_active:
                try:
                    progress = query_info.spark_query.lastProgress
                    if progress:
                        query_info.metrics = {
                            "batch_id": progress.get("batchId"),
                            "num_input_rows": progress.get("numInputRows"),
                            "input_rows_per_second": progress.get("inputRowsPerSecond"),
                            "process_rate": progress.get("processedRowsPerSecond"),
                        }
                except Exception as e:
                    self.logger.warning(f"Could not fetch metrics for query '{query_id}': {e}")

            return query_info

    def list_active_queries(self) -> List[StreamingQueryInfo]:
        """
        List all active streaming queries.

        Returns:
            List of StreamingQueryInfo for active queries
        """
        with self._lock:
            return [query_info for query_info in self._queries.values() if query_info.is_active]

    def list_all_queries(self) -> List[StreamingQueryInfo]:
        """
        List all registered queries regardless of state.

        Returns:
            List of all StreamingQueryInfo objects
        """
        with self._lock:
            return list(self._queries.values())

    def get_query_count(self) -> Dict[str, int]:
        """
        Get count of queries by state.

        Returns:
            Dictionary mapping state to count
        """
        with self._lock:
            counts = {}
            for query_info in self._queries.values():
                state_name = query_info.state.value
                counts[state_name] = counts.get(state_name, 0) + 1
            return counts
