"""Integrated runtime orchestration for Kindling streaming plans.

This service owns the lifecycle around streaming execution:
- register and attach the Spark streaming listener
- start health monitoring and recovery services
- start streaming pipes through StreamingQueryManager
- await termination for one or more running queries
- stop queries and runtime components in a coordinated order
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from injector import inject
from kindling.execution_strategy import ExecutionPlan
from kindling.generation_executor import (
    ErrorStrategy,
    ExecutionResult,
    GenerationResult,
    PipeResult,
)
from kindling.injection import GlobalInjector
from kindling.pipe_streaming import PipeStreamStarter
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.streaming_health_monitor import StreamingHealthMonitor
from kindling.streaming_listener import KindlingStreamingListener
from kindling.streaming_query_manager import (
    StreamingQueryInfo,
    StreamingQueryManager,
    StreamingQueryState,
)
from kindling.streaming_recovery_manager import StreamingRecoveryManager


@dataclass
class StreamingOrchestratorStatus:
    """Snapshot of orchestrator runtime state."""

    run_id: Optional[str] = None
    is_running: bool = False
    listener_registered: bool = False
    query_ids_by_pipe: Dict[str, str] = field(default_factory=dict)
    query_states: Dict[str, str] = field(default_factory=dict)
    active_query_count: int = 0


@GlobalInjector.singleton_autobind()
class StreamingOrchestrator(SignalEmitter):
    """High-level runtime supervisor for Kindling streaming plans.

    The orchestrator starts monitoring services before any queries are launched,
    starts streaming pipes through StreamingQueryManager so they can be restarted,
    and optionally blocks until all managed queries terminate.
    """

    EMITS = [
        "streaming.orchestrator_started",
        "streaming.orchestrator_completed",
        "streaming.orchestrator_failed",
        "streaming.orchestrator_stopped",
    ]

    @inject
    def __init__(
        self,
        pipe_stream_starter: PipeStreamStarter,
        query_manager: StreamingQueryManager,
        streaming_listener: KindlingStreamingListener,
        health_monitor: StreamingHealthMonitor,
        recovery_manager: StreamingRecoveryManager,
        logger_provider: PythonLoggerProvider,
        signal_provider: SignalProvider = None,
    ):
        self.pipe_stream_starter = pipe_stream_starter
        self.query_manager = query_manager
        self.streaming_listener = streaming_listener
        self.health_monitor = health_monitor
        self.recovery_manager = recovery_manager
        self.logger = logger_provider.get_logger("StreamingOrchestrator")
        self._init_signal_emitter(signal_provider)

        self._current_result: Optional[ExecutionResult] = None
        self._query_ids_by_pipe: Dict[str, str] = {}
        self._listener_registered = False

    def start(
        self,
        plan: ExecutionPlan,
        streaming_options: Optional[Dict[str, Any]] = None,
        error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST,
    ) -> ExecutionResult:
        """Start all streaming pipes in a plan and return immediately."""
        if self._current_result is not None and self.get_status().active_query_count > 0:
            raise RuntimeError("StreamingOrchestrator already has active queries")

        plan.validate()
        self.start_runtime()

        run_id = str(uuid.uuid4())
        result = ExecutionResult(plan=plan, run_id=run_id)
        self._current_result = result
        self._query_ids_by_pipe = {}
        start_time = time.time()

        try:
            for generation in plan.generations:
                generation_start = time.time()
                generation_result = GenerationResult(generation_number=generation.number)

                for pipe_id in generation.pipe_ids:
                    pipe_result = self._start_pipe(pipe_id, run_id, streaming_options)
                    generation_result.pipe_results.append(pipe_result)

                    if pipe_result.status == "failed" and error_strategy == ErrorStrategy.FAIL_FAST:
                        raise RuntimeError(
                            f"Failed to start streaming pipe '{pipe_id}': {pipe_result.error}"
                        )

                    if pipe_result.streaming_query is not None:
                        result.streaming_queries[pipe_id] = pipe_result.streaming_query

                generation_result.duration_seconds = time.time() - generation_start
                result.generation_results.append(generation_result)

            result.duration_seconds = time.time() - start_time

            self.emit(
                "streaming.orchestrator_started",
                run_id=run_id,
                pipe_ids=list(plan.pipe_ids),
                active_query_count=len(result.streaming_queries),
            )
            return result

        except Exception as exc:
            result.duration_seconds = time.time() - start_time
            self.emit(
                "streaming.orchestrator_failed",
                run_id=run_id,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            raise

    def run(
        self,
        plan: ExecutionPlan,
        streaming_options: Optional[Dict[str, Any]] = None,
        error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST,
        timeout: Optional[float] = None,
        poll_interval: float = 1.0,
    ) -> ExecutionResult:
        """Start streaming plan execution and block until termination."""
        result = self.start(
            plan=plan,
            streaming_options=streaming_options,
            error_strategy=error_strategy,
        )
        try:
            return self.await_termination(timeout=timeout, poll_interval=poll_interval)
        finally:
            self._shutdown_runtime_components()

    def await_termination(
        self,
        timeout: Optional[float] = None,
        poll_interval: float = 1.0,
    ) -> ExecutionResult:
        """Wait until all managed streaming queries terminate."""
        if self._current_result is None:
            raise RuntimeError("StreamingOrchestrator has no active run")

        deadline = None if timeout is None else (time.time() + timeout)

        while True:
            active_count = 0
            failed_queries = []

            for pipe_id, query_id in self._query_ids_by_pipe.items():
                query_info = self.query_manager.get_query_status(query_id)
                query = query_info.spark_query

                if query is None:
                    continue

                if getattr(query, "isActive", False):
                    active_count += 1
                    continue

                if self._is_query_recovering(query_id, query_info):
                    active_count += 1
                    continue

                if query_info.state in {
                    StreamingQueryState.ACTIVE,
                    StreamingQueryState.STARTING,
                    StreamingQueryState.RESTARTING,
                }:
                    error = self._get_query_exception(query)
                    query_info.stop_time = datetime.now()
                    if error is not None:
                        query_info.state = StreamingQueryState.FAILED
                        query_info.last_error = str(error)
                        failed_queries.append((pipe_id, str(error)))
                    else:
                        query_info.state = StreamingQueryState.STOPPED

            if active_count == 0:
                if failed_queries:
                    message = ", ".join(f"{pipe_id}: {error}" for pipe_id, error in failed_queries)
                    self.emit(
                        "streaming.orchestrator_failed",
                        run_id=self._current_result.run_id,
                        error=message,
                        error_type="StreamingQueryFailure",
                    )
                    raise RuntimeError(f"Streaming queries failed: {message}")

                self.emit(
                    "streaming.orchestrator_completed",
                    run_id=self._current_result.run_id,
                    active_query_count=0,
                )
                return self._current_result

            if deadline is not None and time.time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for streaming queries to terminate after {timeout}s"
                )

            time.sleep(poll_interval)

    def stop(self, await_termination: bool = True) -> None:
        """Stop all managed streaming queries and monitoring components."""
        for query_id in reversed(list(self._query_ids_by_pipe.values())):
            try:
                query_info = self.query_manager.get_query_status(query_id)
            except Exception:
                continue

            if query_info.spark_query is None:
                continue

            if getattr(query_info.spark_query, "isActive", False):
                self.query_manager.stop_query(query_id, await_termination=await_termination)

        self.stop_runtime()

        self.emit(
            "streaming.orchestrator_stopped",
            run_id=self._current_result.run_id if self._current_result else None,
            stopped_query_count=len(self._query_ids_by_pipe),
        )

    def get_status(self) -> StreamingOrchestratorStatus:
        """Return a snapshot of the orchestrator's current runtime state."""
        query_states = {}
        active_query_count = 0

        for pipe_id, query_id in self._query_ids_by_pipe.items():
            try:
                info = self.query_manager.get_query_status(query_id)
            except Exception:
                continue

            query_states[pipe_id] = info.state.value
            if info.spark_query is not None and getattr(info.spark_query, "isActive", False):
                active_query_count += 1

        return StreamingOrchestratorStatus(
            run_id=self._current_result.run_id if self._current_result else None,
            is_running=self._current_result is not None,
            listener_registered=self._listener_registered,
            query_ids_by_pipe=dict(self._query_ids_by_pipe),
            query_states=query_states,
            active_query_count=active_query_count,
        )

    def start_runtime(self) -> None:
        """Start listener, monitoring, and recovery services without starting queries."""
        self._ensure_runtime_started()

    def stop_runtime(self) -> None:
        """Stop listener, monitoring, and recovery services."""
        self._shutdown_runtime_components()

    def _start_pipe(
        self,
        pipe_id: str,
        run_id: str,
        streaming_options: Optional[Dict[str, Any]],
    ) -> PipeResult:
        pipe_options = self._resolve_pipe_options(pipe_id, streaming_options)
        query_id = f"{run_id}:{pipe_id}"

        def builder(_spark, _config, pipe_id=pipe_id, pipe_options=pipe_options):
            return self.pipe_stream_starter.start_pipe_stream(pipe_id, pipe_options)

        try:
            self.query_manager.register_query(
                query_id,
                builder,
                {"query_name": pipe_id, "pipe_id": pipe_id},
            )
            query_info = self.query_manager.start_query(query_id)
            self._query_ids_by_pipe[pipe_id] = query_id
            return PipeResult(
                pipe_id=pipe_id,
                status="success",
                streaming_query=query_info.spark_query,
            )
        except Exception as exc:
            self.logger.error(
                f"Failed to start orchestrated streaming pipe '{pipe_id}': {exc}",
                include_traceback=True,
            )
            return PipeResult(
                pipe_id=pipe_id,
                status="failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )

    def _ensure_runtime_started(self) -> None:
        spark = getattr(self.query_manager, "spark", None)
        if spark is None:
            raise RuntimeError("StreamingQueryManager does not expose a Spark session")

        self.streaming_listener.start()

        if not self._listener_registered:
            spark.streams.addListener(self.streaming_listener)
            self._listener_registered = True

        self.health_monitor.start()
        self.recovery_manager.start()

    def _shutdown_runtime_components(self) -> None:
        spark = getattr(self.query_manager, "spark", None)

        self.recovery_manager.stop()
        self.health_monitor.stop()

        if self._listener_registered and spark is not None:
            remove_listener = getattr(spark.streams, "removeListener", None)
            if callable(remove_listener):
                try:
                    remove_listener(self.streaming_listener)
                except Exception:
                    pass
            self._listener_registered = False

        self.streaming_listener.stop()
        self._current_result = None
        self._query_ids_by_pipe = {}

    @staticmethod
    def _resolve_pipe_options(
        pipe_id: str,
        streaming_options: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        pipe_options: Dict[str, Any] = {}
        if streaming_options:
            if pipe_id in streaming_options:
                pipe_options = streaming_options[pipe_id]
            elif not any(isinstance(v, dict) for v in streaming_options.values()):
                pipe_options = streaming_options
        return dict(pipe_options or {})

    def _is_query_recovering(self, query_id: str, query_info: StreamingQueryInfo) -> bool:
        try:
            recovery_states = self.recovery_manager.get_all_recovery_states()
        except Exception:
            return False

        spark_query_id = query_info.spark_query_id
        for state in recovery_states.values():
            state_query_id = getattr(state, "query_id", None)
            resolved = self.query_manager.resolve_registered_query_id(str(state_query_id))
            if resolved == query_id:
                return True
            if spark_query_id and str(state_query_id) == spark_query_id:
                return True
        return False

    @staticmethod
    def _get_query_exception(query: Any) -> Optional[Exception]:
        try:
            if hasattr(query, "exception"):
                return query.exception()
        except Exception:
            return None
        return None
