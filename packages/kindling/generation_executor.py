"""Generation Executor for the Unified DAG Orchestrator.

Executes pipes within an ExecutionPlan, generation by generation,
with support for both batch and streaming modes.

Batch mode:
    Executes pipes using DataPipesExecuter (read → transform → persist).
    Pipes within a generation can run in parallel (ThreadPoolExecutor).

Streaming mode:
    Starts pipes as streaming processors using entity providers directly.
    Reads input via StreamableEntityProvider, transforms via pipe, writes via
    append_as_stream. Sinks start first (reverse topological order from
    StreamingExecutionStrategy). Monitors streaming queries until completion
    or timeout.

See: GitHub Issue #24 - Generation Executor
Part of: Capability #15 - Unified DAG Orchestrator
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from threading import Lock
from typing import Any, Callable, Dict, List, Optional

from injector import inject
from kindling.cache_optimizer import CacheOptimizer
from kindling.data_entities import DataEntityRegistry, EntityPathLocator
from kindling.data_pipes import (
    DataPipesExecution,
    DataPipesRegistry,
    EntityReadPersistStrategy,
    PipeMetadata,
)
from kindling.entity_provider import (
    StreamableEntityProvider,
    StreamWritableEntityProvider,
    is_stream_writable,
    is_streamable,
)
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.execution_strategy import (
    ExecutionPlan,
    ExecutionPlanGenerator,
    Generation,
    StreamingExecutionStrategy,
)
from kindling.injection import GlobalInjector
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_trace import SparkTraceProvider


class ErrorStrategy(Enum):
    """How to handle errors during execution."""

    FAIL_FAST = "fail_fast"  # Stop immediately on first error
    CONTINUE = "continue"  # Run all pipes, collect errors


@dataclass
class PipeResult:
    """Result of executing a single pipe."""

    pipe_id: str
    status: str  # "success", "failed", "skipped"
    duration_seconds: float = 0.0
    error: Optional[str] = None
    error_type: Optional[str] = None
    streaming_query: Optional[Any] = None  # For streaming mode


@dataclass
class GenerationResult:
    """Result of executing a generation."""

    generation_number: int
    pipe_results: List[PipeResult] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.pipe_results if r.status == "success")

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.pipe_results if r.status == "failed")

    @property
    def skipped_count(self) -> int:
        return sum(1 for r in self.pipe_results if r.status == "skipped")

    @property
    def all_succeeded(self) -> bool:
        return all(r.status in ("success", "skipped") for r in self.pipe_results)

    @property
    def failed_pipes(self) -> List[str]:
        return [r.pipe_id for r in self.pipe_results if r.status == "failed"]


@dataclass
class ExecutionResult:
    """Result of executing an entire plan."""

    plan: ExecutionPlan
    generation_results: List[GenerationResult] = field(default_factory=list)
    duration_seconds: float = 0.0
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    streaming_queries: Dict[str, Any] = field(default_factory=dict)
    cache_metrics: Dict[str, Any] = field(default_factory=dict)

    @property
    def success_count(self) -> int:
        return sum(g.success_count for g in self.generation_results)

    @property
    def failed_count(self) -> int:
        return sum(g.failed_count for g in self.generation_results)

    @property
    def skipped_count(self) -> int:
        return sum(g.skipped_count for g in self.generation_results)

    @property
    def all_succeeded(self) -> bool:
        return all(g.all_succeeded for g in self.generation_results)

    @property
    def failed_pipes(self) -> List[str]:
        pipes = []
        for g in self.generation_results:
            pipes.extend(g.failed_pipes)
        return pipes

    def get_summary(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "strategy": self.plan.strategy,
            "total_pipes": self.plan.total_pipes(),
            "generations_executed": len(self.generation_results),
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "skipped_count": self.skipped_count,
            "duration_seconds": round(self.duration_seconds, 2),
            "all_succeeded": self.all_succeeded,
            "failed_pipes": self.failed_pipes,
            "cache_metrics": self.cache_metrics,
        }


@GlobalInjector.singleton_autobind()
class GenerationExecutor(SignalEmitter):
    """Executes pipes generation by generation from an ExecutionPlan.

    Supports two execution modes:

    **Batch mode** (default):
        Uses EntityReadPersistStrategy to read input entities, transform
        via pipe.execute(), and persist results. Pipes within a generation
        can run sequentially or in parallel.

    **Streaming mode**:
        Uses PipeStreamOrchestrator to start each pipe as a streaming
        processor. Streaming plans start sinks first (reverse topo order).
        Returns streaming queries for monitoring.

    Signals emitted:
        - orchestrator.execution_started
        - orchestrator.execution_completed
        - orchestrator.execution_failed
        - orchestrator.generation_started
        - orchestrator.generation_completed
        - orchestrator.pipe_started
        - orchestrator.pipe_completed
        - orchestrator.pipe_failed
        - orchestrator.pipe_skipped
        - orchestrator.cache_recommended
        - orchestrator.cache_hit
        - orchestrator.cache_miss
    """

    EMITS = [
        "orchestrator.execution_started",
        "orchestrator.execution_completed",
        "orchestrator.execution_failed",
        "orchestrator.generation_started",
        "orchestrator.generation_completed",
        "orchestrator.pipe_started",
        "orchestrator.pipe_completed",
        "orchestrator.pipe_failed",
        "orchestrator.pipe_skipped",
        "orchestrator.cache_recommended",
        "orchestrator.cache_hit",
        "orchestrator.cache_miss",
    ]

    @inject
    def __init__(
        self,
        pipes_registry: DataPipesRegistry,
        entity_registry: DataEntityRegistry,
        provider_registry: EntityProviderRegistry,
        entity_path_locator: EntityPathLocator,
        config_service: ConfigService,
        persist_strategy: EntityReadPersistStrategy,
        trace_provider: SparkTraceProvider,
        logger_provider: PythonLoggerProvider,
        signal_provider: SignalProvider = None,
    ):
        self.pipes_registry = pipes_registry
        self.entity_registry = entity_registry
        self.provider_registry = provider_registry
        self.entity_path_locator = entity_path_locator
        self.config_service = config_service
        self.persist_strategy = persist_strategy
        self.tp = trace_provider
        self.logger = logger_provider.get_logger("generation_executor")
        self._init_signal_emitter(signal_provider)
        self._cache_optimizer = CacheOptimizer()
        self._cache_lock = Lock()
        self._reset_cache_state()

    def execute(
        self,
        plan: ExecutionPlan,
        parallel: bool = False,
        max_workers: int = 4,
        error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST,
        pipe_timeout: Optional[float] = None,
        streaming_options: Optional[Dict[str, Any]] = None,
        auto_cache: bool = False,
    ) -> ExecutionResult:
        """Execute an ExecutionPlan generation by generation.

        For batch plans (strategy="batch" or "config_based" with batch mode):
            Runs each pipe via read → transform → persist pattern.

        For streaming plans (strategy="streaming" or "config_based" with streaming mode):
            Starts each pipe as a streaming processor and returns queries.

        Args:
            plan: The execution plan to execute
            parallel: Whether to run pipes within a generation in parallel
            max_workers: Max parallel workers (only used if parallel=True)
            error_strategy: How to handle errors (FAIL_FAST or CONTINUE)
            pipe_timeout: Per-pipe timeout in seconds (None = no timeout)
            streaming_options: Options passed to stream orchestrator per pipe.
                Can be a dict of {pipe_id: options_dict} or a single dict
                applied to all pipes.
            auto_cache: If True, call `persist()/cache()` on recommended shared
                entities when first read in batch mode.

        Returns:
            ExecutionResult with per-pipe and per-generation results
        """
        run_id = str(uuid.uuid4())
        start_time = time.time()

        is_streaming = self._is_streaming_plan(plan)
        mode = "streaming" if is_streaming else "batch"

        self.logger.info(
            f"Executing plan: {plan.strategy} mode={mode} "
            f"pipes={plan.total_pipes()} generations={plan.total_generations()} "
            f"parallel={parallel} error_strategy={error_strategy.value} "
            f"auto_cache={auto_cache} run_id={run_id}"
        )

        self.emit(
            "orchestrator.execution_started",
            run_id=run_id,
            strategy=plan.strategy,
            mode=mode,
            pipe_count=plan.total_pipes(),
            generation_count=plan.total_generations(),
            parallel=parallel,
        )

        result = ExecutionResult(plan=plan, run_id=run_id)
        self._initialize_cache_state(plan=plan, is_streaming=is_streaming, auto_cache=auto_cache)
        self._emit_cache_recommendations(plan=plan, run_id=run_id)

        try:
            with self.tp.span(
                component="generation_executor",
                operation=f"execute_{mode}",
            ):
                for generation in plan.generations:
                    gen_result = self._execute_generation(
                        generation=generation,
                        run_id=run_id,
                        is_streaming=is_streaming,
                        parallel=parallel,
                        max_workers=max_workers,
                        error_strategy=error_strategy,
                        pipe_timeout=pipe_timeout,
                        streaming_options=streaming_options,
                    )
                    result.generation_results.append(gen_result)

                    # Fail fast if generation had failures
                    if not gen_result.all_succeeded and error_strategy == ErrorStrategy.FAIL_FAST:
                        self.logger.error(
                            f"Generation {generation.number} failed "
                            f"({gen_result.failed_pipes}), stopping execution"
                        )
                        break

            result.duration_seconds = time.time() - start_time

            # Collect streaming queries
            if is_streaming:
                for gen_result in result.generation_results:
                    for pipe_result in gen_result.pipe_results:
                        if pipe_result.streaming_query is not None:
                            result.streaming_queries[pipe_result.pipe_id] = (
                                pipe_result.streaming_query
                            )

            result.cache_metrics = self._build_cache_metrics()

            self.logger.info(
                f"Execution completed: {result.success_count} succeeded, "
                f"{result.failed_count} failed, {result.skipped_count} skipped "
                f"in {result.duration_seconds:.2f}s"
            )

            self.emit(
                "orchestrator.execution_completed",
                run_id=run_id,
                success_count=result.success_count,
                failed_count=result.failed_count,
                skipped_count=result.skipped_count,
                duration_seconds=result.duration_seconds,
                all_succeeded=result.all_succeeded,
                cache_metrics=result.cache_metrics,
            )

            return result

        except Exception as e:
            result.duration_seconds = time.time() - start_time
            result.cache_metrics = self._build_cache_metrics()

            self.logger.error(
                f"Execution failed after {result.duration_seconds:.2f}s: {e}",
                include_traceback=True,
            )

            self.emit(
                "orchestrator.execution_failed",
                run_id=run_id,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=result.duration_seconds,
                failed_pipes=result.failed_pipes,
                cache_metrics=result.cache_metrics,
            )

            raise
        finally:
            self._cleanup_cached_frames()
            self._reset_cache_state()

    def execute_batch(
        self,
        plan: ExecutionPlan,
        parallel: bool = False,
        max_workers: int = 4,
        error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST,
        pipe_timeout: Optional[float] = None,
        auto_cache: bool = False,
    ) -> ExecutionResult:
        """Execute a batch plan (convenience method).

        Shorthand for execute() with batch-specific defaults.
        """
        return self.execute(
            plan=plan,
            parallel=parallel,
            max_workers=max_workers,
            error_strategy=error_strategy,
            pipe_timeout=pipe_timeout,
            auto_cache=auto_cache,
        )

    def execute_streaming(
        self,
        plan: ExecutionPlan,
        streaming_options: Optional[Dict[str, Any]] = None,
        error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST,
    ) -> ExecutionResult:
        """Execute a streaming plan (convenience method).

        Starts pipes as streaming processors in plan order (sinks first).
        Returns ExecutionResult with streaming_queries dict for monitoring.

        Args:
            plan: Streaming execution plan (reverse topo order)
            streaming_options: Options for stream orchestrator.
                Can be {pipe_id: {options}} or single dict for all pipes.
            error_strategy: How to handle errors
        """
        return self.execute(
            plan=plan,
            parallel=False,  # Streaming starts sequentially (order matters)
            error_strategy=error_strategy,
            streaming_options=streaming_options,
        )

    # ---- Internal Methods ----

    def _is_streaming_plan(self, plan: ExecutionPlan) -> bool:
        """Determine if plan should use streaming execution."""
        strategy = plan.strategy.lower()
        if strategy == "streaming":
            return True
        if strategy == "config_based":
            detected = plan.metadata.get("detected_mode", "batch")
            return detected == "streaming"
        return False

    def _reset_cache_state(self):
        """Reset per-run cache tracking state."""
        self._cache_enabled = False
        self._auto_cache_enabled = False
        self._cache_recommendations: Dict[str, str] = {}
        self._cached_entities: Dict[str, Any] = {}
        self._cache_hits_by_entity: Dict[str, int] = {}
        self._cache_misses_by_entity: Dict[str, int] = {}
        self._cache_hits = 0
        self._cache_misses = 0

    def _initialize_cache_state(
        self,
        plan: ExecutionPlan,
        is_streaming: bool,
        auto_cache: bool,
    ):
        """Initialize cache recommendations and runtime cache state."""
        self._reset_cache_state()

        if is_streaming:
            return

        recommendations = plan.metadata.get("cache_recommendations")
        if isinstance(recommendations, dict):
            self._cache_recommendations = {
                str(entity_id): str(level)
                for entity_id, level in recommendations.items()
                if level
            }
        else:
            self._cache_recommendations = self._cache_optimizer.as_level_map(
                self._cache_optimizer.recommend(plan.graph, plan.pipe_ids)
            )

        self._cache_enabled = bool(self._cache_recommendations)
        self._auto_cache_enabled = auto_cache and self._cache_enabled

    def _emit_cache_recommendations(self, plan: ExecutionPlan, run_id: str):
        """Emit cache recommendation signals for observability."""
        if not self._cache_enabled:
            return

        for entity_id in sorted(self._cache_recommendations.keys()):
            consumers = sorted(plan.graph.entity_consumers.get(entity_id, []))
            self.emit(
                "orchestrator.cache_recommended",
                run_id=run_id,
                entity_id=entity_id,
                consumer_pipes=consumers,
                cache_level=self._cache_recommendations[entity_id],
                auto_cache=self._auto_cache_enabled,
            )

    def _build_cache_metrics(self) -> Dict[str, Any]:
        """Build cache metrics payload for result summary and signals."""
        if not self._cache_enabled:
            return {
                "enabled": False,
                "auto_cache": False,
                "recommended_count": 0,
                "recommended_entities": [],
                "hit_count": 0,
                "miss_count": 0,
                "hits_by_entity": {},
                "misses_by_entity": {},
            }

        return {
            "enabled": True,
            "auto_cache": self._auto_cache_enabled,
            "recommended_count": len(self._cache_recommendations),
            "recommended_entities": sorted(self._cache_recommendations.keys()),
            "hit_count": self._cache_hits,
            "miss_count": self._cache_misses,
            "hits_by_entity": dict(self._cache_hits_by_entity),
            "misses_by_entity": dict(self._cache_misses_by_entity),
        }

    def _cleanup_cached_frames(self):
        """Best-effort unpersist for auto-cached frames after execution."""
        if not self._auto_cache_enabled:
            return

        for frame in self._cached_entities.values():
            if frame is None or not hasattr(frame, "unpersist"):
                continue
            try:
                frame.unpersist(blocking=False)
            except TypeError:
                frame.unpersist()
            except Exception:
                self.logger.debug("Failed to unpersist cached entity frame")

    def _execute_generation(
        self,
        generation: Generation,
        run_id: str,
        is_streaming: bool,
        parallel: bool,
        max_workers: int,
        error_strategy: ErrorStrategy,
        pipe_timeout: Optional[float],
        streaming_options: Optional[Dict[str, Any]],
    ) -> GenerationResult:
        """Execute all pipes in a generation."""
        gen_start = time.time()

        self.logger.info(
            f"Starting generation {generation.number}: "
            f"{len(generation.pipe_ids)} pipes ({', '.join(generation.pipe_ids)})"
        )

        self.emit(
            "orchestrator.generation_started",
            run_id=run_id,
            generation=generation.number,
            pipe_ids=generation.pipe_ids,
            pipe_count=len(generation.pipe_ids),
        )

        gen_result = GenerationResult(generation_number=generation.number)

        if parallel and not is_streaming and len(generation.pipe_ids) > 1:
            # Parallel batch execution
            gen_result = self._execute_generation_parallel(
                generation, run_id, max_workers, error_strategy, pipe_timeout
            )
        else:
            # Sequential execution (batch or streaming)
            for pipe_id in generation.pipe_ids:
                pipe_result = self._execute_pipe(
                    pipe_id=pipe_id,
                    run_id=run_id,
                    is_streaming=is_streaming,
                    pipe_timeout=pipe_timeout,
                    streaming_options=streaming_options,
                )
                gen_result.pipe_results.append(pipe_result)

                if pipe_result.status == "failed" and error_strategy == ErrorStrategy.FAIL_FAST:
                    self.logger.error(
                        f"Pipe '{pipe_id}' failed in generation {generation.number}, "
                        f"stopping generation (fail_fast)"
                    )
                    break

        gen_result.duration_seconds = time.time() - gen_start

        self.logger.info(
            f"Generation {generation.number} completed: "
            f"{gen_result.success_count} succeeded, {gen_result.failed_count} failed "
            f"in {gen_result.duration_seconds:.2f}s"
        )

        self.emit(
            "orchestrator.generation_completed",
            run_id=run_id,
            generation=generation.number,
            success_count=gen_result.success_count,
            failed_count=gen_result.failed_count,
            skipped_count=gen_result.skipped_count,
            duration_seconds=gen_result.duration_seconds,
        )

        return gen_result

    def _execute_generation_parallel(
        self,
        generation: Generation,
        run_id: str,
        max_workers: int,
        error_strategy: ErrorStrategy,
        pipe_timeout: Optional[float],
    ) -> GenerationResult:
        """Execute pipes within a generation in parallel using ThreadPoolExecutor."""
        gen_result = GenerationResult(generation_number=generation.number)

        effective_workers = min(max_workers, len(generation.pipe_ids))
        self.logger.debug(
            f"Parallel execution: {len(generation.pipe_ids)} pipes with "
            f"{effective_workers} workers"
        )

        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_to_pipe = {
                executor.submit(
                    self._execute_pipe,
                    pipe_id=pipe_id,
                    run_id=run_id,
                    is_streaming=False,
                    pipe_timeout=pipe_timeout,
                    streaming_options=None,
                ): pipe_id
                for pipe_id in generation.pipe_ids
            }

            for future in as_completed(future_to_pipe):
                pipe_id = future_to_pipe[future]
                try:
                    pipe_result = future.result(timeout=pipe_timeout)
                    gen_result.pipe_results.append(pipe_result)
                except Exception as e:
                    gen_result.pipe_results.append(
                        PipeResult(
                            pipe_id=pipe_id,
                            status="failed",
                            error=str(e),
                            error_type=type(e).__name__,
                        )
                    )
                    if error_strategy == ErrorStrategy.FAIL_FAST:
                        # Cancel remaining futures
                        for f in future_to_pipe:
                            f.cancel()
                        break

        return gen_result

    def _execute_pipe(
        self,
        pipe_id: str,
        run_id: str,
        is_streaming: bool,
        pipe_timeout: Optional[float],
        streaming_options: Optional[Dict[str, Any]],
    ) -> PipeResult:
        """Execute a single pipe (batch or streaming)."""
        pipe_start = time.time()

        self.logger.debug(f"Executing pipe: {pipe_id} (streaming={is_streaming})")

        self.emit(
            "orchestrator.pipe_started",
            run_id=run_id,
            pipe_id=pipe_id,
            is_streaming=is_streaming,
        )

        try:
            pipe = self.pipes_registry.get_pipe_definition(pipe_id)
            if pipe is None:
                raise ValueError(f"Pipe '{pipe_id}' not found in registry")

            if is_streaming:
                result = self._execute_pipe_streaming(pipe, streaming_options)
            else:
                result = self._execute_pipe_batch(pipe, run_id)

            duration = time.time() - pipe_start
            result.duration_seconds = duration

            if result.status == "skipped":
                self.logger.debug(f"Pipe '{pipe_id}' skipped (no data)")
                self.emit(
                    "orchestrator.pipe_skipped",
                    run_id=run_id,
                    pipe_id=pipe_id,
                    duration_seconds=duration,
                )
            else:
                self.logger.info(f"Pipe '{pipe_id}' completed in {duration:.2f}s")
                self.emit(
                    "orchestrator.pipe_completed",
                    run_id=run_id,
                    pipe_id=pipe_id,
                    duration_seconds=duration,
                    is_streaming=is_streaming,
                )

            return result

        except Exception as e:
            duration = time.time() - pipe_start

            self.logger.error(
                f"Pipe '{pipe_id}' failed after {duration:.2f}s: {e}",
                include_traceback=True,
            )

            self.emit(
                "orchestrator.pipe_failed",
                run_id=run_id,
                pipe_id=pipe_id,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=duration,
            )

            return PipeResult(
                pipe_id=pipe_id,
                status="failed",
                duration_seconds=duration,
                error=str(e),
                error_type=type(e).__name__,
            )

    def _execute_pipe_batch(self, pipe: PipeMetadata, run_id: str) -> PipeResult:
        """Execute a pipe in batch mode (read → transform → persist)."""
        entity_reader = self.persist_strategy.create_pipe_entity_reader(pipe)
        activator = self.persist_strategy.create_pipe_persist_activator(pipe)

        # Read input entities
        input_entities = {}
        for i, entity_id in enumerate(pipe.input_entity_ids):
            is_first = i == 0
            key = entity_id.replace(".", "_")
            input_entities[key] = self._read_batch_input_entity(
                entity_id=entity_id,
                entity_reader=entity_reader,
                is_first=is_first,
                run_id=run_id,
            )

        # Check if first source has data (only if there are inputs)
        if input_entities:
            first_source = list(input_entities.values())[0]
            if first_source is None:
                return PipeResult(pipe_id=pipe.pipeid, status="skipped")

        # Transform
        processed_df = pipe.execute(**input_entities)

        # Persist
        activator(processed_df)

        return PipeResult(pipe_id=pipe.pipeid, status="success")

    def _read_batch_input_entity(
        self,
        entity_id: str,
        entity_reader: Callable[[Any, bool], Any],
        is_first: bool,
        run_id: str,
    ) -> Any:
        """Read an input entity with shared-entity cache tracking."""
        entity = self.entity_registry.get_entity_definition(entity_id)

        if not self._cache_enabled or entity_id not in self._cache_recommendations:
            return entity_reader(entity, is_first)

        with self._cache_lock:
            if entity_id in self._cached_entities:
                self._cache_hits += 1
                self._cache_hits_by_entity[entity_id] = (
                    self._cache_hits_by_entity.get(entity_id, 0) + 1
                )
                self.emit(
                    "orchestrator.cache_hit",
                    run_id=run_id,
                    entity_id=entity_id,
                    hit_count=self._cache_hits_by_entity[entity_id],
                )
                return self._cached_entities[entity_id]

            frame = entity_reader(entity, is_first)
            self._cached_entities[entity_id] = frame
            self._cache_misses += 1
            self._cache_misses_by_entity[entity_id] = (
                self._cache_misses_by_entity.get(entity_id, 0) + 1
            )

        self._maybe_auto_cache_frame(entity_id, frame)
        self.emit(
            "orchestrator.cache_miss",
            run_id=run_id,
            entity_id=entity_id,
            miss_count=self._cache_misses_by_entity[entity_id],
            cache_level=self._cache_recommendations.get(entity_id),
            auto_cached=self._auto_cache_enabled,
        )

        return frame

    def _maybe_auto_cache_frame(self, entity_id: str, frame: Any):
        """Persist/cache a recommended entity frame when auto_cache is enabled."""
        if not self._auto_cache_enabled or frame is None:
            return

        try:
            if hasattr(frame, "persist"):
                frame.persist()
            elif hasattr(frame, "cache"):
                frame.cache()
        except Exception:
            self.logger.debug(
                f"Auto-cache failed for entity '{entity_id}', continuing without persistence"
            )

    def _execute_pipe_streaming(
        self,
        pipe: PipeMetadata,
        streaming_options: Optional[Dict[str, Any]],
    ) -> PipeResult:
        """Execute a pipe in streaming mode via entity providers.

        Reads the input entity as a stream (via its provider),
        applies the pipe transformation, and writes to the output
        entity as a stream (via its provider).
        """
        # Resolve per-pipe options
        pipe_options = {}
        if streaming_options:
            if pipe.pipeid in streaming_options:
                pipe_options = streaming_options[pipe.pipeid]
            elif not any(isinstance(v, dict) for v in streaming_options.values()):
                pipe_options = streaming_options

        if not pipe.input_entity_ids:
            raise ValueError(f"Streaming pipe '{pipe.pipeid}' has no input entities")

        # Convention:
        # - first input entity is streaming input
        # - remaining input entities are direct (batch/static) reads for joins/lookups
        input_entity_frames: Dict[str, Any] = {}

        stream_input_entity_id = pipe.input_entity_ids[0]
        stream_input_entity = self.entity_registry.get_entity_definition(stream_input_entity_id)
        stream_input_provider = self.provider_registry.get_provider_for_entity(stream_input_entity)

        if not is_streamable(stream_input_provider):
            raise TypeError(
                f"Input provider for entity '{stream_input_entity.entityid}' "
                f"(type={stream_input_entity.tags.get('provider_type')}) "
                f"does not support streaming reads"
            )

        stream_df = stream_input_provider.read_entity_as_stream(stream_input_entity)
        input_entity_frames[stream_input_entity_id.replace(".", "_")] = stream_df

        for static_entity_id in pipe.input_entity_ids[1:]:
            static_entity = self.entity_registry.get_entity_definition(static_entity_id)
            static_provider = self.provider_registry.get_provider_for_entity(static_entity)
            static_df = static_provider.read_entity(static_entity)
            input_entity_frames[static_entity_id.replace(".", "_")] = static_df

        # Backwards compatible invocation:
        # - single input pipes may define execute(df)
        # - multi input pipes should use named kwargs keyed by entity ids
        if len(input_entity_frames) == 1:
            transformed_df = pipe.execute(next(iter(input_entity_frames.values())))
        else:
            transformed_df = pipe.execute(**input_entity_frames)

        # Resolve output entity and its provider
        output_entity = self.entity_registry.get_entity_definition(pipe.output_entity_id)
        output_provider = self.provider_registry.get_provider_for_entity(output_entity)
        if not is_stream_writable(output_provider) and not hasattr(
            output_provider, "append_as_stream"
        ):
            raise TypeError(
                f"Output provider for entity '{output_entity.entityid}' "
                f"(type={output_entity.tags.get('provider_type')}) "
                f"does not support streaming writes"
            )

        # Determine checkpoint path
        base_chkpt = pipe_options.get("base_checkpoint_path") or self.config_service.get(
            "base_checkpoint_path"
        )
        checkpoint_path = f"{base_chkpt}/{pipe.pipeid}"

        # Write as stream — resolve output path from entity tags first,
        # falling back to entity_path_locator (matching DeltaEntityProvider logic)
        output_path = output_entity.tags.get(
            "provider.path"
        ) or self.entity_path_locator.get_table_path(output_entity)
        stream_handle = output_provider.append_as_stream(
            transformed_df, output_entity, checkpoint_path
        )
        query = (
            stream_handle.start(output_path) if hasattr(stream_handle, "start") else stream_handle
        )

        return PipeResult(
            pipe_id=pipe.pipeid,
            status="success",
            streaming_query=query,
        )
