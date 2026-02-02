# Blinker Events Implementation Plan for Kindling Framework

**Date:** January 31, 2026
**Status:** Implementation Plan
**Based On:**
- [signal_dag_streaming_proposal.md](signal_dag_streaming_proposal.md)
- [signal_dag_streaming_evaluation.md](signal_dag_streaming_evaluation.md)
- [signal_dag_streaming_meta_evaluation.md](signal_dag_streaming_meta_evaluation.md)

---

## Executive Summary

This plan outlines the implementation strategy for applying blinker-based eventing throughout the Kindling framework. Based on the thorough analysis in the proposal documents, we will use the **Hybrid Approach (Option C)** - typed signals for core framework components with flexible string-based signals for extensions.

### Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Signal Pattern** | Hybrid (Option C) | Type-safe core + flexible extensions |
| **Naming Convention** | `{namespace}.before_{op}` / `{namespace}.after_{op}` / `{namespace}.{op}_failed` | Django-style, documented in signal_quick_reference.md |
| **Async Support** | Deferred to Phase 3 | Spark doesn't have default event loop |
| **Parallel Execution** | ThreadPoolExecutor | Independent pipes in same generation can run in parallel |

---

## Implementation Phases

### Phase 1: Signal Infrastructure & Core Services (2-3 weeks)

#### 1.1 Enhance SignalProvider with Emit Helper

**File:** `packages/kindling/signaling.py`

```python
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Tuple, Type
from dataclasses import dataclass
import uuid

@dataclass
class SignalPayload:
    """Base class for typed signal payloads."""
    operation_id: str = None
    timestamp: str = None

    def __post_init__(self):
        if self.operation_id is None:
            self.operation_id = str(uuid.uuid4())
        if self.timestamp is None:
            from datetime import datetime
            self.timestamp = datetime.utcnow().isoformat()


class SignalEmitter:
    """Mixin class that provides emit() helper for services.

    Services inherit from this to gain easy signal emission capabilities.
    Uses class-level EMITS list for documentation/introspection.
    """

    # Declare signals this class emits (for documentation/introspection)
    EMITS: List[str] = []

    def __init__(self):
        self._signal_provider: Optional[SignalProvider] = None

    def set_signal_provider(self, provider: SignalProvider):
        """Set the signal provider for this emitter."""
        self._signal_provider = provider

    def emit(self, signal_name: str, **kwargs) -> List[Tuple[Callable, Any]]:
        """Emit a signal with the given payload.

        Args:
            signal_name: Name of the signal (e.g., 'datapipes.before_run')
            **kwargs: Signal payload fields

        Returns:
            List of (receiver, return_value) tuples from handlers
        """
        if self._signal_provider is None:
            return []

        signal = self._signal_provider.get_signal(signal_name)
        if signal is None:
            # Create signal on first use (lazy creation)
            signal = self._signal_provider.create_signal(signal_name)

        return signal.send(self, **kwargs)

    def get_emitted_signals(self) -> List[str]:
        """Return list of signals this emitter can emit."""
        return self.EMITS.copy()


class TypedSignal(Signal):
    """Signal with typed payload validation (Phase 2).

    Validates that emitted payloads match the expected type.
    """

    def __init__(self, blinker_signal, payload_type: Type = None):
        super().__init__()
        self._signal = blinker_signal
        self._payload_type = payload_type

    def send(self, sender: Any, payload: Any = None, **kwargs) -> List[Tuple[Callable, Any]]:
        """Send signal with optional type validation."""
        if self._payload_type and payload is not None:
            if not isinstance(payload, self._payload_type):
                raise TypeError(
                    f"Expected payload of type {self._payload_type.__name__}, "
                    f"got {type(payload).__name__}"
                )
            # Convert dataclass to kwargs
            if hasattr(payload, '__dataclass_fields__'):
                kwargs.update(vars(payload))

        return self._signal.send(sender, **kwargs)
```

#### 1.2 Add Signal Emissions to DataPipesExecuter

**File:** `packages/kindling/data_pipes.py`

```python
from kindling.signaling import SignalEmitter, SignalProvider
import time
import uuid

@GlobalInjector.singleton_autobind()
class DataPipesExecuter(DataPipesExecution, SignalEmitter):
    """Enhanced DataPipesExecuter with signal emissions."""

    EMITS = [
        "datapipes.before_run",
        "datapipes.after_run",
        "datapipes.run_failed",
        "datapipes.before_pipe",
        "datapipes.after_pipe",
        "datapipes.pipe_failed",
        "datapipes.pipe_skipped",
    ]

    @inject
    def __init__(
        self,
        lp: PythonLoggerProvider,
        dpe: DataEntityRegistry,
        dpr: DataPipesRegistry,
        erps: EntityReadPersistStrategy,
        tp: SparkTraceProvider,
        signal_provider: SignalProvider = None,
    ):
        SignalEmitter.__init__(self)
        self.erps = erps
        self.dpr = dpr
        self.dpe = dpe
        self.logger = lp.get_logger("data_pipes_executer")
        self.tp = tp
        if signal_provider:
            self.set_signal_provider(signal_provider)

    def run_datapipes(self, pipes: List[str]):
        """Execute pipes with signal emissions."""
        run_id = str(uuid.uuid4())
        start_time = time.time()
        success_count = 0
        failed_pipes = []

        # Emit before_run signal
        self.emit("datapipes.before_run",
            pipe_ids=pipes,
            pipe_count=len(pipes),
            run_id=run_id
        )

        pipe_entity_reader = self.erps.create_pipe_entity_reader
        pipe_activator = self.erps.create_pipe_persist_activator

        try:
            with self.tp.span(component="data_pipes_executer", operation="execute_datapipes"):
                for index, pipeid in enumerate(pipes):
                    pipe = self.dpr.get_pipe_definition(pipeid)
                    pipe_start = time.time()

                    # Emit before_pipe signal
                    self.emit("datapipes.before_pipe",
                        pipe_id=pipeid,
                        pipe_name=pipe.name,
                        pipe_index=index,
                        run_id=run_id
                    )

                    try:
                        with self.tp.span(
                            operation="execute_datapipe",
                            component=f"pipe-{pipeid}",
                            details=pipe.tags
                        ):
                            rows_processed = self._execute_datapipe(
                                pipe_entity_reader(pipe),
                                pipe_activator(pipe),
                                pipe
                            )

                        pipe_duration = time.time() - pipe_start
                        success_count += 1

                        # Emit after_pipe signal
                        self.emit("datapipes.after_pipe",
                            pipe_id=pipeid,
                            pipe_name=pipe.name,
                            duration_seconds=pipe_duration,
                            rows_processed=rows_processed or 0,
                            run_id=run_id
                        )

                    except Exception as e:
                        pipe_duration = time.time() - pipe_start
                        failed_pipes.append(pipeid)

                        # Emit pipe_failed signal
                        self.emit("datapipes.pipe_failed",
                            pipe_id=pipeid,
                            pipe_name=pipe.name,
                            error=str(e),
                            error_type=type(e).__name__,
                            duration_seconds=pipe_duration,
                            run_id=run_id
                        )
                        raise

            total_duration = time.time() - start_time

            # Emit after_run signal
            self.emit("datapipes.after_run",
                pipe_ids=pipes,
                success_count=success_count,
                failed_count=len(failed_pipes),
                duration_seconds=total_duration,
                run_id=run_id
            )

        except Exception as e:
            total_duration = time.time() - start_time

            # Emit run_failed signal
            self.emit("datapipes.run_failed",
                pipe_ids=pipes,
                failed_pipe=failed_pipes[-1] if failed_pipes else None,
                success_count=success_count,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=total_duration,
                run_id=run_id
            )
            raise
```

#### 1.3 Add Signal Emissions to DataEntityManager

**File:** `packages/kindling/data_entities.py`

```python
from kindling.signaling import SignalEmitter, SignalProvider
import time
import uuid

@GlobalInjector.singleton_autobind()
class DataEntityManager(DataEntityRegistry, SignalEmitter):
    """Enhanced DataEntityManager with signal emissions."""

    EMITS = [
        "entity.before_ensure_table",
        "entity.after_ensure_table",
        "entity.ensure_failed",
        "entity.before_merge",
        "entity.after_merge",
        "entity.merge_failed",
        "entity.before_append",
        "entity.after_append",
        "entity.append_failed",
        "entity.before_write",
        "entity.after_write",
        "entity.write_failed",
        "entity.schema_changed",
        "entity.version_updated",
    ]

    @inject
    def __init__(self, signal_provider: SignalProvider = None):
        SignalEmitter.__init__(self)
        self.registry = {}
        if signal_provider:
            self.set_signal_provider(signal_provider)

    # ... existing methods remain, add emit() calls to operations
```

#### 1.4 Add Signal Emissions to FileIngestionProcessor

**Priority:** HIGH - Critical for monitoring file ingestion pipelines

```python
EMITS = [
    "file_ingestion.before_process",
    "file_ingestion.after_process",
    "file_ingestion.process_failed",
    "file_ingestion.before_file",
    "file_ingestion.after_file",
    "file_ingestion.file_failed",
    "file_ingestion.file_moved",
    "file_ingestion.batch_built",
    "file_ingestion.batch_written",
]
```

#### 1.5 Add Signal Emissions to WatermarkManager

**Priority:** HIGH - Critical for CDC monitoring

```python
EMITS = [
    "watermark.before_get",
    "watermark.watermark_found",
    "watermark.watermark_missing",
    "watermark.before_save",
    "watermark.after_save",
    "watermark.save_failed",
    "watermark.watermark_advanced",
    "watermark.watermark_reset",
    "watermark.version_changed",
]
```

---

### Phase 2: DAG Execution with Signals (4-5 weeks)

#### 2.1 Create PipeGraphBuilder

**File:** `packages/kindling/pipe_graph.py`

```python
"""DAG-based pipe execution planning.

Uses NetworkX for graph operations with fallback to graphlib.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set
import logging

try:
    import networkx as nx
    HAS_NETWORKX = True
except ImportError:
    import graphlib
    HAS_NETWORKX = False

from kindling.data_pipes import PipeMetadata, DataPipesRegistry
from kindling.signaling import SignalProvider

class ExecutionMode(Enum):
    BATCH = "batch"           # Forward topological sort
    STREAMING = "streaming"   # Reverse topological sort
    HYBRID = "hybrid"         # Mixed mode

@dataclass
class ExecutionPlan:
    """Execution plan generated by strategy."""
    generations: List[List[str]]  # Pipe IDs grouped by generation
    total_pipes: int
    max_parallelism: int
    mode: ExecutionMode
    cache_recommendations: Dict[str, str] = field(default_factory=dict)
    dependency_graph: Optional[object] = None  # nx.DiGraph if available

class PipeGraphBuilder:
    """Builds dependency graph from pipe metadata."""

    def __init__(
        self,
        pipe_registry: DataPipesRegistry,
        signal_provider: SignalProvider = None
    ):
        self.registry = pipe_registry
        self.signals = signal_provider
        self.logger = logging.getLogger("pipe_graph_builder")

    def build_graph(self, pipe_ids: List[str]) -> object:
        """Build dependency graph for given pipes.

        Returns NetworkX DiGraph if available, else dict for graphlib.
        """
        pipes = {pid: self.registry.get_pipe_definition(pid) for pid in pipe_ids}

        # Build entity -> pipe mapping
        entity_to_producer: Dict[str, str] = {}
        for pipe_id, pipe in pipes.items():
            if pipe.output_entity_id:
                entity_to_producer[pipe.output_entity_id] = pipe_id

        if HAS_NETWORKX:
            graph = nx.DiGraph()
            for pipe_id, pipe in pipes.items():
                graph.add_node(pipe_id, metadata=pipe)
                for input_entity in pipe.input_entity_ids:
                    if input_entity in entity_to_producer:
                        producer = entity_to_producer[input_entity]
                        if producer in pipes:
                            graph.add_edge(producer, pipe_id)
            return graph
        else:
            # Fallback: dict for graphlib.TopologicalSorter
            dependencies = {}
            for pipe_id, pipe in pipes.items():
                deps = set()
                for input_entity in pipe.input_entity_ids:
                    if input_entity in entity_to_producer:
                        producer = entity_to_producer[input_entity]
                        if producer in pipes:
                            deps.add(producer)
                dependencies[pipe_id] = deps
            return dependencies

    def get_generations(
        self,
        graph: object,
        mode: ExecutionMode = ExecutionMode.BATCH
    ) -> List[List[str]]:
        """Get execution generations from graph.

        Batch mode: Forward topological sort (producers first)
        Streaming mode: Reverse topological sort (consumers first)
        """
        if HAS_NETWORKX:
            generations = list(nx.topological_generations(graph))
        else:
            # graphlib fallback (sequential only)
            sorter = graphlib.TopologicalSorter(graph)
            generations = [[node] for node in sorter.static_order()]

        if mode == ExecutionMode.STREAMING:
            generations = list(reversed(generations))

        return generations
```

#### 2.2 Implement ExecutionStrategy Classes

```python
class ExecutionStrategy(ABC):
    """Abstract strategy for execution ordering."""

    @abstractmethod
    def create_plan(
        self,
        graph: object,
        pipes: Dict[str, PipeMetadata]
    ) -> ExecutionPlan:
        pass

class BatchExecutionStrategy(ExecutionStrategy):
    """Forward topological sort - dependencies first."""

    def __init__(self, signal_provider: SignalProvider = None):
        self.signals = signal_provider

    def create_plan(self, graph, pipes) -> ExecutionPlan:
        if HAS_NETWORKX:
            generations = list(nx.topological_generations(graph))
        else:
            sorter = graphlib.TopologicalSorter(graph)
            generations = [[node] for node in sorter.static_order()]

        plan = ExecutionPlan(
            generations=generations,
            total_pipes=len(pipes),
            max_parallelism=max(len(g) for g in generations) if generations else 0,
            mode=ExecutionMode.BATCH,
            cache_recommendations=self._identify_cache_candidates(graph, pipes),
            dependency_graph=graph
        )

        if self.signals:
            self.signals.emit("execution.plan_created",
                mode="batch",
                total_pipes=plan.total_pipes,
                generation_count=len(generations),
                max_parallelism=plan.max_parallelism
            )

        return plan

    def _identify_cache_candidates(
        self,
        graph,
        pipes: Dict[str, PipeMetadata]
    ) -> Dict[str, str]:
        """Identify entities consumed by multiple pipes."""
        entity_consumers: Dict[str, List[str]] = {}
        for pipe_id, pipe in pipes.items():
            for entity_id in pipe.input_entity_ids:
                entity_consumers.setdefault(entity_id, []).append(pipe_id)

        return {
            entity_id: "MEMORY_AND_DISK"
            for entity_id, consumers in entity_consumers.items()
            if len(consumers) > 1
        }

class StreamingExecutionStrategy(ExecutionStrategy):
    """Reverse topological sort - consumers ready before producers."""

    def create_plan(self, graph, pipes) -> ExecutionPlan:
        if HAS_NETWORKX:
            generations = list(reversed(list(nx.topological_generations(graph))))
        else:
            sorter = graphlib.TopologicalSorter(graph)
            generations = list(reversed([[node] for node in sorter.static_order()]))

        return ExecutionPlan(
            generations=generations,
            total_pipes=len(pipes),
            max_parallelism=max(len(g) for g in generations) if generations else 0,
            mode=ExecutionMode.STREAMING,
            cache_recommendations={},  # Streaming uses checkpoints
            dependency_graph=graph
        )
```

#### 2.3 Create GenerationExecutor with Parallel Support

Pipes within the same generation have no dependencies on each other by definition (DAG property). Each pipe operates on independent input/output entities, so they can safely run in parallel using ThreadPoolExecutor.

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

class GenerationExecutor:
    """Executes pipes within a generation with optional parallelism.

    Pipes in the same generation are independent (no shared DataFrames),
    so parallel execution with ThreadPoolExecutor is safe.

    Caveats:
    - Avoid shared accumulators across parallel pipes
    - Avoid creating broadcast variables simultaneously
    - Each pipe should use independent input/output entities
    """

    def __init__(
        self,
        max_workers: int = 4,
        signal_provider: SignalProvider = None
    ):
        self.max_workers = max_workers
        self.signals = signal_provider
        self.logger = logging.getLogger("generation_executor")

    def execute_generation(
        self,
        generation: List[str],
        pipe_executor: Callable[[str], int],
        generation_index: int
    ) -> Dict[str, bool]:
        """Execute all pipes in a generation, potentially in parallel.

        Args:
            generation: List of pipe IDs to execute
            pipe_executor: Function to execute a single pipe (returns row count)
            generation_index: Index of this generation

        Returns:
            Dict mapping pipe_id to success status
        """
        results = {}

        if self.signals:
            self.signals.emit("execution.before_generation",
                generation_index=generation_index,
                pipe_ids=generation,
                pipe_count=len(generation)
            )

        if len(generation) == 1 or self.max_workers == 1:
            # Sequential execution for single pipe or when parallelism disabled
            for pipe_id in generation:
                try:
                    rows = pipe_executor(pipe_id)
                    results[pipe_id] = True
                    self.logger.debug(f"Pipe {pipe_id} completed: {rows} rows")
                except Exception as e:
                    results[pipe_id] = False
                    self.logger.error(f"Pipe {pipe_id} failed: {e}")
                    raise
        else:
            # Parallel execution - safe because pipes are independent
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(pipe_executor, pipe_id): pipe_id
                    for pipe_id in generation
                }
                for future in as_completed(futures):
                    pipe_id = futures[future]
                    try:
                        rows = future.result()
                        results[pipe_id] = True
                        self.logger.debug(f"Pipe {pipe_id} completed: {rows} rows")
                    except Exception as e:
                        results[pipe_id] = False
                        self.logger.error(f"Pipe {pipe_id} failed: {e}")
                        # Cancel remaining futures and fail fast
                        for f in futures:
                            f.cancel()
                        raise

        if self.signals:
            self.signals.emit("execution.after_generation",
                generation_index=generation_index,
                results=results,
                success_count=sum(1 for v in results.values() if v),
                failed_count=sum(1 for v in results.values() if not v)
            )

        return results
```

#### 2.4 Update DataPipesExecuter to Use DAG Planning

```python
@GlobalInjector.singleton_autobind()
class DataPipesExecuter(DataPipesExecution, SignalEmitter):
    """Enhanced with DAG-based execution planning."""

    EMITS = [
        # Existing signals...
        "execution.plan_created",
        "execution.before_generation",
        "execution.after_generation",
    ]

    @inject
    def __init__(
        self,
        lp: PythonLoggerProvider,
        dpe: DataEntityRegistry,
        dpr: DataPipesRegistry,
        erps: EntityReadPersistStrategy,
        tp: SparkTraceProvider,
        signal_provider: SignalProvider = None,
    ):
        # ... existing init ...
        self.graph_builder = PipeGraphBuilder(dpr, signal_provider)
        self.generation_executor = GenerationExecutor(signal_provider)

    def run_datapipes(
        self,
        pipes: List[str],
        use_graph: bool = True,
        mode: ExecutionMode = ExecutionMode.BATCH
    ):
        """Execute pipes with optional DAG-based ordering."""
        if use_graph and len(pipes) > 1:
            self._run_with_graph(pipes, mode)
        else:
            self._run_sequential(pipes)

    def _run_with_graph(self, pipes: List[str], mode: ExecutionMode):
        """Execute pipes using DAG-based generation ordering."""
        run_id = str(uuid.uuid4())

        # Build execution plan
        graph = self.graph_builder.build_graph(pipes)

        if mode == ExecutionMode.BATCH:
            strategy = BatchExecutionStrategy(self._signal_provider)
        else:
            strategy = StreamingExecutionStrategy(self._signal_provider)

        pipe_defs = {pid: self.dpr.get_pipe_definition(pid) for pid in pipes}
        plan = strategy.create_plan(graph, pipe_defs)

        self.emit("datapipes.before_run",
            pipe_ids=pipes,
            pipe_count=len(pipes),
            run_id=run_id,
            execution_mode=mode.value,
            generation_count=len(plan.generations)
        )

        # Apply cache recommendations
        self._apply_cache_hints(plan.cache_recommendations)

        # Execute by generation
        start_time = time.time()
        for gen_idx, generation in enumerate(plan.generations):
            self.generation_executor.execute_generation(
                generation,
                lambda pid: self._execute_single_pipe(pid, run_id),
                gen_idx
            )

        self.emit("datapipes.after_run",
            pipe_ids=pipes,
            success_count=len(pipes),
            failed_count=0,
            duration_seconds=time.time() - start_time,
            run_id=run_id
        )
```

---

### Phase 3: Structured Streaming Support (5-6 weeks)

#### 3.1 Create StreamingQueryManager

**File:** `packages/kindling/streaming.py`

```python
"""Structured streaming support with signal-driven orchestration.

Provides lifecycle management, monitoring, and recovery for streaming queries.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Callable
from datetime import datetime
import threading
import queue
import time
import logging

from kindling.signaling import SignalProvider, SignalEmitter

class QueryStatus(Enum):
    PENDING = "pending"
    STARTING = "starting"
    RUNNING = "running"
    TERMINATED = "terminated"
    FAILED = "failed"
    RECOVERING = "recovering"

@dataclass
class QueryMetrics:
    """Metrics for a streaming query."""
    query_id: str
    name: str
    status: QueryStatus
    start_time: Optional[datetime] = None
    last_progress_time: Optional[datetime] = None
    batches_processed: int = 0
    records_processed: int = 0
    processing_rate: float = 0.0
    errors: List[str] = field(default_factory=list)
    restart_count: int = 0

@dataclass
class StreamingConfig:
    """Configuration for streaming orchestration."""
    health_check_interval_seconds: int = 30
    stale_threshold_seconds: int = 300
    auto_restart: bool = True
    max_restarts: int = 3
    restart_delay_seconds: int = 30
    backoff_multiplier: float = 2.0
    emit_progress_signals: bool = True
    progress_signal_interval_seconds: int = 60

class StreamingQueryManager(SignalEmitter):
    """Manages lifecycle of streaming queries with signal emissions."""

    EMITS = [
        "streaming.before_start",
        "streaming.after_start",
        "streaming.start_failed",
        "streaming.before_stop",
        "streaming.after_stop",
        "streaming.query_progress",
        "streaming.query_stale",
        "streaming.query_terminated",
    ]

    def __init__(
        self,
        spark,
        signal_provider: SignalProvider,
        config: StreamingConfig = None
    ):
        super().__init__()
        self.spark = spark
        self.config = config or StreamingConfig()
        self._queries: Dict[str, QueryMetrics] = {}
        self._query_objects: Dict[str, object] = {}  # StreamingQuery
        self._lock = threading.Lock()
        self.logger = logging.getLogger("streaming_query_manager")

        if signal_provider:
            self.set_signal_provider(signal_provider)

    def register_query(
        self,
        name: str,
        query_builder: Callable[[], object]
    ) -> str:
        """Register a query builder for later start."""
        query_id = f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with self._lock:
            self._queries[query_id] = QueryMetrics(
                query_id=query_id,
                name=name,
                status=QueryStatus.PENDING
            )
        return query_id

    def start_query(
        self,
        query_id: str,
        query_builder: Callable
    ) -> object:
        """Start a streaming query with signal emissions."""
        with self._lock:
            metrics = self._queries.get(query_id)
            if not metrics:
                raise ValueError(f"Unknown query: {query_id}")
            metrics.status = QueryStatus.STARTING
            metrics.start_time = datetime.now()

        self.emit("streaming.before_start",
            query_id=query_id,
            name=metrics.name
        )

        try:
            query = query_builder()
            with self._lock:
                self._query_objects[query_id] = query
                metrics.status = QueryStatus.RUNNING

            self.emit("streaming.after_start",
                query_id=query_id,
                spark_query_id=query.id
            )
            return query

        except Exception as e:
            with self._lock:
                metrics.status = QueryStatus.FAILED
                metrics.errors.append(str(e))

            self.emit("streaming.start_failed",
                query_id=query_id,
                error=str(e),
                error_type=type(e).__name__
            )
            raise

    def stop_query(self, query_id: str, await_termination: bool = True):
        """Stop a streaming query gracefully."""
        with self._lock:
            query = self._query_objects.get(query_id)
            if not query:
                return

        self.emit("streaming.before_stop", query_id=query_id)

        query.stop()
        if await_termination:
            query.awaitTermination()

        with self._lock:
            self._queries[query_id].status = QueryStatus.TERMINATED
            del self._query_objects[query_id]

        self.emit("streaming.after_stop", query_id=query_id)

    def get_metrics(self, query_id: str) -> Optional[QueryMetrics]:
        """Get current metrics for a query."""
        with self._lock:
            return self._queries.get(query_id)

    def get_all_metrics(self) -> Dict[str, QueryMetrics]:
        """Get metrics for all queries."""
        with self._lock:
            return dict(self._queries)
```

#### 3.2 Create StreamingWatchdog

```python
class StreamingWatchdog(SignalEmitter):
    """Monitors streaming queries and detects issues.

    Runs in background thread, emits signals for stale/terminated queries.
    """

    EMITS = [
        "streaming.watchdog_started",
        "streaming.watchdog_stopped",
        "streaming.watchdog_error",
        "streaming.query_stale",
        "streaming.query_terminated",
    ]

    def __init__(
        self,
        query_manager: StreamingQueryManager,
        signal_provider: SignalProvider,
        config: StreamingConfig
    ):
        super().__init__()
        self.manager = query_manager
        self.config = config
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger("streaming_watchdog")

        if signal_provider:
            self.set_signal_provider(signal_provider)

    def start(self):
        """Start the watchdog monitoring thread."""
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        self.emit("streaming.watchdog_started")

    def stop(self):
        """Stop the watchdog."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        self.emit("streaming.watchdog_stopped")

    def _monitor_loop(self):
        """Main monitoring loop."""
        while self._running:
            try:
                self._check_queries()
            except Exception as e:
                self.logger.error(f"Watchdog error: {e}")
                self.emit("streaming.watchdog_error", error=str(e))

            time.sleep(self.config.health_check_interval_seconds)

    def _check_queries(self):
        """Check health of all queries."""
        now = datetime.now()

        for query_id, metrics in self.manager.get_all_metrics().items():
            if metrics.status != QueryStatus.RUNNING:
                continue

            # Check for stale queries
            if metrics.last_progress_time:
                stale_seconds = (now - metrics.last_progress_time).total_seconds()
                if stale_seconds > self.config.stale_threshold_seconds:
                    self.emit("streaming.query_stale",
                        query_id=query_id,
                        last_progress_seconds_ago=stale_seconds
                    )

            # Check if query terminated unexpectedly
            query_obj = self.manager._query_objects.get(query_id)
            if query_obj and not query_obj.isActive:
                self.emit("streaming.query_terminated",
                    query_id=query_id,
                    exception=str(query_obj.exception()) if query_obj.exception() else None
                )
```

#### 3.3 Create KindlingStreamingListener with Queue-Based Processing

**CRITICAL:** Listener callbacks must not block! Use queue-based event processing.

```python
from pyspark.sql.streaming import StreamingQueryListener

class KindlingStreamingListener(StreamingQueryListener, SignalEmitter):
    """PySpark listener that bridges to Kindling signal system.

    CRITICAL: Uses queue-based processing to avoid blocking Spark's
    listener thread. All signal emissions happen in background thread.
    """

    EMITS = [
        "streaming.spark_query_started",
        "streaming.spark_query_progress",
        "streaming.spark_query_terminated",
    ]

    def __init__(
        self,
        signal_provider: SignalProvider,
        query_manager: StreamingQueryManager
    ):
        super().__init__()
        self.manager = query_manager
        self._event_queue: queue.Queue = queue.Queue()
        self._running = True
        self._processor_thread = threading.Thread(
            target=self._process_events,
            daemon=True
        )
        self._processor_thread.start()
        self.logger = logging.getLogger("streaming_listener")

        if signal_provider:
            self.set_signal_provider(signal_provider)

    def stop(self):
        """Stop the event processor."""
        self._running = False
        self._event_queue.put(None)  # Sentinel to unblock
        self._processor_thread.join(timeout=5)

    def onQueryStarted(self, event):
        """Called when a query is started - MUST NOT BLOCK."""
        self._event_queue.put(('started', {
            'query_id': event.id,
            'run_id': event.runId,
            'name': event.name
        }))

    def onQueryProgress(self, event):
        """Called when there is progress - MUST NOT BLOCK."""
        progress = event.progress
        self._event_queue.put(('progress', {
            'query_id': progress.id,
            'batch_id': progress.batchId,
            'input_rows_per_second': progress.inputRowsPerSecond,
            'processed_rows_per_second': progress.processedRowsPerSecond,
            'num_input_rows': progress.numInputRows
        }))

    def onQueryTerminated(self, event):
        """Called when a query is terminated - MUST NOT BLOCK."""
        self._event_queue.put(('terminated', {
            'query_id': event.id,
            'run_id': event.runId,
            'exception': str(event.exception) if event.exception else None
        }))

    def _process_events(self):
        """Process queued events in background thread."""
        while self._running:
            try:
                item = self._event_queue.get(timeout=1)
                if item is None:
                    break

                event_type, data = item
                self._handle_event(event_type, data)

            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error processing event: {e}")

    def _handle_event(self, event_type: str, data: dict):
        """Handle a single event with signal emission."""
        try:
            if event_type == 'started':
                self.emit("streaming.spark_query_started", **data)
            elif event_type == 'progress':
                self.emit("streaming.spark_query_progress", **data)
                # Update manager metrics
                self._update_metrics(data)
            elif event_type == 'terminated':
                self.emit("streaming.spark_query_terminated", **data)
                # Trigger recovery if needed
                if data.get('exception') and self.manager.config.auto_restart:
                    self._schedule_recovery(data['query_id'])
        except Exception as e:
            self.logger.error(f"Error handling {event_type}: {e}")

    def _update_metrics(self, progress_data: dict):
        """Update query metrics from progress event."""
        query_id = progress_data['query_id']
        metrics = self.manager.get_metrics(query_id)
        if metrics:
            metrics.last_progress_time = datetime.now()
            metrics.batches_processed += 1
            metrics.records_processed += progress_data.get('num_input_rows', 0)
            metrics.processing_rate = progress_data.get('processed_rows_per_second', 0)

    def _schedule_recovery(self, query_id: str):
        """Schedule recovery for failed query."""
        # Delegate to recovery manager (not implemented here)
        pass
```

---

### Phase 4: Dimension-Driven Stream Restarts (3-4 weeks)

#### 4.1 Create DimensionChangeDetector with Batch Checking

```python
class DimensionChangeDetector(SignalEmitter):
    """Monitors dimension tables for changes with batch version checking.

    Uses batch queries to efficiently check multiple dimensions.
    """

    EMITS = [
        "dimension.watch_started",
        "dimension.watch_stopped",
        "dimension.changed",
        "dimension.check_failed",
    ]

    def __init__(
        self,
        spark,
        signal_provider: SignalProvider,
        entity_provider,  # EntityProvider
        check_interval_seconds: int = 60
    ):
        super().__init__()
        self.spark = spark
        self.entity_provider = entity_provider
        self.check_interval = check_interval_seconds
        self._watched_dimensions: Dict[str, int] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger("dimension_change_detector")

        if signal_provider:
            self.set_signal_provider(signal_provider)

    def watch_dimension(self, entity_id: str):
        """Start watching a dimension table for changes."""
        current_version = self.entity_provider.get_entity_version(entity_id)
        self._watched_dimensions[entity_id] = current_version

        self.emit("dimension.watch_started",
            entity_id=entity_id,
            initial_version=current_version
        )

    def unwatch_dimension(self, entity_id: str):
        """Stop watching a dimension table."""
        if entity_id in self._watched_dimensions:
            del self._watched_dimensions[entity_id]
            self.emit("dimension.watch_stopped", entity_id=entity_id)

    def start(self):
        """Start the dimension monitoring thread."""
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop monitoring."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _monitor_loop(self):
        """Check for dimension changes periodically."""
        while self._running:
            self._check_all_dimensions()
            time.sleep(self.check_interval)

    def _check_all_dimensions(self):
        """Batch check all dimensions for efficiency."""
        changed_dimensions = []

        for entity_id, last_version in list(self._watched_dimensions.items()):
            try:
                current_version = self.entity_provider.get_entity_version(entity_id)

                if current_version > last_version:
                    self._watched_dimensions[entity_id] = current_version
                    changed_dimensions.append({
                        'entity_id': entity_id,
                        'old_version': last_version,
                        'new_version': current_version
                    })

            except Exception as e:
                self.logger.error(f"Error checking {entity_id}: {e}")
                self.emit("dimension.check_failed",
                    entity_id=entity_id,
                    error=str(e)
                )

        # Emit change signals
        for change in changed_dimensions:
            self.emit("dimension.changed", **change)
```

#### 4.2 Create StreamRestartController with Throttling

```python
class StreamRestartController(SignalEmitter):
    """Handles signal-driven restarts with throttling.

    Prevents restart storms by queuing and throttling restarts.
    """

    EMITS = [
        "streaming.dependencies_registered",
        "streaming.dimension_change_detected",
        "streaming.restart_scheduled",
        "streaming.restart_throttled",
        "streaming.before_restart",
        "streaming.after_restart",
        "streaming.restart_failed",
        "streaming.graph_restart_initiated",
    ]

    def __init__(
        self,
        query_manager: StreamingQueryManager,
        signal_provider: SignalProvider,
        dimension_detector: DimensionChangeDetector,
        max_concurrent_restarts: int = 2
    ):
        super().__init__()
        self.manager = query_manager
        self.dimension_detector = dimension_detector
        self._query_dependencies: Dict[str, 'StreamDependencies'] = {}
        self._query_builders: Dict[str, Callable] = {}
        self._restart_queue: queue.Queue = queue.Queue()
        self._restart_semaphore = threading.Semaphore(max_concurrent_restarts)
        self._restart_processor = threading.Thread(
            target=self._process_restarts,
            daemon=True
        )
        self._running = True
        self._restart_processor.start()
        self.logger = logging.getLogger("stream_restart_controller")

        if signal_provider:
            self.set_signal_provider(signal_provider)
            # Subscribe to dimension changes
            sig = signal_provider.get_signal("dimension.changed")
            if sig:
                sig.connect(self._on_dimension_changed)

    def stop(self):
        """Stop the restart processor."""
        self._running = False
        self._restart_queue.put(None)
        self._restart_processor.join(timeout=5)

    def register_stream_with_dependencies(
        self,
        query_id: str,
        query_builder: Callable,
        dependencies: 'StreamDependencies'
    ):
        """Register a streaming query with its dependencies."""
        self._query_dependencies[query_id] = dependencies
        self._query_builders[query_id] = query_builder

        # Start watching dimension dependencies
        for dim_dep in dependencies.dimensions:
            self.dimension_detector.watch_dimension(dim_dep.entity_id)

        self.emit("streaming.dependencies_registered",
            query_id=query_id,
            dimension_count=len(dependencies.dimensions)
        )

    def _on_dimension_changed(self, sender, **kwargs):
        """Handle dimension change - find affected queries."""
        changed_entity = kwargs['entity_id']
        affected = self._find_affected_queries(changed_entity)

        if affected:
            self.emit("streaming.dimension_change_detected",
                entity_id=changed_entity,
                affected_queries=affected
            )

            for query_id in affected:
                self._schedule_restart(query_id, 'dimension_changed', **kwargs)

    def _find_affected_queries(self, entity_id: str) -> List[str]:
        """Find queries that depend on the changed entity."""
        affected = []
        for query_id, deps in self._query_dependencies.items():
            for dim_dep in deps.dimensions:
                if dim_dep.entity_id == entity_id and dim_dep.restart_on_change:
                    affected.append(query_id)
                    break
        return affected

    def _schedule_restart(self, query_id: str, reason: str, **context):
        """Queue restart with throttling."""
        self.emit("streaming.restart_scheduled",
            query_id=query_id,
            reason=reason,
            **context
        )
        self._restart_queue.put((query_id, reason, context))

    def _process_restarts(self):
        """Process restart queue with concurrency control."""
        while self._running:
            try:
                item = self._restart_queue.get(timeout=1)
                if item is None:
                    break

                query_id, reason, context = item

                # Acquire semaphore for throttling
                if self._restart_semaphore.acquire(timeout=30):
                    try:
                        self._execute_restart(query_id, reason, context)
                    finally:
                        self._restart_semaphore.release()
                else:
                    self.emit("streaming.restart_throttled",
                        query_id=query_id,
                        reason="max_concurrent_restarts_exceeded"
                    )
                    # Re-queue for later
                    self._restart_queue.put((query_id, reason, context))

            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error processing restart: {e}")

    def _execute_restart(self, query_id: str, reason: str, context: dict):
        """Execute the restart sequence."""
        self.emit("streaming.before_restart",
            query_id=query_id,
            reason=reason,
            **context
        )

        try:
            # Stop query gracefully
            self.manager.stop_query(query_id, await_termination=True)

            # Get builder and restart
            builder = self._query_builders.get(query_id)
            if builder:
                self.manager.start_query(query_id, builder)

                self.emit("streaming.after_restart",
                    query_id=query_id,
                    reason=reason
                )
        except Exception as e:
            self.emit("streaming.restart_failed",
                query_id=query_id,
                reason=reason,
                error=str(e)
            )
```

---

## Testing Strategy

### Unit Tests

```python
# tests/unit/test_signal_emissions.py

def test_datapipes_emits_before_run():
    """Verify before_run signal is emitted."""
    mock_provider = Mock(spec=SignalProvider)
    mock_signal = Mock()
    mock_provider.get_signal.return_value = mock_signal
    mock_provider.create_signal.return_value = mock_signal

    executer = DataPipesExecuter(signal_provider=mock_provider, ...)
    executer.run_datapipes(['pipe1'])

    # Verify signal was emitted
    calls = [c for c in mock_signal.send.call_args_list
             if 'datapipes.before_run' in str(c)]
    assert len(calls) >= 1

def test_datapipes_emits_after_pipe_with_duration():
    """Verify after_pipe includes duration."""
    received_signals = []

    def capture(sender, **kwargs):
        received_signals.append(kwargs)

    provider = BlinkerSignalProvider()
    provider.create_signal("datapipes.after_pipe").connect(capture)

    executer = DataPipesExecuter(signal_provider=provider, ...)
    executer.run_datapipes(['pipe1'])

    assert len(received_signals) == 1
    assert 'duration_seconds' in received_signals[0]
    assert received_signals[0]['duration_seconds'] >= 0

def test_streaming_listener_does_not_block():
    """Verify listener uses queue-based processing."""
    provider = BlinkerSignalProvider()
    manager = StreamingQueryManager(spark, provider)
    listener = KindlingStreamingListener(provider, manager)

    # Simulate rapid progress events
    import time
    start = time.time()
    for _ in range(100):
        listener.onQueryProgress(MockProgressEvent())
    elapsed = time.time() - start

    # Should complete quickly (not blocking on signal processing)
    assert elapsed < 0.1  # Less than 100ms for 100 events
```

### Integration Tests

```python
# tests/integration/test_signal_flow.py

def test_full_pipeline_signal_flow():
    """Test signals flow through complete pipeline execution."""
    signals_received = []

    def capture_all(sender, **kwargs):
        signals_received.append((sender.__class__.__name__, kwargs))

    provider = BlinkerSignalProvider()
    for signal_name in DataPipesExecuter.EMITS:
        provider.create_signal(signal_name).connect(capture_all)

    # Run a multi-pipe pipeline
    executer = DataPipesExecuter(signal_provider=provider, ...)
    executer.run_datapipes(['pipe1', 'pipe2', 'pipe3'], use_graph=True)

    # Verify expected signals
    signal_types = [s[1].get('pipe_id') for s in signals_received]
    assert 'pipe1' in str(signals_received)
    assert 'pipe2' in str(signals_received)
    assert 'pipe3' in str(signals_received)
```

---

## Complete Signal Catalog (Updated)

### Core Signals (Phase 1)

| Namespace | Signal | Payload |
|-----------|--------|---------|
| `datapipes` | `before_run` | pipe_ids, pipe_count, run_id |
| `datapipes` | `after_run` | success_count, failed_count, duration_seconds |
| `datapipes` | `run_failed` | error, error_type, failed_pipe |
| `datapipes` | `before_pipe` | pipe_id, pipe_name, pipe_index |
| `datapipes` | `after_pipe` | pipe_id, duration_seconds, rows_processed |
| `datapipes` | `pipe_failed` | pipe_id, error, error_type |
| `entity` | `before_merge` | entity_id, merge_keys |
| `entity` | `after_merge` | rows_inserted, rows_updated |
| `entity` | `merge_failed` | entity_id, error |

### DAG Execution Signals (Phase 2)

| Namespace | Signal | Payload |
|-----------|--------|---------|
| `execution` | `plan_created` | mode, total_pipes, generation_count |
| `execution` | `before_generation` | generation_index, pipe_ids |
| `execution` | `after_generation` | generation_index, results |

### Streaming Signals (Phase 3)

| Namespace | Signal | Payload |
|-----------|--------|---------|
| `streaming` | `before_start` | query_id, name |
| `streaming` | `after_start` | query_id, spark_query_id |
| `streaming` | `query_progress` | query_id, batch_id, rows_per_second |
| `streaming` | `query_stale` | query_id, last_progress_seconds_ago |
| `streaming` | `watchdog_started` | - |

### Dimension Signals (Phase 4)

| Namespace | Signal | Payload |
|-----------|--------|---------|
| `dimension` | `changed` | entity_id, old_version, new_version |
| `dimension` | `watch_started` | entity_id, initial_version |
| `streaming` | `restart_scheduled` | query_id, reason |
| `streaming` | `before_restart` | query_id, reason |

---

## Timeline Summary

| Phase | Duration | Key Deliverables |
|-------|----------|------------------|
| **Phase 1** | 2-3 weeks | SignalEmitter mixin, DataPipes signals, Entity signals |
| **Phase 2** | 4-5 weeks | PipeGraphBuilder, ExecutionStrategy, Generation executor |
| **Phase 3** | 5-6 weeks | StreamingQueryManager, Watchdog, Listener with queue |
| **Phase 4** | 3-4 weeks | DimensionChangeDetector, RestartController with throttling |
| **Integration** | 3-4 weeks | Testing, documentation, examples |
| **Total** | **17-22 weeks** | Full signal-driven framework |

---

## Checklist

```markdown
## Pre-Implementation
- [ ] Fix thread-safety issues (no parallel DataFrame operations)
- [ ] Refactor StreamingQueryListener to use queues
- [ ] Add restart throttling design
- [ ] Create comprehensive test plan

## Phase 1: Signal Infrastructure
- [ ] Add SignalEmitter base class to signaling.py
- [ ] Add EMITS declarations convention
- [ ] Update DataPipesExecuter with signal emissions
- [ ] Update DataEntityManager with signal emissions
- [ ] Add signals to FileIngestionProcessor
- [ ] Add signals to WatermarkManager
- [ ] Write unit tests for signal patterns

## Phase 2: DAG Execution
- [ ] Create pipe_graph.py module
- [ ] Implement PipeGraphBuilder
- [ ] Implement BatchExecutionStrategy
- [ ] Implement StreamingExecutionStrategy
- [ ] Implement GenerationExecutor (with ThreadPoolExecutor)
- [ ] Update DataPipesExecuter to use DAG
- [ ] Add execution signals
- [ ] Write integration tests

## Phase 3: Streaming Support
- [ ] Create streaming.py module
- [ ] Implement StreamingQueryManager
- [ ] Implement StreamingWatchdog
- [ ] Implement KindlingStreamingListener (queue-based)
- [ ] Add streaming signals
- [ ] Write system tests

## Phase 4: Dimension Restarts
- [ ] Implement DimensionChangeDetector
- [ ] Implement StreamRestartController with throttling
- [ ] Add dimension signals
- [ ] Test cascading restarts
- [ ] Document alternative approaches

## Integration & Polish
- [ ] Integration testing across all phases
- [ ] Update documentation
- [ ] Create example notebooks
- [ ] Performance benchmarks
- [ ] Error handling review
```

---

## Risk Mitigations Applied

| Risk | Mitigation |
|------|------------|
| **Parallel Execution** | Safe for independent pipes; avoid shared accumulators/broadcast variables |
| **Listener Blocking** | Queue-based event processing in KindlingStreamingListener |
| **Restart Storms** | Semaphore-based throttling in StreamRestartController |
| **Event Loop** | Async support deferred - sync signals only |
| **NetworkX Dependency** | Fallback to graphlib with sequential-only execution |

---

## Conclusion

This plan provides a comprehensive, phased approach to implementing blinker-based eventing throughout the Kindling framework. The key principles are:

1. **Parallel Generations**: Independent pipes can run in parallel with ThreadPoolExecutor
2. **Non-Blocking**: Queue-based processing for listeners
3. **Throttled Restarts**: Prevent restart storms with semaphores
4. **Progressive Enhancement**: Start simple, add typed signals later
5. **Observability**: Comprehensive signal catalog for monitoring

Following this plan will result in a production-ready, signal-driven framework that enables loose coupling, better monitoring, and sophisticated orchestration capabilities.
