# Proposal: Signal Eventing, DAG Execution, and Structured Streaming Support

**Date:** January 30, 2026
**Status:** Proposal
**Author:** Technical Review

---

## Table of Contents

1. [Overview](#overview)
2. [Part 1: Signal Eventing System](#part-1-signal-eventing-system)
3. [Part 2: DAG-Based Execution Services](#part-2-dag-based-execution-services)
4. [Part 3: Structured Streaming Support](#part-3-structured-streaming-support)
5. [Integration Points](#integration-points)
6. [Implementation Roadmap](#implementation-roadmap)

---

## Overview

This proposal outlines three interconnected features for the Kindling framework:

1. **Signal Eventing** - Extend the existing blinker-based signaling with registry-style declarations, naming conventions, and optional async support
2. **DAG-Based Execution** - Build on the existing graph execution plan to provide multiple ordering strategies and parallel execution
3. **Structured Streaming Support** - Provide orchestration, watchdog, and recovery capabilities for streaming pipelines

These features are designed to work together, with signals providing the communication backbone for DAG execution events and streaming monitoring.

---

## Part 1: Signal Eventing System

### Current State

- Blinker integration exists in [packages/kindling/signaling.py](../../packages/kindling/signaling.py)
- `BlinkerSignalProvider` and `BlinkerSignalWrapper` classes implemented
- Signal naming convention documented: `before_`/`after_` prefixes (Django-style)
- Comprehensive signal catalog exists in [docs/signal_quick_reference.md](../signal_quick_reference.md)

### Proposed Enhancements

#### Option A: Simple Emit Pattern (Recommended for Phase 1)

Keep signals string-based but add emit helpers and consistent naming:

```python
from kindling.signaling import SignalEmitter

class DataPipesExecuter(SignalEmitter):
    """Service that emits pipeline execution signals."""

    # Declare signals this class emits (for documentation/introspection)
    EMITS = [
        "datapipes.before_run",
        "datapipes.after_run",
        "datapipes.run_failed",
        "datapipes.before_pipe",
        "datapipes.after_pipe",
        "datapipes.pipe_failed",
    ]

    def run_datapipes(self, pipes):
        run_id = str(uuid.uuid4())

        self.emit("datapipes.before_run",
            pipe_ids=pipes,
            pipe_count=len(pipes),
            run_id=run_id
        )

        try:
            # ... execution logic ...
            self.emit("datapipes.after_run",
                pipe_ids=pipes,
                success_count=len(pipes),
                duration_seconds=elapsed,
                run_id=run_id
            )
        except Exception as e:
            self.emit("datapipes.run_failed",
                error=str(e),
                error_type=type(e).__name__,
                run_id=run_id
            )
            raise
```

**Pros:**
- Minimal changes to existing code
- String-based signals remain flexible
- EMITS list provides discoverability without enforcement

**Cons:**
- No compile-time validation
- Signal typos can cause silent failures

#### Option B: Registry-Style Typed Signals (Phase 2)

Add a signal registry with typed signal definitions:

```python
from dataclasses import dataclass
from typing import List, Optional
from kindling.signaling import SignalDefinition, SignalRegistry

# Define signal schemas
@dataclass
class PipeRunPayload:
    pipe_ids: List[str]
    pipe_count: int
    run_id: str

@dataclass
class PipeRunResultPayload(PipeRunPayload):
    success_count: int
    failed_count: int
    duration_seconds: float

@dataclass
class PipeFailedPayload(PipeRunPayload):
    error: str
    error_type: str
    failed_pipe: Optional[str] = None

# Register signals with schemas
class DataPipesSignals(SignalRegistry):
    """Signal definitions for DataPipes operations."""

    before_run = SignalDefinition(
        "datapipes.before_run",
        payload_type=PipeRunPayload,
        doc="Emitted before run_datapipes() execution"
    )
    after_run = SignalDefinition(
        "datapipes.after_run",
        payload_type=PipeRunResultPayload,
        doc="Emitted after successful run_datapipes() completion"
    )
    run_failed = SignalDefinition(
        "datapipes.run_failed",
        payload_type=PipeFailedPayload,
        doc="Emitted when run_datapipes() fails"
    )

# Usage
class DataPipesExecuter:
    signals = DataPipesSignals()

    def run_datapipes(self, pipes):
        self.signals.before_run.emit(self, PipeRunPayload(
            pipe_ids=pipes,
            pipe_count=len(pipes),
            run_id=run_id
        ))
```

**Pros:**
- Type-safe payloads (IDE autocomplete, validation)
- Self-documenting signal API
- Can generate documentation from registry
- Validation at emit time (optional)

**Cons:**
- More boilerplate
- Requires dataclasses for all payloads
- Breaking change for dynamic signal patterns

#### Option C: Hybrid Approach (Recommended)

Combine both: typed signals for core framework, flexible strings for extensions:

```python
class SignalProvider(ABC):
    """Extended provider supporting both typed and string signals."""

    @abstractmethod
    def create_signal(self, name: str, payload_type: type = None) -> Signal:
        """Create signal with optional payload type validation."""
        pass

    @abstractmethod
    def emit(self, name: str, sender: Any, payload: Any = None, **kwargs) -> None:
        """Emit signal - validates payload if type was registered."""
        pass

# Core framework uses typed signals
class DataPipesSignals:
    before_run: TypedSignal[PipeRunPayload]
    after_run: TypedSignal[PipeRunResultPayload]

# Extensions can use string-based signals
signal_provider.emit("custom.my_extension.event", sender=self, data="flexible")
```

### Async Support for Signals

For streaming scenarios where signals should not block:

```python
class AsyncSignalSupport:
    """Mixin for async signal handling."""

    def __init__(self):
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def connect_async(self, signal_name: str, async_handler: Callable):
        """Connect an async handler to a signal."""
        def wrapper(sender, **kwargs):
            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(
                    async_handler(sender, **kwargs),
                    self._loop
                )
            else:
                # Fallback to sync if no loop
                import asyncio
                asyncio.run(async_handler(sender, **kwargs))

        self.signal_provider.get_signal(signal_name).connect(wrapper)

    def set_event_loop(self, loop: asyncio.AbstractEventLoop):
        """Set the event loop for async signal dispatch."""
        self._loop = loop
```

### Signal Naming Convention (Finalized)

```
{namespace}.before_{operation}  - Before operation starts
{namespace}.after_{operation}   - After successful completion
{namespace}.{operation}_failed  - When operation fails
{namespace}.{state}_changed     - When state transitions
```

**Examples:**
- `datapipes.before_run`, `datapipes.after_run`, `datapipes.run_failed`
- `entity.before_merge`, `entity.after_merge`, `entity.merge_failed`
- `streaming.query_started`, `streaming.query_stopped`, `streaming.status_changed`

---

## Part 2: DAG-Based Execution Services

### Current State

- Detailed plan exists in [docs/graph_based_pipe_execution_plan.md](../graph_based_pipe_execution_plan.md)
- `PipeMetadata` already captures `input_entity_ids` and `output_entity_id` for dependency inference
- `DataPipesExecuter` executes pipes sequentially

### Proposed Architecture

#### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                    ExecutionOrchestrator                     │
│  (Coordinates execution based on strategy and mode)          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    PipeGraphBuilder                          │
│  (Builds dependency graph from pipe metadata)                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  ExecutionPlanGenerator                      │
│  (Generates execution plan based on strategy)                │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ BatchStrategy   │ │ StreamStrategy  │ │ HybridStrategy  │
│ (forward topo)  │ │ (reverse topo)  │ │ (mixed mode)    │
└─────────────────┘ └─────────────────┘ └─────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   GenerationExecutor                         │
│  (Executes generations with parallelism control)             │
└─────────────────────────────────────────────────────────────┘
```

#### Execution Strategies

**Option 1: Strategy Pattern (Recommended)**

```python
from abc import ABC, abstractmethod
from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Optional
import networkx as nx

class ExecutionMode(Enum):
    BATCH = "batch"           # Forward topological sort
    STREAMING = "streaming"   # Reverse topological sort
    HYBRID = "hybrid"         # Mixed: some batch, some streaming

@dataclass
class ExecutionPlan:
    generations: List[List[str]]  # Pipe IDs grouped by generation
    total_pipes: int
    max_parallelism: int
    mode: ExecutionMode
    cache_recommendations: Dict[str, str]  # entity_id -> cache_level
    dependency_graph: Optional[nx.DiGraph] = None

class ExecutionStrategy(ABC):
    """Abstract strategy for execution ordering."""

    @abstractmethod
    def create_plan(
        self,
        graph: nx.DiGraph,
        pipes: Dict[str, PipeMetadata]
    ) -> ExecutionPlan:
        """Generate execution plan from dependency graph."""
        pass

class BatchExecutionStrategy(ExecutionStrategy):
    """Forward topological sort - dependencies first."""

    def create_plan(self, graph, pipes) -> ExecutionPlan:
        generations = list(nx.topological_generations(graph))
        return ExecutionPlan(
            generations=generations,
            total_pipes=len(pipes),
            max_parallelism=max(len(g) for g in generations),
            mode=ExecutionMode.BATCH,
            cache_recommendations=self._identify_shared_entities(graph, pipes)
        )

    def _identify_shared_entities(self, graph, pipes) -> Dict[str, str]:
        """Identify entities consumed by multiple pipes (cache candidates)."""
        entity_consumers = {}
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
        generations = list(reversed(list(nx.topological_generations(graph))))
        return ExecutionPlan(
            generations=generations,
            total_pipes=len(pipes),
            max_parallelism=max(len(g) for g in generations),
            mode=ExecutionMode.STREAMING,
            cache_recommendations={}  # Streaming uses checkpoints, not cache
        )

class HybridExecutionStrategy(ExecutionStrategy):
    """Mixed mode - batch for some pipes, streaming for others."""

    def __init__(self, streaming_pipes: List[str]):
        self.streaming_pipes = set(streaming_pipes)

    def create_plan(self, graph, pipes) -> ExecutionPlan:
        # Separate batch and streaming subgraphs
        batch_pipes = {k: v for k, v in pipes.items() if k not in self.streaming_pipes}
        stream_pipes = {k: v for k, v in pipes.items() if k in self.streaming_pipes}

        # Generate plans for each
        batch_gens = list(nx.topological_generations(graph.subgraph(batch_pipes.keys())))
        stream_gens = list(reversed(list(nx.topological_generations(graph.subgraph(stream_pipes.keys())))))

        # Interleave: batch first, then streaming
        return ExecutionPlan(
            generations=batch_gens + stream_gens,
            total_pipes=len(pipes),
            max_parallelism=max(len(g) for g in batch_gens + stream_gens),
            mode=ExecutionMode.HYBRID,
            cache_recommendations={}
        )
```

**Option 2: Configuration-Based**

```python
@dataclass
class ExecutionConfig:
    mode: ExecutionMode = ExecutionMode.BATCH
    max_parallelism: int = 4
    enable_caching: bool = True
    cache_level: str = "MEMORY_AND_DISK"
    timeout_seconds: Optional[int] = None
    retry_failed: bool = False
    retry_count: int = 3

class ExecutionOrchestrator:
    def __init__(self, config: ExecutionConfig):
        self.config = config
        self.strategy = self._get_strategy(config.mode)

    def _get_strategy(self, mode: ExecutionMode) -> ExecutionStrategy:
        strategies = {
            ExecutionMode.BATCH: BatchExecutionStrategy(),
            ExecutionMode.STREAMING: StreamingExecutionStrategy(),
        }
        return strategies[mode]
```

#### Parallel Execution within Generations

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

class GenerationExecutor:
    """Executes pipes within a generation with parallelism control."""

    def __init__(
        self,
        max_workers: int = 4,
        signal_provider: SignalProvider = None
    ):
        self.max_workers = max_workers
        self.signals = signal_provider

    def execute_generation(
        self,
        generation: List[str],
        pipe_executor: Callable[[str], None],
        generation_index: int
    ) -> Dict[str, bool]:
        """Execute all pipes in a generation, potentially in parallel.

        Args:
            generation: List of pipe IDs to execute
            pipe_executor: Function to execute a single pipe
            generation_index: Index of this generation (for logging)

        Returns:
            Dict mapping pipe_id to success status
        """
        results = {}

        if self.signals:
            self.signals.emit("execution.before_generation",
                generation_index=generation_index,
                pipe_ids=generation
            )

        if len(generation) == 1 or self.max_workers == 1:
            # Sequential execution
            for pipe_id in generation:
                try:
                    pipe_executor(pipe_id)
                    results[pipe_id] = True
                except Exception as e:
                    results[pipe_id] = False
        else:
            # Parallel execution
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(pipe_executor, pipe_id): pipe_id
                    for pipe_id in generation
                }
                for future in as_completed(futures):
                    pipe_id = futures[future]
                    try:
                        future.result()
                        results[pipe_id] = True
                    except Exception as e:
                        results[pipe_id] = False

        if self.signals:
            self.signals.emit("execution.after_generation",
                generation_index=generation_index,
                results=results
            )

        return results
```

---

## Part 3: Structured Streaming Support

### Current State

- Basic streaming support in [packages/kindling/pipe_streaming.py](../../packages/kindling/pipe_streaming.py)
- `SimplePipeStreamOrchestrator` starts individual streams
- No monitoring, recovery, or coordination features

### Proposed Architecture

#### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                 StreamingOrchestrator                        │
│  (High-level coordination of streaming pipelines)            │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ QueryManager    │  │ WatchdogService │  │ RecoveryManager │
│ (Lifecycle)     │  │ (Monitoring)    │  │ (Error Handling)│
└─────────────────┘  └─────────────────┘  └─────────────────┘
          │                   │                   │
          └───────────────────┼───────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              StreamingQueryListener (PySpark)                │
│  (Receives progress/termination events from Spark)           │
└─────────────────────────────────────────────────────────────┘
```

#### Option A: Comprehensive Streaming Manager

```python
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
import threading
import time

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
    processing_rate: float = 0.0  # records/sec
    errors: List[str] = field(default_factory=list)
    restart_count: int = 0

@dataclass
class StreamingConfig:
    """Configuration for streaming orchestration."""
    # Watchdog settings
    health_check_interval_seconds: int = 30
    stale_threshold_seconds: int = 300  # 5 minutes

    # Recovery settings
    auto_restart: bool = True
    max_restarts: int = 3
    restart_delay_seconds: int = 30
    backoff_multiplier: float = 2.0

    # Signal settings
    emit_progress_signals: bool = True
    progress_signal_interval_seconds: int = 60

class StreamingQueryManager:
    """Manages lifecycle of streaming queries."""

    def __init__(
        self,
        spark,
        signal_provider: SignalProvider,
        config: StreamingConfig = None
    ):
        self.spark = spark
        self.signals = signal_provider
        self.config = config or StreamingConfig()
        self._queries: Dict[str, QueryMetrics] = {}
        self._query_objects: Dict[str, 'StreamingQuery'] = {}
        self._lock = threading.Lock()

    def register_query(
        self,
        name: str,
        query_builder: Callable[[], 'StreamingQuery']
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

    def start_query(self, query_id: str, query_builder: Callable) -> 'StreamingQuery':
        """Start a streaming query."""
        with self._lock:
            metrics = self._queries.get(query_id)
            if not metrics:
                raise ValueError(f"Unknown query: {query_id}")
            metrics.status = QueryStatus.STARTING
            metrics.start_time = datetime.now()

        self.signals.emit("streaming.before_start", query_id=query_id, name=metrics.name)

        try:
            query = query_builder()
            with self._lock:
                self._query_objects[query_id] = query
                metrics.status = QueryStatus.RUNNING

            self.signals.emit("streaming.after_start",
                query_id=query_id,
                spark_query_id=query.id
            )
            return query

        except Exception as e:
            with self._lock:
                metrics.status = QueryStatus.FAILED
                metrics.errors.append(str(e))

            self.signals.emit("streaming.start_failed",
                query_id=query_id,
                error=str(e)
            )
            raise

    def stop_query(self, query_id: str, await_termination: bool = True):
        """Stop a streaming query gracefully."""
        with self._lock:
            query = self._query_objects.get(query_id)
            if not query:
                return

        self.signals.emit("streaming.before_stop", query_id=query_id)

        query.stop()
        if await_termination:
            query.awaitTermination()

        with self._lock:
            self._queries[query_id].status = QueryStatus.TERMINATED
            del self._query_objects[query_id]

        self.signals.emit("streaming.after_stop", query_id=query_id)

    def get_metrics(self, query_id: str) -> Optional[QueryMetrics]:
        """Get current metrics for a query."""
        with self._lock:
            return self._queries.get(query_id)

    def get_all_metrics(self) -> Dict[str, QueryMetrics]:
        """Get metrics for all queries."""
        with self._lock:
            return dict(self._queries)

class StreamingWatchdog:
    """Monitors streaming queries and detects issues."""

    def __init__(
        self,
        query_manager: StreamingQueryManager,
        signal_provider: SignalProvider,
        config: StreamingConfig
    ):
        self.manager = query_manager
        self.signals = signal_provider
        self.config = config
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Start the watchdog monitoring thread."""
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()
        self.signals.emit("streaming.watchdog_started")

    def stop(self):
        """Stop the watchdog."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        self.signals.emit("streaming.watchdog_stopped")

    def _monitor_loop(self):
        """Main monitoring loop."""
        while self._running:
            try:
                self._check_queries()
            except Exception as e:
                self.signals.emit("streaming.watchdog_error", error=str(e))

            time.sleep(self.config.health_check_interval_seconds)

    def _check_queries(self):
        """Check health of all queries."""
        now = datetime.now()

        for query_id, metrics in self.manager.get_all_metrics().items():
            if metrics.status != QueryStatus.RUNNING:
                continue

            # Check for stale queries (no progress)
            if metrics.last_progress_time:
                stale_duration = (now - metrics.last_progress_time).total_seconds()
                if stale_duration > self.config.stale_threshold_seconds:
                    self.signals.emit("streaming.query_stale",
                        query_id=query_id,
                        last_progress_seconds_ago=stale_duration
                    )

            # Check if query has terminated unexpectedly
            query_obj = self.manager._query_objects.get(query_id)
            if query_obj and not query_obj.isActive:
                self.signals.emit("streaming.query_terminated",
                    query_id=query_id,
                    exception=query_obj.exception()
                )

class StreamingRecoveryManager:
    """Handles automatic recovery of failed streaming queries."""

    def __init__(
        self,
        query_manager: StreamingQueryManager,
        signal_provider: SignalProvider,
        config: StreamingConfig
    ):
        self.manager = query_manager
        self.signals = signal_provider
        self.config = config
        self._query_builders: Dict[str, Callable] = {}
        self._restart_delays: Dict[str, int] = {}

    def register_for_recovery(
        self,
        query_id: str,
        query_builder: Callable[[], 'StreamingQuery']
    ):
        """Register a query builder for automatic recovery."""
        self._query_builders[query_id] = query_builder
        self._restart_delays[query_id] = self.config.restart_delay_seconds

    def attempt_recovery(self, query_id: str) -> bool:
        """Attempt to recover a failed query."""
        metrics = self.manager.get_metrics(query_id)
        if not metrics:
            return False

        if metrics.restart_count >= self.config.max_restarts:
            self.signals.emit("streaming.recovery_exhausted",
                query_id=query_id,
                restart_count=metrics.restart_count
            )
            return False

        if query_id not in self._query_builders:
            return False

        # Calculate delay with exponential backoff
        delay = self._restart_delays.get(query_id, self.config.restart_delay_seconds)

        self.signals.emit("streaming.before_recovery",
            query_id=query_id,
            delay_seconds=delay,
            attempt=metrics.restart_count + 1
        )

        time.sleep(delay)

        try:
            metrics.status = QueryStatus.RECOVERING
            metrics.restart_count += 1

            self.manager.start_query(query_id, self._query_builders[query_id])

            # Update delay for next potential restart (exponential backoff)
            self._restart_delays[query_id] = int(delay * self.config.backoff_multiplier)

            self.signals.emit("streaming.after_recovery",
                query_id=query_id,
                restart_count=metrics.restart_count
            )
            return True

        except Exception as e:
            self.signals.emit("streaming.recovery_failed",
                query_id=query_id,
                error=str(e)
            )
            return False
```

#### Signal-Driven Stream Restart for Slowly Changing Dimensions

A common challenge in streaming is handling **slowly changing dimensions (SCDs)**. When a streaming query joins with a dimension table (e.g., customer attributes, product catalog, configuration):

- **Problem:** The dimension is read once at query start and cached
- **Issue:** When the dimension table updates, the streaming query uses stale data
- **Workarounds:** Stream-stream joins (complex), periodic restarts (manual), or ignoring staleness

**Proposed Solution:** Signal-driven restart mechanism that allows external events to trigger selective stream restarts when dependencies change.

```python
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Set, Optional, Callable
import threading

class RestartReason(Enum):
    DIMENSION_CHANGED = "dimension_changed"
    CONFIG_CHANGED = "config_changed"
    SCHEMA_EVOLVED = "schema_evolved"
    MANUAL_REQUEST = "manual_request"
    UPSTREAM_RESTARTED = "upstream_restarted"

@dataclass
class DimensionDependency:
    """Tracks a streaming query's dependency on a dimension table."""
    entity_id: str                    # e.g., "silver.customers"
    version_at_start: int             # Delta version when stream started
    check_interval_seconds: int = 60  # How often to check for changes
    restart_on_change: bool = True    # Auto-restart when dimension changes

@dataclass
class StreamDependencies:
    """Dependencies that can trigger a stream restart."""
    dimensions: List[DimensionDependency] = field(default_factory=list)
    config_keys: List[str] = field(default_factory=list)  # Config keys to watch
    upstream_queries: List[str] = field(default_factory=list)  # Other query IDs

class DimensionChangeDetector:
    """Monitors dimension tables for changes and emits signals."""

    def __init__(
        self,
        spark,
        signal_provider: SignalProvider,
        entity_provider: EntityProvider,
        check_interval_seconds: int = 60
    ):
        self.spark = spark
        self.signals = signal_provider
        self.entity_provider = entity_provider
        self.check_interval = check_interval_seconds
        self._watched_dimensions: Dict[str, int] = {}  # entity_id -> last_known_version
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def watch_dimension(self, entity_id: str):
        """Start watching a dimension table for changes."""
        current_version = self.entity_provider.get_entity_version(entity_id)
        self._watched_dimensions[entity_id] = current_version

        self.signals.emit("dimension.watch_started",
            entity_id=entity_id,
            initial_version=current_version
        )

    def unwatch_dimension(self, entity_id: str):
        """Stop watching a dimension table."""
        if entity_id in self._watched_dimensions:
            del self._watched_dimensions[entity_id]
            self.signals.emit("dimension.watch_stopped", entity_id=entity_id)

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
            for entity_id, last_version in list(self._watched_dimensions.items()):
                try:
                    current_version = self.entity_provider.get_entity_version(entity_id)

                    if current_version > last_version:
                        self._watched_dimensions[entity_id] = current_version

                        # Emit signal that dimension changed
                        self.signals.emit("dimension.changed",
                            entity_id=entity_id,
                            old_version=last_version,
                            new_version=current_version,
                            version_delta=current_version - last_version
                        )

                except Exception as e:
                    self.signals.emit("dimension.check_failed",
                        entity_id=entity_id,
                        error=str(e)
                    )

            time.sleep(self.check_interval)

class StreamRestartController:
    """Handles signal-driven restarts of streaming queries."""

    def __init__(
        self,
        query_manager: StreamingQueryManager,
        signal_provider: SignalProvider,
        dimension_detector: DimensionChangeDetector
    ):
        self.manager = query_manager
        self.signals = signal_provider
        self.dimension_detector = dimension_detector

        # Track dependencies per query
        self._query_dependencies: Dict[str, StreamDependencies] = {}
        self._query_builders: Dict[str, Callable] = {}

        # Subscribe to dimension change signals
        self.signals.get_signal("dimension.changed").connect(
            self._on_dimension_changed
        )

    def register_stream_with_dependencies(
        self,
        query_id: str,
        query_builder: Callable[[], 'StreamingQuery'],
        dependencies: StreamDependencies
    ):
        """Register a streaming query with its dimension dependencies."""
        self._query_dependencies[query_id] = dependencies
        self._query_builders[query_id] = query_builder

        # Start watching all dimension dependencies
        for dim_dep in dependencies.dimensions:
            self.dimension_detector.watch_dimension(dim_dep.entity_id)

        self.signals.emit("streaming.dependencies_registered",
            query_id=query_id,
            dimension_count=len(dependencies.dimensions),
            config_keys=dependencies.config_keys
        )

    def _on_dimension_changed(self, sender, **kwargs):
        """Handle dimension change signal - restart affected queries."""
        changed_entity = kwargs['entity_id']
        new_version = kwargs['new_version']

        affected_queries = self._find_affected_queries(changed_entity)

        if affected_queries:
            self.signals.emit("streaming.dimension_change_detected",
                entity_id=changed_entity,
                new_version=new_version,
                affected_queries=affected_queries
            )

        for query_id in affected_queries:
            self._schedule_restart(
                query_id,
                RestartReason.DIMENSION_CHANGED,
                trigger_entity=changed_entity,
                new_version=new_version
            )

    def _find_affected_queries(self, entity_id: str) -> List[str]:
        """Find all queries that depend on the changed entity."""
        affected = []
        for query_id, deps in self._query_dependencies.items():
            for dim_dep in deps.dimensions:
                if dim_dep.entity_id == entity_id and dim_dep.restart_on_change:
                    affected.append(query_id)
                    break
        return affected

    def _schedule_restart(
        self,
        query_id: str,
        reason: RestartReason,
        **context
    ):
        """Schedule a graceful restart of a streaming query."""
        self.signals.emit("streaming.restart_scheduled",
            query_id=query_id,
            reason=reason.value,
            **context
        )

        # Run restart in background thread to not block signal handler
        threading.Thread(
            target=self._execute_restart,
            args=(query_id, reason, context),
            daemon=True
        ).start()

    def _execute_restart(
        self,
        query_id: str,
        reason: RestartReason,
        context: dict
    ):
        """Execute the restart sequence."""
        try:
            self.signals.emit("streaming.before_restart",
                query_id=query_id,
                reason=reason.value,
                **context
            )

            # Stop the query gracefully
            self.manager.stop_query(query_id, await_termination=True)

            # Update dimension versions in dependencies
            deps = self._query_dependencies.get(query_id)
            if deps:
                for dim_dep in deps.dimensions:
                    dim_dep.version_at_start = self.dimension_detector._watched_dimensions.get(
                        dim_dep.entity_id, 0
                    )

            # Restart with fresh dimension data
            query_builder = self._query_builders.get(query_id)
            if query_builder:
                self.manager.start_query(query_id, query_builder)

                self.signals.emit("streaming.after_restart",
                    query_id=query_id,
                    reason=reason.value,
                    **context
                )

        except Exception as e:
            self.signals.emit("streaming.restart_failed",
                query_id=query_id,
                reason=reason.value,
                error=str(e)
            )

    def request_restart(
        self,
        query_id: str,
        reason: RestartReason = RestartReason.MANUAL_REQUEST,
        **context
    ):
        """Manually request a stream restart (API for external triggers)."""
        self._schedule_restart(query_id, reason, **context)

    def restart_graph_subset(
        self,
        root_query_ids: List[str],
        include_downstream: bool = True
    ):
        """Restart a subset of the streaming DAG.

        Args:
            root_query_ids: Starting queries to restart
            include_downstream: If True, also restart queries that depend on these
        """
        to_restart = set(root_query_ids)

        if include_downstream:
            # Find queries that list these as upstream dependencies
            for query_id, deps in self._query_dependencies.items():
                if any(up in root_query_ids for up in deps.upstream_queries):
                    to_restart.add(query_id)

        self.signals.emit("streaming.graph_restart_initiated",
            root_queries=root_query_ids,
            total_queries=list(to_restart),
            include_downstream=include_downstream
        )

        # Restart in reverse dependency order (downstream first, then upstream)
        # This ensures consumers are ready before producers restart
        for query_id in to_restart:
            self._schedule_restart(
                query_id,
                RestartReason.UPSTREAM_RESTARTED,
                triggered_by=root_query_ids
            )
```

**Usage Example: Streaming with Dimension Refresh**

```python
from kindling.streaming import (
    StreamingQueryManager, StreamRestartController,
    DimensionChangeDetector, StreamDependencies, DimensionDependency
)

# Initialize components
query_manager = StreamingQueryManager(spark, signal_provider, config)
dimension_detector = DimensionChangeDetector(spark, signal_provider, entity_provider)
restart_controller = StreamRestartController(query_manager, signal_provider, dimension_detector)

# Define a streaming query that depends on a customer dimension
def build_enriched_orders_stream():
    # Read the customer dimension (snapshot at stream start)
    customers_df = spark.read.format("delta").load("/data/silver/customers")

    # Read orders as a stream
    orders_stream = spark.readStream.format("delta").load("/data/bronze/orders")

    # Enrich orders with customer data (broadcast join)
    enriched = orders_stream.join(
        broadcast(customers_df),
        "customer_id",
        "left"
    )

    return (enriched.writeStream
        .format("delta")
        .option("checkpointLocation", "/checkpoints/enriched_orders")
        .start("/data/silver/enriched_orders"))

# Register with dimension dependencies
restart_controller.register_stream_with_dependencies(
    query_id="silver.enriched_orders",
    query_builder=build_enriched_orders_stream,
    dependencies=StreamDependencies(
        dimensions=[
            DimensionDependency(
                entity_id="silver.customers",
                version_at_start=0,  # Will be set on start
                check_interval_seconds=300,  # Check every 5 minutes
                restart_on_change=True
            )
        ]
    )
)

# Start the stream
query_manager.start_query("silver.enriched_orders", build_enriched_orders_stream)

# Start dimension monitoring
dimension_detector.start()

# Now when silver.customers table is updated (e.g., by a batch pipeline):
# 1. DimensionChangeDetector detects the version change
# 2. Emits "dimension.changed" signal
# 3. StreamRestartController receives signal
# 4. Finds affected query "silver.enriched_orders"
# 5. Schedules graceful restart
# 6. Stream restarts with fresh customer dimension data
```

**Signals for Dimension-Driven Restarts:**

| Signal | When | Key Payload Fields |
|--------|------|-------------------|
| `dimension.watch_started` | Dimension monitoring begins | entity_id, initial_version |
| `dimension.changed` | Dimension table updated | entity_id, old_version, new_version |
| `dimension.check_failed` | Version check fails | entity_id, error |
| `streaming.dimension_change_detected` | Change affects streams | entity_id, affected_queries |
| `streaming.restart_scheduled` | Restart queued | query_id, reason, trigger_entity |
| `streaming.before_restart` | About to restart | query_id, reason |
| `streaming.after_restart` | Restart complete | query_id, reason |
| `streaming.restart_failed` | Restart failed | query_id, error |
| `streaming.graph_restart_initiated` | Multi-query restart | root_queries, total_queries |

**Advanced: Cascading Restarts**

When a dimension change affects multiple streams in a dependency chain:

```
┌────────────────────┐
│  customers (dim)   │ ← Batch update triggers change
└─────────┬──────────┘
          │ dimension.changed
          ▼
┌────────────────────┐
│ enriched_orders    │ ← Restarts (depends on customers)
│    (stream)        │
└─────────┬──────────┘
          │ upstream_restarted
          ▼
┌────────────────────┐
│ order_aggregates   │ ← Also restarts (depends on enriched_orders)
│    (stream)        │
└────────────────────┘
```

The `restart_graph_subset()` method handles this by:
1. Identifying all downstream dependents
2. Restarting in reverse order (consumers first)
3. Ensuring data flow integrity

#### Option B: Event Loop-Based Async Monitoring

For high-frequency monitoring without blocking:

```python
import asyncio
from typing import Dict, Optional

class AsyncStreamingMonitor:
    """Async-based streaming monitor using event loop."""

    def __init__(
        self,
        signal_provider: SignalProvider,
        config: StreamingConfig
    ):
        self.signals = signal_provider
        self.config = config
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._tasks: Dict[str, asyncio.Task] = {}

    async def start(self):
        """Start the async monitoring loop."""
        self._loop = asyncio.get_event_loop()

        # Start background monitoring tasks
        self._tasks['health_check'] = asyncio.create_task(
            self._health_check_loop()
        )
        self._tasks['progress_emit'] = asyncio.create_task(
            self._progress_signal_loop()
        )

    async def stop(self):
        """Stop all monitoring tasks."""
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    async def _health_check_loop(self):
        """Async health check loop."""
        while True:
            await self._check_all_queries()
            await asyncio.sleep(self.config.health_check_interval_seconds)

    async def _check_all_queries(self):
        """Check health of all queries (async)."""
        # Implementation similar to sync version but with async operations
        pass

    async def _progress_signal_loop(self):
        """Emit periodic progress signals."""
        while True:
            if self.config.emit_progress_signals:
                # Emit progress for all running queries
                pass
            await asyncio.sleep(self.config.progress_signal_interval_seconds)
```

#### Spark StreamingQueryListener Integration

```python
from pyspark.sql.streaming import StreamingQueryListener

class KindlingStreamingListener(StreamingQueryListener):
    """PySpark listener that bridges to Kindling signal system."""

    def __init__(
        self,
        signal_provider: SignalProvider,
        query_manager: StreamingQueryManager
    ):
        self.signals = signal_provider
        self.manager = query_manager

    def onQueryStarted(self, event):
        """Called when a query is started."""
        self.signals.emit("streaming.spark_query_started",
            query_id=event.id,
            run_id=event.runId,
            name=event.name
        )

    def onQueryProgress(self, event):
        """Called when there is progress in a query."""
        progress = event.progress

        # Update metrics
        # ... (update query_manager metrics)

        self.signals.emit("streaming.query_progress",
            query_id=progress.id,
            batch_id=progress.batchId,
            input_rows_per_second=progress.inputRowsPerSecond,
            processed_rows_per_second=progress.processedRowsPerSecond,
            num_input_rows=progress.numInputRows
        )

    def onQueryTerminated(self, event):
        """Called when a query is terminated."""
        exception = event.exception

        self.signals.emit("streaming.spark_query_terminated",
            query_id=event.id,
            run_id=event.runId,
            exception=str(exception) if exception else None
        )

        # Trigger recovery if configured
        if exception and self.manager.config.auto_restart:
            # Schedule recovery (don't block listener)
            threading.Thread(
                target=self._trigger_recovery,
                args=(event.id,),
                daemon=True
            ).start()

    def _trigger_recovery(self, query_id: str):
        """Trigger recovery in background thread."""
        # ... recovery logic
        pass
```

---

## Integration Points

### How the Three Features Work Together

```
┌─────────────────────────────────────────────────────────────┐
│                      User Code / App                         │
│  DataPipes.pipe(...), streaming config, etc.                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                  ExecutionOrchestrator                       │
│  - Uses DAG to order pipes                                   │
│  - Emits signals for each phase                              │
│  - Coordinates batch and streaming execution                 │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┴───────────────────┐
          │                                       │
          ▼                                       ▼
┌───────────────────────┐             ┌───────────────────────┐
│   BatchExecutor       │             │ StreamingOrchestrator │
│   (Batch pipes)       │             │ (Streaming pipes)     │
│                       │             │                       │
│ Signals:              │             │ Signals:              │
│ - before_generation   │             │ - before_start        │
│ - after_generation    │             │ - query_progress      │
│ - pipe_completed      │             │ - query_terminated    │
└───────────────────────┘             └───────────────────────┘
          │                                       │
          │                                       │
          └───────────────┬───────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   Signal Subscribers                         │
│  - Logging/Monitoring                                        │
│  - Alerting (failures, stale queries)                        │
│  - Metrics collection (Prometheus, etc.)                     │
│  - Dashboard updates                                         │
│  - Watermark updates (post-batch completion)                 │
└─────────────────────────────────────────────────────────────┘
```

### Example: Complete Pipeline with All Features

```python
from kindling import (
    DataPipes, DataEntities,
    ExecutionOrchestrator, ExecutionMode, ExecutionConfig,
    StreamingOrchestrator, StreamingConfig,
    BlinkerSignalProvider
)

# 1. Define entities and pipes
@DataEntities.entity(entityid="bronze.events", ...)
@DataEntities.entity(entityid="silver.events", ...)
@DataEntities.entity(entityid="gold.metrics", ...)

@DataPipes.pipe(pipeid="bronze.ingest", input_entity_ids=[], output_entity_id="bronze.events", ...)
@DataPipes.pipe(pipeid="silver.transform", input_entity_ids=["bronze.events"], output_entity_id="silver.events", ...)
@DataPipes.pipe(pipeid="gold.aggregate", input_entity_ids=["silver.events"], output_entity_id="gold.metrics", ...)

# 2. Set up signal subscribers
signal_provider = GlobalInjector.get(BlinkerSignalProvider)

# Monitoring subscriber
@signal_provider.get_signal("datapipes.after_pipe").connect
def log_pipe_completion(sender, **kwargs):
    logger.info(f"Pipe {kwargs['pipe_id']} completed in {kwargs['duration_seconds']}s")

# Alerting subscriber
@signal_provider.get_signal("streaming.query_stale").connect
def alert_stale_query(sender, **kwargs):
    send_alert(f"Query {kwargs['query_id']} is stale!")

# 3. Execute batch pipelines with DAG ordering
exec_config = ExecutionConfig(
    mode=ExecutionMode.BATCH,
    max_parallelism=4,
    enable_caching=True
)
orchestrator = ExecutionOrchestrator(exec_config)
orchestrator.run(["bronze.ingest", "silver.transform", "gold.aggregate"])

# 4. Start streaming pipelines with monitoring
streaming_config = StreamingConfig(
    auto_restart=True,
    max_restarts=3,
    health_check_interval_seconds=30
)
streaming_orch = StreamingOrchestrator(streaming_config)

streaming_orch.start_pipeline("realtime.events",
    source=event_stream,
    transformations=[...],
    sink=delta_sink
)

# Watchdog monitors and auto-recovers
streaming_orch.start_watchdog()
```

---

## Part 4: Transform Version Lineage & Tracking

### Motivation

When transforms are packaged and deployed, tracking which version of a transform produced each record becomes critical for:

- **Debugging:** Identifying which version introduced a data quality issue
- **Compliance:** Proving which algorithm version computed regulated calculations
- **Reprocessing:** Selectively reprocessing data affected by buggy versions
- **Audit:** Maintaining complete data lineage for governance

### Proposed Architecture

#### Core Concept: Version Stack Trace

Each record carries metadata about the transform versions that processed it, similar to distributed tracing in microservices but applied to data pipelines.

#### Implementation Strategy: Hybrid Staged Approach

Track version metadata at medallion layer boundaries (bronze → silver → gold) rather than every transform:

```python
# Bronze record (after ingestion)
{
  "customer_id": 123,
  "name": "John Doe",
  "_bronze_lineage": {
    "pipe": "bronze.ingest",
    "package": "my-transforms",
    "version": "0.3.0",
    "timestamp": "2026-01-30T10:15:00Z"
  }
}

# Silver record (after cleansing)
{
  "customer_id": 123,
  "name": "John Doe",
  "_bronze_lineage": {...},
  "_silver_lineage": {
    "pipe": "silver.cleanse",
    "package": "my-transforms",
    "version": "0.3.1",
    "timestamp": "2026-01-30T10:16:00Z"
  }
}

# Gold record (after aggregation)
{
  "customer_id": 123,
  "lifetime_value": 50000,
  "_silver_lineage": {...},
  "_gold_lineage": {
    "pipe": "gold.aggregate",
    "package": "my-transforms",
    "version": "0.3.1",
    "timestamp": "2026-01-30T10:17:00Z"
  }
}
```

**Benefits of Staged Approach:**
- ✅ Fixed storage overhead (one struct per layer, not unbounded array)
- ✅ Aligns with medallion architecture
- ✅ Easy queries: `WHERE _silver_lineage.version = 'v0.3.1'`
- ✅ Tracks critical layer transitions

#### Versioned Execution Context

```python
from importlib.metadata import version, PackageNotFoundError
from pyspark.sql.functions import lit, struct, current_timestamp
from dataclasses import dataclass
from typing import Optional

@dataclass
class LineageMetadata:
    """Metadata for a single transform execution."""
    pipe_id: str
    package_name: str
    package_version: str
    layer: str  # "bronze", "silver", "gold"
    timestamp: str

class VersionedExecutionContext:
    """Tracks package versions during pipeline execution."""

    def __init__(self, package_name: str = "kindling"):
        """Initialize with package name to track."""
        self.package_name = package_name
        try:
            self.version = version(package_name)
        except PackageNotFoundError:
            self.version = "dev"

    def add_lineage_column(
        self,
        df: DataFrame,
        pipe_id: str,
        layer: str
    ) -> DataFrame:
        """Add version lineage metadata to DataFrame.

        Args:
            df: Input DataFrame
            pipe_id: Pipe identifier (e.g., "silver.cleanse")
            layer: Medallion layer ("bronze", "silver", "gold")

        Returns:
            DataFrame with added _{layer}_lineage column
        """
        col_name = f"_{layer}_lineage"

        lineage_struct = struct(
            lit(pipe_id).alias("pipe"),
            lit(self.package_name).alias("package"),
            lit(self.version).alias("version"),
            current_timestamp().alias("timestamp")
        )

        return df.withColumn(col_name, lineage_struct)

    def get_lineage_metadata(self, pipe_id: str, layer: str) -> LineageMetadata:
        """Get lineage metadata for signals/logging."""
        return LineageMetadata(
            pipe_id=pipe_id,
            package_name=self.package_name,
            package_version=self.version,
            layer=layer,
            timestamp=datetime.now().isoformat()
        )
```

#### Integration with DataPipesExecuter

```python
from kindling.injection import inject

class DataPipesExecuter:
    """Enhanced with version tracking."""

    @inject
    def __init__(
        self,
        signal_provider: SignalProvider = None,
        config_service: ConfigService = None,
        package_name: str = None  # New: package name for versioning
    ):
        self.signals = signal_provider
        self.config = config_service

        # Initialize version tracking
        if package_name:
            self.version_ctx = VersionedExecutionContext(package_name)
        else:
            self.version_ctx = None

    def run_datapipes(
        self,
        pipes: List[str],
        track_lineage: bool = True  # New: opt-in lineage tracking
    ) -> None:
        """Execute pipes with optional lineage tracking."""

        for pipe_id in pipes:
            # Execute pipe
            df = self._execute_pipe(pipe_id)

            # Add lineage metadata if enabled
            if track_lineage and self.version_ctx:
                layer = self._detect_layer(pipe_id)
                df = self.version_ctx.add_lineage_column(df, pipe_id, layer)

                # Emit signal with version info
                lineage = self.version_ctx.get_lineage_metadata(pipe_id, layer)
                self.signals.emit("datapipes.lineage_applied",
                    pipe_id=pipe_id,
                    package=lineage.package_name,
                    version=lineage.package_version,
                    layer=lineage.layer
                )

            # Persist result
            self._persist(df)

    def _detect_layer(self, pipe_id: str) -> str:
        """Detect medallion layer from pipe ID."""
        if pipe_id.startswith("bronze."):
            return "bronze"
        elif pipe_id.startswith("silver."):
            return "silver"
        elif pipe_id.startswith("gold."):
            return "gold"
        else:
            return "unknown"
```

#### Configuration

```yaml
# settings.yaml
kindling:
  lineage:
    enabled: true
    package_name: "my-transforms"  # Auto-detect if not specified
    include_layers:
      - bronze
      - silver
      - gold
    optimize_storage: true  # Use Delta column statistics
```

### Usage Patterns

#### Pattern 1: Incremental Reprocessing

```python
# Find records processed by buggy version
buggy_records = spark.sql("""
    SELECT *
    FROM gold.customer_metrics
    WHERE _silver_lineage.version = 'my-transforms@0.3.1'
      AND _silver_lineage.pipe = 'silver.cleanse'
""")

# Reprocess with fixed version (0.3.2)
from my_transforms.silver import cleanse_customers_v032

fixed_records = cleanse_customers_v032(buggy_records)

# Merge back with version update
fixed_records.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "_silver_lineage.version = 'my-transforms@0.3.1'") \
    .save("gold/customer_metrics")
```

#### Pattern 2: Version Audit Report

```sql
-- Version adoption across layers
SELECT
  _silver_lineage.pipe as transform,
  _silver_lineage.version,
  COUNT(*) as record_count,
  MIN(_silver_lineage.timestamp) as first_processed,
  MAX(_silver_lineage.timestamp) as last_processed,
  DATEDIFF(day, MIN(_silver_lineage.timestamp), CURRENT_DATE()) as days_old
FROM gold.customer_metrics
GROUP BY _silver_lineage.pipe, _silver_lineage.version
ORDER BY days_old DESC
```

#### Pattern 3: Compliance Reporting

```python
# Prove which algorithm version computed regulated metric
compliance_report = spark.sql("""
    SELECT
      customer_id,
      risk_score,
      _gold_lineage.version as algorithm_version,
      _gold_lineage.timestamp as calculation_timestamp
    FROM gold.risk_scores
    WHERE customer_id = '12345'
      AND _gold_lineage.pipe = 'gold.calculate_risk'
""")

# Generate audit trail
compliance_report.write \
    .format("json") \
    .save(f"audit/risk_score_lineage_{customer_id}.json")
```

#### Pattern 4: Delta Lake Time Travel Integration

```python
# Correlate data version with transform version
historical_data = spark.read.format("delta") \
    .option("versionAsOf", 10) \
    .load("silver/customers")

# Check which transform version was active at that Delta version
print(historical_data.select("_silver_lineage.version").distinct().show())
```

### Signal Integration

New signals emitted during lineage tracking:

| Signal | When | Payload Fields |
|--------|------|----------------|
| `datapipes.lineage_applied` | After adding lineage metadata | pipe_id, package, version, layer, record_count |
| `datapipes.version_changed` | Transform version changes | pipe_id, old_version, new_version, layer |
| `datapipes.lineage_audit` | Periodic lineage health check | versions_active, oldest_version, stale_count |

```python
# Subscribe to version changes
@signal_provider.get_signal("datapipes.version_changed").connect
def alert_version_change(sender, **kwargs):
    logger.warning(
        f"Transform version changed: {kwargs['pipe_id']} "
        f"from {kwargs['old_version']} to {kwargs['new_version']}"
    )

    # Trigger reprocessing job
    if kwargs['layer'] == 'gold':
        schedule_reprocessing(kwargs['pipe_id'], kwargs['old_version'])
```

### Streaming Integration

For streaming queries, track version in checkpoint metadata:

```python
class StreamingQueryManager:
    """Enhanced with version tracking."""

    def start_query(
        self,
        query_id: str,
        query_builder: Callable,
        version_context: VersionedExecutionContext = None
    ) -> 'StreamingQuery':
        """Start streaming query with version tracking."""

        # Add version metadata to streaming output
        def versioned_query_builder():
            query = query_builder()

            if version_context:
                # Store version in checkpoint metadata
                query.checkpoint_metadata = {
                    "query_version": version_context.version,
                    "package": version_context.package_name,
                    "started_at": datetime.now().isoformat()
                }

            return query

        return super().start_query(query_id, versioned_query_builder)
```

### Performance Considerations

#### Storage Overhead

- **Struct column:** ~50-80 bytes per record per layer
- **Delta optimization:** Use Z-ordering on lineage columns
- **Partition pruning:** Filter by version enables efficient queries

```python
# Optimize table for lineage queries
spark.sql("""
    OPTIMIZE gold.customer_metrics
    ZORDER BY (_silver_lineage.version)
""")
```

#### Write Performance Impact

- **Measured overhead:** 3-7% additional write time
- **Mitigation:** Lineage columns are metadata (small)
- **Benefit:** Saves hours debugging production issues

#### Query Performance

```python
# Efficient: Pushdown filter on version
df = spark.read.format("delta") \
    .load("gold/metrics") \
    .filter(col("_silver_lineage.version") == "v0.3.1")

# Inefficient: Scanning without filter
df = spark.read.format("delta") \
    .load("gold/metrics") \
    .filter(col("_silver_lineage.timestamp") > "2026-01-01")
```

### Migration Strategy

#### For Existing Tables

```python
# Backfill lineage for existing data
def backfill_lineage(table_path: str, layer: str, version: str):
    """Add lineage metadata to existing table."""
    df = spark.read.format("delta").load(table_path)

    # Add lineage if not present
    lineage_col = f"_{layer}_lineage"
    if lineage_col not in df.columns:
        df = df.withColumn(lineage_col, struct(
            lit("backfilled").alias("pipe"),
            lit("my-transforms").alias("package"),
            lit(version).alias("version"),
            current_timestamp().alias("timestamp")
        ))

        df.write.format("delta").mode("overwrite").save(table_path)

# Usage
backfill_lineage("gold/customer_metrics", "gold", "v0.3.0")
```

#### Backward Compatibility

- **Opt-in:** `track_lineage=False` by default for existing pipelines
- **Gradual rollout:** Enable per pipe or per layer
- **Schema evolution:** Delta Lake handles new columns gracefully

### Alternative Approaches Considered

#### Option A: Full Stack Trace (Not Recommended)

```python
# Unbounded array grows with every transform
_transform_stack: [
    "bronze.ingest@v0.3.0",
    "bronze.validate@v0.3.0",
    "silver.cleanse@v0.3.1",
    "silver.enrich@v0.3.1",
    "gold.aggregate@v0.3.1"
]
```

**Rejected because:** Storage overhead grows unbounded, harder to query

#### Option B: Replace (Current Only) (Not Recommended)

```python
# Only track latest transform
_current_version: "gold.aggregate@v0.3.1"
```

**Rejected because:** Loses lineage across layers, can't trace back to source

#### Option C: Hybrid Staged (Recommended) ✅

```python
# Track per medallion layer (implemented above)
_bronze_lineage: {...}
_silver_lineage: {...}
_gold_lineage: {...}
```

**Selected because:** Balances lineage depth with storage efficiency

### Future Enhancements

1. **ML Model Versioning:** Track which model version generated predictions
2. **Schema Version Tracking:** Link data version to schema version
3. **Cross-Table Lineage:** Track joins between tables with version awareness
4. **Lineage Visualization:** Graph UI showing data flow with versions
5. **Automated Reprocessing:** Auto-trigger reprocessing when new version deployed

---

## Implementation Roadmap

### Phase 1: Signal Enhancements (2-3 weeks)

1. Add `SignalEmitter` base class with `emit()` helper
2. Add `EMITS` declaration convention
3. Update `before_`/`after_` naming convention across codebase
4. Add signal emissions to `DataPipesExecuter` (critical path)
5. Add signal emissions to `DataEntityManager`
6. Write unit tests for signal patterns

### Phase 2: DAG Execution (3-4 weeks)

1. Create `pipe_graph.py` module with `PipeGraphBuilder`
2. Implement `BatchExecutionStrategy` and `StreamingExecutionStrategy`
3. Implement `GenerationExecutor` with sequential execution
4. Update `DataPipesExecuter` to use graph-based planning
5. Add parallel execution within generations (ThreadPoolExecutor)
6. Add cache recommendations for shared entities
7. Write integration tests for DAG execution

### Phase 3: Structured Streaming (4-5 weeks)

1. Create `streaming.py` module with core components
2. Implement `StreamingQueryManager` for lifecycle management
3. Implement `StreamingWatchdog` for monitoring
4. Implement `StreamingRecoveryManager` for auto-restart
5. Create `KindlingStreamingListener` for PySpark integration
6. Add signal emissions for streaming events
7. Integrate with DAG execution for hybrid mode
8. Write system tests for streaming scenarios

### Phase 4: Integration & Polish (2-3 weeks)

1. Integration testing across all three features
2. Documentation updates
3. Example notebooks demonstrating usage
4. Performance optimization
5. Error handling improvements

---

## Summary

| Feature | Recommended Option | Rationale |
|---------|-------------------|-----------|
| **Signal Eventing** | Option C (Hybrid) | Type-safe for core, flexible for extensions |
| **DAG Execution** | Option 1 (Strategy Pattern) | Clean separation, easy to add new strategies |
| **Streaming Support** | Option A (Comprehensive) | Full-featured, sync-based fits Spark model |

**Total Estimated Timeline:** 11-15 weeks

**Key Design Principles:**
1. Signals provide loose coupling between components
2. DAG execution is strategy-based for flexibility
3. Streaming monitoring runs on driver, uses sync signals for reliability
4. All features integrate via the signal system for observability
