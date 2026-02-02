# DAG/Execution Framework Implementation Plan

> **Created:** 2026-02-02
> **Status:** Planned
> **Reference:** [signal_dag_streaming_proposal.md](signal_dag_streaming_proposal.md)

## Current State Assessment

### What Already Exists

| Component | Location | Status |
|-----------|----------|--------|
| `SignalProvider` and `SignalEmitter` abstractions | `signaling.py` | ✅ Complete |
| `BlinkerSignalProvider` implementation | `signaling.py` | ✅ Complete |
| Basic signal emissions in `DataPipesExecuter` | `data_pipes.py` | ✅ Complete |
| Basic signal emissions in `DataEntityManager` | `data_entities.py` | ✅ Complete |
| `EMITS` class attribute convention | `signaling.py` | ✅ Complete |
| `SignalPayload` base class | `signaling.py` | ✅ Complete |
| Basic `SimplePipeStreamOrchestrator` | `pipe_streaming.py` | ✅ Basic |
| Unit tests for signaling module | `tests/unit/test_signaling.py` | ✅ Complete |

### What Needs to Be Built

- DAG-based execution planning and orchestration
- Execution strategies (batch vs streaming)
- Parallel generation execution
- Streaming query lifecycle management
- Streaming watchdog and recovery
- Dimension change detection and stream restart
- Transform version lineage tracking

---

## Phase 1: Signal Enhancements (Foundation)

**Estimated Time:** 2-3 weeks
**Dependencies:** None (foundation layer)

### 1.1 Add Typed Signal Payload Classes

Create typed payload classes for all existing signals to improve type safety and documentation.

```
- [ ] Create `PipeRunPayload` dataclass (for before_run/after_run)
- [ ] Create `PipeExecutePayload` dataclass (for before_pipe/after_pipe)
- [ ] Create `PipeFailedPayload` dataclass (for pipe_failed/run_failed)
- [ ] Create `EntityRegisterPayload` dataclass
- [ ] Create `EntityResolvePayload` dataclass
- [ ] Update existing emit() calls to use typed payloads
```

### 1.2 Add Async Signal Support

Optional async emission for non-blocking signal handling.

```
- [ ] Create `AsyncSignalEmitter` mixin with `emit_async()` method
- [ ] Add ThreadPoolExecutor-based async emission
- [ ] Add configuration for async vs sync mode
```

### 1.3 Signal Naming Convention Enforcement

Standardize all signal names.

```
- [ ] Update all signals to use `before_`/`after_` naming convention
- [ ] Add signal name validation in SignalEmitter.emit()
- [ ] Document naming conventions
```

### 1.4 Signal Documentation Tooling

Enable introspection and documentation generation.

```
- [ ] Create signal registry introspection methods
- [ ] Add get_all_signals() to SignalProvider
- [ ] Add list_emitters() to get all classes with EMITS
- [ ] Create docs/signals_reference.md auto-generated file
```

### 1.5 Unit Tests for Signal Enhancements

```
- [ ] Test typed payload serialization
- [ ] Test signal naming validation
- [ ] Test signal introspection methods
- [ ] Test async emission
```

---

## Phase 2: DAG Execution Services (Core)

**Estimated Time:** 3-4 weeks
**Dependencies:** Phase 1

### 2.1 Create pipe_graph.py Module

Core graph building and dependency resolution.

```
- [ ] Create PipeGraphBuilder class
    - [ ] build_graph(pipe_ids: List[str]) -> DAG method
    - [ ] Dependency resolution via input_entity_ids/output_entity_id
    - [ ] Cycle detection with clear error messages
- [ ] Create TopologicalSorter utility
- [ ] Create graph data structures (PipeNode, PipeEdge, PipeGraph)
- [ ] Add graph visualization helpers (for debugging)
```

**Key Design:**
```python
class PipeGraphBuilder:
    def build_graph(self, pipe_ids: List[str]) -> PipeGraph:
        """Build dependency graph from pipe definitions."""

    def detect_cycles(self, graph: PipeGraph) -> List[List[str]]:
        """Return list of cycles if any exist."""

    def get_generations(self, graph: PipeGraph) -> List[List[str]]:
        """Return pipes grouped by parallel execution generation."""
```

### 2.2 Create execution_strategy.py Module

Strategy pattern for different execution modes.

```
- [ ] Create ExecutionStrategy ABC
    - [ ] plan(graph: PipeGraph) -> ExecutionPlan abstract method
- [ ] Implement BatchExecutionStrategy
    - [ ] Forward topological sort (sources → sinks)
    - [ ] Group into parallel generations
- [ ] Implement StreamingExecutionStrategy
    - [ ] Reverse topological sort (sinks → sources)
    - [ ] Cascading checkpoint dependencies
```

**Key Design:**
```python
class ExecutionStrategy(ABC):
    @abstractmethod
    def plan(self, graph: PipeGraph) -> ExecutionPlan:
        """Generate execution plan from graph."""
        pass

class BatchExecutionStrategy(ExecutionStrategy):
    """Forward topological sort - sources first, sinks last."""

class StreamingExecutionStrategy(ExecutionStrategy):
    """Reverse topological sort - sinks first (start consumers before producers)."""
```

### 2.3 Create ExecutionPlanGenerator Class

Facade for plan generation.

```
- [ ] Strategy injection (BatchStrategy or StreamingStrategy)
- [ ] generate_plan(pipe_ids) -> ExecutionPlan method
- [ ] Plan visualization/debugging output
- [ ] Plan validation
```

### 2.4 Create GenerationExecutor Class

Execute pipes within a generation (potentially in parallel).

```
- [ ] Sequential execution mode (default)
- [ ] Parallel execution mode (ThreadPoolExecutor)
- [ ] Max workers configuration
- [ ] Per-pipe timeout handling
- [ ] Result aggregation
- [ ] Error handling (fail-fast vs continue)
```

**Key Design:**
```python
class GenerationExecutor:
    def __init__(self, max_workers: int = 4, parallel: bool = False):
        self.max_workers = max_workers
        self.parallel = parallel

    def execute_generation(
        self,
        generation: List[str],
        pipe_executor: Callable[[str], None]
    ) -> GenerationResult:
        """Execute all pipes in a generation."""
```

### 2.5 Create ExecutionOrchestrator Class

Top-level coordination of DAG execution.

```
- [ ] Coordinate graph building, planning, and execution
- [ ] Emit orchestration signals:
    - [ ] orchestrator.plan_generated
    - [ ] orchestrator.generation_started
    - [ ] orchestrator.generation_completed
    - [ ] orchestrator.execution_completed
- [ ] Integration with existing DataPipesExecuter
```

**Key Design:**
```python
class ExecutionOrchestrator(SignalEmitter):
    EMITS = [
        "orchestrator.plan_generated",
        "orchestrator.generation_started",
        "orchestrator.generation_completed",
        "orchestrator.execution_completed",
    ]

    def execute(self, pipe_ids: List[str], strategy: ExecutionStrategy = None):
        """Execute pipes using DAG-based planning."""
```

### 2.6 Update DataPipesExecuter for DAG Support

Backward-compatible integration.

```
- [ ] Add optional DAG mode (preserve backward compatibility)
- [ ] Add run_datapipes_dag(pipe_ids, strategy) method
- [ ] Emit signals for DAG execution phases
- [ ] Add use_dag=False parameter to run_datapipes() for opt-in
```

### 2.7 Add Cache Recommendations for Shared Entities

Optimize execution by identifying reusable DataFrames.

```
- [ ] Detect entities read by multiple pipes in same execution
- [ ] Emit cache_recommended signal with entity_id
- [ ] Optional auto-caching behavior (configurable)
- [ ] Track cache hits/misses for optimization
```

### 2.8 Integration Tests for DAG Execution

```
- [ ] Test topological sorting (forward and reverse)
- [ ] Test cycle detection with clear error messages
- [ ] Test parallel generation execution
- [ ] Test streaming strategy (reverse sort)
- [ ] Test cache recommendations
- [ ] Test backward compatibility with existing run_datapipes()
```

---

## Phase 3: Structured Streaming Support

**Estimated Time:** 4-5 weeks
**Dependencies:** Phase 1, Phase 2

### 3.1 Enhance pipe_streaming.py Module

Build on existing `SimplePipeStreamOrchestrator`.

```
- [ ] Create StreamingQueryInfo dataclass
    - [ ] query_id, start_time, checkpoint_path, status
    - [ ] restart_count, last_error, metrics
- [ ] Create StreamingQueryState enum (STARTING, RUNNING, STOPPING, STOPPED, FAILED)
```

### 3.2 Implement StreamingQueryManager

Lifecycle management for streaming queries.

```
- [ ] start_query(query_id, builder_fn, config) method
- [ ] stop_query(query_id, await_termination) method
- [ ] restart_query(query_id) method
- [ ] get_query_status(query_id) method
- [ ] list_active_queries() method
- [ ] Query state tracking dict
- [ ] Signal emissions:
    - [ ] streaming.query_started
    - [ ] streaming.query_stopped
    - [ ] streaming.query_restarted
    - [ ] streaming.query_failed
```

**Key Design:**
```python
class StreamingQueryManager(SignalEmitter):
    EMITS = [
        "streaming.query_started",
        "streaming.query_stopped",
        "streaming.query_restarted",
        "streaming.query_failed",
    ]

    def start_query(
        self,
        query_id: str,
        query_builder: Callable[[], StreamingQuery],
        config: StreamingQueryConfig = None
    ) -> StreamingQueryInfo:
        """Start a streaming query with lifecycle tracking."""
```

### 3.3 Implement StreamingWatchdog

Health monitoring for active queries.

```
- [ ] Monitor thread that checks query health
- [ ] Configurable polling interval (default 30s)
- [ ] Query.status.isDataAvailable checking
- [ ] Query.exception() checking
- [ ] Query.lastProgress metrics collection
- [ ] Signal emissions:
    - [ ] streaming.query_healthy
    - [ ] streaming.query_unhealthy
    - [ ] streaming.query_exception
    - [ ] streaming.query_stalled (no progress)
```

**Key Design:**
```python
class StreamingWatchdog:
    def __init__(
        self,
        query_manager: StreamingQueryManager,
        poll_interval: int = 30,
        stall_threshold: int = 300  # 5 minutes
    ):
        self._running = False
        self._monitor_thread = None

    def start(self):
        """Start monitoring thread."""

    def stop(self):
        """Stop monitoring thread."""
```

### 3.4 Implement StreamingRecoveryManager

Auto-restart with exponential backoff.

```
- [ ] Auto-restart with exponential backoff (1s, 2s, 4s, 8s...)
- [ ] Max retry configuration (default 5)
- [ ] Cooldown period tracking
- [ ] Dead letter handling for failed queries
- [ ] Recovery strategy configuration (restart vs alert vs ignore)
- [ ] Signal emissions:
    - [ ] streaming.recovery_attempted
    - [ ] streaming.recovery_succeeded
    - [ ] streaming.recovery_failed
    - [ ] streaming.recovery_exhausted
```

**Key Design:**
```python
class StreamingRecoveryManager(SignalEmitter):
    EMITS = [
        "streaming.recovery_attempted",
        "streaming.recovery_succeeded",
        "streaming.recovery_failed",
        "streaming.recovery_exhausted",
    ]

    def __init__(
        self,
        max_retries: int = 5,
        initial_backoff: float = 1.0,
        max_backoff: float = 300.0,
        backoff_multiplier: float = 2.0
    ):
        pass
```

### 3.5 Implement Signal-Driven Stream Restart

Dimension change detection and cascading restart.

```
- [ ] Create DimensionChangeDetector
    - [ ] Subscribe to entity change signals
    - [ ] Detect SCD Type 2 changes (new records in dimension)
    - [ ] Track dimension→stream dependencies
- [ ] Create StreamRestartController
    - [ ] Map dimension entities to dependent streams
    - [ ] Coordinate cascading restarts
    - [ ] Configurable restart delay (batch updates)
- [ ] Signal emissions:
    - [ ] streaming.dimension_changed
    - [ ] streaming.restart_triggered
    - [ ] streaming.cascade_restart_started
    - [ ] streaming.cascade_restart_completed
```

**Key Design:**
```python
class DimensionChangeDetector(SignalEmitter):
    """Detects changes to slowly-changing dimension tables."""

    def register_dimension(
        self,
        entity_id: str,
        change_detection: Literal["watermark", "row_count", "hash"]
    ):
        """Register a dimension entity for change detection."""

class StreamRestartController(SignalEmitter):
    """Coordinates stream restarts when dimensions change."""

    def register_dependency(
        self,
        stream_query_id: str,
        dimension_entity_ids: List[str]
    ):
        """Register stream's dependency on dimension entities."""
```

### 3.6 Create KindlingStreamingListener

PySpark StreamingQueryListener integration.

```
- [ ] Extend StreamingQueryListener
- [ ] onQueryStarted() → emit streaming.spark_query_started signal
- [ ] onQueryProgress() → emit streaming.spark_query_progress signal with metrics
- [ ] onQueryTerminated() → emit streaming.spark_query_terminated signal
- [ ] Automatic registration with SparkSession
```

**Key Design:**
```python
class KindlingStreamingListener(StreamingQueryListener, SignalEmitter):
    """Bridge between Spark's streaming events and Kindling signals."""

    EMITS = [
        "streaming.spark_query_started",
        "streaming.spark_query_progress",
        "streaming.spark_query_terminated",
    ]

    def onQueryStarted(self, event: QueryStartedEvent):
        self.emit("streaming.spark_query_started",
                  query_id=event.id,
                  name=event.name,
                  run_id=event.runId)
```

### 3.7 Integrate Streaming with DAG Execution

Hybrid batch/streaming support.

```
- [ ] StreamingExecutionStrategy uses reverse topological sort
- [ ] Checkpoint dependencies between streams
- [ ] Hybrid batch/streaming mode support
- [ ] Stream-aware cache recommendations
```

### 3.8 System Tests for Streaming Scenarios

```
- [ ] Test query lifecycle management (start/stop/restart)
- [ ] Test watchdog monitoring and health signals
- [ ] Test auto-recovery with exponential backoff
- [ ] Test dimension change detection
- [ ] Test cascading stream restart
- [ ] Test hybrid batch/streaming execution
- [ ] Test KindlingStreamingListener integration
```

---

## Phase 4: Version Lineage & Tracking (Optional)

**Estimated Time:** 2-3 weeks
**Dependencies:** Phase 1
**Priority:** Optional/Advanced

### 4.1 Create lineage.py Module

```
- [ ] LineageMetadata dataclass
    - [ ] pipe_id, package_name, package_version, layer, timestamp
- [ ] VersionedExecutionContext class
    - [ ] Auto-detect version from package metadata
    - [ ] add_lineage_column(df, pipe_id, layer) method
    - [ ] get_lineage_metadata(pipe_id, layer) method
```

### 4.2 Implement Per-Layer Lineage Columns

```
- [ ] _bronze_lineage struct column
- [ ] _silver_lineage struct column
- [ ] _gold_lineage struct column
- [ ] Layer detection from pipe_id prefix (bronze.*, silver.*, gold.*)
- [ ] Configurable layer names
```

### 4.3 Add Lineage Signals

```
- [ ] datapipes.lineage_applied - After adding lineage metadata
- [ ] datapipes.version_changed - When transform version changes
- [ ] datapipes.lineage_audit - Periodic lineage health check
```

### 4.4 Create Lineage Query Utilities

```
- [ ] find_records_by_version(table, version) SQL generator
- [ ] version_adoption_report() SQL generator
- [ ] stale_version_report() to find old transform versions
```

### 4.5 Add Migration Utilities

```
- [ ] backfill_lineage(table_path, layer, version) function
- [ ] Schema evolution handling for adding lineage columns
- [ ] Backward compatibility for tables without lineage
```

### 4.6 Tests for Lineage Tracking

```
- [ ] Test lineage column addition
- [ ] Test version detection from package metadata
- [ ] Test backfill migration
- [ ] Test lineage query utilities
```

---

## Phase 5: Integration & Polish

**Estimated Time:** 2-3 weeks
**Dependencies:** Phase 1-3

### 5.1 Integration Testing

```
- [ ] End-to-end DAG execution with signals
- [ ] Streaming + DAG hybrid mode
- [ ] Dimension change → stream restart flow
- [ ] Lineage tracking through full pipeline (if Phase 4 done)
- [ ] Performance benchmarks
```

### 5.2 Documentation Updates

```
- [ ] Update docs/data_pipes.md with DAG execution
- [ ] Create docs/streaming.md comprehensive guide
- [ ] Create docs/signals_reference.md (all signals documented)
- [ ] Update README.md with new features
- [ ] Add architecture diagrams
```

### 5.3 Example Notebooks

```
- [ ] DAG execution example notebook
- [ ] Streaming monitoring example notebook
- [ ] Dimension change handling example notebook
- [ ] Signal subscription patterns example
```

### 5.4 Performance Optimization

```
- [ ] Profile DAG building for large graphs (100+ pipes)
- [ ] Optimize parallel generation executor
- [ ] Tune watchdog polling intervals
- [ ] Benchmark signal emission overhead
```

### 5.5 Error Handling Improvements

```
- [ ] Detailed error messages for cycle detection
- [ ] Recovery suggestions in exceptions
- [ ] Signal-based error aggregation
- [ ] Structured error payloads
```

---

## Summary

| Phase | Focus | Time | Dependencies | Priority |
|-------|-------|------|--------------|----------|
| **Phase 1** | Signal Enhancements | 2-3 weeks | None | High |
| **Phase 2** | DAG Execution | 3-4 weeks | Phase 1 | High |
| **Phase 3** | Structured Streaming | 4-5 weeks | Phase 1, 2 | High |
| **Phase 4** | Version Lineage | 2-3 weeks | Phase 1 | Medium |
| **Phase 5** | Integration & Polish | 2-3 weeks | Phase 1-3 | High |

**Total Estimated Time:**
- With Phase 4: 13-18 weeks
- Without Phase 4: 11-15 weeks

## Recommended Starting Point

**Start with Phase 2.1-2.3** (DAG core) because:
1. Signal infrastructure already exists and works
2. DAG execution is the most impactful feature for users
3. Can be developed and tested independently
4. Streaming (Phase 3) depends on DAG for execution strategy

## Key Design Principles

1. **Signals provide loose coupling** - Components communicate via signals, not direct calls
2. **DAG execution is strategy-based** - Easy to add new execution strategies
3. **Streaming monitoring is synchronous** - Runs on driver, uses sync signals for reliability
4. **Backward compatibility** - All new features are opt-in, existing code works unchanged
5. **All features integrate via signals** - Unified observability pattern
