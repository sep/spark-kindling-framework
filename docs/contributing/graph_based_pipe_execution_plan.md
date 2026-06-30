# Graph-Based Pipe Execution Plan

## Executive Summary

This document describes the graph-based pipe execution system implemented in kindling.
The system replaces manual sequential pipe ordering with automatic dependency resolution using
a custom DAG built on Python's standard library (`collections`, `graphlib`). No third-party
graph library (e.g., NetworkX) is required or used.

Key capabilities:
1. **Automatic dependency resolution** - No need for manual ordering
2. **Parallel execution** - Identify independent pipes that can run concurrently
3. **Cycle detection** - Detect circular dependencies before execution
4. **Optimization** - Minimize total execution time through intelligent scheduling

## Current State Analysis

### Legacy Execution Model (Sequential)

```python
# DataPipesExecuter.run_datapipes(pipes)
for pipeid in pipes:
    pipe = self.dpr.get_pipe_definition(pipeid)
    self._execute_datapipe(...)  # Executes one at a time
```

**Limitations:**
- User must manually specify correct execution order
- No parallelization of independent pipes
- No validation of dependency satisfaction
- Cannot detect circular dependencies until runtime failure

### Pipe Metadata Structure

```python
@dataclass
class PipeMetadata:
    pipeid: str
    input_entity_ids: List[str]  # Dependencies (what this pipe needs)
    output_entity_id: str         # What this pipe produces
    # ... other fields
```

All dependency information is already captured in pipe metadata. The graph layer uses it
to build a DAG and derive execution order automatically.

## Implemented Architecture

### Core Modules

| Module | Class / Type | Role |
|--------|-------------|------|
| `pipe_graph.py` | `PipeNode` | A pipe vertex in the graph |
| `pipe_graph.py` | `PipeEdge` | A directed dependency edge between two pipes |
| `pipe_graph.py` | `PipeGraph` | The complete dependency DAG |
| `pipe_graph.py` | `PipeGraphBuilder` | Builds, validates, and analyses `PipeGraph` |
| `pipe_graph.py` | `GraphCycleError` | Raised when a cycle is detected |
| `pipe_graph.py` | `GraphValidationError` | Raised when validation fails |
| `execution_strategy.py` | `ExecutionPlan` | Immutable plan: ordered `Generation` list + graph |
| `execution_strategy.py` | `Generation` | One parallel tier (a list of pipe IDs) |
| `execution_strategy.py` | `BatchExecutionStrategy` | Forward topological sort (sources first) |
| `execution_strategy.py` | `StreamingExecutionStrategy` | Reverse topological sort (sinks first) |
| `execution_strategy.py` | `ConfigBasedExecutionStrategy` | Auto-detects mode from pipe tags |
| `execution_strategy.py` | `ExecutionPlanGenerator` | Facade: build graph then call a strategy |
| `generation_executor.py` | `GenerationExecutor` | Runs an `ExecutionPlan` generation-by-generation |
| `generation_executor.py` | `ExecutionResult` | Aggregated outcome of an entire run |
| `execution_orchestrator.py` | `ExecutionOrchestrator` | High-level facade: plan + execute in one call |

### Output Format: ExecutionPlan

```python
# execution_strategy.py
@dataclass
class Generation:
    number: int
    pipe_ids: List[str]     # Pipes that can run in parallel
    dependencies: List[str] # Pipe IDs that must finish first

@dataclass
class ExecutionPlan:
    pipe_ids: List[str]
    generations: List[Generation]
    graph: PipeGraph        # The underlying DAG (no NetworkX)
    strategy: str           # "batch", "streaming", or "config_based"
    metadata: Dict[str, any]
```

Example for batch execution (forward topo sort):
```
Generation 0: ["pipe_a", "pipe_b"]   # No dependencies — run in parallel
Generation 1: ["pipe_c"]              # Depends on pipe_a or pipe_b
Generation 2: ["pipe_d", "pipe_e"]   # Depend on pipe_c — run in parallel
```

Example for streaming execution (reverse topo sort):
```
Generation 0: ["pipe_d", "pipe_e"]   # Sinks start first (consumers ready)
Generation 1: ["pipe_c"]              # Middle layer
Generation 2: ["pipe_a", "pipe_b"]   # Sources start last (producers)
```

### Execution Modes

**Batch Mode (`BatchExecutionStrategy` — forward topological sort):**
- Dependencies execute BEFORE consumers
- Data flows downstream as each generation completes
- Standard ETL pipeline pattern

**Streaming Mode (`StreamingExecutionStrategy` — reverse topological sort):**
- Consumers start BEFORE producers
- Downstream pipes are ready and listening when upstream starts emitting
- Prevents data loss and backpressure issues

**Why reverse for streaming?**
```
Forward (batch):  Start ingester → it produces data → THEN start aggregator
                  Problem: aggregator not ready, data might be lost or buffered

Reverse (stream): Start aggregator first (listening) → THEN start ingester
                  Benefit: aggregator ready to consume as soon as ingester emits
```

**Config-Based Mode (`ConfigBasedExecutionStrategy`):**
- Reads the `processing_mode` tag from pipe metadata
- If any pipe is tagged `processing_mode: streaming`, uses streaming order
- Otherwise defaults to batch order

### Graph Construction (`PipeGraphBuilder`)

The graph is a Directed Acyclic Graph (DAG) built from `PipeNode` and `PipeEdge` objects
stored inside a `PipeGraph` dataclass. No third-party library is used.

**Node:** Each pipe becomes a `PipeNode` tracking `input_entities` and `output_entity`.

**Edge:** An edge `A → B` (a `PipeEdge`) means pipe A produces an entity consumed by pipe B,
so B depends on A.

```python
# pipe_graph.py
builder = PipeGraphBuilder(registry, logger_provider)
graph = builder.build_graph(pipe_ids)       # Returns PipeGraph
generations = builder.get_generations(graph) # Returns List[List[str]]
```

Example graph:
```
    pipe_clean_raw
         |
         v
    entity.clean_data
         |
         v
    pipe_aggregate
```

### Key Algorithms in `PipeGraphBuilder`

| Method | Description |
|--------|-------------|
| `build_graph(pipe_ids)` | Creates nodes and edges, validates, and runs cycle detection |
| `detect_cycles(graph)` | DFS-based cycle detection; returns list of cycles |
| `topological_sort(graph)` | Kahn's algorithm; returns pipes in dependency order |
| `get_generations(graph)` | Groups pipes into parallel tiers (forward order) |
| `get_reverse_generations(graph)` | Same as above but reversed (for streaming) |
| `reverse_topological_sort(graph)` | Sinks first, sources last |
| `visualize_mermaid(graph)` | Returns a Mermaid diagram string for debugging |
| `get_graph_stats(graph)` | Returns a dict of graph statistics |

Cycle detection raises `GraphCycleError`; missing pipes raise `GraphValidationError`.

## Entry Points

### Option 1: `DataPipesExecuter.run_datapipes` (legacy + DAG facade)

```python
# Sequential execution (original behavior)
executer.run_datapipes(["pipe_a", "pipe_b"])

# DAG-based execution (delegates to ExecutionOrchestrator)
executer.run_datapipes(["pipe_a", "pipe_b"], use_dag=True)

# DAG with explicit strategy
from kindling.execution_strategy import BatchExecutionStrategy, StreamingExecutionStrategy

executer.run_datapipes(
    ["pipe_a", "pipe_b"],
    use_dag=True,
    dag_strategy=BatchExecutionStrategy(logger_provider),
    parallel=True,
    max_workers=4,
)
```

`run_datapipes_dag` is the underlying method; it delegates to `ExecutionOrchestrator`.

### Option 2: `ExecutionOrchestrator.execute` (preferred for new code)

```python
from kindling.execution_orchestrator import ExecutionOrchestrator
from kindling.execution_strategy import BatchExecutionStrategy, StreamingExecutionStrategy
from kindling.generation_executor import ErrorStrategy
from kindling.injection import GlobalInjector

orchestrator = GlobalInjector.get(ExecutionOrchestrator)

# Batch execution (default — BatchExecutionStrategy if strategy=None)
result = orchestrator.execute(
    pipe_ids=["pipe_a", "pipe_b", "pipe_c"],
)

# Parallel batch execution
result = orchestrator.execute(
    pipe_ids=["pipe_a", "pipe_b", "pipe_c"],
    parallel=True,
    max_workers=4,
    error_strategy=ErrorStrategy.CONTINUE,
)

# Streaming execution (sinks start first)
result = orchestrator.execute(
    pipe_ids=["stream_ingest_raw", "stream_clean", "stream_aggregate"],
    strategy=StreamingExecutionStrategy(logger_provider),
)

# Convenience methods
result = orchestrator.execute_batch(pipe_ids=[...])
result = orchestrator.execute_streaming(pipe_ids=[...])
```

### Option 3: `ExecutionPlanGenerator` + `GenerationExecutor` (direct)

```python
from kindling.execution_strategy import ExecutionPlanGenerator, BatchExecutionStrategy
from kindling.generation_executor import GenerationExecutor, ErrorStrategy
from kindling.injection import GlobalInjector

generator = GlobalInjector.get(ExecutionPlanGenerator)
executor  = GlobalInjector.get(GenerationExecutor)

plan = generator.generate_batch_plan(["pipe_a", "pipe_b"])
# or: generator.generate_streaming_plan([...])
# or: generator.generate_config_based_plan([...])

result = executor.execute(
    plan=plan,
    parallel=False,
    error_strategy=ErrorStrategy.FAIL_FAST,
)
print(result.get_summary())
```

## GenerationExecutor Options

`GenerationExecutor.execute()` signature:

```python
def execute(
    self,
    plan: ExecutionPlan,
    parallel: bool = False,         # Run pipes within a generation concurrently
    max_workers: int = 4,           # Thread pool size (only if parallel=True)
    error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST,
    pipe_timeout: Optional[float] = None,   # Per-pipe timeout in seconds
    streaming_options: Optional[Dict[str, Any]] = None,
    auto_cache: bool = False,       # Auto-persist shared input entities
    no_watermark: bool = False,
) -> ExecutionResult:
```

Convenience methods:
- `execute_batch(plan, ...)` - Shorthand for batch plans
- `execute_streaming(plan, ...)` - Shorthand for streaming plans (sequential start)

## ExecutionResult

```python
result.all_succeeded       # bool
result.success_count       # int
result.failed_count        # int
result.skipped_count       # int
result.failed_pipes        # List[str]
result.streaming_queries   # Dict[pipe_id, query] — streaming mode only
result.cache_metrics       # Dict with cache hit/miss info
result.duration_seconds    # float
result.get_summary()       # Dict summary
```

## Stage Processing Integration

Stage processing (via `StageProcessor` / `DataPipesExecuter`) continues to delegate to
the same execution path. Stages can opt into DAG execution by passing `use_dag=True`:

```python
# Stage-level batch execution
dep.run_datapipes(stage_pipe_ids)

# Stage-level DAG execution
dep.run_datapipes(stage_pipe_ids, use_dag=True)
```

## Streaming Pipeline Example

```python
# Define streaming pipes
@DataPipes.pipe(
    pipeid="stream_ingest_raw",
    name="Ingest Raw Events",
    input_entity_ids=[],
    output_entity_id="stream.raw_events",
    output_type="stream",
    tags={"processing_mode": "streaming"},
)
def ingest_raw_events():
    return spark.readStream.format("kafka").load(...)

@DataPipes.pipe(
    pipeid="stream_clean",
    name="Clean Events",
    input_entity_ids=["stream.raw_events"],
    output_entity_id="stream.clean_events",
    output_type="stream",
    tags={"processing_mode": "streaming"},
)
def clean_events(stream_raw_events):
    return stream_raw_events.filter(...).select(...)

@DataPipes.pipe(
    pipeid="stream_aggregate",
    name="Aggregate Events",
    input_entity_ids=["stream.clean_events"],
    output_entity_id="stream.aggregated",
    output_type="stream",
    tags={"processing_mode": "streaming"},
)
def aggregate_events(stream_clean_events):
    return stream_clean_events.groupBy(...).agg(...)

# Start streaming pipes — use ConfigBasedExecutionStrategy to auto-detect mode
# or pass StreamingExecutionStrategy explicitly
orchestrator = GlobalInjector.get(ExecutionOrchestrator)
result = orchestrator.execute(
    pipe_ids=["stream_ingest_raw", "stream_clean", "stream_aggregate"],
    strategy=StreamingExecutionStrategy(logger_provider),
)

# Execution order (REVERSE topological sort):
# Generation 0: stream_aggregate  (consumer ready first)
# Generation 1: stream_clean      (middle layer ready)
# Generation 2: stream_ingest_raw (producer starts last)
#
# Result: No data loss, consumers ready when data arrives
```

## Testing Strategy

### Unit Tests

**Location:** `tests/unit/test_pipe_graph.py`

```python
import pytest
from kindling.pipe_graph import PipeGraphBuilder, GraphCycleError
from kindling.data_pipes import PipeMetadata

def test_simple_linear_dependency(builder, mock_registry):
    """Test A -> B -> C linear dependency"""
    # pipe_a produces entity_x, pipe_b consumes entity_x and produces entity_y, etc.
    graph = builder.build_graph(["pipe_a", "pipe_b", "pipe_c"])
    generations = builder.get_generations(graph)

    assert len(generations) == 3
    assert generations[0] == ["pipe_a"]
    assert generations[1] == ["pipe_b"]
    assert generations[2] == ["pipe_c"]

def test_parallel_independent_pipes(builder, mock_registry):
    """Test two independent pipes can run in parallel"""
    graph = builder.build_graph(["pipe_a", "pipe_b"])
    generations = builder.get_generations(graph)

    assert len(generations) == 1
    assert set(generations[0]) == {"pipe_a", "pipe_b"}

def test_diamond_dependency(builder, mock_registry):
    """Test diamond: A -> B,C -> D"""
    graph = builder.build_graph(["pipe_a", "pipe_b", "pipe_c", "pipe_d"])
    generations = builder.get_generations(graph)

    assert len(generations) == 3
    assert generations[0] == ["pipe_a"]
    assert set(generations[1]) == {"pipe_b", "pipe_c"}
    assert generations[2] == ["pipe_d"]

def test_circular_dependency_detected(builder, mock_registry):
    """Test that circular dependencies are caught"""
    with pytest.raises(GraphCycleError):
        builder.build_graph(["pipe_a", "pipe_b"])  # pipe_a -> pipe_b -> pipe_a
```

### Integration Tests

**Location:** `tests/integration/test_graph_execution.py`

- Test graph execution with real Spark DataFrames
- Verify parallel execution completes successfully
- Compare results between graph mode and legacy mode
- Test error propagation from failed pipes

### System Tests

Add graph execution tests to existing system test suites:
- Fabric: `tests/system/test_fabric_job_deployment.py`
- Synapse: `tests/system/test_synapse_job_deployment.py`
- Databricks: `tests/system/test_databricks_job_deployment.py`

## Configuration and Environment Variables

### Implemented

No environment variables are currently implemented for graph execution control.
All configuration is passed explicitly as parameters to `execute()` / `run_datapipes()`.

### Not Implemented (Proposed Only)

The following env vars were mentioned during design but have NOT been implemented:

| Variable | Status | Alternative |
|----------|--------|-------------|
| `KINDLING_USE_GRAPH_EXECUTION` | Not implemented | Pass `use_dag=True` explicitly |
| `KINDLING_MAX_PIPE_WORKERS` | Not implemented | Pass `max_workers=N` to `execute()` |
| `KINDLING_EXPORT_PIPE_GRAPH` | Not implemented | Call `builder.visualize_mermaid(graph)` |
| `KINDLING_GRAPH_EXPORT_PATH` | Not implemented | Write the mermaid string yourself |
| `KINDLING_EXECUTION_MODE` | Not implemented | Pass an explicit strategy object |

### Execution Mode is Always Explicit

The `strategy` parameter must be an `ExecutionStrategy` instance (or `None` for the
default `BatchExecutionStrategy`). It is never read from an environment variable.

```python
# Batch mode - explicit strategy (or omit for default)
orchestrator.execute(pipe_ids, strategy=BatchExecutionStrategy(lp))

# Streaming mode - explicit strategy
orchestrator.execute(pipe_ids, strategy=StreamingExecutionStrategy(lp))

# Auto-detect from pipe tags
orchestrator.execute(pipe_ids, strategy=ConfigBasedExecutionStrategy(lp))
```

**Rationale:**
- Execution mode is an architectural decision, not a deployment configuration
- Different parts of the same application may need different modes
- Explicit parameters make intent clear and visible in code

## Dependencies

### Not Required: NetworkX

The design originally considered using [NetworkX](https://networkx.org/) for graph
operations. NetworkX was **never added as a dependency** and is not used anywhere in
the codebase. The implementation uses only Python standard-library primitives:

- `collections.defaultdict`, `collections.deque` — adjacency lists and BFS/Kahn's queue
- `graphlib.TopologicalSorter` — available in Python 3.9+ (not used directly; Kahn's
  algorithm is implemented manually in `PipeGraphBuilder`)
- `dataclasses` — `PipeNode`, `PipeEdge`, `PipeGraph`, `ExecutionPlan`, `Generation`
- `concurrent.futures.ThreadPoolExecutor` — parallel generation execution

Do not add NetworkX. The current implementation covers all required operations.

## Platform Compatibility

| Platform | Notes |
|----------|-------|
| Fabric | Full support — standard library only |
| Synapse | Full support — standard library only |
| Databricks | Full support — standard library only |
| Local Dev | Full support — standard library only |

## Performance Considerations

### Benefits

1. **Parallelism**: Independent pipes run concurrently (pass `parallel=True`)
   - Example: 10 independent bronze-layer ingestion pipes can overlap up to `max_workers`
2. **Optimal Ordering**: Always executes in correct dependency order automatically
3. **Early Failure Detection**: Cycles caught during graph construction, before any pipe runs

### Overhead

1. **Graph Construction**: O(P + E) where P = number of pipes, E = edges
   - Negligible for typical pipelines (< 1 000 pipes)
2. **Memory**: Graph held in memory during execution (~1 KB per pipe)
3. **ThreadPool**: Created only when `parallel=True` and a generation has > 1 pipe

### Tuning

```python
# Increase parallelism for large pipelines with many independent pipes
orchestrator.execute(pipe_ids, parallel=True, max_workers=8)

# Force sequential (useful for debugging or memory-constrained environments)
orchestrator.execute(pipe_ids, parallel=False)

# Continue on error instead of stopping at first failure
orchestrator.execute(pipe_ids, error_strategy=ErrorStrategy.CONTINUE)
```

## Signals Emitted

`GenerationExecutor` emits the following signals (see `SignalEmitter`):

| Signal | When |
|--------|------|
| `orchestrator.execution_started` | Before any generation runs |
| `orchestrator.execution_completed` | After all generations succeed |
| `orchestrator.execution_failed` | If execution raises |
| `orchestrator.generation_started` | Before each generation |
| `orchestrator.generation_completed` | After each generation |
| `orchestrator.pipe_started` | Before each pipe |
| `orchestrator.pipe_completed` | After each pipe succeeds |
| `orchestrator.pipe_failed` | When a pipe raises |
| `orchestrator.pipe_skipped` | When a pipe is skipped (no data) |
| `orchestrator.cache_recommended` | When a shared entity is identified |
| `orchestrator.cache_hit` | When a shared entity is served from cache |
| `orchestrator.cache_miss` | When a shared entity is first read |

`ExecutionOrchestrator` also emits `orchestrator.plan_generated` after plan generation.

## Open Questions

1. Should we support custom schedulers? (e.g., Airflow, Prefect integration)
2. Should pipe graph visualization export (Mermaid → file) be wired into a config option?
3. Should we support pipe priorities or weights for scheduling decisions?
4. Should `parallel=True` become the default once battle-tested?
5. Should we add cost-based optimization? (e.g., prefer pipes with cached inputs)
6. Should streaming mode support startup delays between generations?

---

**Document Version:** 2.0
**Date:** June 2026
**Updated by:** Audit pass — corrected class names, removed NetworkX references,
aligned all API examples with actual implementation in `pipe_graph.py`,
`execution_strategy.py`, `generation_executor.py`, `execution_orchestrator.py`,
and `data_pipes.py`.
