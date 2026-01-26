# Graph-Based Pipe Execution Plan

## Executive Summary

Replace the current sequential pipe execution mechanism with a graph-based approach using NetworkX. This enables:
1. **Automatic dependency resolution** - No need for manual ordering
2. **Parallel execution** - Identify independent pipes that can run concurrently
3. **Cycle detection** - Detect circular dependencies before execution
4. **Optimization** - Minimize total execution time through intelligent scheduling

## Current State Analysis

### Current Execution Model (Sequential)
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
- Can't detect circular dependencies until runtime failure

### Current Metadata Structure
```python
@dataclass
class PipeMetadata:
    pipeid: str
    input_entity_ids: List[str]  # Dependencies (what this pipe needs)
    output_entity_id: str         # What this pipe produces
    # ... other fields
```

**Good News:** All dependency information is already captured! We just need to use it.

## Proposed Architecture

### Output Format: Execution Plan

```python
ExecutionPlan = List[List[str]]  # List of generations, each containing parallelizable pipes

# Example for BATCH execution (forward topo sort):
# [
#   ["pipe_a", "pipe_b"],      # Generation 0: Run A and B in parallel (no dependencies)
#   ["pipe_c"],                 # Generation 1: Run C (depends on A or B)
#   ["pipe_d", "pipe_e"]        # Generation 2: Run D and E in parallel (depend on C)
# ]

# Example for STREAMING execution (reverse topo sort):
# [
#   ["pipe_d", "pipe_e"],      # Generation 0: Start D and E first (consumers ready)
#   ["pipe_c"],                 # Generation 1: Start C (producer for D/E)
#   ["pipe_a", "pipe_b"]        # Generation 2: Start A and B last (root producers)
# ]
```

### Execution Modes

**Batch Mode (Forward Topological Sort):**
- Dependencies execute BEFORE consumers
- Data flows downstream as each generation completes
- Standard ETL pipeline pattern
- Use case: Traditional data transformation pipelines

**Streaming Mode (Reverse Topological Sort):**
- Consumers start BEFORE producers
- Downstream pipes are ready and listening when upstream starts emitting
- Prevents data loss and backpressure issues
- Use case: Structured streaming pipelines, real-time processing

**Why Reverse for Streaming?**
```
Forward (batch):  Start ingester → it produces data → THEN start aggregator
                  Problem: aggregator not ready, data might be lost or buffered

Reverse (stream): Start aggregator first (listening) → THEN start ingester
                  Benefit: aggregator ready to consume as soon as ingester emits
```

### Graph Construction

**Node Types:**
1. **Pipe nodes** - Represent pipe operations
2. **Entity nodes** - Represent data entities (intermediate representation)

**Edge Types:**
- **Pipe → Entity**: Pipe produces entity (output_entity_id)
- **Entity → Pipe**: Pipe consumes entity (input_entity_ids)

**Graph Type:** Directed Acyclic Graph (DAG)

```
Example:
    pipe_clean_raw
         |
         v
    entity.clean_data
         |
         v
    pipe_aggregate
```

### Key Algorithm: `topological_generations()`

**NetworkX Function:** `networkx.algorithms.dag.topological_generations(G)`

**What it does:**
- Stratifies DAG into generations (layers)
- Each generation contains nodes with no dependencies within that layer
- Guarantees all dependencies are in previous generations
- Returns iterator of sets (we'll convert to list of lists)

**Example:**
```python
import networkx as nx

# Build graph
G = nx.DiGraph()
G.add_edges_from([
    ("pipe_a", "entity_x"),
    ("pipe_b", "entity_y"),
    ("entity_x", "pipe_c"),
    ("entity_y", "pipe_c"),
])

# Get generations
generations = [list(gen) for gen in nx.topological_generations(G)]
# Result: [['pipe_a', 'pipe_b'], ['entity_x', 'entity_y'], ['pipe_c']]

# Filter to only pipe nodes
pipe_generations = [
    [node for node in gen if node.startswith('pipe_')]
    for gen in generations
]
# Result: [['pipe_a', 'pipe_b'], ['pipe_c']]
```

## Implementation Plan

### Phase 1: Core Graph Engine (New Module)

**Create:** `packages/kindling/pipe_graph.py`

```python
"""
Graph-based pipe execution planner using NetworkX.
Provides automatic dependency resolution and parallel execution planning.
"""

import logging
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass

try:
    import networkx as nx
    NETWORKX_AVAILABLE = True
except ImportError:
    NETWORKX_AVAILABLE = False
    # Fallback to graphlib for basic topological sort
    from graphlib import TopologicalSorter


@dataclass
class PipeExecutionPlan:
    """Execution plan with parallelization support"""
    generations: List[List[str]]  # List of pipe ID arrays (parallel within, sequential between)
    total_pipes: int
    max_parallelism: int  # Maximum pipes that can run concurrently
    execution_mode: str  # "batch" or "streaming"
    dependency_graph: Optional[object] = None  # NetworkX graph for visualization/debugging


class PipeGraphBuilder:
    """Builds dependency graph from pipe metadata"""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def build_graph(self, pipes: Dict[str, 'PipeMetadata']) -> 'nx.DiGraph':
        """
        Build a directed graph representing pipe dependencies.

        Nodes: pipe IDs
        Edges: A -> B means pipe A must run before pipe B

        Args:
            pipes: Dictionary of pipe_id -> PipeMetadata

        Returns:
            NetworkX DiGraph

        Raises:
            ValueError: If circular dependencies detected
        """
        if not NETWORKX_AVAILABLE:
            raise ImportError("NetworkX is required for graph-based execution. "
                            "Install with: pip install networkx")

        G = nx.DiGraph()

        # Add all pipes as nodes
        for pipe_id in pipes.keys():
            G.add_node(pipe_id)

        # Build entity-to-producer mapping
        entity_producers = {}  # entity_id -> pipe_id that produces it
        for pipe_id, pipe in pipes.items():
            entity_producers[pipe.output_entity_id] = pipe_id

        # Add edges based on input dependencies
        for pipe_id, pipe in pipes.items():
            for input_entity in pipe.input_entity_ids:
                if input_entity in entity_producers:
                    # This pipe depends on the pipe that produces the input entity
                    producer_pipe = entity_producers[input_entity]
                    G.add_edge(producer_pipe, pipe_id)
                    self.logger.debug(f"Dependency: {producer_pipe} -> {pipe_id} (via {input_entity})")

        # Validate: Check for cycles
        if not nx.is_directed_acyclic_graph(G):
            cycles = list(nx.simple_cycles(G))
            raise ValueError(f"Circular dependencies detected: {cycles}")

        return G

    def create_execution_plan(
        self,
        pipes: Dict[str, 'PipeMetadata'],
        mode: str = "batch"
    ) -> PipeExecutionPlan:
        """
        Create execution plan with parallel generations.

        Args:
            pipes: Dictionary of pipe_id -> PipeMetadata
            mode: "batch" (forward topo sort) or "streaming" (reverse topo sort)

        Returns:
            PipeExecutionPlan with generations for parallel execution

        Raises:
            ValueError: If mode is not "batch" or "streaming"
        """
        if mode not in ("batch", "streaming"):
            raise ValueError(f"Invalid execution mode: {mode}. Must be 'batch' or 'streaming'")

        if not pipes:
            return PipeExecutionPlan(
                generations=[],
                total_pipes=0,
                max_parallelism=0,
                execution_mode=mode
            )

        G = self.build_graph(pipes)

        # Use topological_generations to get parallel layers
        generations = [list(gen) for gen in nx.topological_generations(G)]

        # For streaming mode, reverse the generations
        # Start consumers first (end of graph), then producers (start of graph)
        if mode == "streaming":
            generations = list(reversed(generations))
            self.logger.info("Reversed generations for streaming mode (consumers before producers)")

        # Calculate statistics
        total_pipes = len(pipes)
        max_parallelism = max(len(gen) for gen in generations) if generations else 0

        self.logger.info(f"Execution plan created: {len(generations)} generations, "
                        f"{total_pipes} total pipes, max parallelism {max_parallelism}, "
                        f"mode={mode}")

        for i, gen in enumerate(generations):
            self.logger.debug(f"Generation {i}: {gen}")

        return PipeExecutionPlan(
            generations=generations,
            total_pipes=total_pipes,
            max_parallelism=max_parallelism,
            execution_mode=mode,
            dependency_graph=G
        )


class PipeGraphPlanner:
    """
    High-level planner for pipe execution using graph analysis.
    Handles both full pipeline planning and subset planning.
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.graph_builder = PipeGraphBuilder(logger)

    def plan_execution(
        self,
        all_pipes: Dict[str, 'PipeMetadata'],
        requested_pipes: Optional[List[str]] = None,
        mode: str = "batch"
    ) -> PipeExecutionPlan:
        """
        Create execution plan for requested pipes and their dependencies.

        Args:
            all_pipes: All registered pipes
            requested_pipes: Specific pipes to execute (None = all pipes)
            mode: "batch" (forward) or "streaming" (reverse) execution order

        Returns:
            PipeExecutionPlan
        """
        if requested_pipes is None:
            # Execute all registered pipes
            return self.graph_builder.create_execution_plan(all_pipes, mode=mode)

        # Filter to requested pipes and their dependencies
        required_pipes = self._find_required_pipes(all_pipes, requested_pipes)
        return self.graph_builder.create_execution_plan(required_pipes, mode=mode)

    def _find_required_pipes(
        self,
        all_pipes: Dict[str, 'PipeMetadata'],
        requested_pipes: List[str]
    ) -> Dict[str, 'PipeMetadata']:
        """
        Find all pipes needed to execute the requested pipes.
        Includes requested pipes and all their transitive dependencies.
        """
        # Build full graph to find dependencies
        full_graph = self.graph_builder.build_graph(all_pipes)

        required_pipe_ids = set()
        for pipe_id in requested_pipes:
            if pipe_id not in all_pipes:
                raise ValueError(f"Unknown pipe: {pipe_id}")
            # Add this pipe and all its ancestors (dependencies)
            required_pipe_ids.add(pipe_id)
            required_pipe_ids.update(nx.ancestors(full_graph, pipe_id))

        return {pid: all_pipes[pid] for pid in required_pipe_ids}


class PipeGraphFallback:
    """
    Fallback implementation using Python's graphlib.
    Provides basic topological sort without parallel generations.
    Used when NetworkX is not available (e.g., Databricks without manual install).
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def create_execution_plan(
        self,
        pipes: Dict[str, 'PipeMetadata'],
        mode: str = "batch"
    ) -> PipeExecutionPlan:
        """
        Create sequential execution plan using graphlib.
        All pipes run sequentially (no parallelization).

        Args:
            pipes: Dictionary of pipe_id -> PipeMetadata
            mode: "batch" (forward) or "streaming" (reverse) - streaming not supported in fallback
        """
        self.logger.warning("NetworkX not available - using sequential fallback mode")

        if mode == "streaming":
            self.logger.warning("Streaming mode not supported in fallback - using batch mode")
            mode = "batch"

        # Build dependency mapping for graphlib
        entity_producers = {}
        for pipe_id, pipe in pipes.items():
            entity_producers[pipe.output_entity_id] = pipe_id

        dependencies = {}
        for pipe_id, pipe in pipes.items():
            deps = []
            for input_entity in pipe.input_entity_ids:
                if input_entity in entity_producers:
                    deps.append(entity_producers[input_entity])
            dependencies[pipe_id] = deps

        # Topological sort
        sorter = TopologicalSorter(dependencies)
        ordered_pipes = list(sorter.static_order())

        # Return as single-pipe generations (fully sequential)
        generations = [[pipe_id] for pipe_id in ordered_pipes]

        return PipeExecutionPlan(
            generations=generations,
            total_pipes=len(pipes),
            max_parallelism=1,  # No parallelism in fallback mode
            execution_mode=mode
        )
```

### Phase 2: Update DataPipesExecuter

**Modify:** `packages/kindling/data_pipes.py`

**Changes:**
1. Add PipeGraphPlanner as dependency
2. Replace sequential loop with generation-based execution
3. Add parallel execution support (initially sequential, Phase 3 adds true parallelism)
4. Maintain backward compatibility with explicit pipe ordering

```python
@GlobalInjector.singleton_autobind()
class DataPipesExecuter(DataPipesExecution):
    @inject
    def __init__(
        self,
        lp: PythonLoggerProvider,
        dpe: DataEntityRegistry,
        dpr: DataPipesRegistry,
        erps: EntityReadPersistStrategy,
        tp: SparkTraceProvider,
    ):
        self.erps = erps
        self.dpr = dpr
        self.dpe = dpe
        self.logger = lp.get_logger("data_pipes_executer")
        self.tp = tp

        # Initialize graph planner
        from kindling.pipe_graph import PipeGraphPlanner, NETWORKX_AVAILABLE
        if NETWORKX_AVAILABLE:
            self.graph_planner = PipeGraphPlanner(self.logger)
            self.logger.info("Graph-based execution enabled (NetworkX available)")
        else:
            from kindling.pipe_graph import PipeGraphFallback
            self.graph_planner = PipeGraphFallback(self.logger)
            self.logger.warning("Graph-based execution using fallback mode")

    def run_datapipes(self, pipes, use_graph=True, mode="batch"):
        """
        Execute pipes with optional graph-based ordering.

        Args:
            pipes: List of pipe IDs to execute, or None for all registered pipes
            use_graph: If True, use graph-based execution planning.
                      If False, execute in provided order (legacy mode)
            mode: "batch" (forward topo sort) or "streaming" (reverse topo sort)
                 Only applies when use_graph=True
        """
        if use_graph:
            self._run_datapipes_graph_mode(pipes, mode=mode)
        else:
            self._run_datapipes_legacy_mode(pipes)

    def _run_datapipes_graph_mode(self, requested_pipes, mode="batch"):
        """Execute pipes using graph-based dependency resolution"""
        pipe_entity_reader = self.erps.create_pipe_entity_reader
        pipe_activator = self.erps.create_pipe_persist_activator

        # Get all registered pipes
        all_pipe_ids = list(self.dpr.get_pipe_ids())
        all_pipes = {pid: self.dpr.get_pipe_definition(pid) for pid in all_pipe_ids}

        # Create execution plan with specified mode
        plan = self.graph_planner.plan_execution(all_pipes, requested_pipes, mode=mode)

        self.logger.info(f"Executing {plan.total_pipes} pipes in {len(plan.generations)} "
                        f"generations (mode={plan.execution_mode})")

        with self.tp.span(component="data_pipes_executer", operation="execute_datapipes"):
            for gen_idx, generation in enumerate(plan.generations):
                self.logger.debug(f"Starting generation {gen_idx} with {len(generation)} pipes")

                # Execute all pipes in this generation
                # Phase 2: Sequential within generation
                # Phase 3: Parallel within generation
                for pipeid in generation:
                    pipe = self.dpr.get_pipe_definition(pipeid)
                    with self.tp.span(
                        operation="execute_datapipe",
                        component=f"pipe-{pipeid}",
                        details=pipe.tags
                    ):
                        self._execute_datapipe(
                            pipe_entity_reader(pipe),
                            pipe_activator(pipe),
                            pipe
                        )

                self.logger.debug(f"Completed generation {gen_idx}")

    def _run_datapipes_legacy_mode(self, pipes):
        """Execute pipes in provided order (backward compatibility)"""
        pipe_entity_reader = self.erps.create_pipe_entity_reader
        pipe_activator = self.erps.create_pipe_persist_activator

        self.logger.info(f"Executing {len(pipes)} pipes in legacy mode (provided order)")

        with self.tp.span(component="data_pipes_executer", operation="execute_datapipes"):
            for pipeid in pipes:
                pipe = self.dpr.get_pipe_definition(pipeid)
                with self.tp.span(
                    operation="execute_datapipe", component=f"pipe-{pipeid}", details=pipe.tags
                ):
                    self._execute_datapipe(pipe_entity_reader(pipe), pipe_activator(pipe), pipe)

    # _execute_datapipe and _populate_source_dict remain unchanged
```

### Phase 3: Parallel Execution Within Generations

**Add ThreadPool-based parallel execution:**

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

class DataPipesExecuter(DataPipesExecution):
    # ... existing code ...

    def __init__(self, ...):
        # ... existing init ...

        # Configure parallel execution
        # Default: min(4, cpu_count) to avoid overwhelming Spark driver
        self.max_workers = int(os.environ.get("KINDLING_MAX_PIPE_WORKERS",
                                               min(4, os.cpu_count() or 1)))
        self.logger.info(f"Parallel execution configured: max_workers={self.max_workers}")

    def _run_datapipes_graph_mode(self, requested_pipes, mode="batch"):
        """Execute pipes using graph-based dependency resolution with parallelism"""
        # ... setup code ...

        plan = self.graph_planner.plan_execution(all_pipes, requested_pipes, mode=mode)

        with self.tp.span(component="data_pipes_executer", operation="execute_datapipes"):
            for gen_idx, generation in enumerate(plan.generations):
                self.logger.info(f"Generation {gen_idx}: executing {len(generation)} pipes "
                               f"(mode={plan.execution_mode})")

                if len(generation) == 1:
                    # Single pipe - execute directly
                    self._execute_single_pipe(generation[0], pipe_entity_reader, pipe_activator)
                else:
                    # Multiple pipes - execute in parallel
                    self._execute_pipes_parallel(generation, pipe_entity_reader, pipe_activator)

                self.logger.info(f"Generation {gen_idx}: completed")

    def _execute_single_pipe(self, pipeid, pipe_entity_reader, pipe_activator):
        """Execute a single pipe"""
        pipe = self.dpr.get_pipe_definition(pipeid)
        with self.tp.span(
            operation="execute_datapipe",
            component=f"pipe-{pipeid}",
            details=pipe.tags
        ):
            self._execute_datapipe(
                pipe_entity_reader(pipe),
                pipe_activator(pipe),
                pipe
            )

    def _execute_pipes_parallel(self, pipeids, pipe_entity_reader, pipe_activator):
        """Execute multiple pipes in parallel using ThreadPool"""
        # Use min of configured workers and number of pipes
        workers = min(self.max_workers, len(pipeids))

        self.logger.debug(f"Executing {len(pipeids)} pipes with {workers} workers")

        def execute_pipe_worker(pipeid):
            """Worker function for parallel execution"""
            try:
                self._execute_single_pipe(pipeid, pipe_entity_reader, pipe_activator)
                return (pipeid, True, None)
            except Exception as e:
                self.logger.error(f"Pipe {pipeid} failed: {str(e)}")
                return (pipeid, False, e)

        # Execute pipes in parallel
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(execute_pipe_worker, pid): pid for pid in pipeids}

            results = []
            for future in as_completed(futures):
                pipeid = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    if result[1]:  # Success
                        self.logger.debug(f"Pipe {result[0]} completed successfully")
                    else:  # Failure
                        self.logger.error(f"Pipe {result[0]} failed: {result[2]}")
                except Exception as e:
                    self.logger.error(f"Unexpected error executing pipe {pipeid}: {str(e)}")
                    results.append((pipeid, False, e))

        # Check for failures
        failures = [(pid, err) for pid, success, err in results if not success]
        if failures:
            error_msg = f"{len(failures)} pipes failed: {[pid for pid, _ in failures]}"
            raise RuntimeError(error_msg)
```

### Phase 4: Add NetworkX Dependency

**Modify:** `pyproject.toml`

```toml
[tool.poetry.dependencies]
python = "^3.10"
# ... existing dependencies ...
networkx = ">=3.0"  # Add NetworkX for graph-based execution
```

**Note on Platform Support:**
- ✅ **Fabric**: NetworkX pre-installed in runtime
- ✅ **Synapse**: NetworkX pre-installed in runtime
- ⚠️ **Databricks**: Not pre-installed, but will be added as framework dependency

### Phase 5: Update Stage Processing & Streaming

**Modify:** `packages/kindling/simple_stage_processor.py`

Stage processing already delegates to `DataPipesExecuter`, so minimal changes needed:

```python
@GlobalInjector.singleton_autobind()
class StageProcessor(StageProcessingService):
    # ... existing code ...

    def execute(self, stage: str, stage_description: str, stage_details: Dict, layer: str):
        with self.tp.span(...):
            self.ep.ensure_entity_table(self.wef.get_watermark_entity_for_layer(layer))
            pipe_ids = self.dpr.get_pipe_ids()
            stage_pipe_ids = [pipe_id for pipe_id in pipe_ids if pipe_id.startswith(stage)]

            # Graph-based batch execution (forward topo sort)
            self.dep.run_datapipes(stage_pipe_ids, use_graph=True, mode="batch")
```

**Modify:** `packages/kindling/pipe_streaming.py`

Update `SimplePipeStreamOrchestrator` to use graph-based streaming execution:

```python
@GlobalInjector.singleton_autobind()
class SimplePipeStreamOrchestrator(PipeStreamOrchestrator):
    @inject
    def __init__(
        self,
        cs: ConfigService,
        dpr: DataPipesRegistry,
        ep: EntityProvider,
        der: DataEntityRegistry,
        epl: EntityPathLocator,
        plp: PythonLoggerProvider,
        dep: DataPipesExecution,  # Add executer for graph-based planning
    ):
        self.dpr = dpr
        self.ep = ep
        self.der = der
        self.epl = epl
        self.dep = dep
        self.logger = plp.get_logger("SimplePipeStreamOrchestrator")
        self.cs = cs

    def start_pipes_as_streams(self, pipe_ids: List[str], options: Dict = None):
        """
        Start multiple streaming pipes with graph-based dependency ordering.
        Uses REVERSE topological sort to start consumers before producers.

        Args:
            pipe_ids: List of pipe IDs to start as streams
            options: Optional configuration for streaming
        """
        if options is None:
            options = {}

        # Use graph planner to determine reverse execution order
        # This ensures downstream consumers are ready before upstream producers start
        plan = self.dep.graph_planner.plan_execution(
            all_pipes={pid: self.dpr.get_pipe_definition(pid) for pid in pipe_ids},
            requested_pipes=pipe_ids,
            mode="streaming"  # REVERSE topological sort
        )

        self.logger.info(f"Starting {plan.total_pipes} streaming pipes in "
                        f"{len(plan.generations)} generations (consumers first)")

        stream_handles = []
        for gen_idx, generation in enumerate(plan.generations):
            self.logger.info(f"Starting generation {gen_idx}: {generation}")

            for pipeid in generation:
                handle = self.start_pipe_as_stream_processor(pipeid, options)
                stream_handles.append((pipeid, handle))

        return stream_handles
```

## Testing Strategy

### Unit Tests

**Create:** `tests/unit/test_pipe_graph.py`

```python
import pytest
from kindling.pipe_graph import PipeGraphBuilder, PipeGraphPlanner
from kindling.data_pipes import PipeMetadata

def test_simple_linear_dependency():
    """Test A -> B -> C linear dependency"""
    pipes = {
        "pipe_a": PipeMetadata(
            pipeid="pipe_a",
            input_entity_ids=[],
            output_entity_id="entity_x",
            # ... other fields
        ),
        "pipe_b": PipeMetadata(
            pipeid="pipe_b",
            input_entity_ids=["entity_x"],
            output_entity_id="entity_y",
        ),
        "pipe_c": PipeMetadata(
            pipeid="pipe_c",
            input_entity_ids=["entity_y"],
            output_entity_id="entity_z",
        ),
    }

    # Test batch mode (forward)
    plan_batch = builder.create_execution_plan(pipes, mode="batch")
    assert plan_batch.total_pipes == 4
    assert len(plan_batch.generations) == 3
    assert plan_batch.generations[0] == ["pipe_a"]
    assert set(plan_batch.generations[1]) == {"pipe_b", "pipe_c"}  # Parallel
    assert plan_batch.generations[2] == ["pipe_d"]

    # Test streaming mode (reverse)
    plan_stream = builder.create_execution_plan(pipes, mode="streaming")
    assert plan_stream.total_pipes == 4
    assert len(plan_stream.generations) == 3
    assert plan_stream.generations[0] == ["pipe_d"]  # Start consumer first
    assert set(plan_stream.generations[1]) == {"pipe_b", "pipe_c"}  # Then middle
    assert plan_stream.generations[2] == ["pipe_a"]  # Finally producer
    assert plan.generations[1] == ["pipe_b"]
    assert plan.generations[2] == ["pipe_c"]

def test_parallel_independent_pipes():
    """Test two independent pipes can run in parallel"""
    pipes = {
        "pipe_a": PipeMetadata(
            pipeid="pipe_a",
            input_entity_ids=[],
            output_entity_id="entity_x",
        ),
        "pipe_b": PipeMetadata(
            pipeid="pipe_b",
            input_entity_ids=[],
            output_entity_id="entity_y",
        ),
    }

    builder = PipeGraphBuilder(logger)
    plan = builder.create_execution_plan(pipes)

    assert plan.total_pipes == 2
    assert len(plan.generations) == 1
    assert set(plan.generations[0]) == {"pipe_a", "pipe_b"}
    assert plan.max_parallelism == 2

def test_diamond_dependency():
    """Test diamond: A -> B,C -> D"""
    pipes = {
        "pipe_a": PipeMetadata(
            pipeid="pipe_a",
            input_entity_ids=[],
            output_entity_id="entity_x",
        ),
        "pipe_b": PipeMetadata(
            pipeid="pipe_b",
            input_entity_ids=["entity_x"],
            output_entity_id="entity_y",
        ),
        "pipe_c": PipeMetadata(
            pipeid="pipe_c",
            input_entity_ids=["entity_x"],
            output_entity_id="entity_z",
        ),
        "pipe_d": PipeMetadata(
            pipeid="pipe_d",
            input_entity_ids=["entity_y", "entity_z"],
            output_entity_id="entity_final",
        ),
    }

    builder = PipeGraphBuilder(logger)
    plan = builder.create_execution_plan(pipes)

    assert plan.total_pipes == 4
    assert len(plan.generations) == 3
    assert plan.generations[0] == ["pipe_a"]
    assert set(plan.generations[1]) == {"pipe_b", "pipe_c"}  # Parallel
    assert plan.generations[2] == ["pipe_d"]

def test_circular_dependency_detected():
    """Test that circular dependencies are caught"""
    pipes = {
        "pipe_a": PipeMetadata(
            pipeid="pipe_a",
            input_entity_ids=["entity_y"],
            output_entity_id="entity_x",
        ),
        "pipe_b": PipeMetadata(
            pipeid="pipe_b",
            input_entity_ids=["entity_x"],
            output_entity_id="entity_y",
        ),
    }

    builder = PipeGraphBuilder(logger)

    with pytest.raises(ValueError, match="Circular dependencies"):
        builder.create_execution_plan(pipes)
```

### Integration Tests

**Create:** `tests/integration/test_graph_execution.py`

- Test graph execution with real Spark DataFrames
- Verify parallel execution completes successfully
- Compare results between graph mode and legacy mode
- Test error propagation from failed pipes

### System Tests

Add graph execution tests to existing system test suites:
- Fabric: `tests/system/test_fabric_job_deployment.py`
- Synapse: `tests/system/test_synapse_job_deployment.py`
- Databricks: `tests/system/test_databricks_job_deployment.py`

## Migration Strategy

### Phase 2 (Current Implementation): Opt-In

```python
# Batch execution with graph mode
executer.run_datapipes(["pipe_a", "pipe_b"], use_graph=True, mode="batch")

# Streaming execution with graph mode
executer.run_datapipes(["pipe_a", "pipe_b"], use_graph=True, mode="streaming")

# Or keep legacy mode (no graph planning)
executer.run_datapipes(["pipe_a", "pipe_b"], use_graph=False)
```

### Phase 3: Default On with Opt-Out

```python
# Graph mode becomes default, explicit mode parameter
executer.run_datapipes(["pipe_a", "pipe_b"], mode="batch")  # use_graph=True by default

# Streaming mode explicit
executer.run_datapipes(["pipe_a", "pipe_b"], mode="streaming")

# Can still opt-out for compatibility
executer.run_datapipes(["pipe_a", "pipe_b"], use_graph=False)
```

### Phase 4: Remove Legacy Mode

```python
# Only graph-based execution, mode is always explicit
executer.run_datapipes(["pipe_a", "pipe_b"], mode="batch")     # Default for batch
executer.run_datapipes(["pipe_a", "pipe_b"], mode="streaming")  # For streaming
```
Execution mode: "batch" or "streaming"
KINDLING_EXECUTION_MODE=batch  # Default: batch (forward topo sort)

#
## Configuration Options

Add environment variables for performance tuning:

```bash
# Enable/disable graph execution (Phase 2 migration only)
KINDLING_USE_GRAPH_EXECUTION=true

# Parallel execution workers (Phase 3)
KINDLING_MAX_PIPE_WORKERS=4  # Default: min(4, cpu_count)

# Graph visualization export (debugging)
KINDLING_EXPORT_PIPE_GRAPH=true
KINDLING_GRAPH_EXPORT_PATH=/tmp/pipe_graph.dot
```

**Important: Execution Mode is NOT an Environment Variable**

The `mode` parameter (`"batch"` or `"streaming"`) must be **explicitly specified** at the call site:

```python
# Batch mode - explicit parameter
executer.run_datapipes(pipes, mode="batch")

# Streaming mode - explicit parameter
orchestrator.start_pipes_as_streams(pipes)  # Uses mode="streaming" internally
```

**Rationale:**
- Execution mode is an architectural decision, not a deployment configuration
- Different parts of the same application may need different modes
- Explicit parameters make intent clear and visible in code
- Type-safe: IDEs and linters can validate mode values

## Documentation Updates

### Update Existing Docs

1. **`docs/data_pipes.md`**
   - Add section on "Automatic Dependency Resolution"
   - Explain execution generations
   - Show parallelism benefits
   - Migration guide

2. **`docs/stage_processing.md`**
   - Explain how stages now auto-resolve dependencies
   - Update examples

3. **`docs/intro.md`**
   - Add graph-based execution to key features

### Create New Docs

4. **`docs/graph_execution.md`** (new)
   - Deep dive on graph algorithm
   - Performance tuning
   - Debugging with graph visualization
   - Common patterns (diamond, fan-out, fan-in)

## Performance Considerations

### Benefits

1. **Parallelism**: Independent pipes run concurrently
   - Example: 10 independent bronze layer ingestion pipes = 10x speedup (up to worker limit)

2. **Optimal Ordering**: Always executes in correct dependency order
   - No manual trial-and-error to find correct sequence

3. **Early Failure Detection**: Circular dependencies caught before execution

### Overhead

1. **Graph Construction**: O(P + E) where P=pipes, E=dependencies
   - Negligible for typical pipelines (< 1000 pipes)

2. **Memory**: Graph stored in memory during execution
   - ~1KB per pipe, 1MB for 1000 pipes

3. **ThreadPool**: Overhead for thread creation/management
   - Only used when parallelism beneficial (generation size > 1)

### Tuning Guidelines

```python
# For large pipelines with many independent pipes
KINDLING_MAX_PIPE_WORKERS=8  # Increase parallelism

# For small pipelines or memory-constrained environments
KINDLING_MAX_PIPE_WORKERS=1  # Sequential execution

# For batch ETL pipelines
executer.run_datapipes(pipes, mode="batch")  # Dependencies execute first

# For streaming pipelines
executer.run_datapipes(pipes, mode="streaming")  # Consumers start first

# For debugging
use_graph=False  # Fallback to legacy mode
```

## Risk Mitigation

### Backward Compatibility

- ✅ **Phase 2**: Opt-in, default is legacy mode
- ✅ **`use_graph` parameter**: Explicit control
- ✅ **Fallback mode**: Works without NetworkX (graphlib)
- ✅ **Legacy API preserved**: Existing code continues to work

### Platform Compatibility

| Platform | NetworkX Available | Fallback Mode | Notes |
|----------|-------------------|---------------|-------|
| Fabric | ✅ Pre-installed | N/A | Full graph support |
| Synapse | ✅ Pre-installed | N/A | Full graph support |
| Databricks | ⚠️ Manual install | ✅ graphlib | Add to requirements |
| Local Dev | ⚠️ Varies | ✅ graphlib | Add to requirements |

### Error Handling

1. **Circular Dependencies**: Caught during graph construction
2. **Missing Pipes**: Validation before execution
3. **Parallel Execution Failures**: Aggregate errors, fail-fast option
4. **NetworkX Import Failure**: Automatic fallback to graphlib

## Success Criteria

### Functional

- ✅ All existing tests pass with graph mode enabled
- ✅ Circular dependency detection works
- ✅ Parallel execution produces same results as sequential
- ✅ Backward compatibility maintained

### Performance

- ✅ Graph construction overhead < 5% of total execution time
- ✅ Parallel execution shows measurable speedup for independent pipes
- ✅ No performance regression in legacy mode

### Quality

- ✅ Test coverage > 85% for new graph code
- ✅ Documentation complete and reviewed
- ✅ System tests passing on all 3 platforms

## Timeline Estimate

| Phase | Description | Effort | Dependencies |
|-------|-------------|--------|--------------|
| 1 | Core graph engine module | 2-3 days | None |
| 2 | Update DataPipesExecuter | 1-2 days | Phase 1 |
| 3 | Add parallel execution | 1-2 days | Phase 2 |
| 4 | Add NetworkX dependency | 1 hour | Phase 1-3 |
| 5 | Update stage processing | 1 hour | Phase 2 |
| Testing | Unit + integration tests | 2-3 days | Phases 1-3 |
| Docs | Documentation updates | 1-2 days | All phases |
| **Total** | **End-to-end** | **~10-15 days** | - |

## Next Steps

1. ✅ **Review this plan** - Get team feedback
2. **Approve approach** - Confirm architecture decisions
3. **Create feature branch** - `feature/graph-based-execution`
4. **Phase 1 implementation** - Build core graph engine
5. **Incremental review** - PR per phase for easier review
6. **Integration testing** - Test on all 3 platforms
7. Streaming Pipeline Example

**Use Case:** Real-time data processing pipeline

```python
# Define streaming pipes
@DataPipes.pipe(
    pipeid="stream_ingest_raw",
    name="Ingest Raw Events",
    input_entity_ids=[],  # Reads from Kafka/EventHub
    output_entity_id="stream.raw_events",
    output_type="stream"
)
def ingest_raw_events():
    return spark.readStream.format("kafka").load(...)

@DataPipes.pipe(
    pipeid="stream_clean",
    name="Clean Events",
    input_entity_ids=["stream.raw_events"],
    output_entity_id="stream.clean_events",
    output_type="stream"
)
def clean_events(stream_raw_events):
    return stream_raw_events.filter(...).select(...)

@DataPipes.pipe(
    pipeid="stream_aggregate",
    name="Aggregate Events",
    input_entity_ids=["stream.clean_events"],
    output_entity_id="stream.aggregated",
    output_type="stream"
)
def aggregate_events(stream_clean_events):
    return stream_clean_events.groupBy(...).agg(...)

# Start streaming pipes with graph-based ordering
orchestrator = GlobalInjector.get(SimplePipeStreamOrchestrator)
stream_handles = orchestrator.start_pipes_as_streams([
    "stream_ingest_raw",
    "stream_clean",
    "stream_aggregate"
])

# Execution order (REVERSE topological sort):
# 1. Start stream_aggregate (consumer ready, listening)
# 2. Start stream_clean (middle layer ready, listening)
# 3. Start stream_ingest_raw (producer starts emitting)
#
# Result: No data loss, consumers ready when data arrives
```

## Open Questions

1. **Should we support custom schedulers?** (e.g., Airflow, Prefect integration)
2. **Should we add graph visualization export?** (e.g., GraphViz DOT format)
3. **Should we support pipe priorities/weights?** (for scheduling decisions)
4. **Should parallel execution be opt-out instead of opt-in?** (more aggressive default)
5. **Should we add cost-based optimization?** (e.g., prefer pipes with cached inputs)
6. **Should streaming mode support startup delays?** (e.g., wait N seconds between generation
3. **Should we support pipe priorities/weights?** (for scheduling decisions)
4. **Should parallel execution be opt-out instead of opt-in?** (more aggressive default)
5. **Should we add cost-based optimization?** (e.g., prefer pipes with cached inputs)

---

**Document Version:** 1.0
**Date:** January 21, 2026
**Author:** GitHub Copilot (Claude Sonnet 4.5)
