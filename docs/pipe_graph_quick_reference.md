# Pipe Graph Module - Quick Reference

## Overview
The `pipe_graph` module provides dependency graph analysis for data pipes, enabling DAG-based execution planning with cycle detection, topological sorting, and generation-based parallelization.

## Key Components

### Data Structures

- **`PipeNode`**: Represents a pipe with its input/output entities
- **`PipeEdge`**: Represents a dependency between two pipes (via shared entity)
- **`PipeGraph`**: Complete dependency graph with nodes, edges, and adjacency information

### PipeGraphBuilder

Main class for building and analyzing dependency graphs:

```python
from kindling.pipe_graph import PipeGraphBuilder
from kindling.data_pipes import DataPipesRegistry

# Initialize builder (uses dependency injection)
builder = PipeGraphBuilder(registry, logger_provider)

# Build graph from pipe IDs
graph = builder.build_graph(["pipe1", "pipe2", "pipe3"])

# Get topological sort (dependencies first)
execution_order = builder.topological_sort(graph)

# Get generations for parallel execution
generations = builder.get_generations(graph)
# Result: [["pipe1"], ["pipe2", "pipe3"], ["pipe4"]]
# Pipes in same generation can run in parallel

# Get reverse order (for streaming - sinks first)
reverse_order = builder.reverse_topological_sort(graph)
```

## Features

### ✅ Cycle Detection
Automatically detects cycles with clear error messages:
```python
try:
    graph = builder.build_graph(pipe_ids)
except GraphCycleError as e:
    print(f"Cycle detected: {e}")
    # "Cycle detected in pipe dependencies: pipe1 → pipe2 → pipe3 → pipe1"
```

### ✅ Topological Sorting
Orders pipes respecting dependencies:
- **Forward sort**: Sources first, sinks last (batch mode)
- **Reverse sort**: Sinks first, sources last (streaming mode)

### ✅ Generation Grouping
Groups pipes that can execute in parallel:
```python
generations = builder.get_generations(graph)
for gen_num, pipes in enumerate(generations):
    print(f"Generation {gen_num}: {pipes} (can run in parallel)")
```

### ✅ Cache Optimization
Identifies entities read by multiple pipes:
```python
shared = graph.get_shared_inputs()
# {"silver.customers": ["analytics1", "analytics2", "report1"]}
# These entities are good candidates for caching
```

### ✅ Graph Validation
- Detects missing pipes
- Warns about external data sources
- Validates all dependencies are satisfiable

### ✅ Graph Visualization
Generate Mermaid diagrams for debugging:
```python
mermaid = builder.visualize_mermaid(graph)
# Returns Mermaid markdown for visualization tools
```

### ✅ Graph Statistics
```python
stats = builder.get_graph_stats(graph)
# {
#   "node_count": 10,
#   "edge_count": 15,
#   "generation_count": 4,
#   "max_parallelism": 3,
#   "shared_entities": 2,
#   ...
# }
```

## Usage Patterns

### Medallion Architecture Example
```python
# Bronze layer (sources)
register_pipe("ingest_orders", [], "bronze.orders")
register_pipe("ingest_customers", [], "bronze.customers")

# Silver layer (cleaning)
register_pipe("clean_orders", ["bronze.orders"], "silver.orders")
register_pipe("clean_customers", ["bronze.customers"], "silver.customers")

# Gold layer (analytics)
register_pipe("analytics", ["silver.orders", "silver.customers"], "gold.dashboard")

graph = builder.build_graph([...])
generations = builder.get_generations(graph)

# Generation 0: ["ingest_orders", "ingest_customers"] - parallel
# Generation 1: ["clean_orders", "clean_customers"] - parallel
# Generation 2: ["analytics"] - waits for generation 1
```

### Batch vs Streaming Execution
```python
# Batch mode - forward order (sources → sinks)
batch_generations = builder.get_generations(graph)

# Streaming mode - reverse order (sinks → sources)
streaming_generations = builder.get_reverse_generations(graph)
```

## Error Handling

### GraphCycleError
Raised when circular dependencies are detected:
```python
from kindling.pipe_graph import GraphCycleError

try:
    graph = builder.build_graph(pipe_ids)
except GraphCycleError as e:
    print(f"Fix cycle: {e.cycle}")
    # e.cycle = ["pipe1", "pipe2", "pipe3", "pipe1"]
```

### GraphValidationError
Raised when graph structure is invalid:
```python
from kindling.pipe_graph import GraphValidationError

try:
    graph = builder.build_graph(["nonexistent_pipe"])
except GraphValidationError as e:
    print(f"Validation failed: {e}")
```

## Testing

**Unit Tests**: `tests/unit/test_pipe_graph.py` (32 tests)
- Data structure tests
- Graph building algorithms  
- Cycle detection
- Topological sorting
- Generation grouping
- Edge cases

**Integration Tests**: `tests/integration/test_pipe_graph_integration.py` (8 tests)
- Medallion architecture patterns
- Fan-in/fan-out patterns
- Complex real-world workflows
- External data sources
- Streaming patterns

## Performance

- **Graph building**: O(V + E) where V = pipes, E = dependencies
- **Cycle detection**: O(V + E) using DFS
- **Topological sort**: O(V + E) using Kahn's algorithm
- **Generation grouping**: O(V + E)

## Next Steps

This module is **Issue #22** of the Unified DAG Orchestrator capability and is implemented.

Implemented alongside this module:
- **Issue #23**: Execution Strategies (batch vs streaming)
- **Issue #24**: Generation Executor (parallel execution)

Remaining DAG feature work:
- **Issue #25**: Cache Optimization (auto-caching shared entities)
- **Issue #15**: Capability closure and status reconciliation for Unified DAG Orchestrator

## Related Documentation

- [DAG Execution Implementation Plan](../docs/proposals/dag_execution_implementation_plan.md)
- [Data Pipes Documentation](../docs/data_pipes.md)
- [GitHub Issue #22](https://github.com/sep/spark-kindling-framework/issues/22)
