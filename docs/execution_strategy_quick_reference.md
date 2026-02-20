# Execution Strategy - Quick Reference

## Overview

The execution strategy module provides strategy pattern implementations for batch vs streaming execution ordering of data pipelines.

**Module:** `packages/kindling/execution_strategy.py`  
**Dependencies:** `pipe_graph.py`, `data_pipes.py`  
**Tests:** 31 unit + 13 integration = 44 total  
**Coverage:** 94% on `execution_strategy.py` (latest targeted test run)

## Core Classes

### ExecutionStrategy (ABC)

Abstract base class for execution strategies.

```python
from kindling.execution_strategy import ExecutionStrategy

class CustomStrategy(ExecutionStrategy):
    def plan(self, graph: PipeGraph, pipe_ids: List[str]) -> ExecutionPlan:
        # Custom planning logic
        pass
    
    def get_strategy_name(self) -> str:
        return "custom"
```

### BatchExecutionStrategy

Forward topological order (sources → sinks) for batch ETL.

```python
from kindling.execution_strategy import BatchExecutionStrategy

strategy = BatchExecutionStrategy(logger_provider)
plan = strategy.plan(graph, pipe_ids)

# Result: Generation 0: [sources], Generation N: [sinks]
```

**Use Cases:**
- Traditional ETL pipelines
- Nightly batch processing
- Data warehouse loading
- Forward dependency chains

### StreamingExecutionStrategy

Reverse topological order (sinks → sources) for streaming.

```python
from kindling.execution_strategy import StreamingExecutionStrategy

strategy = StreamingExecutionStrategy(logger_provider)
plan = strategy.plan(graph, pipe_ids)

# Result: Generation 0: [sinks], Generation N: [sources]
```

**Use Cases:**
- Real-time streaming pipelines
- Event-driven architectures
- Downstream-first checkpoint establishment
- Reverse dependency chains

### ExecutionPlan

Immutable execution plan with generations and validation.

```python
from kindling.execution_strategy import ExecutionPlan

plan = ExecutionPlan(
    pipe_ids=["pipe1", "pipe2"],
    generations=[gen0, gen1],
    graph=graph,
    strategy="batch",
    metadata={"key": "value"}
)

# Query methods
plan.total_pipes()           # → 2
plan.total_generations()     # → 2
plan.max_parallelism()       # → Max pipes in any generation
plan.get_generation(0)       # → Generation object
plan.find_pipe_generation("pipe1")  # → 0
plan.get_summary()           # → Dict with stats

# Validation
plan.validate()  # Raises ValueError if invalid
```

**Validation Rules:**
- **Batch**: All dependencies must execute before consumers
- **Streaming**: Allows consumers before producers (reverse order)
- **Both**: All pipes must be accounted for exactly once

### Generation

Represents a parallelizable group of pipes.

```python
from kindling.execution_strategy import Generation

gen = Generation(
    number=0,
    pipe_ids=["pipe1", "pipe2"],
    dependencies=["pipe0"]
)

len(gen)           # → 2
"pipe1" in gen     # → True
list(gen)          # → ["pipe1", "pipe2"]
```

### ExecutionPlanGenerator (Facade)

High-level facade with dependency injection.

```python
from kindling.execution_strategy import ExecutionPlanGenerator

generator = ExecutionPlanGenerator(
    pipes_manager,
    graph_builder,
    logger_provider
)

# Generate batch plan
batch_plan = generator.generate_batch_plan(pipe_ids)

# Generate streaming plan
streaming_plan = generator.generate_streaming_plan(pipe_ids)

# Generate with custom strategy
custom_plan = generator.generate_plan(pipe_ids, custom_strategy)

# Visualize plan
print(generator.visualize_plan(batch_plan))
```

## Batch vs Streaming Comparison

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| **Order** | Forward (sources → sinks) | Reverse (sinks → sources) |
| **Use Case** | ETL, data warehouse | Event processing, real-time |
| **Execution** | Source data read first | Sink registered first |
| **Dependencies** | Must satisfy before execution | Can start consumers before producers |
| **Checkpointing** | Upstream first | Downstream first |
| **Parallelism** | Same (based on graph structure) | Same (based on graph structure) |

## Common Patterns

### Medallion Architecture

```python
# Bronze → Silver → Gold pipeline

# Batch plan (forward):
# Gen 0: [bronze_orders, bronze_customers]
# Gen 1: [silver_orders, silver_customers]
# Gen 2: [gold_analytics]

# Streaming plan (reverse):
# Gen 0: [gold_analytics]
# Gen 1: [silver_orders, silver_customers]
# Gen 2: [bronze_orders, bronze_customers]
```

### Fan-Out / Fan-In

```python
# Single source → Multiple processors → Single sink

# Both strategies:
# - 3 generations
# - Max parallelism = number of parallel processors
# - Batch: source first, streaming: sink first
```

### Multiple Independent Chains

```python
# Chain A: A1 → A2 → A3
# Chain B: B1 → B2 → B3

# Both strategies:
# Gen 0: [A1, B1]  (or [A3, B3] for streaming)
# Gen 1: [A2, B2]  (or [A2, B2] for streaming)
# Gen 2: [A3, B3]  (or [A1, B1] for streaming)
# Max parallelism: 2 (two chains)
```

## Usage Examples

### Example 1: Simple Batch Pipeline

```python
from kindling.execution_strategy import ExecutionPlanGenerator

# Set up generator (DI auto-wires in notebooks)
generator = ExecutionPlanGenerator(
    pipes_manager,
    graph_builder,
    logger_provider
)

# Generate plan
pipe_ids = ["ingest_orders", "clean_orders", "aggregate_orders"]
plan = generator.generate_batch_plan(pipe_ids)

# Inspect plan
print(f"Total generations: {plan.total_generations()}")
print(f"Max parallelism: {plan.max_parallelism()}")

# Iterate generations
for gen in plan.generations:
    print(f"Generation {gen.number}: {gen.pipe_ids}")
```

### Example 2: Streaming Multi-Sink

```python
# Multiple outputs from single source
pipe_ids = [
    "read_events",
    "write_to_db",
    "write_to_warehouse", 
    "write_to_lake"
]

# Streaming plan starts sinks first
streaming_plan = generator.generate_streaming_plan(pipe_ids)

# Verify checkpoint order (sinks first)
assert streaming_plan.metadata["checkpoint_order"] == "downstream_first"
```

### Example 3: Plan Comparison

```python
pipe_ids = ["source", "transform", "sink"]

# Generate both plans
batch_plan = generator.generate_batch_plan(pipe_ids)
streaming_plan = generator.generate_streaming_plan(pipe_ids)

# Compare
print("Batch generations:")
for gen in batch_plan.generations:
    print(f"  Gen {gen.number}: {gen.pipe_ids}")

print("\nStreaming generations:")
for gen in streaming_plan.generations:
    print(f"  Gen {gen.number}: {gen.pipe_ids}")

# Output:
# Batch generations:
#   Gen 0: ['source']
#   Gen 1: ['transform']
#   Gen 2: ['sink']
#
# Streaming generations:
#   Gen 0: ['sink']
#   Gen 1: ['transform']
#   Gen 2: ['source']
```

### Example 4: Plan Visualization

```python
plan = generator.generate_batch_plan(pipe_ids)

# Get formatted visualization
viz = generator.visualize_plan(plan)
print(viz)

# Output:
# Execution Plan (batch)
# ==================================================
# Total Pipes: 5
# Total Generations: 3
# Max Parallelism: 2
#
# Execution Order:
# --------------------------------------------------
#
# Generation 0: (2 pipes)
#   - pipe1
#   - pipe2
#
# Generation 1: (2 pipes)
#   - pipe3 (deps: ['pipe1'])
#   - pipe4 (deps: ['pipe2'])
# ...
```

## Integration with Execution

Execution strategies are currently integrated with `ExecutionPlanGenerator` and `GenerationExecutor`:

```python
from kindling.execution_strategy import ExecutionPlanGenerator
from kindling.generation_executor import GenerationExecutor

# Generate execution plan
generator = ExecutionPlanGenerator(...)
plan = generator.generate_batch_plan(pipe_ids)

# Execute by generation (sequential or parallel per generation)
executor = GenerationExecutor(...)
result = executor.execute_batch(plan, parallel=True, max_workers=4)
```

## Performance Characteristics

### Batch Strategy

- ✅ Optimal for: Forward dependency chains, ETL workflows
- ✅ Data availability: Ensures upstream data exists before downstream processing
- ✅ Debugging: Easy to trace data flow from source to sink
- ⚠️ Limitation: Must wait for all upstream processing to complete

### Streaming Strategy

- ✅ Optimal for: Event processing, real-time pipelines
- ✅ Checkpointing: Downstream checkpoints established first
- ✅ Backpressure: Natural flow control from consumers to producers
- ⚠️ Limitation: Requires streaming-compatible operations

## Testing

### Unit Tests

```bash
# Run execution strategy unit tests
pytest tests/unit/test_execution_strategy.py -v

# Coverage: 94% on execution_strategy.py
# Tests: 31 passing
```

**Test Coverage:**
- Generation dataclass operations
- ExecutionPlan validation (batch and streaming)
- BatchExecutionStrategy planning
- StreamingExecutionStrategy planning
- ExecutionPlanGenerator facade
- Complex graph patterns

### Integration Tests

```bash
# Run execution strategy integration tests
pytest tests/integration/test_execution_strategy_integration.py -v

# Tests: 13 passing
```

**Test Scenarios:**
- Medallion architecture (bronze/silver/gold)
- Fan-out / fan-in patterns
- Multiple independent chains
- Shared entity optimization
- Streaming checkpoint ordering
- Multi-sink streaming
- Plan validation rules
- Custom strategies

## Architecture Notes

### Why Two Strategies?

**Batch (Forward)**:
- Traditional data warehouse ETL
- Source data is finite and available
- Processing flows downstream (sources produce, sinks consume)
- Dependencies must be satisfied before execution

**Streaming (Reverse)**:
- Real-time event processing
- Data is infinite streams
- Processing starts at sinks (establish checkpoints first)
- Consumers pull from producers (backpressure)

### Strategy Pattern Benefits

1. **Extensibility**: Easy to add new strategies (e.g., hybrid, custom priority)
2. **Testability**: Strategies can be tested independently
3. **Flexibility**: Choose strategy per workflow execution
4. **Clarity**: Separation of concerns (strategy vs execution)

### Design Decisions

**Why immutable ExecutionPlan?**
- Plans should not be modified after generation
- Makes reasoning about execution deterministic
- Enables plan caching and reuse

**Why validate() raises exceptions?**
- Plans should be validated before execution
- Fail fast on configuration errors
- Clear error messages for debugging

**Why metadata dict?**
- Extensible for custom strategy information
- No breaking changes when adding new fields
- Easy to serialize/deserialize

## Future Enhancements

**Planned for Issue #25: Cache Optimization**
- Shared entity read detection
- LRU cache for frequently read entities
- Cache recommendation signals
- Memory-aware caching strategies

**Additional DAG capability closure work**
- `ExecutionOrchestrator` facade wiring
- Optional `DataPipesExecuter` DAG entrypoint compatibility helpers
- Project/issue status cleanup for capability `#15`

## Related Documentation

- [Pipe Graph Quick Reference](pipe_graph_quick_reference.md) - Foundation for execution strategies
- [docs/graph_based_pipe_execution_plan.md](../docs/graph_based_pipe_execution_plan.md) - Overall design document
- [GitHub Issue #23](https://github.com/sep/spark-kindling-framework/issues/23) - Execution Strategies feature

## Summary

The execution strategy module provides clean separation between:
1. **What to execute** (PipeGraph - dependency structure)
2. **When to execute** (ExecutionStrategy - ordering logic)
3. **How to execute** (GenerationExecutor - sequential/parallel generation execution)

Use `BatchExecutionStrategy` for traditional ETL and `StreamingExecutionStrategy` for real-time processing.
