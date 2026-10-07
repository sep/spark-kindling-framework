# Data Pipes Framework Documentation

## Overview

The Data Pipes Framework is a notebook-based data processing system built on Apache Spark with dependency injection. It provides a declarative way to define, register, and execute data transformation pipelines with automatic dependency resolution and tracing capabilities.

## Core Concepts

### Data Pipes
Data pipes are individual transformation units that take input entities (DataFrames) and produce output entities. Each pipe is defined using a decorator and contains metadata about its inputs, outputs, and execution logic.

### Entities
Entities represent data sources or transformed datasets, typically as Spark DataFrames. They are identified by unique entity IDs and managed through the framework's registry system.

### Pipeline Execution
The framework orchestrates the execution of multiple pipes, handling data flow between them and providing tracing and logging capabilities.

## Public Interfaces

### 1. PipeMetadata

```python
@dataclass
class PipeMetadata:
    pipeid: str
    name: str
    execute: Callable
    tags: Dict[str,str]
    input_entity_ids: List[str]
    output_entity_id: str
    output_type: str
    use_watermark: bool = False
    driving_entity_ids: Optional[List[str]] = None
```

**Purpose**: Defines metadata for a data pipe.

**Fields**:
- `pipeid`: Unique identifier for the pipe
- `name`: Human-readable name for the pipe
- `execute`: The function that performs the transformation
- `tags`: Key-value pairs for categorization and filtering
- `input_entity_ids`: List of input entity identifiers
- `output_entity_id`: Identifier for the output entity
- `output_type`: Free-form label for the output (e.g., "table", "delta"). Required by
  the decorator and shown by the CLI, but never read at runtime: how the output is
  written comes from the output entity's `provider_type` tag and its write tags
  (`write.mode`, `dataset.kind`, `merge_columns`), not from this field
- `use_watermark`: Whether to apply watermark-based incremental reads (default `False`)
- `driving_entity_ids`: Optional subset of `input_entity_ids` that drive the pipe
  (default `None`, meaning the first input only). Driving inputs are the ones read
  incrementally when `use_watermark` is set, and the ones that decide whether the
  pipe is skipped. Must be a non-empty list of ids that appear in
  `input_entity_ids`, or `ValueError` is raised

### 2. @DataPipes.pipe() Decorator

```python
@DataPipes.pipe(
    pipeid="unique_pipe_id",
    name="Human Readable Name",
    tags={"category": "transformation", "env": "prod"},
    input_entity_ids=["input.entity1", "input.entity2"],
    output_entity_id="output.transformed_data",
    output_type="table"
)
def my_transformation_function(input_entity1, input_entity2):
    # Your transformation logic here
    return transformed_dataframe
```

**Purpose**: Decorator to register data transformation functions as pipes.

**Parameters**: All parameters correspond to `PipeMetadata` fields except `execute` (automatically set).

**Usage Notes**:
- `pipeid`, `name`, `tags`, `input_entity_ids`, `output_entity_id`, and `output_type` are required; `use_watermark` and `driving_entity_ids` are optional
- Input entity IDs with dots (.) are converted to underscores (_) in function parameters
- The decorated function receives DataFrames as named parameters
- Function must return a DataFrame

### 3. EntityReadPersistStrategy (Abstract)

```python
class EntityReadPersistStrategy(ABC):
    @abstractmethod
    def create_pipe_entity_reader(self, pipe: PipeMetadata):
        """Create a reader function for pipe entities"""
        pass

    @abstractmethod
    def create_pipe_persist_activator(self, pipe: PipeMetadata):
        """Create a persist function for pipe output"""
        pass
```

**Purpose**: Abstract interface for defining how entities are read and persisted.

**Default Implementation**: `SimpleReadPersistStrategy` is bound automatically. It reads each input through the entity's provider (applying watermarks to driving inputs) and writes the output according to the output entity's tags and provider. Implement this interface only to replace that behavior.

### 4. DataPipesRegistry (Abstract)

```python
class DataPipesRegistry(ABC):
    @abstractmethod
    def register_pipe(self, pipeid, **decorator_params):
        """Register a pipe with given parameters"""
        pass

    @abstractmethod
    def unregister_pipe(self, pipeid):
        """Remove a registered pipe"""
        pass

    @abstractmethod
    def get_pipe_ids(self):
        """Get all registered pipe IDs"""
        pass

    @abstractmethod
    def get_pipe_definition(self, name):
        """Get pipe definition by name"""
        pass
```

**Purpose**: Abstract interface for pipe registry operations.

**Default Implementation**: `DataPipesManager` provides the concrete implementation.

### 5. DataPipesExecution (Abstract)

```python
class DataPipesExecution(ABC):
    @abstractmethod
    def run_datapipes(self, pipes):
        """Execute a list of pipes"""
        pass

    @abstractmethod
    def run_datapipes_dag(self, pipes, strategy=None, **kwargs):
        """Execute a list of pipes through the DAG orchestrator"""
        pass
```

**Purpose**: Abstract interface for pipe execution.

**Default Implementation**: `DataPipesExecuter` provides the concrete implementation.

### 6. DataPipesManager

```python
@GlobalInjector.singleton_autobind()
class DataPipesManager(DataPipesRegistry):
    def get_pipe_ids(self):
        """Returns all registered pipe IDs"""

    def get_pipe_definition(self, name):
        """Returns PipeMetadata for given pipe name"""
```

**Purpose**: Concrete implementation of pipe registry with automatic dependency injection.

**Key Features**:
- Singleton pattern with automatic binding
- Debug logging for pipe registration
- Config overlays from the `datapipes:` and `datapipes-bytag:` sections

### 7. DataPipesExecuter

```python
@GlobalInjector.singleton_autobind()
class DataPipesExecuter(DataPipesExecution):
    def run_datapipes(self, pipes):
        """Execute a list of pipes in order"""
```

**Purpose**: Concrete implementation of pipe execution engine.

**Key Features**:
- Distributed tracing support
- Automatic entity reading and persistence
- Conditional execution (skips a pipe when every driving input read returns `None`; by default the only driving input is the first one)
- Debug logging throughout execution

## Usage Examples

### Basic Pipe Definition

```python
@DataPipes.pipe(
    pipeid="clean_customer_data",
    name="Clean Customer Data",
    tags={"category": "cleaning", "domain": "customer"},
    input_entity_ids=["raw.customers"],
    output_entity_id="clean.customers",
    output_type="table"
)
def clean_customers(raw_customers):
    return raw_customers.filter(col("email").isNotNull()) \
                       .dropDuplicates(["customer_id"])
```

### Multi-Input Pipe

```python
@DataPipes.pipe(
    pipeid="customer_orders_summary",
    name="Customer Orders Summary",
    tags={"category": "aggregation", "domain": "analytics"},
    input_entity_ids=["clean.customers", "clean.orders"],
    output_entity_id="summary.customer_orders",
    output_type="table"
)
def create_customer_summary(clean_customers, clean_orders):
    return clean_customers.join(clean_orders, "customer_id") \
                         .groupBy("customer_id", "customer_name") \
                         .agg(count("order_id").alias("total_orders"),
                              sum("order_amount").alias("total_spent"))
```

### Executing Pipes

```python
# Get the executer from dependency injection
executer = GlobalInjector.get(DataPipesExecuter)

# Execute specific pipes
pipes_to_run = ["clean_customer_data", "customer_orders_summary"]
executer.run_datapipes(pipes_to_run)
```

### DAG-Based Execution

`run_datapipes` accepts a `use_dag=True` flag to delegate to the
`ExecutionOrchestrator`, which builds a dependency graph and runs pipes
in topological generation order. This is the recommended path for any
pipeline with non-trivial dependencies.

```python
# Dependency-aware execution — runs pipes in correct order automatically
executer.run_datapipes(pipes_to_run, use_dag=True)
```

Execution options (parallelism, worker count, error strategy, timeouts,
caching) are **config-first**: set them once under `kindling.execution.*`
and they apply to every DAG run, per environment:

```yaml
kindling:
  execution:
    parallel: true        # run independent pipes within a generation concurrently
    max_workers: 4
    error_strategy: fail_fast   # or: continue, skip_dependents
    auto_cache: true
```

Parameters remain available as just-in-time overrides for spot-testing —
a passed value beats config for that run only:

```python
from kindling.generation_executor import ErrorStrategy
executer.run_datapipes(
    pipes_to_run,
    use_dag=True,
    parallel=False,          # override config for this one run
    error_strategy=ErrorStrategy.CONTINUE,
)
```

You can also use `ExecutionOrchestrator` directly for batch or streaming:

```python
from kindling.execution_orchestrator import ExecutionOrchestrator

orchestrator = GlobalInjector.get(ExecutionOrchestrator)

# Batch mode
result = orchestrator.execute_batch(pipes_to_run, parallel=True)

# Streaming mode
result = orchestrator.execute_streaming(pipes_to_run)

# Inspect result
print(f"Succeeded: {result.success_count}, Failed: {result.failed_count}")
```

`ExecutionOrchestrator` emits an `orchestrator.plan_generated` signal before
execution begins, carrying the resolved strategy, pipe count, and generation
count for observability hooks.

### Getting Registered Pipes

```python
# Get the registry from dependency injection
registry = GlobalInjector.get(DataPipesRegistry)

# List all registered pipes
all_pipes = registry.get_pipe_ids()
print(f"Registered pipes: {list(all_pipes)}")

# Get specific pipe definition
pipe_def = registry.get_pipe_definition("clean_customer_data")
print(f"Pipe: {pipe_def.name}, Inputs: {pipe_def.input_entity_ids}")
```

## Implementation Requirements

`initialize()` binds default implementations of everything the framework needs
(`SimpleReadPersistStrategy`, `DataEntityManager`, and the platform's logging and
tracing providers). You only implement these interfaces to replace a default:

1. **EntityReadPersistStrategy**: Change how data is read from and written to storage
2. **DataEntityRegistry**: Change how entity definitions are looked up
3. **Logging and Tracing Providers**: Change the logging and distributed tracing infrastructure

## Dependencies

The framework requires these components to be available through dependency injection:
- `PythonLoggerProvider`: For logging capabilities
- `DataEntityRegistry`: For entity definition management
- `SparkTraceProvider`: For distributed tracing
- `EntityReadPersistStrategy`: For data I/O operations

## Error Handling

- **Missing Decorator Parameters**: Raises `ValueError` if required `PipeMetadata` fields are missing
- **No New Data**: A pipe is skipped (and `datapipes.pipe_skipped` is emitted) when every driving input read returns `None`, for example when a watermarked read finds nothing new. A non-driving input that reads `None` does not skip the pipe
- **Execution Failures**: `run_datapipes` without `use_dag` stops at the first failing pipe: it emits `datapipes.pipe_failed` and `datapipes.run_failed`, then re-raises the exception, so later pipes in the list do not run. With `use_dag=True`, `error_strategy` decides (`fail_fast` by default, `continue`, or `skip_dependents`)

## Best Practices

1. **Pipe Design**: Keep pipes focused on single transformations
2. **Entity Naming**: Use consistent, hierarchical naming (e.g., `domain.entity_name`)
3. **Tags**: Use tags for categorization and pipeline filtering
4. **Error Handling**: Implement robust error handling in pipe functions
5. **Testing**: Test pipe functions independently before registration
6. **Documentation**: Document complex transformation logic within pipe functions

## Logging and Monitoring

The framework provides built-in logging at debug level for:
- Pipe registration events
- Individual pipe execution
- Pipe skipping due to missing inputs

Run start and end are reported through the `datapipes.before_run`,
`datapipes.after_run`, and `datapipes.run_failed` signals rather than log lines.

When tracing is enabled (the default), distributed tracing spans are created for:
- Overall pipeline execution
- Individual pipe execution

This enables comprehensive monitoring and debugging of data pipeline performance and behavior.

## Cloning and extending pipes

`DataPipes.clone` declares a new pipe from another's declaration (retarget
its output, add inputs, wrap its transform); `DataPipes.extend` adds to a
pipe in place. `transform(previous_output, **added_inputs)` receives the
previous execute's output and the DataFrames of its own `add_inputs` by
keyword (entity id with dots replaced by underscores). Extensions stack with
the last registered outermost, and the result is still one pipe with one
execute, so the runner, streaming and the declarative engine see nothing new;
a cloned pipe keeps its own watermark state under its own id.

```python
DataPipes.clone(
    "silver.enrich_orders", from_pipe="silver.build_orders",
    output_entity_id="silver.orders_enriched",
    add_inputs=["ref.regions"],
    transform=lambda df, ref_regions: df.join(ref_regions, "region_code"),
)
DataPipes.extend("silver.build_orders", tags={"sla": "gold"})
```

In settings YAML, `clone_of` and `add_inputs` on an exact id under
`datapipes:` do the same without a transform. See
`docs/proposals/declaration_derivations.md`.
