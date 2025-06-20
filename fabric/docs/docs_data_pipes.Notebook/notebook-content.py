# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Data Pipes Framework Documentation
# 
# ## Overview
# 
# The Data Pipes Framework is a notebook-based data processing system built on Apache Spark with dependency injection. It provides a declarative way to define, register, and execute data transformation pipelines with automatic dependency resolution and tracing capabilities.
# 
# ## Core Concepts
# 
# ### Data Pipes
# Data pipes are individual transformation units that take input entities (DataFrames) and produce output entities. Each pipe is defined using a decorator and contains metadata about its inputs, outputs, and execution logic.
# 
# ### Entities
# Entities represent data sources or transformed datasets, typically as Spark DataFrames. They are identified by unique entity IDs and managed through the framework's registry system.
# 
# ### Pipeline Execution
# The framework orchestrates the execution of multiple pipes, handling data flow between them and providing tracing and logging capabilities.
# 
# ## Public Interfaces
# 
# ### 1. PipeMetadata
# 
# ```python
# @dataclass
# class PipeMetadata:
#     pipeid: str
#     name: str        
#     execute: Callable
#     tags: Dict[str,str]
#     input_entity_ids: List[str]
#     output_entity_id: str
#     output_type: str
# ```
# 
# **Purpose**: Defines metadata for a data pipe.
# 
# **Fields**:
# - `pipeid`: Unique identifier for the pipe
# - `name`: Human-readable name for the pipe
# - `execute`: The function that performs the transformation
# - `tags`: Key-value pairs for categorization and filtering
# - `input_entity_ids`: List of input entity identifiers
# - `output_entity_id`: Identifier for the output entity
# - `output_type`: Type of the output (e.g., "table", "view")
# 
# ### 2. @DataPipes.pipe() Decorator
# 
# ```python
# @DataPipes.pipe(
#     pipeid="unique_pipe_id",
#     name="Human Readable Name",
#     tags={"category": "transformation", "env": "prod"},
#     input_entity_ids=["input.entity1", "input.entity2"],
#     output_entity_id="output.transformed_data",
#     output_type="table"
# )
# def my_transformation_function(input_entity1, input_entity2):
#     # Your transformation logic here
#     return transformed_dataframe
# ```
# 
# **Purpose**: Decorator to register data transformation functions as pipes.
# 
# **Parameters**: All parameters correspond to `PipeMetadata` fields except `execute` (automatically set).
# 
# **Usage Notes**:
# - All parameters are required
# - Input entity IDs with dots (.) are converted to underscores (_) in function parameters
# - The decorated function receives DataFrames as named parameters
# - Function must return a DataFrame
# 
# ### 3. EntityReadPersistStrategy (Abstract)
# 
# ```python
# class EntityReadPersistStrategy(ABC):
#     @abstractmethod
#     def create_pipe_entity_reader(self, pipe: str):
#         """Create a reader function for pipe entities"""
#         pass
# 
#     @abstractmethod
#     def create_pipe_persist_activator(self, pipe: PipeMetadata):
#         """Create a persist function for pipe output"""
#         pass
# ```
# 
# **Purpose**: Abstract interface for defining how entities are read and persisted.
# 
# **Implementation Required**: Users must implement both methods to define their storage strategy.
# 
# ### 4. DataPipesRegistry (Abstract)
# 
# ```python
# class DataPipesRegistry(ABC):
#     @abstractmethod
#     def register_pipe(self, pipeid, **decorator_params):
#         """Register a pipe with given parameters"""
#         pass
# 
#     @abstractmethod
#     def get_pipe_ids(self):
#         """Get all registered pipe IDs"""
#         pass
# 
#     @abstractmethod
#     def get_pipe_definition(self, name):
#         """Get pipe definition by name"""
#         pass
# ```
# 
# **Purpose**: Abstract interface for pipe registry operations.
# 
# **Default Implementation**: `DataPipesManager` provides the concrete implementation.
# 
# ### 5. DataPipesExecution (Abstract)
# 
# ```python
# class DataPipesExecution(ABC):
#     @abstractmethod
#     def run_datapipes(self, pipes):
#         """Execute a list of pipes"""
#         pass
# ```
# 
# **Purpose**: Abstract interface for pipe execution.
# 
# **Default Implementation**: `DataPipesExecuter` provides the concrete implementation.
# 
# ### 6. DataPipesManager
# 
# ```python
# @GlobalInjector.singleton_autobind()
# class DataPipesManager(BaseServiceProvider, DataPipesRegistry):
#     def get_pipe_ids(self):
#         """Returns all registered pipe IDs"""
#         
#     def get_pipe_definition(self, name):
#         """Returns PipeMetadata for given pipe name"""
# ```
# 
# **Purpose**: Concrete implementation of pipe registry with automatic dependency injection.
# 
# **Key Features**:
# - Singleton pattern with automatic binding
# - Debug logging for pipe registration
# - Thread-safe registry storage
# 
# ### 7. DataPipesExecuter
# 
# ```python
# @GlobalInjector.singleton_autobind()
# class DataPipesExecuter(BaseServiceProvider, DataPipesExecution):
#     def run_datapipes(self, pipes):
#         """Execute a list of pipes in order"""
# ```
# 
# **Purpose**: Concrete implementation of pipe execution engine.
# 
# **Key Features**:
# - Distributed tracing support
# - Automatic entity reading and persistence
# - Conditional execution (skips if first input entity is None)
# - Debug logging throughout execution
# 
# ## Usage Examples
# 
# ### Basic Pipe Definition
# 
# ```python
# @DataPipes.pipe(
#     pipeid="clean_customer_data",
#     name="Clean Customer Data",
#     tags={"category": "cleaning", "domain": "customer"},
#     input_entity_ids=["raw.customers"],
#     output_entity_id="clean.customers",
#     output_type="table"
# )
# def clean_customers(raw_customers):
#     return raw_customers.filter(col("email").isNotNull()) \
#                        .dropDuplicates(["customer_id"])
# ```
# 
# ### Multi-Input Pipe
# 
# ```python
# @DataPipes.pipe(
#     pipeid="customer_orders_summary",
#     name="Customer Orders Summary",
#     tags={"category": "aggregation", "domain": "analytics"},
#     input_entity_ids=["clean.customers", "clean.orders"],
#     output_entity_id="summary.customer_orders",
#     output_type="table"
# )
# def create_customer_summary(clean_customers, clean_orders):
#     return clean_customers.join(clean_orders, "customer_id") \
#                          .groupBy("customer_id", "customer_name") \
#                          .agg(count("order_id").alias("total_orders"),
#                               sum("order_amount").alias("total_spent"))
# ```
# 
# ### Executing Pipes
# 
# ```python
# # Get the executer from dependency injection
# executer = GlobalInjector.get(DataPipesExecuter)
# 
# # Execute specific pipes
# pipes_to_run = ["clean_customer_data", "customer_orders_summary"]
# executer.run_datapipes(pipes_to_run)
# ```
# 
# ### Getting Registered Pipes
# 
# ```python
# # Get the registry from dependency injection
# registry = GlobalInjector.get(DataPipesRegistry)
# 
# # List all registered pipes
# all_pipes = registry.get_pipe_ids()
# print(f"Registered pipes: {list(all_pipes)}")
# 
# # Get specific pipe definition
# pipe_def = registry.get_pipe_definition("clean_customer_data")
# print(f"Pipe: {pipe_def.name}, Inputs: {pipe_def.input_entity_ids}")
# ```
# 
# ## Implementation Requirements
# 
# To use this framework, you must implement:
# 
# 1. **EntityReadPersistStrategy**: Define how your data is read from and written to storage
# 2. **Data Entity Registry**: Implement entity definition lookup (referenced but not shown in code)
# 3. **Logging and Tracing Providers**: Set up logging and distributed tracing infrastructure
# 
# ## Dependencies
# 
# The framework requires these components to be available through dependency injection:
# - `PythonLoggerProvider`: For logging capabilities
# - `DataEntityRegistry`: For entity definition management
# - `SparkTraceProvider`: For distributed tracing
# - `EntityReadPersistStrategy`: For data I/O operations
# 
# ## Error Handling
# 
# - **Missing Decorator Parameters**: Raises `ValueError` if required `PipeMetadata` fields are missing
# - **Entity Not Found**: Pipes are skipped if the first input entity returns `None`
# - **Execution Failures**: Individual pipe failures are logged but don't stop the entire pipeline
# 
# ## Best Practices
# 
# 1. **Pipe Design**: Keep pipes focused on single transformations
# 2. **Entity Naming**: Use consistent, hierarchical naming (e.g., `domain.entity_name`)
# 3. **Tags**: Use tags for categorization and pipeline filtering
# 4. **Error Handling**: Implement robust error handling in pipe functions
# 5. **Testing**: Test pipe functions independently before registration
# 6. **Documentation**: Document complex transformation logic within pipe functions
# 
# ## Logging and Monitoring
# 
# The framework provides built-in logging at debug level for:
# - Pipe registration events
# - Pipeline execution start/end
# - Individual pipe execution
# - Pipe skipping due to missing inputs
# 
# Distributed tracing spans are automatically created for:
# - Overall pipeline execution
# - Individual pipe execution
# 
# This enables comprehensive monitoring and debugging of data pipeline performance and behavior.

