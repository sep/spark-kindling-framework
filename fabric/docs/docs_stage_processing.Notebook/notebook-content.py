# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Stage Processing
# 
# The Stage Processing module provides a higher-level orchestration layer for organizing and executing data pipes in logical groups or stages. It simplifies complex multi-step pipeline execution and provides unified error handling and monitoring.
# 
# ## Core Concepts
# 
# ### Stages
# 
# Stages are logical groupings of related data pipes that should be executed together. Each stage typically represents a distinct phase in the data processing workflow, such as:
# 
# - Data ingestion
# - Validation
# - Transformation
# - Enrichment
# - Aggregation
# 
# ### Stage Execution
# 
# The stage processor handles executing all pipes within a stage, with:
# - Automatic dependency resolution
# - Unified error handling
# - Comprehensive tracing
# - Watermark management

# METADATA ********************

# META {
# META   "language": "markdown"
# META }

# MARKDOWN ********************

# ## Key Components
# 
# ### StageProcessingService
# 
# Interface defining the stage execution functionality.
# 
# ```python
# class StageProcessingService(ABC):
#     @abstractmethod
#     def execute(self, stage: str, stage_description: str, stage_details: Dict, layer: str):
#         """Execute all pipes within a stage"""
#         pass
# ```
# 
# ### StageProcessor
# 
# Default implementation of the StageProcessingService.
# 
# ```python
# @GlobalInjector.singleton_autobind()
# class StageProcessor(StageProcessingService):
#     @inject
#     def __init__(self, dpr: DataPipesRegistry, ep: EntityProvider, 
#                 dep: DataPipesExecution, wef: WatermarkEntityFinder, 
#                 tp: SparkTraceProvider):
#         # Initialization with required dependencies
#         pass
# 
#     def execute(self, stage: str, stage_description: str, stage_details: Dict, layer: str):
#         # Implementation of stage execution logic
#         pass
# ```
# 
# ### execute_process_stage Function
# 
# Helper function to execute a stage from notebooks.
# 
# ```python
# def execute_process_stage(stage: str, stage_description: str, stage_details: Dict, layer: str):
#     """
#     Execute all pipes within a stage.
#     
#     Args:
#         stage: Stage identifier prefix for matching pipes
#         stage_description: Human-readable description
#         stage_details: Additional details/parameters for the stage
#         layer: Data layer identifier (e.g., 'bronze', 'silver', 'gold')
#     """
#     GlobalInjector.get(StageProcessingService).execute(
#         stage, stage_description, stage_details, layer
#     )
# ```

# METADATA ********************

# META {
# META   "language": "markdown"
# META }

# MARKDOWN ********************

# ## Usage Examples
# 
# ### Executing a Stage
# 
# ```python
# # Execute the customer validation stage
# execute_process_stage(
#     stage="validate_customer",
#     stage_description="Validate Customer Data",
#     stage_details={"source": "CRM", "validation_level": "strict"},
#     layer="silver"
# )
# ```
# 
# ### Pipe Naming Convention
# 
# To include a pipe in a stage, prefix its ID with the stage name:
# 
# ```python
# # This pipe will be included in the "validate_customer" stage
# @DataPipes.pipe(
#     pipeid="validate_customer.check_email",
#     name="Validate Customer Email",
#     tags={"validation": "email"},
#     input_entity_ids=["bronze.customers"],
#     output_entity_id="silver.validated_customers",
#     output_type="table"
# )
# def validate_email(customers):
#     # Email validation logic
#     return customers.filter(...)
# ```
# 
# ## Stage Watermarking
# 
# The stage processor automatically ensures watermark tables exist for the specified layer:
# 
# ```python
# # Inside StageProcessor.execute
# self.ep.ensure_entity_table(self.wef.get_watermark_entity_for_layer(layer))
# ```

# METADATA ********************

# META {
# META   "language": "markdown"
# META }

# MARKDOWN ********************

# ## Advanced Features
# 
# ### Stage Dependencies
# 
# You can implement stage dependencies by chaining stage executions:
# 
# ```python
# # First execute ingestion
# execute_process_stage("ingest", "Data Ingestion", {}, "bronze")
# 
# # Then execute transformations that depend on ingested data
# execute_process_stage("transform", "Data Transformation", {}, "silver")
# ```
# 
# ### Parallel Stage Execution
# 
# For independent stages, you can implement parallel execution:
# 
# ```python
# from concurrent.futures import ThreadPoolExecutor
# 
# def execute_parallel_stages(stages, layer):
#     with ThreadPoolExecutor(max_workers=3) as executor:
#         futures = []
#         for stage in stages:
#             futures.append(executor.submit(
#                 execute_process_stage, 
#                 stage["id"], 
#                 stage["description"], 
#                 stage["details"], 
#                 layer
#             ))
#         
#         # Wait for all to complete
#         for future in futures:
#             future.result()
# ```
# 
# ## Best Practices
# 
# 1. **Logical Stage Division**: Group pipes into stages based on logical processing steps.
# 
# 2. **Layer Association**: Typically associate stages with data layers (bronze, silver, gold).
# 
# 3. **Error Handling**: Configure appropriate error handling at the stage level.
# 
# 4. **Monitoring**: Use the stage details to capture execution metadata.
# 
# 5. **Naming Convention**: Use a consistent prefix naming convention for pipes within stages.
# 
# 6. **Documentation**: Document stage dependencies and execution order requirements.

# METADATA ********************

# META {
# META   "language": "markdown"
# META }
