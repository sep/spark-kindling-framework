# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Entity Providers
# 
# Entity Providers are a core component of the Kindling Framework, responsible for abstracting the storage and access mechanisms for data entities. They provide a consistent interface for performing CRUD operations, regardless of the underlying storage technology.
# 
# ## Core Concepts
# 
# ### Entity Provider
# 
# An Entity Provider is responsible for:
# 
# - Creating and managing entity tables
# - Reading entity data
# - Writing and updating entity data
# - Handling merge operations
# - Managing entity versions
# 
# ### Provider Abstraction
# 
# The framework uses an abstract interface (`EntityProvider`) to enable multiple provider implementations for different storage systems (e.g., Delta Lake, Parquet, Hive tables).
# 
# ### Delta Lake Integration
# 
# The primary implementation, `DeltaEntityProvider`, provides seamless integration with Delta Lake, leveraging features like:
# 
# - ACID transactions
# - Schema evolution
# - Time travel
# - Merge operations
# - Partitioning

# METADATA ********************

# META {
# META   "language": "markdown"
# META }

# MARKDOWN ********************

# ## Key Components
# 
# ### EntityProvider Interface
# 
# The abstract interface defining all operations that entity providers must implement.
# 
# ```python
# class EntityProvider(ABC):
#     @abstractmethod
#     def ensure_entity_table(self, entity):
#         """Ensure the entity table exists, create if necessary"""
#         pass
# 
#     @abstractmethod
#     def check_entity_exists(self, entity):
#         """Check if entity table exists"""
#         pass
# 
#     @abstractmethod
#     def merge_to_entity(self, df, entity):
#         """Merge DataFrame into entity using merge columns"""
#         pass
# 
#     @abstractmethod
#     def append_to_entity(self, df, entity):
#         """Append DataFrame to entity"""
#         pass
# 
#     @abstractmethod
#     def read_entity(self, entity):
#         """Read entire entity as DataFrame"""
#         pass
# 
#     @abstractmethod
#     def read_entity_since_version(self, entity, since_version):
#         """Read entity changes since specific version"""
#         pass
# 
#     @abstractmethod
#     def write_to_entity(self, df, entity):
#         """Write DataFrame to entity (overwrite)"""
#         pass
# 
#     @abstractmethod
#     def get_entity_version(self, entity):
#         """Get current version of entity"""
#         pass
# ```
# 
# ### DeltaEntityProvider
# 
# The default implementation for Delta Lake storage.
# 
# ```python
# @GlobalInjector.singleton_autobind()
# class DeltaEntityProvider(BaseServiceProvider, EntityProvider):
#     @inject
#     def __init__(self, entity_name_mapper: EntityNameMapper, 
#                  path_locator: EntityPathLocator):
#         # Initialize provider with required dependencies
#         pass
#         
#     # Implementation of all EntityProvider methods
#     def ensure_entity_table(self, entity):
#         # Create the table if it doesn't exist
#         pass
#         
#     def merge_to_entity(self, df, entity):
#         # Perform a Delta merge operation
#         pass
#         
#     # Additional methods...
# ```

# METADATA ********************

# META {
# META   "language": "markdown"
# META }

# MARKDOWN ********************

# ### DeltaTableReference
# 
# A utility class for handling different ways of accessing Delta tables.
# 
# ```python
# class DeltaTableReference:
#     """Encapsulates how to reference a Delta table"""
#     def __init__(self, table_name: str, table_path: str, access_mode: DeltaAccessMode):
#         # Initialize with both name and path
#         pass
#     
#     def get_delta_table(self) -> DeltaTable:
#         """Get DeltaTable instance using appropriate method"""
#         pass
#     
#     def get_read_path(self) -> str:
#         """Get path for spark.read operations"""
#         pass
# ```
# 
# ### DeltaAccessMode
# 
# An enumeration of different ways to access Delta tables.
# 
# ```python
# class DeltaAccessMode(Enum):
#     """Defines how Delta tables are accessed"""
#     FOR_NAME = "forName"     # Synapse style - tables registered in catalog
#     FOR_PATH = "forPath"     # Fabric style - direct path access
#     AUTO = "auto"           # Auto-detect based on environment
# ```
# 
# ## Usage Examples
# 
# ### Basic CRUD Operations
# 
# ```python
# # Get entity provider
# entity_provider = GlobalInjector.get(EntityProvider)
# 
# # Get entity definition
# entity = data_entity_registry.get_entity_definition("sales.transactions")
# 
# # Read entity
# df = entity_provider.read_entity(entity)
# 
# # Write to entity (overwrite)
# entity_provider.write_to_entity(transformed_df, entity)
# 
# # Append to entity
# entity_provider.append_to_entity(new_records_df, entity)
# ```

# METADATA ********************

# META {
# META   "language": "markdown"
# META }

# MARKDOWN ********************

# ### Merge Operations
# 
# ```python
# # Get entity provider
# entity_provider = GlobalInjector.get(EntityProvider)
# 
# # Define entity with merge columns
# @DataEntities.entity(
#     entityid="customers.profiles",
#     name="Customer Profiles",
#     partition_columns=["country"],
#     merge_columns=["customer_id"],  # These columns define the merge key
#     tags={"domain": "customer"},
#     schema=customer_schema
# )
# 
# # Later, merge new/updated records
# entity_provider.merge_to_entity(
#     updated_customers_df,
#     entity_registry.get_entity_definition("customers.profiles")
# )
# ```
# 
# ### Versioning and Time Travel
# 
# ```python
# # Get entity version
# current_version = entity_provider.get_entity_version(entity)
# 
# # Read changes since a specific version
# changes_df = entity_provider.read_entity_since_version(entity, last_processed_version)
# ```
# 
# ## Implementation Details
# 
# ### Table Creation
# 
# The `ensure_entity_table` method creates tables with appropriate configuration:
# 
# ```python
# def ensure_entity_table(self, entity):
#     # Create empty DataFrame with entity schema
#     empty_df = self.spark.createDataFrame([], entity.schema)
#     
#     # Write with appropriate options
#     empty_df.write \
#         .format("delta") \
#         .partitionBy(*entity.partition_columns) \
#         .save(table_path)
# ```

# METADATA ********************

# META {
# META   "language": "markdown"
# META }

# MARKDOWN ********************

# ### Merge Operations
# 
# The `merge_to_entity` method implements Delta Lake's merge capabilities:
# 
# ```python
# def merge_to_entity(self, df, entity):
#     # Get Delta table reference
#     delta_table = self._get_table_reference(entity).get_delta_table()
#     
#     # Build merge condition from entity's merge columns
#     merge_condition = " AND ".join([f"source.{col} = target.{col}" 
#                                    for col in entity.merge_columns])
#     
#     # Perform merge operation
#     delta_table.alias("target") \
#         .merge(df.alias("source"), merge_condition) \
#         .whenMatchedUpdateAll() \
#         .whenNotMatchedInsertAll() \
#         .execute()
# ```
# 
# ## Advanced Features
# 
# ### Schema Evolution
# 
# The DeltaEntityProvider supports automatic schema evolution:
# 
# ```python
# # Enable schema evolution in Spark config
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
# 
# # During merge operations, new columns will be added automatically
# ```
# 
# ### Z-Ordering
# 
# Optimize table read performance with Z-ordering:
# 
# ```python
# def optimize_entity(self, entity, z_order_cols=None):
#     # Get table reference
#     table_ref = self._get_table_reference(entity)
#     
#     # Build optimize command
#     optimize_cmd = f"OPTIMIZE {table_ref.get_read_path()}"
#     
#     # Add Z-ORDER if columns specified
#     if z_order_cols:
#         z_order_cols_str = ", ".join(z_order_cols)
#         optimize_cmd += f" ZORDER BY ({z_order_cols_str})"
#     
#     # Execute command
#     self.spark.sql(optimize_cmd)
# ```

# METADATA ********************

# META {
# META   "language": "markdown"
# META }

# MARKDOWN ********************

# ### Vacuum Operations
# 
# Control data retention through vacuum operations:
# 
# ```python
# def vacuum_entity(self, entity, retention_hours=168):  # Default 7 days
#     # Get table reference
#     table_ref = self._get_table_reference(entity)
#     
#     # Execute vacuum command
#     self.spark.sql(f"VACUUM {table_ref.get_read_path()} RETAIN {retention_hours} HOURS")
# ```
# 
# ## Best Practices
# 
# 1. **Use Merge Operations**: Prefer merge operations over appends for upsert scenarios.
# 
# 2. **Define Proper Merge Keys**: Carefully select merge columns that uniquely identify records.
# 
# 3. **Partitioning Strategy**: Choose partitioning columns based on query patterns and data distribution.
# 
# 4. **Schema Evolution**: Enable schema evolution for development, but manage schema carefully in production.
# 
# 5. **Version Management**: Use versioning for incremental processing and auditing.
# 
# 6. **Performance Optimization**: Consider Z-ordering for frequently filtered columns.
# 
# 7. **Data Retention**: Plan vacuum operations according to your retention requirements.
# 
# ## Custom Entity Providers
# 
# To implement a custom entity provider:
# 
# 1. Create a class that implements the EntityProvider interface
# 2. Register it with the dependency injection system
# 3. Ensure all method contracts are properly fulfilled
# 
# Example:
# 
# ```python
# @GlobalInjector.singleton_autobind()
# class CustomEntityProvider(BaseServiceProvider, EntityProvider):
#     # Implement all required methods
#     
#     def ensure_entity_table(self, entity):
#         # Custom implementation
#         pass
#     
#     # ... other methods
# ```

# METADATA ********************

# META {
# META   "language": "markdown"
# META }
