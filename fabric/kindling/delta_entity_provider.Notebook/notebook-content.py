# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from typing import Callable, Optional, Literal
from delta.tables import DeltaTable
from functools import reduce
import pyspark.sql.utils
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from enum import Enum
import logging

# Import your existing modules
notebook_import(".injection")
notebook_import(".spark_config") 
notebook_import(".data_entities")
notebook_import(".common_transforms")
notebook_import(".spark_log_provider")

class DeltaAccessMode:
    """Defines how Delta tables are accessed"""
    FOR_NAME = "forName"     # Synapse style - tables registered in catalog
    FOR_PATH = "forPath"     # Fabric style - direct path access
    AUTO = "auto"           # Auto-detect based on environment

class DeltaTableReference:
    """Encapsulates how to reference a Delta table"""
    def __init__(self, table_name: str, table_path: str, access_mode: DeltaAccessMode):
        self.table_name = table_name
        self.table_path = table_path  
        self.access_mode = access_mode
        self.spark = get_or_create_spark_session()
    
    def get_delta_table(self) -> DeltaTable:
        """Get DeltaTable instance using appropriate method"""
        if self.access_mode == DeltaAccessMode.FOR_NAME:
            return DeltaTable.forName(self.spark, self.get_read_path())
        elif self.access_mode == DeltaAccessMode.FOR_PATH:
            return DeltaTable.forPath(self.spark, self.get_read_path())
        else:
            try:
                return DeltaTable.forName(self.spark, self.get_read_path())
            except Exception:
                return DeltaTable.forPath(self.spark, self.get_read_path())

    def get_spark_read_stream(self, spark, format = None, options = None):
        strmformat = format or "delta"
        base = spark.readStream.format(strmformat)

        if options is not None:
            path = options.get("path", None)
            if path is not None:
                del options["path"]
            base = base.options(**options)

        if strmformat != "delta":
            base = base.load()
        elif self.access_mode == DeltaAccessMode.FOR_NAME:
            base = base.table(self.table_name)
        elif self.access_mode == DeltaAccessMode.FOR_PATH:
            base = base.load(self.table_path)
        else:
            base = base.table(self.table_name)

        return base

    def get_read_path(self) -> str:
        """Get path for spark.read operations"""
        if self.access_mode == DeltaAccessMode.FOR_NAME:
            return self.table_name
        else:
            return self.table_path

@GlobalInjector.singleton_autobind()
class DeltaEntityProvider(EntityProvider):
    
    @inject
    def __init__(self, 
                 config: ConfigService,
                 entity_name_mapper: EntityNameMapper, 
                 path_locator: EntityPathLocator,
                 tp: PythonLoggerProvider):
        self.config = config
        self.epl = path_locator
        self.enm = entity_name_mapper
        self.access_mode = self.config.get("DELTA_TABLE_ACCESS_MODE") or "AUTO"
        self.spark = get_or_create_spark_session()
        self.logger = tp.get_logger("DeltaEntityProvider")
        
    def _get_table_reference(self, entity) -> DeltaTableReference:
        """Create table reference for entity"""
        return DeltaTableReference(
            table_name=self.enm.get_table_name(entity),
            table_path=self.epl.get_table_path(entity),
            access_mode=self.access_mode
        )
    
    def _check_table_exists(self, table_ref: DeltaTableReference) -> bool:
        """Enhanced existence check that works with both modes"""
        try:
            # Try to get the Delta table
            table_ref.get_delta_table()
            return True
        except Exception as e1:
            self.logger.debug(f"get_delta_table failed: {e1}")
            
            # For FOR_NAME mode, only check catalog (don't check path)
            if table_ref.access_mode == DeltaAccessMode.FOR_NAME:
                try:
                    self.spark.read.table(table_ref.table_name)
                    return True
                except Exception as e2:
                    self.logger.debug(f"Table not in catalog: {e2}")
                    return False
            
            # For FOR_PATH mode, check if path has Delta files
            else:
                try:
                    self.spark.read.format("delta").load(table_ref.table_path)
                    return True
                except Exception as e2:
                    self.logger.debug(f"Path check failed: {e2}")
                    return False
    
    def _ensure_table_exists(self, entity, table_ref: DeltaTableReference):
        """Ensure table exists, create if needed"""
        if self._check_table_exists(table_ref):
            self.logger.info(f"Table {table_ref.table_name} already exists")
            return
            
        self.logger.info(f"Creating Delta table {table_ref.table_name} at {table_ref.table_path}")
        
        try:
            # Create table using appropriate method
            if self.access_mode == DeltaAccessMode.FOR_NAME:
                # Synapse style - create registered table
                dt = DeltaTable.createIfNotExists(self.spark) \
                    .tableName(table_ref.table_name) \
                    .location(table_ref.table_path) \
                    .property("delta.columnMapping.mode", "name") \
                    .property("delta.enableChangeDataFeed", "true")
                
                # Add columns FIRST
                if entity.schema:    
                    dt = dt.addColumns(entity.schema)
                
                # Then partition by them
                if entity.partition_columns:
                    dt = dt.partitionedBy(*entity.partition_columns)
                
                self.logger.debug(f"Executing FOR_NAME create {table_ref.table_name} if not exists at {table_ref.table_path}")
                dt.execute()
            else:
                # Fabric style - create via path
                dt = DeltaTable.createIfNotExists(self.spark) \
                    .location(table_ref.table_path) \
                    .property("delta.columnMapping.mode", "name") \
                    .property("delta.enableChangeDataFeed", "true")
                
                # Add columns FIRST
                if entity.schema:    
                    dt = dt.addColumns(entity.schema)
                
                # Then partition by them
                if entity.partition_columns:
                    dt = dt.partitionedBy(*entity.partition_columns)
                
                self.logger.debug("Executing FOR_PATH create {table_ref.table_name} if not exists at {table_ref.table_path}")
                dt.execute()
            
            self.logger.debug("Registering table with SQL to create {table_ref.table_name} at {table_ref.table_path}")
            if table_ref.table_name and "." in table_ref.table_name:
                try:
                    self.spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS {table_ref.table_name} 
                        USING DELTA 
                        LOCATION '{table_ref.table_path}'
                    """)
                except Exception as e:
                    self.logger.warning(f"Could not register table in catalog: {e}")
                        
        except Exception as e:
            self.logger.error(f"Failed to create Delta table: {e}")
            raise
    
    def _read_delta_table(self, table_ref: DeltaTableReference, since_version: Optional[int] = None) -> DataFrame:
        """Read Delta table with optional change feed"""
        self.logger.debug(f"Reading Delta Table - {table_ref.table_name} version: {since_version}")
        
        if since_version is not None:
            self.logger.debug(f"Reading change feed since version: {since_version}")
            return self.spark.read.format("delta") \
                .option("readChangeFeed", "true") \
                .option("startingVersion", since_version) \
                .load(table_ref.get_read_path())
        else:
            dt = table_ref.get_delta_table()
            self.logger.debug(f"Reading full table for {table_ref.table_name}")
            return dt.toDF()
    
    def _write_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference):
        """Write DataFrame to Delta table"""
        df_writer = df.write.format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .option("mergeSchema", "true") \
            .option("path", table_ref.table_path)
        
        if entity.partition_columns:
            df_writer = df_writer.partitionBy(*entity.partition_columns)
        
        df_writer.save()
        
        # Register table if using named access
        if self.access_mode == DeltaAccessMode.FOR_NAME and table_ref.table_name:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_ref.table_name} 
                USING DELTA 
                LOCATION '{table_ref.table_path}'
            """)
    
    def _merge_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference):
        """Merge DataFrame to existing Delta table"""
        merge_condition = self._build_merge_condition('old', 'new', entity.merge_columns)
        
        (table_ref.get_delta_table()
            .alias("old")
            .merge(source=df.alias("new"), condition=merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    

    def _append_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference):
        """Append DataFrame to existing Delta table"""
        writer = df.write.format("delta").mode("append")
        
        if entity.partition_columns:
            writer = writer.partitionBy(*entity.partition_columns)
        
        writer.option("mergeSchema", "true")  # Schema evolution
        writer.save(table_ref.table_path)
        
    def _get_table_version(self, table_ref: DeltaTableReference) -> int:
        """Get current version of Delta table"""
        if not self._check_table_exists(table_ref):
            return 0
            
        try:
            version = (table_ref.get_delta_table()
                      .history(1)
                      .select("version")
                      .collect()[0][0])
            self.logger.debug(f"Retrieved table {table_ref.table_name} version: {version}")
            return version
        except Exception:
            return 0
    
    def _build_merge_condition(self, alias1: str, alias2: str, cols: list[str]) -> str:
        """Build merge condition for Delta table operations"""
        return reduce(
            lambda expr, col: expr + f' and {alias1}.`{col}` = {alias2}.`{col}`',
            cols[1:],
            f'{alias1}.`{cols[0]}` = {alias2}.`{cols[0]}`'
        )
    
    def _transform_delta_feed_to_changes(self, change_feed_df: DataFrame, key_columns: list[str]) -> DataFrame:
        """Transform change feed to latest changes per key"""
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, col
        
        filtered_df = change_feed_df.filter(col("_change_type") != "delete").drop("_change_type")
        
        window_spec = Window.partitionBy(*key_columns).orderBy(
            col("_commit_version").desc(),
            col("_commit_timestamp").desc()
        )
        
        ranked_df = filtered_df.withColumn("row_num", row_number().over(window_spec))
        
        return (ranked_df.filter(col("row_num") == 1)
                .drop("row_num")
                .transform(drop_if_exists, "SourceTimestamp")
                .withColumnRenamed("_commit_version", "SourceVersion")
                .withColumnRenamed("_commit_timestamp", "SourceTimestamp"))

    # EntityProvider interface implementation
    def ensure_entity_table(self, entity):
        """Ensure entity table exists"""
        table_ref = self._get_table_reference(entity)
        self._ensure_table_exists(entity, table_ref)
    
    def check_entity_exists(self, entity) -> bool:
        """Check if entity table exists"""
        table_ref = self._get_table_reference(entity)
        return self._check_table_exists(table_ref)
    
    def append_as_stream(self, entity, df, checkpointLocation, format=None, options=None):
        epl = GlobalInjector.get(EntityPathLocator)
        streamFormat = format or "delta"

        return df.writeStream \
            .outputMode("append") \
            .format(streamFormat) \
            .option("checkpointLocation", checkpointLocation)

    def merge_to_entity(self, df: DataFrame, entity):
        table_ref = self._get_table_reference(entity)
        if self._check_table_exists(table_ref):
            self._merge_to_delta_table(df, entity, table_ref)
        else:
            self.write_to_entity(df, entity)
    
    def append_to_entity(self, df: DataFrame, entity):
        table_ref = self._get_table_reference(entity)
        if self._check_table_exists(table_ref):
            self._append_to_delta_table(df, entity, table_ref)
        else:
            self.write_to_entity(df, entity)
    
    def read_entity_since_version(self, entity, since_version: int) -> DataFrame:
        """Read entity changes since specific version"""
        table_ref = self._get_table_reference(entity)
        df = self._read_delta_table(table_ref, since_version)
        return self._transform_delta_feed_to_changes(df, entity.merge_columns)
    
    def read_entity(self, entity) -> DataFrame:
        """Read full entity table"""
        table_ref = self._get_table_reference(entity)
        return self._read_delta_table(table_ref)
    
    def read_entity_as_stream(self, entity, format=None, options=None) -> DataFrame:
        """Read full entity table"""
        table_ref = self._get_table_reference(entity)
        return table_ref.get_spark_read_stream(self.spark, format, options)

    def write_to_entity(self, df: DataFrame, entity):
        """Write DataFrame to entity table"""
        table_ref = self._get_table_reference(entity)
        self._write_to_delta_table(df, entity, table_ref)
    
    def get_entity_version(self, entity) -> int:
        """Get current version of entity table"""
        table_ref = self._get_table_reference(entity)
        return self._get_table_version(table_ref)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
