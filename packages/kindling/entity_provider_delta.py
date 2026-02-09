import logging
import time
from enum import Enum
from functools import reduce
from typing import Callable, Literal, Optional

import pyspark.sql.utils
from delta.tables import DeltaTable
from kindling.common_transforms import *

# Import your existing modules
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_log_provider import *
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame

from .data_entities import *
from .entity_provider import (
    BaseEntityProvider,
    StreamableEntityProvider,
    StreamWritableEntityProvider,
    WritableEntityProvider,
)


class DeltaAccessMode:
    """Defines how Delta tables are accessed"""

    FOR_NAME = "forName"  # Synapse style - tables registered in catalog
    FOR_PATH = "forPath"  # Fabric style - direct path access
    AUTO = "auto"  # Auto-detect based on environment


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

    def get_spark_read_stream(self, spark, format=None, options=None):
        base = spark.readStream.format("delta")

        if options is not None:
            path = options.get("path", None)
            if path is not None:
                del options["path"]
            base = base.options(**options)

        if self.access_mode == DeltaAccessMode.FOR_NAME:
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
class DeltaEntityProvider(
    EntityProvider,
    BaseEntityProvider,
    StreamableEntityProvider,
    WritableEntityProvider,
    StreamWritableEntityProvider,
    SignalEmitter,
):
    """
    Delta Lake implementation with full entity provider capabilities.

    Implements all 4 provider interfaces:
    - BaseEntityProvider: Batch read operations
    - StreamableEntityProvider: Streaming read operations
    - WritableEntityProvider: Batch write operations
    - StreamWritableEntityProvider: Streaming write operations

    Also maintains backward compatibility with legacy EntityProvider interface.

    Provider configuration options (via entity tags with 'provider.' prefix):
    - provider.path: Override table path (optional, defaults to EntityPathLocator)
    - provider.table_name: Override table name (optional, defaults to EntityNameMapper)
    - provider.access_mode: Override access mode (optional, values: forName, forPath, auto)

    Example entity definition with tag-based configuration:
    ```python
    @DataEntities.entity(
        entityid="sales.transactions",
        name="transactions",
        partition_columns=["date"],
        merge_columns=["transaction_id"],
        tags={
            "provider_type": "delta",
            "provider.path": "Tables/custom/sales_transactions",
            "provider.access_mode": "forPath"
        }
    )
    ```

    Note: Tag-based overrides take precedence over injected services (EntityPathLocator,
    EntityNameMapper). This enables flexible per-entity configuration via YAML.
    """

    @inject
    def __init__(
        self,
        config: ConfigService,
        entity_name_mapper: EntityNameMapper,
        path_locator: EntityPathLocator,
        tp: PythonLoggerProvider,
        signal_provider: SignalProvider = None,
    ):
        self._init_signal_emitter(signal_provider)
        self.config = config
        self.epl = path_locator
        self.enm = entity_name_mapper
        self.access_mode = self.config.get("DELTA_TABLE_ACCESS_MODE") or "AUTO"
        self.spark = get_or_create_spark_session()
        self.logger = tp.get_logger("DeltaEntityProvider")

    def _get_table_reference(self, entity) -> DeltaTableReference:
        """
        Create table reference for entity.

        Supports both tag-based and service-based configuration:
        - provider.path: Override table path (optional)
        - provider.table_name: Override table name (optional)
        - provider.access_mode: Override access mode (optional, values: forName, forPath, auto)

        Falls back to injected services (EntityPathLocator, EntityNameMapper) if tags not provided.
        """
        # Get tag-based configuration
        config = self._get_provider_config(entity)

        # Use tag overrides if provided, otherwise fall back to services
        table_path = config.get("path") or self.epl.get_table_path(entity)
        table_name = config.get("table_name") or self.enm.get_table_name(entity)

        # Check for access_mode override in tags
        access_mode = config.get("access_mode")
        if access_mode:
            # Validate the access mode value
            valid_modes = ["forName", "forPath", "auto"]
            if access_mode not in valid_modes:
                self.logger.warning(
                    f"Invalid provider.access_mode '{access_mode}' in tags for entity '{entity.entityid}'. "
                    f"Valid values: {valid_modes}. Using config default: {self.access_mode}"
                )
                access_mode = self.access_mode
        else:
            access_mode = self.access_mode

        return DeltaTableReference(
            table_name=table_name,
            table_path=table_path,
            access_mode=access_mode,
        )

    def _ensure_schema_applied(self, entity, table_ref: DeltaTableReference):
        """Check if table has schema, apply if missing"""
        try:
            # Read existing table to check schema
            existing_df = self.spark.read.format("delta").load(table_ref.table_path)
            existing_fields = existing_df.schema.fields

            # If table has no columns, write empty df with schema
            if len(existing_fields) == 0:
                self.logger.warning(
                    f"Table {table_ref.table_name} exists but has no schema, applying schema from entity definition"
                )

                from pyspark.sql.types import StructType

                empty_df = self.spark.createDataFrame([], schema=StructType(entity.schema))

                # Write with mergeSchema to add columns
                empty_df.write.format("delta").mode("append").option("mergeSchema", "true").save(
                    table_ref.table_path
                )

                self.logger.info(f"Schema applied to {table_ref.table_name}")
            else:
                self.logger.info(
                    f"Table {table_ref.table_name} already has schema with {len(existing_fields)} columns"
                )

        except Exception as e:
            self.logger.error(f"Failed to check/apply schema for {table_ref.table_name}: {e}")
            raise

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

        # Validate paths exist
        if not table_ref.table_path:
            raise ValueError(f"Table path is None for entity {entity.name}")

        # Check what exists
        physical_exists = self._check_physical_table_exists(table_ref)
        catalog_exists = self._check_catalog_table_exists(table_ref)

        self.logger.info(
            f"Table status for {table_ref.table_name}: physical={physical_exists}, catalog={catalog_exists}"
        )

        if physical_exists and catalog_exists:
            self.logger.info(f"Table {table_ref.table_name} already exists (physical and catalog)")
            if entity.schema:
                self._ensure_schema_applied(entity, table_ref)
            else:
                self.logger.info(
                    f"Table {table_ref.table_name} already exists (physical and catalog)"
                )
            return

        if not physical_exists:
            self.logger.info(f"Creating physical table at {table_ref.table_path}")
            self._create_physical_table(entity, table_ref)

        if not catalog_exists and table_ref.table_name and "." in table_ref.table_name:
            self._attempt_catalog_registration(table_ref)

    def _check_physical_table_exists(self, table_ref: DeltaTableReference) -> bool:
        """Check if physical Delta files exist at the path"""
        try:
            self.spark.read.format("delta").load(table_ref.table_path)
            return True
        except Exception as e:
            self.logger.debug(f"Physical table check failed: {e}")
            return False

    def _check_catalog_table_exists(self, table_ref: DeltaTableReference) -> bool:
        """Check if table is registered in catalog"""
        if not table_ref.table_name or "." not in table_ref.table_name:
            return False

        try:
            self.spark.read.table(table_ref.table_name)
            return True
        except Exception as e:
            self.logger.debug(f"Catalog table check failed: {e}")
            return False

    def _create_physical_table(self, entity, table_ref: DeltaTableReference):
        """Create physical Delta table files - no catalog interaction"""
        try:
            self.logger.debug(f"Creating physical Delta table at {table_ref.table_path}")

            # Save current database and switch to default to avoid Hive metastore issues
            current_db = self.spark.sql("SELECT current_database()").collect()[0][0]
            if current_db != "default":
                self.logger.debug(
                    f"Switching from database '{current_db}' to 'default' for table creation"
                )
                self.spark.sql("USE default")

            try:
                # Always use location-only to avoid Hive metastore issues
                dt = (
                    DeltaTable.createIfNotExists(self.spark)
                    .location(table_ref.table_path)
                    .property("delta.enableChangeDataFeed", "true")
                )

                # Add columns if schema provided
                if entity.schema:
                    dt = dt.addColumns(entity.schema)

                # Then partition
                if entity.partition_columns:
                    dt = dt.partitionedBy(*entity.partition_columns)

                self.logger.debug(f"Executing Delta table creation")
                dt.execute()
                self.logger.info(
                    f"Successfully created physical Delta table at {table_ref.table_path}"
                )

            finally:
                # Restore original database
                if current_db != "default":
                    self.logger.debug(f"Restoring database to '{current_db}'")
                    self.spark.sql(f"USE {current_db}")

        except Exception as e:
            self.logger.error(
                f"Failed to create physical Delta table at {table_ref.table_path}: {e}"
            )
            raise

    def _attempt_catalog_registration(self, table_ref: DeltaTableReference):
        """
        Attempt to register table in catalog - this is best-effort and won't fail the operation.
        Catalog registration can fail if the database has no location configured in Hive metastore.

        FOR_PATH mode: This is completely optional
        FOR_NAME mode: Logs an error but doesn't fail since table still works via path
        """
        # Skip catalog registration entirely for FOR_PATH mode
        if self.access_mode == DeltaAccessMode.FOR_PATH:
            self.logger.debug(f"Skipping catalog registration for FOR_PATH mode")
            return

        try:
            self.logger.info(f"Attempting to register table {table_ref.table_name} in catalog")

            # Try to register using SQL
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {table_ref.table_name}
                USING DELTA
                LOCATION '{table_ref.table_path}'
            """
            )

            self.logger.info(f"Successfully registered {table_ref.table_name} in catalog")

        except Exception as e:
            error_msg = str(e)

            if "null path" in error_msg.lower() or "hiveexception" in error_msg.lower():
                # This is the known Hive metastore configuration issue
                database = (
                    table_ref.table_name.split(".")[0] if "." in table_ref.table_name else "unknown"
                )
                self.logger.error(
                    f"Cannot register table {table_ref.table_name} in catalog - database '{database}' has no location in Hive metastore. "
                    f"The table is still accessible via path: {table_ref.table_path}. "
                    f"To fix: Switch to FOR_PATH access mode (DELTA_TABLE_ACCESS_MODE='forPath') "
                    f"or recreate database with: CREATE DATABASE {database} LOCATION 'abfss://...';"
                )
            else:
                # Some other error
                self.logger.error(
                    f"Could not register table {table_ref.table_name} in catalog: {e}"
                )

            # For FOR_NAME mode, this is a problem but don't fail - table still works via path
            if self.access_mode == DeltaAccessMode.FOR_NAME:
                self.logger.warning(
                    f"FOR_NAME mode requested but catalog registration failed. Table will work via path access only."
                )

    def _read_delta_table(
        self, table_ref: DeltaTableReference, since_version: Optional[int] = None
    ) -> DataFrame:
        """Read Delta table with optional change feed"""
        self.logger.debug(f"Reading Delta Table - {table_ref.table_name} version: {since_version}")

        if since_version is not None:
            self.logger.debug(f"Reading change feed since version: {since_version}")
            return (
                self.spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", since_version)
                .load(table_ref.get_read_path())
            )
        else:
            dt = table_ref.get_delta_table()
            self.logger.debug(f"Reading full table for {table_ref.table_name}")
            return dt.toDF()

    def _write_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference):
        """Write DataFrame to Delta table"""
        df_writer = (
            df.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("mergeSchema", "true")
            .option("path", table_ref.table_path)
        )

        if entity.partition_columns:
            df_writer = df_writer.partitionBy(*entity.partition_columns)

        df_writer.save()

        # Register table if using named access
        if self.access_mode == DeltaAccessMode.FOR_NAME and table_ref.table_name:
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {table_ref.table_name}
                USING DELTA
                LOCATION '{table_ref.table_path}'
            """
            )

    def _merge_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference):
        """Merge DataFrame to existing Delta table"""
        merge_condition = self._build_merge_condition("old", "new", entity.merge_columns)

        (
            table_ref.get_delta_table()
            .alias("old")
            .merge(source=df.alias("new"), condition=merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

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
            version = table_ref.get_delta_table().history(1).select("version").collect()[0][0]
            self.logger.debug(f"Retrieved table {table_ref.table_name} version: {version}")
            return version
        except Exception:
            return 0

    def _build_merge_condition(self, alias1: str, alias2: str, cols: list[str]) -> str:
        """Build merge condition for Delta table operations"""
        return reduce(
            lambda expr, col: expr + f" and {alias1}.`{col}` = {alias2}.`{col}`",
            cols[1:],
            f"{alias1}.`{cols[0]}` = {alias2}.`{cols[0]}`",
        )

    def _transform_delta_feed_to_changes(
        self, change_feed_df: DataFrame, key_columns: list[str]
    ) -> DataFrame:
        """Transform change feed to latest changes per key"""
        from pyspark.sql import Window
        from pyspark.sql.functions import col, row_number

        filtered_df = change_feed_df.filter(col("_change_type") != "delete").drop("_change_type")

        window_spec = Window.partitionBy(*key_columns).orderBy(
            col("_commit_version").desc(), col("_commit_timestamp").desc()
        )

        ranked_df = filtered_df.withColumn("row_num", row_number().over(window_spec))

        return (
            ranked_df.filter(col("row_num") == 1)
            .drop("row_num")
            .transform(drop_if_exists, "SourceTimestamp")
            .withColumnRenamed("_commit_version", "SourceVersion")
            .withColumnRenamed("_commit_timestamp", "SourceTimestamp")
        )

    # EntityProvider interface implementation with signals
    def ensure_entity_table(self, entity):
        """Ensure entity table exists"""
        start_time = time.time()
        table_ref = self._get_table_reference(entity)

        self.emit(
            "entity.before_ensure_table",
            entity_id=entity.entityid,
            entity_name=entity.name,
            table_path=table_ref.table_path,
        )

        try:
            self._ensure_table_exists(entity, table_ref)
            duration = time.time() - start_time

            self.emit(
                "entity.after_ensure_table",
                entity_id=entity.entityid,
                entity_name=entity.name,
                duration_seconds=duration,
            )
        except Exception as e:
            duration = time.time() - start_time
            self.emit(
                "entity.ensure_failed",
                entity_id=entity.entityid,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=duration,
            )
            raise

    def check_entity_exists(self, entity) -> bool:
        """Check if entity table exists"""
        table_ref = self._get_table_reference(entity)
        return self._check_table_exists(table_ref)

    def append_as_stream(self, entity, df, checkpointLocation, format=None, options=None):
        epl = GlobalInjector.get(EntityPathLocator)
        streamFormat = format or "delta"

        return (
            df.writeStream.outputMode("append")
            .format(streamFormat)
            .option("mergeSchema", "true")
            .option("checkpointLocation", checkpointLocation)
        )

    def merge_to_entity(self, df: DataFrame, entity):
        """Merge DataFrame to entity table with signal emissions."""
        start_time = time.time()
        table_ref = self._get_table_reference(entity)

        self.emit(
            "entity.before_merge",
            entity_id=entity.entityid,
            entity_name=entity.name,
            merge_columns=entity.merge_columns,
        )

        try:
            if self._check_table_exists(table_ref):
                self._merge_to_delta_table(df, entity, table_ref)
            else:
                self.write_to_entity(df, entity)

            duration = time.time() - start_time
            self.emit(
                "entity.after_merge",
                entity_id=entity.entityid,
                entity_name=entity.name,
                duration_seconds=duration,
            )
        except Exception as e:
            duration = time.time() - start_time
            self.emit(
                "entity.merge_failed",
                entity_id=entity.entityid,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=duration,
            )
            raise

    def append_to_entity(self, df: DataFrame, entity):
        """Append DataFrame to entity table with signal emissions."""
        start_time = time.time()
        table_ref = self._get_table_reference(entity)

        self.emit("entity.before_append", entity_id=entity.entityid, entity_name=entity.name)

        try:
            if self._check_table_exists(table_ref):
                self._append_to_delta_table(df, entity, table_ref)
            else:
                self.write_to_entity(df, entity)

            duration = time.time() - start_time
            self.emit(
                "entity.after_append",
                entity_id=entity.entityid,
                entity_name=entity.name,
                duration_seconds=duration,
            )
        except Exception as e:
            duration = time.time() - start_time
            self.emit(
                "entity.append_failed",
                entity_id=entity.entityid,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=duration,
            )
            raise

    def read_entity_since_version(self, entity, since_version: int) -> DataFrame:
        """Read entity changes since specific version"""
        table_ref = self._get_table_reference(entity)
        df = self._read_delta_table(table_ref, since_version)
        return self._transform_delta_feed_to_changes(df, entity.merge_columns)

    def read_entity(self, entity) -> DataFrame:
        """Read full entity table with signal emissions."""
        start_time = time.time()
        table_ref = self._get_table_reference(entity)

        self.emit("entity.before_read", entity_id=entity.entityid, entity_name=entity.name)

        df = self._read_delta_table(table_ref)

        duration = time.time() - start_time
        self.emit(
            "entity.after_read",
            entity_id=entity.entityid,
            entity_name=entity.name,
            duration_seconds=duration,
        )

        return df

    def read_entity_as_stream(self, entity, format=None, options=None) -> DataFrame:
        """Read full entity table"""
        table_ref = self._get_table_reference(entity)
        return table_ref.get_spark_read_stream(self.spark, format, options)

    def write_to_entity(self, df: DataFrame, entity):
        """Write DataFrame to entity table with signal emissions."""
        start_time = time.time()
        table_ref = self._get_table_reference(entity)

        self.emit("entity.before_write", entity_id=entity.entityid, entity_name=entity.name)

        try:
            self._write_to_delta_table(df, entity, table_ref)

            duration = time.time() - start_time
            self.emit(
                "entity.after_write",
                entity_id=entity.entityid,
                entity_name=entity.name,
                duration_seconds=duration,
            )
        except Exception as e:
            duration = time.time() - start_time
            self.emit(
                "entity.write_failed",
                entity_id=entity.entityid,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=duration,
            )
            raise

    def get_entity_version(self, entity) -> int:
        """Get current version of entity table"""
        table_ref = self._get_table_reference(entity)
        return self._get_table_version(table_ref)
