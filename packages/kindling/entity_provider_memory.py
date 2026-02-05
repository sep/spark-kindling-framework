"""
In-Memory Entity Provider

Entity provider for in-memory DataFrames, useful for testing and temporary data.
Supports all 4 provider interfaces (batch read/write, streaming read/write).
"""

from typing import Any, Dict, Optional

from injector import inject
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from .data_entities import EntityMetadata
from .entity_provider import (
    BaseEntityProvider,
    StreamableEntityProvider,
    StreamWritableEntityProvider,
    WritableEntityProvider,
)
from .injection import GlobalInjector
from .spark_config import get_or_create_spark_session
from .spark_log_provider import PythonLoggerProvider


@GlobalInjector.singleton_autobind()
class MemoryEntityProvider(
    BaseEntityProvider,
    StreamableEntityProvider,
    WritableEntityProvider,
    StreamWritableEntityProvider,
):
    """
    In-memory entity provider (full capabilities for testing and temporary data).

    Implements all 4 provider interfaces using Spark memory tables and rate sources.
    Ideal for testing DataPipes without external dependencies.

    **Capabilities:**
    - Batch read/write using memory tables
    - Streaming read using rate source or memory stream
    - Streaming write using memory sink

    Provider configuration options (via entity tags with 'provider.' prefix):

    **For batch operations:**
    - provider.table_name: Memory table name (default: entity name)

    **For streaming reads:**
    - provider.stream_type: "rate" or "memory" (default: "rate")
    - provider.rowsPerSecond: Events per second for rate source (default: 10)
    - provider.numPartitions: Number of partitions for rate source (default: 1)

    **For streaming writes:**
    - provider.output_mode: "append", "complete", or "update" (default: "append")
    - provider.query_name: Streaming query name (default: entity name)

    Example entity definitions:
    ```python
    # Batch memory table
    @DataEntities.entity(
        entityid="temp.results",
        name="temp_results",
        partition_columns=[],
        merge_columns=[],
        tags={
            "provider_type": "memory",
            "provider.table_name": "temp_results_table"
        },
        schema=None
    )

    # Streaming rate source
    @DataEntities.entity(
        entityid="stream.test_events",
        name="test_events",
        partition_columns=[],
        merge_columns=[],
        tags={
            "provider_type": "memory",
            "provider.stream_type": "rate",
            "provider.rowsPerSecond": "10"
        },
        schema=None
    )
    ```
    """

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("MemoryEntityProvider")
        self.spark = get_or_create_spark_session()
        # Store in-memory DataFrames
        self._memory_store: Dict[str, DataFrame] = {}

    def _get_provider_config(self, entity_metadata: EntityMetadata) -> Dict[str, Any]:
        """
        Extract provider configuration from entity tags.

        Looks for tags with 'provider.' prefix and converts them to a config dict.

        Args:
            entity_metadata: Entity metadata

        Returns:
            Dict with provider configuration (keys without 'provider.' prefix)
        """
        config = {}
        for key, value in entity_metadata.tags.items():
            if key.startswith("provider."):
                config_key = key[9:]  # Remove 'provider.' prefix
                # Convert string values to appropriate types
                if value.lower() in ("true", "false"):
                    config[config_key] = value.lower() == "true"
                elif value.isdigit():
                    config[config_key] = int(value)
                else:
                    config[config_key] = value
        return config

    def _get_table_name(self, entity_metadata: EntityMetadata) -> str:
        """Get memory table name from config or entity name."""
        config = self._get_provider_config(entity_metadata)
        return config.get("table_name", entity_metadata.name)

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """
        Read entity from memory table or in-memory store.

        Args:
            entity_metadata: Entity metadata

        Returns:
            DataFrame from memory

        Raises:
            ValueError: If entity does not exist in memory
        """
        table_name = self._get_table_name(entity_metadata)

        self.logger.info(
            f"Reading memory entity '{entity_metadata.entityid}' (table: {table_name})"
        )

        # Try reading from memory table first
        try:
            df = self.spark.table(table_name)
            self.logger.info(
                f"Read memory entity '{entity_metadata.entityid}' from table: {df.count()} rows"
            )
            return df
        except Exception:
            pass

        # Try reading from in-memory store
        if entity_metadata.entityid in self._memory_store:
            df = self._memory_store[entity_metadata.entityid]
            self.logger.info(
                f"Read memory entity '{entity_metadata.entityid}' from in-memory store"
            )
            return df

        # Entity not found
        raise ValueError(
            f"Memory entity '{entity_metadata.entityid}' not found. "
            f"Write data first or check table name: {table_name}"
        )

    def read_entity_as_stream(
        self,
        entity_metadata: EntityMetadata,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> DataFrame:
        """
        Read entity as streaming DataFrame.

        Uses either rate source (for testing) or memory stream source.

        Args:
            entity_metadata: Entity metadata with tags containing provider config
            format: Ignored for memory provider
            options: Optional additional options (merged with provider config from tags)

        Returns:
            Streaming DataFrame
        """
        config = self._get_provider_config(entity_metadata)

        # Merge with additional options
        if options:
            config = {**config, **options}

        stream_type = config.get("stream_type", "rate")

        self.logger.info(
            f"Reading memory entity '{entity_metadata.entityid}' as stream (type: {stream_type})"
        )

        if stream_type == "rate":
            # Use rate source for testing (generates continuous data)
            rows_per_second = config.get("rowsPerSecond", 10)
            num_partitions = config.get("numPartitions", 1)

            stream_df = (
                self.spark.readStream.format("rate")
                .option("rowsPerSecond", rows_per_second)
                .option("numPartitions", num_partitions)
                .load()
            )

            self.logger.info(
                f"Created rate stream for '{entity_metadata.entityid}': {rows_per_second} rows/sec"
            )

            return stream_df

        elif stream_type == "memory":
            # Use memory source (reads from memory table as stream)
            table_name = self._get_table_name(entity_metadata)

            stream_df = self.spark.readStream.table(table_name)

            self.logger.info(
                f"Created memory stream for '{entity_metadata.entityid}' (table: {table_name})"
            )

            return stream_df

        else:
            raise ValueError(
                f"Unknown stream_type '{stream_type}' for memory provider. Use 'rate' or 'memory'."
            )

    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """
        Write DataFrame to memory table (overwrite mode).

        Args:
            df: DataFrame to write
            entity_metadata: Entity metadata
        """
        table_name = self._get_table_name(entity_metadata)

        self.logger.info(
            f"Writing memory entity '{entity_metadata.entityid}' (table: {table_name}, rows: {df.count()})"
        )

        try:
            # Write to memory table (overwrite)
            df.write.format("memory").mode("overwrite").saveAsTable(table_name)

            # Also store in in-memory dict for backup
            self._memory_store[entity_metadata.entityid] = df

            self.logger.info(f"Successfully wrote memory entity '{entity_metadata.entityid}'")

        except Exception as e:
            self.logger.error(
                f"Failed to write memory entity '{entity_metadata.entityid}': {e}",
                include_traceback=True,
            )
            raise

    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """
        Append DataFrame to memory table (append mode).

        Args:
            df: DataFrame to append
            entity_metadata: Entity metadata
        """
        table_name = self._get_table_name(entity_metadata)

        self.logger.info(
            f"Appending to memory entity '{entity_metadata.entityid}' (table: {table_name}, rows: {df.count()})"
        )

        try:
            # Write to memory table (append)
            df.write.format("memory").mode("append").saveAsTable(table_name)

            # Update in-memory store (append)
            if entity_metadata.entityid in self._memory_store:
                existing_df = self._memory_store[entity_metadata.entityid]
                self._memory_store[entity_metadata.entityid] = existing_df.union(df)
            else:
                self._memory_store[entity_metadata.entityid] = df

            self.logger.info(f"Successfully appended to memory entity '{entity_metadata.entityid}'")

        except Exception as e:
            self.logger.error(
                f"Failed to append to memory entity '{entity_metadata.entityid}': {e}",
                include_traceback=True,
            )
            raise

    def append_as_stream(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        checkpoint_location: str,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> StreamingQuery:
        """
        Write streaming DataFrame to memory sink.

        Args:
            df: Streaming DataFrame to write
            entity_metadata: Entity metadata with tags containing provider config
            checkpoint_location: Checkpoint location for streaming query
            format: Ignored for memory provider
            options: Optional additional options

        Returns:
            StreamingQuery object
        """
        config = self._get_provider_config(entity_metadata)

        # Merge with additional options
        if options:
            config = {**config, **options}

        output_mode = config.get("output_mode", "append")
        query_name = config.get("query_name", entity_metadata.name)
        table_name = self._get_table_name(entity_metadata)

        self.logger.info(
            f"Starting streaming write for memory entity '{entity_metadata.entityid}' "
            f"(table: {table_name}, mode: {output_mode})"
        )

        try:
            # Write stream to memory table
            query = (
                df.writeStream.format("memory")
                .outputMode(output_mode)
                .queryName(query_name)
                .option("checkpointLocation", checkpoint_location)
                .start(table_name)
            )

            self.logger.info(
                f"Started streaming query '{query_name}' for memory entity '{entity_metadata.entityid}'"
            )

            return query

        except Exception as e:
            self.logger.error(
                f"Failed to start streaming write for memory entity '{entity_metadata.entityid}': {e}",
                include_traceback=True,
            )
            raise

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """
        Check if memory entity exists (in table or in-memory store).

        Args:
            entity_metadata: Entity metadata

        Returns:
            True if entity exists, False otherwise
        """
        table_name = self._get_table_name(entity_metadata)

        # Check memory table
        try:
            self.spark.table(table_name)
            self.logger.debug(
                f"Memory entity '{entity_metadata.entityid}' exists (table: {table_name})"
            )
            return True
        except Exception:
            pass

        # Check in-memory store
        if entity_metadata.entityid in self._memory_store:
            self.logger.debug(
                f"Memory entity '{entity_metadata.entityid}' exists (in-memory store)"
            )
            return True

        self.logger.debug(f"Memory entity '{entity_metadata.entityid}' does not exist")
        return False
