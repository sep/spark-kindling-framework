"""Parquet entity provider for Kindling.

Plain-parquet datasets as entities — for the **boundaries** of a solution:
consuming parquet produced by external systems and publishing extracts for
consumers that can't read Delta. Delta remains the right provider for
internal pipeline storage.

Caveats (no transaction log):
- A failed overwrite can leave a partially-written directory visible.
- A retried append duplicates files (at-least-once). Parquet entities should
  not opt into execution retry, and there is no merge support.
"""

from typing import Optional

from injector import inject
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from .data_entities import EntityMetadata
from .entity_provider import (
    BaseEntityProvider,
    DestinationEnsuringProvider,
    StreamableEntityProvider,
    StreamWritableEntityProvider,
    WritableEntityProvider,
)
from .injection import GlobalInjector
from .spark_log_provider import PythonLoggerProvider
from .spark_session import get_or_create_spark_session


@GlobalInjector.singleton_autobind()
class ParquetEntityProvider(
    BaseEntityProvider,
    WritableEntityProvider,
    StreamableEntityProvider,
    StreamWritableEntityProvider,
    DestinationEnsuringProvider,
):
    """Read/write plain parquet datasets via native Spark.

    Configuration (entity tags):
        provider_type: "parquet"
        provider.path: dataset directory (required)
        provider.merge_schema: enable mergeSchema on reads (default false)
        provider.save_mode: mode for write_to_entity (default "overwrite")
        provider.option.<name>: passed through to the Spark reader/writer

    Streaming reads require an explicit schema (Spark file-source rule);
    the entity's `schema` supplies it. Partitioned writes honor the entity's
    `partition_columns`.
    """

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("ParquetEntityProvider")

    # ---- BaseEntityProvider ----

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """Read the parquet dataset as a batch DataFrame."""
        config = self._get_provider_config(entity_metadata)
        path = self._require_path(entity_metadata, config)

        self.logger.info(f"Reading parquet entity '{entity_metadata.entityid}' from path: {path}")

        spark = get_or_create_spark_session()
        reader = spark.read.format("parquet")
        if config.get("merge_schema", False):
            reader = reader.option("mergeSchema", "true")
        for key, value in self._extra_options(config).items():
            reader = reader.option(key, value)
        return reader.load(path)

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """True when the path holds a readable parquet dataset."""
        config = self._get_provider_config(entity_metadata)
        path = config.get("path")
        if not path:
            self.logger.warning(
                f"Cannot check existence for entity '{entity_metadata.entityid}': "
                "no path in tags (provider.path)"
            )
            return False

        try:
            spark = get_or_create_spark_session()
            spark.read.format("parquet").load(path).schema
            return True
        except Exception as e:  # noqa: BLE001
            self.logger.debug(
                f"Parquet entity '{entity_metadata.entityid}' not found at {path}: {e}"
            )
            return False

    # ---- WritableEntityProvider ----

    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Write the dataset (default mode overwrite; provider.save_mode overrides)."""
        config = self._get_provider_config(entity_metadata)
        mode = str(config.get("save_mode", "overwrite"))
        self._write(df, entity_metadata, config, mode)

    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Append rows to the dataset.

        Not idempotent: a retried append duplicates files (no transaction
        log). Keep execution retry off for parquet-targeted pipes.
        """
        config = self._get_provider_config(entity_metadata)
        self._write(df, entity_metadata, config, "append")

    def _write(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        config: dict,
        mode: str,
    ) -> None:
        path = self._require_path(entity_metadata, config)

        self.logger.info(
            f"Writing parquet entity '{entity_metadata.entityid}' to {path} (mode={mode})"
        )

        writer = df.write.format("parquet").mode(mode)
        partition_columns = list(entity_metadata.partition_columns or [])
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        for key, value in self._extra_options(config).items():
            writer = writer.option(key, value)
        writer.save(path)

    # ---- StreamableEntityProvider ----

    def read_entity_as_stream(
        self,
        entity_metadata: EntityMetadata,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> DataFrame:
        """Read the dataset as a file-source stream.

        Spark's file source requires an explicit schema — the entity's
        `schema` supplies it. `options` are applied directly as Spark
        reader options (e.g. maxFilesPerTrigger).
        """
        config = self._get_provider_config(entity_metadata)
        path = self._require_path(entity_metadata, config)

        schema = entity_metadata.schema
        if schema is None:
            raise ValueError(
                f"Streaming read of parquet entity '{entity_metadata.entityid}' "
                "requires an explicit schema on the entity definition "
                "(Spark file-source streams cannot infer schemas)"
            )

        self.logger.info(
            f"Starting streaming read of parquet entity '{entity_metadata.entityid}' "
            f"from {path}"
        )

        spark = get_or_create_spark_session()
        reader = spark.readStream.format(format or "parquet").schema(schema)
        for key, value in self._extra_options(config).items():
            reader = reader.option(key, value)
        for key, value in (options or {}).items():
            reader = reader.option(key, value)
        return reader.load(path)

    # ---- StreamWritableEntityProvider ----

    def append_as_stream(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        checkpoint_location: str,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> StreamingQuery:
        """Append a streaming DataFrame via the parquet file sink.

        `options` are applied directly as Spark writer options (e.g.
        maxRecordsPerFile).
        """
        config = self._get_provider_config(entity_metadata)
        path = self._require_path(entity_metadata, config)

        self.logger.info(
            f"Starting streaming write of parquet entity '{entity_metadata.entityid}' " f"to {path}"
        )

        writer = (
            df.writeStream.format(format or "parquet")
            .outputMode("append")  # file sinks support append only
            .option("checkpointLocation", checkpoint_location)
            .option("path", path)
        )
        partition_columns = list(entity_metadata.partition_columns or [])
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        for key, value in self._extra_options(config).items():
            writer = writer.option(key, value)
        for key, value in (options or {}).items():
            writer = writer.option(key, value)
        return writer.start()

    # ---- DestinationEnsuringProvider ----

    def ensure_destination(self, entity_metadata: EntityMetadata) -> None:
        """Create the dataset directory if it does not exist."""
        config = self._get_provider_config(entity_metadata)
        path = self._require_path(entity_metadata, config)

        spark = get_or_create_spark_session()
        jvm = spark._jvm
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(hadoop_path):
            self.logger.info(f"Creating parquet destination directory: {path}")
            fs.mkdirs(hadoop_path)

    # ---- Helpers ----

    def _require_path(self, entity_metadata: EntityMetadata, config: dict) -> str:
        path = config.get("path")
        if not path:
            raise ValueError(
                f"Parquet provider requires 'path' in tags (provider.path) for entity "
                f"'{entity_metadata.entityid}'"
            )
        return str(path)

    def _extra_options(self, config: dict) -> dict:
        prefix = "option."
        return {
            key[len(prefix) :]: value
            for key, value in config.items()
            if key.startswith(prefix) and value is not None
        }
