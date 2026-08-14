"""
In-Memory Entity Provider

Entity provider for in-memory DataFrames, useful for testing and temporary data.
Supports all 4 provider interfaces (batch read/write, streaming read/write).
"""

from typing import Any, Dict, List, Optional

from injector import inject
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, lit, sha2, struct, to_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import TimestampType

from .data_entities import (
    EntityMetadata,
    SCDConfig,
    augment_schema_for_scd2,
    build_null_safe_change_condition,
    scd_config_from_tags,
)
from .entity_provider import (
    BaseEntityProvider,
    StreamableEntityProvider,
    StreamWritableEntityProvider,
    WritableEntityProvider,
)
from .injection import GlobalInjector
from .spark_config import get_or_create_spark_session
from .spark_log_provider import PythonLoggerProvider


def _memory_scd1_merge(
    current_df: DataFrame, incoming_df: DataFrame, entity_metadata: EntityMetadata
) -> DataFrame:
    """Full-row upsert: incoming rows replace matching business keys, others pass through."""
    business_keys = entity_metadata.merge_columns
    untouched = current_df.join(incoming_df, on=business_keys, how="left_anti")
    return untouched.unionByName(incoming_df, allowMissingColumns=True)


def _memory_insert_only_merge(
    current_df: DataFrame, incoming_df: DataFrame, entity_metadata: EntityMetadata
) -> DataFrame:
    """Insert-if-absent upsert: existing business keys are left untouched."""
    business_keys = entity_metadata.merge_columns
    new_only = incoming_df.join(current_df, on=business_keys, how="left_anti")
    return current_df.unionByName(new_only, allowMissingColumns=True)


def _memory_scd2_merge(
    current_df: DataFrame,
    incoming_df: DataFrame,
    entity_metadata: EntityMetadata,
    cfg: SCDConfig,
    now_ts: Any,
) -> DataFrame:
    """Execute an SCD Type 2 upsert as plain DataFrame joins/unions.

    Mirrors DeltaEntityProvider._execute_scd2_merge's declared-flow semantics
    (sequence_by ordering, delete_when, close_on_missing) without Delta's
    single-MERGE-statement staging trick: Memory does a full read-modify-write,
    assembling historical rows, untouched current rows, closed (old) versions,
    and new/changed versions, then returns the union for one write_to_entity
    overwrite. ``now_ts`` is a single captured instant reused for every
    timestamp this call would otherwise stamp with current_timestamp(), so a
    closed row's effective_to and its replacement's effective_from agree.
    """
    # A catalog/temp-view-backed DataFrame (current_df typically comes from
    # self.read_entity(), i.e. spark.table(...)) carries attribute lineage
    # that breaks alias-qualified wildcard selects ("target.*") once joined
    # and re-aliased below. Re-projecting through toDF() gives every column
    # a fresh attribute reference, which survives the later join/select.
    current_df = current_df.toDF(*current_df.columns)
    incoming_df = incoming_df.toDF(*incoming_df.columns)

    business_keys = entity_metadata.merge_columns
    temporal_columns = {cfg.effective_from_column, cfg.effective_to_column, cfg.is_current_column}

    if cfg.sequence_by and cfg.sequence_by not in incoming_df.columns:
        raise ValueError(
            f"Entity '{entity_metadata.entityid}': scd.sequence_by column "
            f"'{cfg.sequence_by}' is missing from the incoming DataFrame"
        )
    if cfg.sequence_by and incoming_df.filter(col(cfg.sequence_by).isNull()).head(1):
        raise ValueError(
            f"Entity '{entity_metadata.entityid}': scd.sequence_by column "
            f"'{cfg.sequence_by}' contains null values in the incoming batch; "
            "every row must carry a sequence value"
        )

    tracked_columns = cfg.tracked_columns or [
        column
        for column in incoming_df.columns
        if column not in business_keys
        and column not in temporal_columns
        and column != cfg.sequence_by
    ]

    historical_rows = current_df.filter(col(cfg.is_current_column) == lit(False))
    current_rows = current_df.filter(col(cfg.is_current_column) == lit(True))

    upserts_df = incoming_df
    deletes_df = None
    if cfg.delete_when:
        delete_predicate = expr(cfg.delete_when)
        deletes_df = incoming_df.filter(delete_predicate)
        upserts_df = incoming_df.filter(~delete_predicate)
    upserts_df = upserts_df.drop(*[c for c in temporal_columns if c in upserts_df.columns])

    match = upserts_df.alias("source").join(
        current_rows.alias("target"), on=business_keys, how="inner"
    )
    if cfg.optimize_unchanged:
        hash_source = sha2(to_json(struct(*[col(f"source.{c}") for c in tracked_columns])), 256)
        hash_target = sha2(to_json(struct(*[col(f"target.{c}") for c in tracked_columns])), 256)
        change_where = hash_source != hash_target
    else:
        change_where = expr(
            build_null_safe_change_condition(
                "source", "target", tracked_columns, schema=upserts_df.schema
            )
        )
    if cfg.sequence_by:
        change_where = change_where & (
            col(f"source.{cfg.sequence_by}") > col(f"target.{cfg.effective_from_column}")
        )
    changed_match = match.where(change_where)

    new_effective_from = col(f"source.{cfg.sequence_by}") if cfg.sequence_by else lit(now_ts)
    new_versions_from_changes = (
        changed_match.withColumn("__new_effective_from", new_effective_from)
        .select("source.*", "__new_effective_from")
        .withColumn(cfg.effective_from_column, col("__new_effective_from"))
        .withColumn(cfg.effective_to_column, lit(None).cast(TimestampType()))
        .withColumn(cfg.is_current_column, lit(True))
        .drop("__new_effective_from")
    )

    close_value = col(f"source.{cfg.sequence_by}") if cfg.sequence_by else lit(now_ts)
    closed_versions_from_changes = (
        changed_match.withColumn("__close_value", close_value)
        .select("target.*", "__close_value")
        .withColumn(cfg.effective_to_column, col("__close_value"))
        .withColumn(cfg.is_current_column, lit(False))
        .drop("__close_value")
    )

    new_match = upserts_df.join(current_rows, on=business_keys, how="left_anti")
    new_versions_from_new_keys = (
        new_match.withColumn(
            cfg.effective_from_column,
            col(cfg.sequence_by) if cfg.sequence_by else lit(now_ts),
        )
        .withColumn(cfg.effective_to_column, lit(None).cast(TimestampType()))
        .withColumn(cfg.is_current_column, lit(True))
    )

    closed_versions_from_missing = None
    if cfg.close_on_missing:
        missing_match = current_rows.join(upserts_df, on=business_keys, how="left_anti")
        closed_versions_from_missing = missing_match.withColumn(
            cfg.effective_to_column, lit(now_ts)
        ).withColumn(cfg.is_current_column, lit(False))

    closed_versions_from_deletes = None
    if deletes_df is not None and not cfg.close_on_missing:
        # Mirrors DeltaEntityProvider._execute_scd2_merge: delete_close_rows is
        # computed but only staged in the non-close_on_missing branch. When
        # close_on_missing is enabled, a deleted key is simply absent from the
        # incoming snapshot and gets closed via closed_versions_from_missing;
        # staging it again here would double-close the same current row.
        delete_match = deletes_df.alias("source").join(
            current_rows.alias("target"), on=business_keys, how="inner"
        )
        if cfg.sequence_by:
            delete_match = delete_match.where(
                col(f"source.{cfg.sequence_by}") > col(f"target.{cfg.effective_from_column}")
            )
        delete_close_value = col(f"source.{cfg.sequence_by}") if cfg.sequence_by else lit(now_ts)
        closed_versions_from_deletes = (
            delete_match.withColumn("__close_value", delete_close_value)
            .select("target.*", "__close_value")
            .withColumn(cfg.effective_to_column, col("__close_value"))
            .withColumn(cfg.is_current_column, lit(False))
            .drop("__close_value")
        )

    closed_frames = [
        frame
        for frame in (
            closed_versions_from_changes,
            closed_versions_from_missing,
            closed_versions_from_deletes,
        )
        if frame is not None
    ]

    if closed_frames:
        touched_keys = closed_frames[0].select(*business_keys)
        for frame in closed_frames[1:]:
            touched_keys = touched_keys.unionByName(frame.select(*business_keys))
        current_rows_not_touched = current_rows.join(
            touched_keys, on=business_keys, how="left_anti"
        )
    else:
        current_rows_not_touched = current_rows

    result = historical_rows
    for frame in (
        [current_rows_not_touched]
        + closed_frames
        + [new_versions_from_changes, new_versions_from_new_keys]
    ):
        result = result.unionByName(frame, allowMissingColumns=True)
    return result


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
    - provider.table_name: Memory table name (default: entityid with dots replaced by underscores)

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
        # Spark may be unavailable in restricted environments (e.g., sandboxed unit test runners
        # that deny socket syscalls required by Py4J). Keep this lazy so callers/tests can
        # inject a mock SparkSession by setting `provider.spark`.
        try:
            self.spark = get_or_create_spark_session()
        except Exception:
            self.spark = None
        # Store in-memory DataFrames
        self._memory_store: Dict[str, DataFrame] = {}

    def _get_table_name(self, entity_metadata: EntityMetadata) -> str:
        """Get memory table name from config or sanitized entityid."""
        config = self._get_provider_config(entity_metadata)
        return config.get("table_name", entity_metadata.entityid.replace(".", "_"))

    def _get_seed_rows(self, entity_metadata: EntityMetadata) -> Optional[List[dict]]:
        """Return inline seed rows from provider config, or None if not configured."""
        config = self._get_provider_config(entity_metadata)
        return config.get("seed.rows")

    def _create_seed_dataframe(self, entity_metadata: EntityMetadata, rows: Any) -> DataFrame:
        """Create a DataFrame from inline seed rows, validating structure and schema."""
        eid = entity_metadata.entityid

        if entity_metadata.schema is None:
            raise ValueError(
                f"Memory entity '{eid}' has provider.seed.rows but no schema defined. "
                "A schema is required to materialize seed rows."
            )
        if not isinstance(rows, (list, tuple)):
            raise ValueError(
                f"Memory entity '{eid}' provider.seed.rows must be a list, got {type(rows).__name__}."
            )

        schema_fields = {f.name for f in entity_metadata.schema.fields}
        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                raise ValueError(
                    f"Memory entity '{eid}' provider.seed.rows[{i}] must be a mapping, "
                    f"got {type(row).__name__}."
                )
            unknown = set(row.keys()) - schema_fields
            if unknown:
                raise ValueError(
                    f"Memory entity '{eid}' provider.seed.rows[{i}] contains unknown "
                    f"field(s): {sorted(unknown)}. Schema fields: {sorted(schema_fields)}."
                )

        try:
            return self.spark.createDataFrame(list(rows), entity_metadata.schema)
        except Exception as exc:
            raise ValueError(
                f"Memory entity '{eid}' provider.seed.rows could not be materialized: {exc}"
            ) from exc

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

        # Materialize inline seed rows if configured (first-read fallback before empty DataFrame)
        seed_rows = self._get_seed_rows(entity_metadata)
        if seed_rows is not None:
            self.logger.info(
                f"Seeding memory entity '{entity_metadata.entityid}' from provider.seed.rows "
                f"({len(seed_rows)} row(s))"
            )
            df = self._create_seed_dataframe(entity_metadata, seed_rows)
            self.write_to_entity(df, entity_metadata)
            return df

        # Entity not found — return empty DataFrame if schema is known, else raise
        if entity_metadata.schema is not None:
            self.logger.info(
                f"Memory entity '{entity_metadata.entityid}' has no data; returning empty DataFrame"
            )
            return self.spark.createDataFrame([], entity_metadata.schema)

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
            # Register as a Spark temp view (supported in all Spark modes)
            df.createOrReplaceTempView(table_name)

            # Mirror in the in-memory store for direct dict access
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
            # Union with any existing data then re-register the temp view
            if entity_metadata.entityid in self._memory_store:
                existing_df = self._memory_store[entity_metadata.entityid]
                combined = existing_df.union(df)
            else:
                combined = df

            combined.createOrReplaceTempView(table_name)
            self._memory_store[entity_metadata.entityid] = combined

            self.logger.info(f"Successfully appended to memory entity '{entity_metadata.entityid}'")

        except Exception as e:
            self.logger.error(
                f"Failed to append to memory entity '{entity_metadata.entityid}': {e}",
                include_traceback=True,
            )
            raise

    def merge_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """
        Merge DataFrame into memory entity (SCD2, insert-only, or SCD1 upsert).

        Dispatches on the entity's tags exactly like DeltaEntityProvider's
        merge_to_entity: scd.type=2 runs an SCD2 upsert, write.mode=insert
        only inserts new business keys, and the default runs a full-row SCD1
        upsert. Implemented as plain DataFrame joins/unions (no MERGE INTO
        primitive), then a single write_to_entity overwrite of the result.

        Args:
            df: Incoming DataFrame to merge
            entity_metadata: Entity metadata (tags select the merge strategy)
        """
        cfg = scd_config_from_tags(entity_metadata)
        write_mode = str((entity_metadata.tags or {}).get("write.mode") or "").strip().lower()
        exists = self.check_entity_exists(entity_metadata)

        if cfg.enabled:
            self.logger.info(f"Merging memory entity '{entity_metadata.entityid}' (mode: scd2)")
            if exists:
                current_df = self.read_entity(entity_metadata)
            else:
                if entity_metadata.schema is None:
                    raise ValueError(
                        f"Memory entity '{entity_metadata.entityid}' has scd.type=2 but no "
                        "schema defined. A schema is required to bootstrap the SCD2 "
                        "temporal columns for a not-yet-existing entity."
                    )
                current_df = self.spark.createDataFrame(
                    [], augment_schema_for_scd2(entity_metadata.schema, cfg)
                )
            now_ts = self.spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
            result = _memory_scd2_merge(current_df, df, entity_metadata, cfg, now_ts)
        elif write_mode == "insert":
            self.logger.info(
                f"Merging memory entity '{entity_metadata.entityid}' (mode: insert_only)"
            )
            result = (
                _memory_insert_only_merge(self.read_entity(entity_metadata), df, entity_metadata)
                if exists
                else df
            )
        else:
            self.logger.info(f"Merging memory entity '{entity_metadata.entityid}' (mode: scd1)")
            result = (
                _memory_scd1_merge(self.read_entity(entity_metadata), df, entity_metadata)
                if exists
                else df
            )

        self.write_to_entity(result, entity_metadata)

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
        query_name = config.get("query_name", entity_metadata.entityid.replace(".", "_"))
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
