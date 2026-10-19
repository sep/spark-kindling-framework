"""
In-Memory Entity Provider

Entity provider for in-memory DataFrames, useful for testing and temporary data.
Supports all 4 provider interfaces (batch read/write, streaming read/write).
"""

from typing import Any, Dict, List, Optional

from injector import inject
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, expr, lit, sha2, struct, to_json, when
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import TimestampType

from .data_entities import (
    EntityMetadata,
    SCDConfig,
    augment_schema_for_scd2,
    build_null_safe_change_condition,
    column_name_key,
    quote_sql_identifier,
    scd_change_columns,
    scd_config_from_tags,
    spark_case_sensitive,
    surrogate_key_columns,
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

# Staging helper columns. Like the Delta provider, every incoming column
# named __merge_key or starting with __scd2_ is refused, so the helpers can
# never collide with a business column.
_SCD2_MERGE_KEY_COLUMN = "__merge_key"
_SCD2_RESERVED_PREFIX = "__scd2_"
_SOURCE_PRESENT = "__scd2_source_present"
_SOURCE_DELETE = "__scd2_source_delete"


def _qcol(name: str, alias: Optional[str] = None):
    """Column reference by literal name (``a.b`` is one column, not a path)."""
    return col(quote_sql_identifier(name, alias))


def _memory_scd1_merge(
    current_df: DataFrame,
    incoming_df: DataFrame,
    entity_metadata: EntityMetadata,
    cfg: Optional[SCDConfig] = None,
) -> DataFrame:
    """Full-row upsert: incoming rows replace matching business keys, others pass through.

    With ``scd.untracked``, mirrors the Delta SCD1 change condition: a
    matched row is replaced only when a tracked column (present in both the
    batch and the table) differs, null-safely; an untracked-only difference
    keeps the existing row. A tracked column new to the table replaces every
    matched row, as the Delta merge does when it drops the condition.
    """
    business_keys = entity_metadata.merge_columns
    key_columns = surrogate_key_columns(entity_metadata)
    case_sensitive = True
    if key_columns or (cfg is not None and cfg.untracked_columns):
        case_sensitive = spark_case_sensitive(incoming_df.sparkSession)
    replacing_df = incoming_df
    if cfg is not None and cfg.untracked_columns:
        name_key = column_name_key(case_sensitive)
        tracked_columns, _ = scd_change_columns(
            cfg,
            incoming_df.columns,
            business_keys,
            case_sensitive=case_sensitive,
            insert_only_columns=key_columns,
        )
        current_keys = {name_key(column) for column in current_df.columns}
        if all(name_key(column) in current_keys for column in tracked_columns):
            current_df = current_df.toDF(*current_df.columns)
            incoming_df = incoming_df.toDF(*incoming_df.columns)
            change_condition = build_null_safe_change_condition(
                "new", "old", tracked_columns, schema=incoming_df.schema
            )
            unchanged_keys = (
                incoming_df.alias("new")
                .join(current_df.alias("old"), on=business_keys, how="inner")
                .where(f"NOT ({change_condition})")
                .select(*[_qcol(key) for key in business_keys])
            )
            replacing_df = incoming_df.join(unchanged_keys, on=business_keys, how="left_anti")
    untouched = current_df.join(replacing_df, on=business_keys, how="left_anti")
    if key_columns:
        replacing_df = _keep_existing_keys(
            current_df, replacing_df, business_keys, key_columns, case_sensitive
        )
    return untouched.unionByName(replacing_df, allowMissingColumns=True)


def _keep_existing_keys(
    current_df: DataFrame,
    replacing_df: DataFrame,
    business_keys: List[str],
    key_columns: List[str],
    case_sensitive: bool,
) -> DataFrame:
    """Surrogate keys are insert-only: a replaced (matched) row keeps the key
    it has; only rows for new business keys take the incoming key. Mirrors
    the Delta SCD1 merge's explicit update set without key columns."""

    def norm(name: str) -> str:
        return name if case_sensitive else name.lower()

    wanted = {norm(name) for name in key_columns}
    current_keys = [column for column in current_df.columns if norm(column) in wanted]
    if not current_keys:
        return replacing_df
    incoming_keys = {norm(c): c for c in replacing_df.columns if norm(c) in wanted}
    existing = current_df.select(
        *[_qcol(key) for key in business_keys],
        *[
            _qcol(column).alias(f"__sk_existing_{index}")
            for index, column in enumerate(current_keys)
        ],
        lit(True).alias("__sk_existing_present"),
    )
    joined = replacing_df.join(existing, on=business_keys, how="left")
    present = col("__sk_existing_present").isNotNull()
    for index, column in enumerate(current_keys):
        incoming = incoming_keys.get(norm(column))
        incoming_value = _qcol(incoming) if incoming else lit(None)
        joined = joined.withColumn(
            incoming or column,
            when(present, col(f"__sk_existing_{index}")).otherwise(incoming_value),
        )
    return joined.drop(
        "__sk_existing_present", *[f"__sk_existing_{i}" for i in range(len(current_keys))]
    )


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
    key_plan: Any = None,
) -> DataFrame:
    """Execute an SCD Type 2 upsert as plain DataFrame joins/unions.

    Mirrors DeltaEntityProvider._execute_scd2_merge's declared-flow semantics
    (tracked changes close and re-version, untracked-only changes update the
    current version in place, sequence_by ordering guarded against the
    current row's own sequence value, delete_when, close_on_missing) without
    Delta's single-MERGE-statement staging trick: Memory does a full
    read-modify-write, assembling historical rows, untouched current rows,
    closed (old) versions, current versions updated in place, and
    new/changed versions, then returns the union for one write_to_entity
    overwrite. ``now_ts`` is a single captured instant reused for every
    timestamp this call would otherwise stamp with current_timestamp(), so a
    closed row's effective_to and its replacement's effective_from agree.

    Surrogate keys (``key_plan``, resolved from the entity when omitted) are
    computed on the new versions only, from their final effective_from, and
    never rewritten: current and closed versions keep the keys they have,
    and incoming key columns are ignored — as in the Delta merge.
    """
    # A catalog/temp-view-backed DataFrame (current_df typically comes from
    # self.read_entity(), i.e. spark.table(...)) carries attribute lineage
    # that breaks alias-qualified wildcard selects ("target.*") once joined
    # and re-aliased below. Re-projecting through toDF() gives every column
    # a fresh attribute reference, which survives the later join/select.
    current_df = current_df.toDF(*current_df.columns)
    incoming_df = incoming_df.toDF(*incoming_df.columns)

    # Column names compare the way the session resolves them: by default
    # (spark.sql.caseSensitive=false) `STATUS` in the batch is the table's
    # `status`, not a new column.
    case_sensitive = spark_case_sensitive(incoming_df.sparkSession)
    name_key = column_name_key(case_sensitive)
    reserved = [
        column
        for column in incoming_df.columns
        if name_key(column) == _SCD2_MERGE_KEY_COLUMN
        or name_key(column).startswith(_SCD2_RESERVED_PREFIX)
    ]
    if reserved:
        raise ValueError(
            f"Incoming DataFrame contains reserved column(s) {reserved}: "
            f"'{_SCD2_MERGE_KEY_COLUMN}' and '{_SCD2_RESERVED_PREFIX}*' names are "
            "reserved for SCD2 merge staging. Rename them before writing to an "
            "SCD2 entity."
        )

    from kindling.surrogate_keys import (
        drop_surrogate_key_columns,
        resolve_surrogate_keys,
        with_scd2_surrogate_keys,
    )

    if key_plan is None:
        key_plan = resolve_surrogate_keys(entity_metadata)
    key_columns = key_plan.key_columns if key_plan is not None else []
    if key_plan is not None:
        incoming_df = drop_surrogate_key_columns(incoming_df, key_plan, case_sensitive)

    business_keys = entity_metadata.merge_columns
    temporal_columns = {cfg.effective_from_column, cfg.effective_to_column, cfg.is_current_column}
    temporal_keys = {name_key(column) for column in temporal_columns}
    key_keys = {name_key(column) for column in business_keys}

    # Batch columns the table does not have yet (additive schema): give the
    # current rows typed nulls so they compare (and project) like the Delta
    # provider's evolved table.
    current_keys = {name_key(column) for column in current_df.columns}
    for column in incoming_df.columns:
        if name_key(column) not in current_keys and name_key(column) not in temporal_keys:
            current_df = current_df.withColumn(
                column, lit(None).cast(incoming_df.schema[column].dataType)
            )

    if cfg.sequence_by and name_key(cfg.sequence_by) not in {
        name_key(column) for column in incoming_df.columns
    }:
        raise ValueError(
            f"Entity '{entity_metadata.entityid}': scd.sequence_by column "
            f"'{cfg.sequence_by}' is missing from the incoming DataFrame"
        )
    if cfg.sequence_by and incoming_df.filter(_qcol(cfg.sequence_by).isNull()).head(1):
        raise ValueError(
            f"Entity '{entity_metadata.entityid}': scd.sequence_by column "
            f"'{cfg.sequence_by}' contains null values in the incoming batch; "
            "every row must carry a sequence value"
        )

    # tracked: a change closes the current version and opens a new one.
    # untracked (incl. sequence_by): a change only to these updates the
    # current version in place.
    tracked_columns, untracked_columns = scd_change_columns(
        cfg,
        incoming_df.columns,
        business_keys,
        case_sensitive=case_sensitive,
        insert_only_columns=key_columns,
    )
    untracked_keys = {name_key(column) for column in untracked_columns}

    historical_rows = current_df.filter(_qcol(cfg.is_current_column) == lit(False))
    current_rows = current_df.filter(_qcol(cfg.is_current_column) == lit(True))

    upserts_df = incoming_df
    deletes_df = None
    if cfg.delete_when:
        delete_predicate = expr(cfg.delete_when)
        deletes_df = incoming_df.filter(delete_predicate)
        upserts_df = incoming_df.filter(~delete_predicate)
    upserts_df = upserts_df.drop(*[c for c in upserts_df.columns if name_key(c) in temporal_keys])

    # One source row per key (upserts and change-feed deletes), flagged so a
    # single left join from the current rows classifies every current row.
    # Each extra reference to current_df multiplies the lazily chained plan
    # of the next merge, so the current rows are joined exactly once.
    source_df = upserts_df.withColumn(_SOURCE_PRESENT, lit(True)).withColumn(
        _SOURCE_DELETE, lit(False)
    )
    if deletes_df is not None and not cfg.close_on_missing:
        # Mirrors DeltaEntityProvider._execute_scd2_merge: deletes are only
        # staged for change feeds. Under close_on_missing a deleted key is
        # simply absent from the snapshot and closes as missing.
        source_df = source_df.unionByName(
            deletes_df.drop(*[c for c in deletes_df.columns if name_key(c) in temporal_keys])
            .withColumn(_SOURCE_PRESENT, lit(True))
            .withColumn(_SOURCE_DELETE, lit(True)),
            allowMissingColumns=True,
        )

    joined = current_rows.alias("target").join(
        source_df.alias("source"), on=business_keys, how="left"
    )
    present = _qcol(_SOURCE_PRESENT, "source").isNotNull()
    is_delete = coalesce(_qcol(_SOURCE_DELETE, "source"), lit(False))

    if cfg.optimize_unchanged:
        hash_source = sha2(
            to_json(struct(*[_qcol(c, "source").alias(c) for c in tracked_columns])), 256
        )
        hash_target = sha2(
            to_json(struct(*[_qcol(c, "target").alias(c) for c in tracked_columns])), 256
        )
        tracked_change = hash_source != hash_target
    else:
        tracked_change = expr(
            build_null_safe_change_condition(
                "source", "target", tracked_columns, schema=upserts_df.schema
            )
        )
    # Sequence guard: strictly later than the current row's own sequence
    # value (not its effective_from — an in-place update advances the
    # former), so an older event never overwrites a newer in-place value.
    applies = present
    if cfg.sequence_by:
        applies = present & (
            _qcol(cfg.sequence_by, "target").isNull()
            | (_qcol(cfg.sequence_by, "source") > _qcol(cfg.sequence_by, "target"))
        )

    # close: a tracked change (re-versioned) or a delete (closed only).
    close_cond = applies & (is_delete | tracked_change)
    # touch: only untracked columns changed — update the current row in place.
    touch_cond = lit(False)
    if untracked_columns:
        touch_cond = (
            applies
            & ~is_delete
            & ~tracked_change
            & expr(
                build_null_safe_change_condition(
                    "source", "target", untracked_columns, schema=upserts_df.schema
                )
            )
        )
    # missing (snapshot): the key is absent from the batch.
    missing_cond = ~present if cfg.close_on_missing else lit(False)

    close_value = _qcol(cfg.sequence_by, "source") if cfg.sequence_by else lit(now_ts)
    current_projection = []
    for column in current_rows.columns:
        # Using-join keys resolve unqualified; everything else from target.
        column_key = name_key(column)
        target_value = _qcol(column) if column_key in key_keys else _qcol(column, "target")
        if column_key == name_key(cfg.effective_to_column):
            value = (
                when(close_cond, close_value)
                .when(missing_cond, lit(now_ts))
                .otherwise(target_value)
            )
        elif column_key == name_key(cfg.is_current_column):
            value = when(close_cond | missing_cond, lit(False)).otherwise(target_value)
        elif column_key in untracked_keys:
            value = when(touch_cond, _qcol(column, "source")).otherwise(target_value)
        else:
            value = target_value
        current_projection.append(value.alias(column))
    current_versions = joined.select(*current_projection)

    new_effective_from = _qcol(cfg.sequence_by, "source") if cfg.sequence_by else lit(now_ts)
    new_versions_from_changes = (
        joined.where(close_cond & ~is_delete)
        .select(
            *[
                (_qcol(column) if name_key(column) in key_keys else _qcol(column, "source")).alias(
                    column
                )
                for column in upserts_df.columns
            ],
            new_effective_from.alias(cfg.effective_from_column),
        )
        .withColumn(cfg.effective_to_column, lit(None).cast(TimestampType()))
        .withColumn(cfg.is_current_column, lit(True))
    )

    new_match = upserts_df.join(current_rows, on=business_keys, how="left_anti")
    new_versions_from_new_keys = (
        new_match.withColumn(
            cfg.effective_from_column,
            _qcol(cfg.sequence_by) if cfg.sequence_by else lit(now_ts),
        )
        .withColumn(cfg.effective_to_column, lit(None).cast(TimestampType()))
        .withColumn(cfg.is_current_column, lit(True))
    )
    if key_plan is not None:
        new_versions_from_changes = with_scd2_surrogate_keys(
            new_versions_from_changes, key_plan, cfg.effective_from_column, case_sensitive
        )
        new_versions_from_new_keys = with_scd2_surrogate_keys(
            new_versions_from_new_keys, key_plan, cfg.effective_from_column, case_sensitive
        )

    result = historical_rows
    for frame in (current_versions, new_versions_from_changes, new_versions_from_new_keys):
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
                from kindling.surrogate_keys import augment_schema_for_surrogate_keys

                current_df = self.spark.createDataFrame(
                    [],
                    augment_schema_for_surrogate_keys(
                        augment_schema_for_scd2(entity_metadata.schema, cfg), entity_metadata
                    ),
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
                _memory_scd1_merge(self.read_entity(entity_metadata), df, entity_metadata, cfg)
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

        # Check memory table. ``.columns`` forces analysis: ``spark.table()``
        # is lazy on Spark Connect and would not raise for a missing table.
        try:
            self.spark.table(table_name).columns
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
