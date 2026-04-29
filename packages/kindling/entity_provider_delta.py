import logging
import time
from abc import ABC, abstractmethod
from enum import Enum
from functools import reduce
from typing import Callable, Dict, Literal, Optional, Type

import pyspark.sql.utils
from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, concat_ws, lit, sha2, struct, to_json
from pyspark.sql.types import BooleanType, StructField, StructType, TimestampType

from kindling.common_transforms import *
from kindling.features import get_feature_bool, set_runtime_feature

# Import your existing modules
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_log_provider import *

from .data_entities import *
from .entity_provider import (
    BaseEntityProvider,
    DestinationEnsuringProvider,
    StreamableEntityProvider,
    StreamWritableEntityProvider,
    WritableEntityProvider,
)


class DeltaMergeStrategy(ABC):
    """Strategy protocol for Delta merge semantics."""

    @abstractmethod
    def apply(
        self,
        delta_table,
        df: DataFrame,
        entity,
        merge_condition: str,
    ) -> None:
        """Apply a merge strategy to a Delta table."""


class DeltaMergeStrategies:
    """Class-level registry for Delta merge strategies."""

    _registry: Dict[str, Type[DeltaMergeStrategy]] = {}

    @classmethod
    def register(cls, name: str):
        """Register a Delta merge strategy by name."""

        def decorator(strategy_cls: Type[DeltaMergeStrategy]) -> Type[DeltaMergeStrategy]:
            if name in cls._registry:
                raise ValueError(f"Merge strategy '{name}' is already registered.")
            cls._registry[name] = strategy_cls
            return strategy_cls

        return decorator

    @classmethod
    def get(cls, name: str) -> DeltaMergeStrategy:
        """Return a fresh strategy instance for a registered strategy name."""
        if name not in cls._registry:
            raise ValueError(
                f"Unknown merge strategy '{name}'. "
                f"Registered strategies: {sorted(cls._registry)}"
            )
        return cls._registry[name]()


# [implementer] add Delta merge strategy implementations — TASK-20260429-001
@DeltaMergeStrategies.register(name="scd1")
class SCD1MergeStrategy(DeltaMergeStrategy):
    """Default SCD1-style merge strategy."""

    def apply(
        self,
        delta_table,
        df: DataFrame,
        entity,
        merge_condition: str,
    ) -> None:
        """Run the legacy update-all / insert-all Delta merge."""
        (
            delta_table.alias("old")
            .merge(source=df.alias("new"), condition=merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


@DeltaMergeStrategies.register(name="scd2")
class SCD2MergeStrategy(DeltaMergeStrategy):
    """SCD Type 2 staged-updates merge strategy."""

    def apply(
        self,
        delta_table,
        df: DataFrame,
        entity,
        merge_condition: str,
    ) -> None:
        """Run an SCD2 merge for changed/new business keys."""
        _execute_scd2_merge(delta_table, df, entity)


def _quote_sql_identifier(name: str, alias: Optional[str] = None) -> str:
    escaped = name.replace("`", "``")
    if alias:
        return f"{alias}.`{escaped}`"
    return f"`{escaped}`"


def _build_null_safe_change_condition(
    source_alias: str, target_alias: str, tracked_columns: list[str]
) -> str:
    if not tracked_columns:
        return "false"

    return " OR ".join(
        [
            f"({_quote_sql_identifier(column, source_alias)} != "
            f"{_quote_sql_identifier(column, target_alias)} OR "
            f"({_quote_sql_identifier(column, source_alias)} IS NULL) != "
            f"({_quote_sql_identifier(column, target_alias)} IS NULL))"
            for column in tracked_columns
        ]
    )


def _routing_key_column_expr(business_keys: list[str], method: str):
    if method == "concat":
        # Coalesce to sentinel so (None, "x") and ("x", None) produce distinct keys.
        return concat_ws(
            "||",
            *[coalesce(col(key).cast("string"), lit("__null__")) for key in business_keys],
        )
    return sha2(to_json(struct(*[col(key) for key in business_keys])), 256)


def _routing_key_target_sql(business_keys: list[str], method: str) -> str:
    if method == "concat":
        key_exprs = [
            f"COALESCE(CAST({_quote_sql_identifier(key, 'target')} AS STRING), '__null__')"
            for key in business_keys
        ]
        return f"concat_ws('||', {', '.join(key_exprs)})"

    named_struct_args = []
    for key in business_keys:
        safe_key = key.replace("'", "''")
        named_struct_args.append(f"'{safe_key}'")
        named_struct_args.append(_quote_sql_identifier(key, "target"))
    return f"sha2(to_json(named_struct({', '.join(named_struct_args)})), 256)"


def _execute_scd2_merge(delta_table, df: DataFrame, entity) -> None:
    """Execute an SCD Type 2 staged-updates merge for a Delta table."""
    cfg = scd_config_from_tags(entity)
    business_keys = entity.merge_columns
    temporal_columns = {
        cfg.effective_from_column,
        cfg.effective_to_column,
        cfg.is_current_column,
    }
    tracked_columns = cfg.tracked_columns or [
        column
        for column in df.columns
        if column not in business_keys and column not in temporal_columns
    ]
    change_condition = _build_null_safe_change_condition("source", "target", tracked_columns)
    current_target = (
        delta_table.toDF().filter(col(cfg.is_current_column) == lit(True)).alias("target")
    )

    changed_rows = (
        df.alias("source")
        .join(current_target, on=business_keys, how="inner")
        .where(change_condition)
        .select("source.*")
    )
    new_rows = df.join(current_target, on=business_keys, how="left_anti")
    # [integrator] unchanged rows omitted from Group B — closing a row that hasn't changed creates a false history entry — TASK-20260429-001
    rows_to_close_or_insert = changed_rows.unionByName(new_rows, allowMissingColumns=True)

    insert_rows = changed_rows.withColumn("__merge_key", lit(None).cast("string"))
    keyed_rows = rows_to_close_or_insert.withColumn(
        "__merge_key", _routing_key_column_expr(business_keys, cfg.routing_key_method)
    )
    staged = insert_rows.unionByName(keyed_rows, allowMissingColumns=True)

    source_columns = [column for column in df.columns if column not in temporal_columns]
    insert_values = {
        _quote_sql_identifier(column): _quote_sql_identifier(column, "staged")
        for column in source_columns
    }
    insert_values[_quote_sql_identifier(cfg.effective_from_column)] = "current_timestamp()"
    insert_values[_quote_sql_identifier(cfg.effective_to_column)] = "NULL"
    insert_values[_quote_sql_identifier(cfg.is_current_column)] = "true"
    target_routing_key_sql = _routing_key_target_sql(business_keys, cfg.routing_key_method)
    merge_condition = (
        f"{target_routing_key_sql} = staged.__merge_key "
        f"AND {_quote_sql_identifier(cfg.is_current_column, 'target')} = true"
    )

    (
        delta_table.alias("target")
        .merge(source=staged.alias("staged"), condition=merge_condition)
        .whenMatchedUpdate(
            set={
                _quote_sql_identifier(cfg.effective_to_column): "current_timestamp()",
                _quote_sql_identifier(cfg.is_current_column): "false",
            }
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )


class DeltaAccessMode:
    """Defines how Delta tables are accessed"""

    CATALOG = "catalog"  # Catalog-managed tables
    STORAGE = "storage"  # Direct path access


class DeltaTableReference:
    """Encapsulates how to reference a Delta table"""

    def __init__(self, table_name: str, table_path: Optional[str], access_mode: DeltaAccessMode):
        self.table_name = table_name
        self.table_path = table_path
        self.access_mode = access_mode
        self.spark = get_or_create_spark_session()

    def get_delta_table(self) -> DeltaTable:
        """Get DeltaTable instance using appropriate method"""
        if self.access_mode == DeltaAccessMode.CATALOG:
            return DeltaTable.forName(self.spark, self.get_read_path())
        elif self.access_mode == DeltaAccessMode.STORAGE:
            return DeltaTable.forPath(self.spark, self.get_read_path())
        raise ValueError(f"Unsupported Delta access mode: {self.access_mode}")

    def get_spark_read_stream(self, spark, options=None):
        base = spark.readStream.format("delta")

        if options is not None:
            path = options.get("path", None)
            if path is not None:
                del options["path"]
            base = base.options(**options)

        if self.access_mode == DeltaAccessMode.CATALOG:
            base = base.table(self.table_name)
        elif self.access_mode == DeltaAccessMode.STORAGE:
            base = base.load(self.table_path)
        else:
            raise ValueError(f"Unsupported Delta access mode: {self.access_mode}")

        return base

    def get_read_path(self) -> str:
        """Get path for spark.read operations"""
        if self.access_mode == DeltaAccessMode.CATALOG:
            return self.table_name
        if self.access_mode == DeltaAccessMode.STORAGE:
            return self.table_path
        raise ValueError(f"Unsupported Delta access mode: {self.access_mode}")


@GlobalInjector.singleton_autobind()
class DeltaEntityProvider(
    EntityProvider,
    BaseEntityProvider,
    DestinationEnsuringProvider,
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
    - provider.access_mode: Override access mode (optional, values: catalog, storage)

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
            "provider.access_mode": "storage"
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
        self.access_mode = self.config.get("kindling.delta.access_mode") or "catalog"
        self.spark = get_or_create_spark_session()
        self.logger = tp.get_logger("DeltaEntityProvider")

    def _is_for_name_mode(self, access_mode: str) -> bool:
        return str(access_mode or "").lower() == "catalog"

    def _is_for_path_mode(self, access_mode: str) -> bool:
        return str(access_mode or "").lower() == "storage"

    def _get_cluster_columns(self, entity) -> list[str]:
        cols = getattr(entity, "cluster_columns", None)
        if cols is None:
            return []
        if isinstance(cols, str):
            return [cols]
        try:
            return list(cols)
        except Exception:
            return []

    def _is_auto_clustering_requested(self, cluster_cols: list[str]) -> bool:
        return len(cluster_cols) == 1 and str(cluster_cols[0]).strip().lower() == "auto"

    def _should_partition_files(self, entity) -> bool:
        """True when we should physically partition data files by partition_columns."""
        cluster_cols = self._get_cluster_columns(entity)
        if cluster_cols:
            if getattr(entity, "partition_columns", None):
                self.logger.warning(
                    f"Entity '{getattr(entity, 'entityid', entity)}' specifies both partition_columns "
                    f"and cluster_columns. Preferring cluster_columns and skipping partitionBy."
                )
            return False
        return bool(getattr(entity, "partition_columns", None))

    def _ensure_clustering(self, entity, table_ref: DeltaTableReference) -> None:
        """Best-effort: apply CLUSTER BY when supported by the engine.

        This is state-aware when DESCRIBE DETAIL exposes clusteringColumns; in that
        case we only run ALTER TABLE when the desired columns differ.
        """
        cluster_cols = self._get_cluster_columns(entity)
        if not cluster_cols:
            return

        auto_requested = self._is_auto_clustering_requested(cluster_cols)
        if any(str(c).strip().lower() == "auto" for c in cluster_cols) and not auto_requested:
            raise ValueError(
                f"Entity '{getattr(entity, 'entityid', entity)}' uses cluster_columns containing 'auto' "
                "alongside other columns. Use exactly ['auto'] (or 'auto') to request auto clustering."
            )

        if (
            auto_requested
            and get_feature_bool(self.config, "delta.auto_clustering", default=False) is not True
        ):
            raise ValueError(
                "Auto clustering was requested (cluster_columns='auto'), but the feature flag "
                "`kindling.features.delta.auto_clustering` (or computed "
                "`kindling.runtime.features.delta.auto_clustering`) is not enabled for this runtime."
            )

        if get_feature_bool(self.config, "delta.cluster_by", default=True) is False:
            self.logger.debug("Skipping CLUSTER BY: feature flag delta.cluster_by is false")
            return

        # Choose the correct ALTER TABLE target.
        #
        # For storage mode, we must reference the path-based Delta identifier
        # (delta.`abfss://...`) because the table may not be registered in the catalog.
        # For catalog mode, use the catalog name.
        target = None
        if self._is_for_path_mode(table_ref.access_mode):
            if table_ref.table_path:
                escaped = table_ref.table_path.replace("`", "``")
                target = f"delta.`{escaped}`"
        elif self._is_for_name_mode(table_ref.access_mode):
            if table_ref.table_name:
                try:
                    target = self._quote_table_identifier(table_ref.table_name)
                except Exception:
                    target = table_ref.table_name

        if not target:
            self.logger.warning(
                f"Unable to apply clustering for entity '{getattr(entity, 'entityid', entity)}': "
                "no table_name or table_path available."
            )
            return

        # If the engine reports current clustering, avoid redundant ALTER.
        # For auto clustering we can't reliably compare engine-chosen columns, so always apply.
        current = None if auto_requested else self._get_current_clustering_columns(table_ref)
        desired = [str(c) for c in cluster_cols]
        if current is not None:
            cur_norm = [c.lower() for c in current]
            des_norm = [c.lower() for c in desired]
            if set(cur_norm) == set(des_norm):
                return

        if auto_requested and self._is_for_path_mode(table_ref.access_mode):
            raise ValueError(
                "Auto clustering requires a catalog table target (catalog). "
                f"Entity '{getattr(entity, 'entityid', entity)}' is configured for storage."
            )

        try:
            if auto_requested:
                self.spark.sql(f"ALTER TABLE {target} CLUSTER BY AUTO")
            else:
                cols_sql = ", ".join([f"`{c.replace('`', '``')}`" for c in cluster_cols])
                self.spark.sql(f"ALTER TABLE {target} CLUSTER BY ({cols_sql})")
        except Exception as e:
            # If we hit a parser error, persist a computed runtime feature so future
            # ensures don't keep retrying in this process.
            msg = str(e).lower()
            if "parseexception" in msg or ("mismatched input" in msg and "cluster" in msg):
                try:
                    set_runtime_feature(self.config, "delta.cluster_by", False)
                except Exception:
                    pass
            # Non-fatal: clustering support varies by platform/engine.
            self.logger.warning(
                f"Unable to apply CLUSTER BY for target '{target}' (columns={cluster_cols}): {e}"
            )

    def _extract_detail_field(self, row, field_name: str):
        if row is None:
            return None
        try:
            d = row.asDict(recursive=True)
        except Exception:
            try:
                d = dict(row)
            except Exception:
                d = {}

        for k, v in d.items():
            if str(k).lower() == str(field_name).lower():
                return v
        return None

    def _get_current_clustering_columns(self, table_ref: DeltaTableReference):
        """Return current clustering columns if reported by DESCRIBE DETAIL, else None."""
        try:
            row = None
            # Mirror the target selection logic from _ensure_clustering.
            if self._is_for_path_mode(table_ref.access_mode):
                if not table_ref.table_path:
                    return None
                escaped = table_ref.table_path.replace("`", "``")
                row = self.spark.sql(f"DESCRIBE DETAIL delta.`{escaped}`").first()
            elif self._is_for_name_mode(table_ref.access_mode):
                if not table_ref.table_name:
                    return None
                quoted = self._quote_table_identifier(table_ref.table_name)
                row = self.spark.sql(f"DESCRIBE DETAIL {quoted}").first()
            else:
                return None

            # Record whether DESCRIBE DETAIL exposes the clusteringColumns field.
            try:
                d = row.asDict(recursive=True)
            except Exception:
                try:
                    d = dict(row)
                except Exception:
                    d = {}

            keys = {str(k).lower() for k in d.keys()}
            if "clusteringcolumns" in keys:
                try:
                    set_runtime_feature(
                        self.config, "delta.describe_detail.has_clustering_columns", True
                    )
                except Exception:
                    pass
            else:
                try:
                    set_runtime_feature(
                        self.config, "delta.describe_detail.has_clustering_columns", False
                    )
                except Exception:
                    pass

            val = self._extract_detail_field(row, "clusteringColumns")
            if val is None:
                return None
            # Databricks returns array<string>. Be liberal.
            if isinstance(val, list):
                out = []
                for item in val:
                    if item is None:
                        continue
                    if isinstance(item, str):
                        out.append(item)
                        continue
                    if isinstance(item, dict) and "name" in item:
                        out.append(str(item["name"]))
                        continue
                    out.append(str(item))
                return out
            if isinstance(val, str):
                return [val]
            return None
        except Exception:
            return None

    def _get_table_reference(self, entity) -> DeltaTableReference:
        """
            Create table reference for entity.

            Supports both tag-based and service-based configuration:
            - provider.path: Override table path (optional)
            - provider.table_name: Override table name (optional)
        - provider.access_mode: Override access mode (optional, values: catalog, storage)

            Falls back to injected services (EntityPathLocator, EntityNameMapper) if tags not provided.
        """
        # Get tag-based configuration
        config = self._get_provider_config(entity)

        table_name = config.get("table_name") or self.enm.get_table_name(entity)

        # Check for access_mode override in tags
        access_mode = config.get("access_mode")
        if access_mode:
            # Validate the access mode value
            valid_modes = ["catalog", "storage"]
            if access_mode not in valid_modes:
                self.logger.warning(
                    f"Invalid provider.access_mode '{access_mode}' in tags for entity '{entity.entityid}'. "
                    f"Valid values: {valid_modes}. Using config default: {self.access_mode}"
                )
                access_mode = self.access_mode
        else:
            access_mode = self.access_mode

        # Use tag override path if provided. Otherwise try locator, but allow
        # path-less table bootstrap for name-oriented modes.
        table_path = config.get("path")
        if not table_path:
            try:
                table_path = self.epl.get_table_path(entity)
            except Exception as path_error:
                if self._is_for_name_mode(access_mode):
                    self.logger.debug(
                        f"Path lookup unavailable for {table_name}; proceeding with name-based bootstrap: {path_error}"
                    )
                    table_path = None
                else:
                    raise

        return DeltaTableReference(
            table_name=table_name,
            table_path=table_path,
            access_mode=access_mode,
        )

    def _quote_table_identifier(self, table_name: str) -> str:
        parts = [part.strip() for part in table_name.split(".") if part.strip()]
        if not parts:
            raise ValueError("Table name cannot be empty")
        return ".".join([f"`{part.replace('`', '``')}`" for part in parts])

    def _ensure_configured_table_schema_exists(self) -> None:
        """Best-effort: ensure the configured table schema/database exists.

        This is primarily useful for Synapse-style name-based table creation where
        `saveAsTable()` fails with SCHEMA_NOT_FOUND unless the schema exists and
        has a LOCATION configured.

        No-op unless:
        - `kindling.storage.table_schema` is set
        - `kindling.storage.table_schema_location` is set
        """
        raw_catalog = self.config.get("kindling.storage.table_catalog")
        raw_schema = self.config.get("kindling.storage.table_schema")
        raw_location = self.config.get("kindling.storage.table_schema_location")

        catalog = raw_catalog.strip() if isinstance(raw_catalog, str) else None
        schema = raw_schema.strip() if isinstance(raw_schema, str) else None
        location = raw_location.strip() if isinstance(raw_location, str) else None

        catalog = catalog or None
        schema = schema or None
        location = location or None
        if schema and schema.lower() in {"auto", "none", "null"}:
            schema = None
        if location and location.lower() in {"auto", "none", "null"}:
            location = None
        if catalog and catalog.lower() in {"auto", "none", "null"}:
            catalog = None
        if not schema or not location:
            return

        ident = f"{catalog}.{schema}" if catalog else schema
        quoted_ident = self._quote_table_identifier(ident)
        escaped_loc = location.replace("'", "''")

        # Prefer CREATE SCHEMA (Spark SQL), fall back to CREATE DATABASE for older engines.
        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quoted_ident} LOCATION '{escaped_loc}'")
        except Exception:
            try:
                self.spark.sql(
                    f"CREATE DATABASE IF NOT EXISTS {quoted_ident} LOCATION '{escaped_loc}'"
                )
            except Exception as e:
                # Best-effort: don't fail the caller; downstream writes will raise a clear error.
                self.logger.warning(
                    f"Unable to auto-create schema '{ident}' at location '{location}': {e}"
                )

    def _resolve_catalog_table_location(self, table_name: str) -> Optional[str]:
        try:
            quoted = self._quote_table_identifier(table_name)
            row = self.spark.sql(f"DESCRIBE DETAIL {quoted}").select("location").first()
            if row is None:
                return None
            try:
                return row["location"]
            except Exception:
                return getattr(row, "location", None)
        except Exception:
            return None

    def _create_managed_table(self, entity, table_ref: DeltaTableReference):
        """Create a managed Delta table by name (catalog decides location)."""
        if not table_ref.table_name:
            raise ValueError(
                f"Table name is required for managed table creation: {entity.entityid}"
            )

        # Ensure schema exists when a schema LOCATION is configured (Synapse convention).
        self._ensure_configured_table_schema_exists()

        # Prefer bootstrapping managed tables through the standard Delta writer when
        # a schema is available. Fabric in particular can materialize a non-Delta
        # catalog entry from the builder path, which later causes append/saveAsTable
        # to fail with DELTA_MISSING_DELTA_TABLE.
        if entity.schema:
            scd_config = scd_config_from_tags(entity)
            schema_struct = self._augment_schema_for_scd2(entity.schema, scd_config)

            # Prefer Delta table builder for managed tables when clustering is requested.
            # Some engines require clustering to be enabled at create time and reject
            # ALTER TABLE ... CLUSTER BY on an already-created non-clustered table.
            cluster_cols = self._get_cluster_columns(entity)
            if (
                cluster_cols
                and not self._is_auto_clustering_requested(cluster_cols)
                and get_feature_bool(self.config, "delta.cluster_by", default=True) is not False
            ):
                try:
                    dt = (
                        DeltaTable.createIfNotExists(self.spark)
                        .tableName(table_ref.table_name)
                        .property("delta.enableChangeDataFeed", "true")
                    )
                    dt = dt.addColumns(schema_struct)
                    dt = dt.clusterBy(*cluster_cols)
                    if self._should_partition_files(entity):
                        dt = dt.partitionedBy(*entity.partition_columns)
                    dt.execute()
                    table_ref.table_path = self._resolve_catalog_table_location(
                        table_ref.table_name
                    )
                    return
                except Exception as e:
                    self.logger.warning(
                        "Unable to create managed Delta table via DeltaTable builder with clustering "
                        f"for '{table_ref.table_name}' (columns={cluster_cols}); falling back to dataframe bootstrap: {e}"
                    )

            empty_df = self.spark.createDataFrame([], schema=schema_struct)
            writer = (
                empty_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .option("delta.enableChangeDataFeed", "true")
            )
            if self._should_partition_files(entity):
                writer = writer.partitionBy(*entity.partition_columns)
            writer.saveAsTable(table_ref.table_name)

            # Best-effort: persist desired table properties after creation.
            try:
                quoted = self._quote_table_identifier(table_ref.table_name)
                self.spark.sql(
                    f"ALTER TABLE {quoted} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
                )
            except Exception as e:
                self.logger.warning(
                    f"Unable to set delta.enableChangeDataFeed for '{table_ref.table_name}': {e}"
                )
        else:
            dt = (
                DeltaTable.createIfNotExists(self.spark)
                .tableName(table_ref.table_name)
                .property("delta.enableChangeDataFeed", "true")
            )

            # Liquid clustering: enable at creation time when possible.
            # Some engines do not allow ALTER TABLE ... CLUSTER BY unless the table was created
            # with clustering enabled.
            cluster_cols = self._get_cluster_columns(entity)
            if (
                cluster_cols
                and not self._is_auto_clustering_requested(cluster_cols)
                and get_feature_bool(self.config, "delta.cluster_by", default=True) is not False
            ):
                try:
                    dt = dt.clusterBy(*cluster_cols)
                except Exception as e:
                    # Best-effort: don't fail table creation if clustering isn't supported.
                    self.logger.warning(
                        f"Unable to enable clustering at table creation for '{table_ref.table_name}' "
                        f"(columns={cluster_cols}): {e}"
                    )

            if self._should_partition_files(entity):
                dt = dt.partitionedBy(*entity.partition_columns)

            dt.execute()
        table_ref.table_path = self._resolve_catalog_table_location(table_ref.table_name)

    def _ensure_schema_applied(self, entity, table_ref: DeltaTableReference):
        """Check if table has schema, apply if missing"""
        try:
            # Read existing table to check schema
            if self._is_for_name_mode(table_ref.access_mode):
                existing_df = self.spark.read.table(table_ref.table_name)
            elif table_ref.table_path:
                existing_df = self.spark.read.format("delta").load(table_ref.table_path)
            else:
                existing_df = self.spark.read.table(table_ref.table_name)
            existing_fields = existing_df.schema.fields

            # If table has no columns, write empty df with schema
            if len(existing_fields) == 0:
                self.logger.warning(
                    f"Table {table_ref.table_name} exists but has no schema, applying schema from entity definition"
                )

                from pyspark.sql.types import StructType

                empty_df = self.spark.createDataFrame([], schema=StructType(entity.schema))

                # Write with mergeSchema to add columns
                writer = empty_df.write.format("delta").mode("append").option("mergeSchema", "true")
                if self._is_for_name_mode(table_ref.access_mode):
                    writer.saveAsTable(table_ref.table_name)
                elif table_ref.table_path:
                    writer.save(table_ref.table_path)
                else:
                    writer.saveAsTable(table_ref.table_name)

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
            if self._is_for_name_mode(table_ref.access_mode):
                try:
                    self.spark.read.table(table_ref.table_name)
                    return True
                except Exception as e2:
                    self.logger.debug(f"Table not in catalog: {e2}")
                    return False

            # For FOR_PATH mode, check if path has Delta files
            else:
                try:
                    if not table_ref.table_path:
                        return False
                    self.spark.read.format("delta").load(table_ref.table_path)
                    return True
                except Exception as e2:
                    self.logger.debug(f"Path check failed: {e2}")
                    return False

    def _ensure_table_exists(self, entity, table_ref: DeltaTableReference):
        """Ensure table exists, create if needed"""

        if self._is_for_name_mode(table_ref.access_mode):
            catalog_exists = self._check_catalog_table_exists(table_ref)

            if not catalog_exists:
                self.logger.info(f"Creating managed table by name: {table_ref.table_name}")
                self._create_managed_table(entity, table_ref)

            if entity.schema:
                self._ensure_schema_applied(entity, table_ref)

            if not table_ref.table_path:
                table_ref.table_path = self._resolve_catalog_table_location(table_ref.table_name)

            self._ensure_clustering(entity, table_ref)
            return

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
            self._ensure_clustering(entity, table_ref)
            return

        if not physical_exists:
            self.logger.info(f"Creating physical table at {table_ref.table_path}")
            self._create_physical_table(entity, table_ref)

        if not catalog_exists and table_ref.table_name:
            self._attempt_catalog_registration(table_ref)

        self._ensure_clustering(entity, table_ref)

    def _check_physical_table_exists(self, table_ref: DeltaTableReference) -> bool:
        """Check if physical Delta files exist at the path"""
        if not table_ref.table_path:
            return False
        try:
            self.spark.read.format("delta").load(table_ref.table_path)
            return True
        except Exception as e:
            self.logger.debug(f"Physical table check failed: {e}")
            return False

    def _check_catalog_table_exists(self, table_ref: DeltaTableReference) -> bool:
        """Check if table is registered in catalog"""
        if not table_ref.table_name:
            return False

        try:
            self.spark.read.table(table_ref.table_name)
            return True
        except Exception as e:
            self.logger.debug(f"Catalog table check failed: {e}")
            return False

    def _create_physical_table(self, entity, table_ref: DeltaTableReference):
        """Create physical Delta table files - no catalog interaction"""
        if not table_ref.table_path:
            raise ValueError(f"Cannot create physical table without table path: {entity.entityid}")
        try:
            self.logger.debug(f"Creating physical Delta table at {table_ref.table_path}")
            # Avoid metastore/catalog DDL entirely for FOR_PATH mode.
            # Creating the Delta log by writing an empty dataframe is the most
            # portable approach across Fabric/Synapse/Databricks.
            if not entity.schema:
                # A Delta table can't be bootstrapped without a schema. If the caller
                # wants to ensure storage destinations, they must provide schema;
                # otherwise rely on the first write (which has a schema) to create it.
                raise ValueError(
                    f"Cannot create physical Delta table at '{table_ref.table_path}' without schema "
                    f"for entity '{entity.entityid}'. Provide entity.schema or skip ensure."
                )

            scd_config = scd_config_from_tags(entity)
            schema = self._augment_schema_for_scd2(entity.schema, scd_config)

            # Prefer Delta's table builder API when clustering is requested. Some engines
            # (Synapse in particular) reject `ALTER TABLE ... CLUSTER BY` unless the table
            # was created with clustering enabled.
            cluster_cols = self._get_cluster_columns(entity)
            if (
                cluster_cols
                and not self._is_auto_clustering_requested(cluster_cols)
                and get_feature_bool(self.config, "delta.cluster_by", default=True) is not False
            ):
                try:
                    dt = (
                        DeltaTable.createIfNotExists(self.spark)
                        .location(table_ref.table_path)
                        .property("delta.enableChangeDataFeed", "true")
                    )
                    dt = dt.addColumns(schema)
                    dt = dt.clusterBy(*cluster_cols)
                    # If the user provided partition_columns too, _should_partition_files() will return False.
                    if self._should_partition_files(entity):
                        dt = dt.partitionedBy(*entity.partition_columns)
                    dt.execute()
                    self.logger.info(
                        f"Successfully created physical Delta table at {table_ref.table_path} (clustered={cluster_cols})"
                    )
                    return
                except Exception as e:
                    self.logger.warning(
                        "Unable to create physical Delta table via DeltaTable builder with clustering "
                        f"at '{table_ref.table_path}' (columns={cluster_cols}); falling back to dataframe bootstrap: {e}"
                    )

            empty_df = self.spark.createDataFrame([], schema=schema)
            writer = (
                empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            )
            # Keep behavior aligned with DeltaTable.create*() path: set CDF table property when possible.
            writer = writer.option("delta.enableChangeDataFeed", "true")
            if self._should_partition_files(entity):
                writer = writer.partitionBy(*entity.partition_columns)
            writer.save(table_ref.table_path)

            # Best-effort: ensure the table property is persisted even if the writer option is ignored.
            try:
                escaped = table_ref.table_path.replace("`", "``")
                self.spark.sql(
                    f"ALTER TABLE delta.`{escaped}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
                )
            except Exception as e:
                self.logger.warning(
                    f"Unable to set delta.enableChangeDataFeed for path '{table_ref.table_path}': {e}"
                )

            # Best-effort clustering for path-based tables (engine support varies).
            self._ensure_clustering(entity, table_ref)

            self.logger.info(f"Successfully created physical Delta table at {table_ref.table_path}")

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
        if self._is_for_path_mode(table_ref.access_mode):
            self.logger.debug(f"Skipping catalog registration for FOR_PATH mode")
            return

        if not table_ref.table_path:
            self.logger.debug(
                f"Skipping explicit LOCATION registration for {table_ref.table_name}: path unavailable"
            )
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
                    f"To fix: Switch to storage access mode (kindling.delta.access_mode='storage') "
                    f"or recreate database with: CREATE DATABASE {database} LOCATION 'abfss://...';"
                )
            else:
                # Some other error
                self.logger.error(
                    f"Could not register table {table_ref.table_name} in catalog: {e}"
                )

            # For FOR_NAME mode, this is a problem but don't fail - table still works via path
            if self._is_for_name_mode(table_ref.access_mode):
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
        )
        wrote_managed_by_name = False

        if self._should_partition_files(entity):
            df_writer = df_writer.partitionBy(*entity.partition_columns)

        # Keep catalog semantics table-oriented even when catalog location is known.
        if self._is_for_name_mode(table_ref.access_mode) and table_ref.table_name:
            self._ensure_configured_table_schema_exists()
            df_writer.mode("append").saveAsTable(table_ref.table_name)
            wrote_managed_by_name = True
            table_ref.table_path = self._resolve_catalog_table_location(table_ref.table_name)
        elif table_ref.table_path:
            df_writer = df_writer.option("path", table_ref.table_path)
            df_writer.mode("append").save()
        else:
            raise ValueError(
                f"Cannot write entity '{entity.entityid}' without table path in access mode '{table_ref.access_mode}'"
            )

        # Register table if using named access
        if (
            table_ref.table_path
            and self._is_for_name_mode(table_ref.access_mode)
            and table_ref.table_name
            and not wrote_managed_by_name
        ):
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {table_ref.table_name}
                USING DELTA
                LOCATION '{table_ref.table_path}'
            """
            )

    def _ensure_destination_for_write(self, entity, table_ref: DeltaTableReference) -> None:
        """Best-effort ensure before batch writes so entity metadata can prepare the table."""
        ensure_on_write = self.config.get("kindling.delta.ensure_on_write")
        if ensure_on_write is not None and str(ensure_on_write).strip().lower() in {
            "false",
            "0",
            "no",
            "off",
        }:
            self.logger.debug(
                "Skipping explicit ensure before write: kindling.delta.ensure_on_write is false"
            )
            return

        try:
            self._ensure_table_exists(entity, table_ref)
        except ValueError as e:
            # Storage bootstrap requires schema to create an empty Delta table. If the entity
            # has no schema, keep the legacy implicit-create behavior and let the first write
            # create the table from the DataFrame schema instead.
            if (
                not entity.schema
                and self._is_for_path_mode(table_ref.access_mode)
                and "without schema" in str(e).lower()
            ):
                self.logger.warning(
                    f"Skipping explicit ensure for entity '{entity.entityid}' in storage mode "
                    f"because no entity.schema is defined; falling back to write-driven creation."
                )
                return
            raise

    def _merge_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference):
        """Merge DataFrame to existing Delta table"""
        scd_config = scd_config_from_tags(entity)
        strategy_name = "scd2" if scd_config.enabled else "scd1"
        strategy = DeltaMergeStrategies.get(strategy_name)
        merge_condition = self._build_merge_condition("old", "new", entity.merge_columns)
        strategy.apply(table_ref.get_delta_table(), df, entity, merge_condition)

    def _augment_schema_for_scd2(self, schema: StructType, cfg: SCDConfig) -> StructType:
        """Add SCD2 temporal columns to a schema when SCD2 is enabled."""
        schema_struct = schema if isinstance(schema, StructType) else StructType(schema)
        if not cfg.enabled:
            return schema_struct

        existing_names = {field.name for field in schema_struct.fields}
        extra_fields = []
        if cfg.effective_from_column not in existing_names:
            extra_fields.append(StructField(cfg.effective_from_column, TimestampType(), False))
        if cfg.effective_to_column not in existing_names:
            extra_fields.append(StructField(cfg.effective_to_column, TimestampType(), True))
        if cfg.is_current_column not in existing_names:
            extra_fields.append(StructField(cfg.is_current_column, BooleanType(), False))

        if not extra_fields:
            return schema_struct
        return StructType(schema_struct.fields + extra_fields)

    def _append_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference):
        """Append DataFrame to existing Delta table"""
        writer = df.write.format("delta").mode("append")

        if self._should_partition_files(entity):
            writer = writer.partitionBy(*entity.partition_columns)

        writer.option("mergeSchema", "true")  # Schema evolution
        if self._is_for_name_mode(table_ref.access_mode) and table_ref.table_name:
            self._ensure_configured_table_schema_exists()
            writer.saveAsTable(table_ref.table_name)
            table_ref.table_path = self._resolve_catalog_table_location(table_ref.table_name)
        elif table_ref.table_path:
            writer.save(table_ref.table_path)
        else:
            raise ValueError(
                f"Cannot append entity '{entity.entityid}' without table path in access mode '{table_ref.access_mode}'"
            )

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
        config = self._get_provider_config(entity)

        self.emit(
            "entity.before_ensure_table",
            entity_id=entity.entityid,
            entity_name=entity.name,
            table_path=config.get("path"),
            table_name=config.get("table_name"),
        )

        try:
            table_ref = self._get_table_reference(entity)
            self._ensure_table_exists(entity, table_ref)
            duration = time.time() - start_time

            self.emit(
                "entity.after_ensure_table",
                entity_id=entity.entityid,
                entity_name=entity.name,
                table_path=table_ref.table_path,
                table_name=table_ref.table_name,
                duration_seconds=duration,
            )
        except Exception as e:
            duration = time.time() - start_time
            self.emit(
                "entity.ensure_failed",
                entity_id=entity.entityid,
                entity_name=entity.name,
                table_path=config.get("path"),
                table_name=config.get("table_name"),
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=duration,
            )
            raise

    def ensure_destination(self, entity_metadata: EntityMetadata) -> None:
        # Delta destinations are tables/paths; reuse the existing ensure hook.
        self.ensure_entity_table(entity_metadata)

    def check_entity_exists(self, entity) -> bool:
        """Check if entity table exists"""
        table_ref = self._get_table_reference(entity)
        return self._check_table_exists(table_ref)

    def append_as_stream(self, df, entity, checkpointLocation, format=None, options=None):
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

    def read_entity_as_of(self, entity, point_in_time) -> DataFrame:
        """Read entity state at a specific point in time."""
        scd_config = scd_config_from_tags(entity)
        table_ref = self._get_table_reference(entity)
        if not scd_config.enabled:
            return (
                self.spark.read.format("delta")
                .option("timestampAsOf", str(point_in_time))
                .load(table_ref.get_read_path())
            )

        pit = lit(point_in_time).cast("timestamp")
        return self.read_entity(entity).filter(
            (col(scd_config.effective_from_column) <= pit)
            & (
                col(scd_config.effective_to_column).isNull()
                | (col(scd_config.effective_to_column) > pit)
            )
        )

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
        """Read entity as a streaming DataFrame.

        Args:
            entity: Entity metadata
            format: Ignored — DeltaEntityProvider always uses delta format
            options: Optional read stream options
        """
        table_ref = self._get_table_reference(entity)
        return table_ref.get_spark_read_stream(self.spark, options)

    def write_to_entity(self, df: DataFrame, entity):
        """Write DataFrame to entity table with signal emissions."""
        start_time = time.time()
        table_ref = self._get_table_reference(entity)

        self.emit("entity.before_write", entity_id=entity.entityid, entity_name=entity.name)

        try:
            self._ensure_destination_for_write(entity, table_ref)
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
