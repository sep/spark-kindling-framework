"""
Migration service for Kindling entity schema management.

Provides plan/apply semantics for evolving Delta table schemas and Spark
catalog views to match registered entity definitions. Non-destructive changes
(column additions, SQL entity creates/updates) are applied in place. Destructive
changes (type changes, column removals, partition changes) require
allow_destructive=True.

Delta table rewrites use a blue-green strategy for CATALOG mode or in-place
overwrite for STORAGE mode. SQL entities (permanent catalog views) are always
applied via ``CREATE OR REPLACE VIEW`` and are never destructive.

Typical usage::

    migration_service = GlobalInjector.get(MigrationService)
    plan = migration_service.plan()
    plan.print_summary()
    migration_service.apply(plan)

    # With destructive changes:
    migration_service.apply(plan, allow_destructive=True, backup=BackupStrategy.SNAPSHOT)

Registering a SQL entity (permanent catalog view)::

    @DataEntities.sql_entity(
        entityid="reporting.monthly_summary",
        name="reporting.monthly_summary",
        sql="SELECT month, SUM(amount) AS total FROM transactions GROUP BY month",
    )
"""

from __future__ import annotations

import datetime
import hashlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional

from injector import inject
from kindling.data_entities import DataEntityRegistry, EntityMetadata
from kindling.entity_provider_delta import DeltaEntityProvider, DeltaTableReference
from kindling.injection import GlobalInjector
from kindling.spark_log_provider import PythonLoggerProvider

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ChangeKind(Enum):
    # Table changes
    CREATE_TABLE = "create_table"
    ADD_COLUMNS = "add_columns"
    REMOVE_COLUMNS = "remove_columns"
    TYPE_CHANGE = "type_change"
    PARTITION_CHANGE = "partition_change"
    CLUSTER_CHANGE = "cluster_change"
    TAG_UPDATE = "tag_update"
    # View changes
    CREATE_VIEW = "create_view"
    UPDATE_VIEW = "update_view"
    DROP_VIEW = "drop_view"


class BackupStrategy(Enum):
    NONE = "none"
    SNAPSHOT = "snapshot"


class ChangeDestructiveness(Enum):
    SAFE = "safe"
    DESTRUCTIVE = "destructive"


# ---------------------------------------------------------------------------
# Per-column type change detail
# ---------------------------------------------------------------------------


@dataclass
class ColumnTypeChange:
    column_name: str
    old_type: str
    new_type: str
    auto_castable: bool


# ---------------------------------------------------------------------------
# Per-entity migration change
# ---------------------------------------------------------------------------


@dataclass
class EntityMigrationChange:
    """Describes a single change to be applied to one entity's table."""

    entity: EntityMetadata
    kind: ChangeKind
    destructiveness: ChangeDestructiveness
    detail: str

    # Populated for ADD_COLUMNS
    columns_to_add: List[str] = field(default_factory=list)
    # Populated for REMOVE_COLUMNS
    columns_to_remove: List[str] = field(default_factory=list)
    # Populated for TYPE_CHANGE
    type_changes: List[ColumnTypeChange] = field(default_factory=list)
    # Populated for PARTITION_CHANGE
    old_partitions: List[str] = field(default_factory=list)
    new_partitions: List[str] = field(default_factory=list)
    # Populated for CLUSTER_CHANGE
    old_clusters: List[str] = field(default_factory=list)
    new_clusters: List[str] = field(default_factory=list)
    # Populated for TAG_UPDATE
    tags_to_set: Dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Migration plan
# ---------------------------------------------------------------------------


@dataclass
class EntityMigrationStatus:
    """Aggregates all changes needed for a single entity."""

    entity: EntityMetadata
    changes: List[EntityMigrationChange] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def is_up_to_date(self) -> bool:
        return not self.changes and self.error is None

    @property
    def has_destructive_changes(self) -> bool:
        return any(c.destructiveness == ChangeDestructiveness.DESTRUCTIVE for c in self.changes)


@dataclass
class MigrationPlan:
    """The complete migration plan across all registered entities.

    Both Delta entities and SQL entities (permanent catalog views) are
    represented as ``EntityMigrationStatus`` entries in ``statuses``.
    """

    statuses: List[EntityMigrationStatus] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return any(s.changes for s in self.statuses)

    @property
    def has_destructive_changes(self) -> bool:
        return any(s.has_destructive_changes for s in self.statuses)

    @property
    def errors(self) -> List[EntityMigrationStatus]:
        return [s for s in self.statuses if s.error]

    def print_summary(self) -> None:
        for status in self.statuses:
            entity_id = status.entity.entityid
            kind_label = "view" if status.entity.is_sql_entity else "entity"
            if status.error:
                print(f"  [ERROR] {entity_id} ({kind_label}): {status.error}")
            elif status.is_up_to_date:
                print(f"  [OK]    {entity_id} ({kind_label}): up to date")
            else:
                for change in status.changes:
                    tag = (
                        "DESTRUCTIVE"
                        if change.destructiveness == ChangeDestructiveness.DESTRUCTIVE
                        else "safe"
                    )
                    print(
                        f"  [{tag}] {entity_id} ({kind_label}): {change.kind.value} — {change.detail}"
                    )


# ---------------------------------------------------------------------------
# Cast inference
# ---------------------------------------------------------------------------

# Pairs of (from_type_simplestring, to_type_simplestring) that are safe to
# cast automatically. PySpark DataType.simpleString() is used for comparison.
_SAFE_WIDENINGS = {
    ("tinyint", "smallint"),
    ("tinyint", "int"),
    ("tinyint", "bigint"),
    ("tinyint", "float"),
    ("tinyint", "double"),
    ("smallint", "int"),
    ("smallint", "bigint"),
    ("smallint", "float"),
    ("smallint", "double"),
    ("int", "bigint"),
    ("int", "float"),
    ("int", "double"),
    ("bigint", "float"),
    ("bigint", "double"),
    ("float", "double"),
    ("date", "timestamp"),
}


def _is_auto_castable(old_type, new_type) -> bool:
    """Return True if old_type can be auto-cast to new_type without data loss risk."""
    old_str = old_type.simpleString().lower()
    new_str = new_type.simpleString().lower()
    if old_str == new_str:
        return True
    return (old_str, new_str) in _SAFE_WIDENINGS


# ---------------------------------------------------------------------------
# Planner
# ---------------------------------------------------------------------------


@GlobalInjector.singleton_autobind()
class MigrationPlanner:
    """
    Reads the entity registry and live Delta table state, returns a MigrationPlan.
    Must be called inside a running Spark session.
    """

    @inject
    def __init__(
        self, registry: DataEntityRegistry, provider: DeltaEntityProvider, tp: PythonLoggerProvider
    ):
        self._registry = registry
        self._provider = provider
        self._logger = tp.get_logger("MigrationPlanner")

    def plan(self) -> MigrationPlan:
        statuses: List[EntityMigrationStatus] = []
        for entity_id in self._registry.get_entity_ids():
            entity = self._registry.get_entity_definition(entity_id)
            if entity.is_sql_entity:
                status = self._plan_sql_entity(entity)
            else:
                status = self._plan_delta_entity(entity)
            statuses.append(status)
        return MigrationPlan(statuses=statuses)

    def _plan_sql_entity(self, entity: EntityMetadata) -> EntityMigrationStatus:
        """Plan changes for a SQL-defined entity (permanent catalog view).

        SQL change detection uses a hash stored in TBLPROPERTIES rather than
        comparing raw DDL strings, because SQL engines often reformat stored DDL
        in ways that make string comparison unreliable across platforms.
        """
        status = EntityMigrationStatus(entity=entity)
        try:
            from kindling.spark_session import get_or_create_spark_session

            spark = get_or_create_spark_session()
            view_name = entity.tags.get("provider.table_name") or entity.name

            if not _catalog_object_exists(spark, view_name):
                status.changes.append(
                    EntityMigrationChange(
                        entity=entity,
                        kind=ChangeKind.CREATE_VIEW,
                        destructiveness=ChangeDestructiveness.SAFE,
                        detail=f"view '{view_name}' does not exist, will be created",
                    )
                )
                return status

            # Compare SQL via stored hash rather than raw DDL string.
            stored_hash = self._get_view_sql_hash(spark, view_name)
            current_hash = _sql_hash(entity.sql)
            if stored_hash is None or stored_hash != current_hash:
                status.changes.append(
                    EntityMigrationChange(
                        entity=entity,
                        kind=ChangeKind.UPDATE_VIEW,
                        destructiveness=ChangeDestructiveness.SAFE,
                        detail=f"view '{view_name}' SQL has changed (or hash not recorded), will be replaced",
                    )
                )
        except Exception as e:
            self._logger.warning(f"Failed to plan SQL entity {entity.entityid}: {e}")
            status.error = str(e)
        return status

    def _plan_delta_entity(self, entity: EntityMetadata) -> EntityMigrationStatus:
        status = EntityMigrationStatus(entity=entity)
        try:
            table_ref = self._provider._get_table_reference(entity)
            exists = self._provider._check_table_exists(table_ref)

            if not exists:
                status.changes.append(
                    EntityMigrationChange(
                        entity=entity,
                        kind=ChangeKind.CREATE_TABLE,
                        destructiveness=ChangeDestructiveness.SAFE,
                        detail="table does not exist, will be created",
                    )
                )
                return status

            self._diff_schema(entity, table_ref, status)
            self._diff_partitions(entity, table_ref, status)
            self._diff_clusters(entity, table_ref, status)

        except Exception as e:
            self._logger.warning(f"Failed to plan entity {entity.entityid}: {e}")
            status.error = str(e)
        return status

    def _diff_schema(
        self, entity: EntityMetadata, table_ref: DeltaTableReference, status: EntityMigrationStatus
    ) -> None:
        if not entity.schema:
            return

        from pyspark.sql.types import StructType

        if self._provider._is_for_name_mode(table_ref.access_mode):
            from kindling.spark_session import get_or_create_spark_session

            spark = get_or_create_spark_session()
            live_schema = spark.read.table(table_ref.table_name).schema
        else:
            from kindling.spark_session import get_or_create_spark_session

            spark = get_or_create_spark_session()
            live_schema = spark.read.format("delta").load(table_ref.table_path).schema

        desired_schema = (
            entity.schema if isinstance(entity.schema, StructType) else StructType(entity.schema)
        )
        desired = {f.name: f for f in desired_schema.fields}
        actual = {f.name: f for f in live_schema.fields}

        # Columns to add (safe)
        to_add = [name for name in desired if name not in actual]
        if to_add:
            status.changes.append(
                EntityMigrationChange(
                    entity=entity,
                    kind=ChangeKind.ADD_COLUMNS,
                    destructiveness=ChangeDestructiveness.SAFE,
                    detail=f"add columns: {', '.join(to_add)}",
                    columns_to_add=to_add,
                )
            )

        # Columns to remove (destructive)
        to_remove = [name for name in actual if name not in desired]
        if to_remove:
            status.changes.append(
                EntityMigrationChange(
                    entity=entity,
                    kind=ChangeKind.REMOVE_COLUMNS,
                    destructiveness=ChangeDestructiveness.DESTRUCTIVE,
                    detail=f"remove columns: {', '.join(to_remove)}",
                    columns_to_remove=to_remove,
                )
            )

        # Type changes
        type_changes = []
        for name in desired:
            if name in actual and desired[name].dataType != actual[name].dataType:
                auto_castable = _is_auto_castable(actual[name].dataType, desired[name].dataType)
                type_changes.append(
                    ColumnTypeChange(
                        column_name=name,
                        old_type=actual[name].dataType.simpleString(),
                        new_type=desired[name].dataType.simpleString(),
                        auto_castable=auto_castable,
                    )
                )

        if type_changes:
            status.changes.append(
                EntityMigrationChange(
                    entity=entity,
                    kind=ChangeKind.TYPE_CHANGE,
                    destructiveness=ChangeDestructiveness.DESTRUCTIVE,
                    detail=", ".join(
                        f"{tc.column_name}: {tc.old_type} -> {tc.new_type}"
                        + (" [auto-cast]" if tc.auto_castable else " [needs transform]")
                        for tc in type_changes
                    ),
                    type_changes=type_changes,
                )
            )

    def _diff_partitions(
        self, entity: EntityMetadata, table_ref: DeltaTableReference, status: EntityMigrationStatus
    ) -> None:
        try:
            from kindling.spark_session import get_or_create_spark_session

            spark = get_or_create_spark_session()
            detail_df = spark.sql(f"DESCRIBE DETAIL {self._table_target(table_ref)}")
            row = detail_df.first()
            actual_partitions = (
                list(row["partitionColumns"]) if row and row["partitionColumns"] else []
            )
        except Exception as e:
            self._logger.debug(f"Could not read partition info for {entity.entityid}: {e}")
            return

        desired_partitions = list(entity.partition_columns or [])
        if sorted(actual_partitions) != sorted(desired_partitions):
            status.changes.append(
                EntityMigrationChange(
                    entity=entity,
                    kind=ChangeKind.PARTITION_CHANGE,
                    destructiveness=ChangeDestructiveness.DESTRUCTIVE,
                    detail=f"partition columns: {actual_partitions} -> {desired_partitions}",
                    old_partitions=actual_partitions,
                    new_partitions=desired_partitions,
                )
            )

    def _diff_clusters(
        self, entity: EntityMetadata, table_ref: DeltaTableReference, status: EntityMigrationStatus
    ) -> None:
        try:
            from kindling.spark_session import get_or_create_spark_session

            spark = get_or_create_spark_session()
            detail_df = spark.sql(f"DESCRIBE DETAIL {self._table_target(table_ref)}")
            row = detail_df.first()
            actual_clusters = (
                list(row["clusteringColumns"]) if row and row["clusteringColumns"] else []
            )
        except Exception as e:
            self._logger.debug(f"Could not read cluster info for {entity.entityid}: {e}")
            return

        desired_clusters = list(entity.cluster_columns or [])
        if sorted(actual_clusters) != sorted(desired_clusters):
            status.changes.append(
                EntityMigrationChange(
                    entity=entity,
                    kind=ChangeKind.CLUSTER_CHANGE,
                    destructiveness=ChangeDestructiveness.SAFE,
                    detail=f"cluster columns: {actual_clusters} -> {desired_clusters}",
                    old_clusters=actual_clusters,
                    new_clusters=desired_clusters,
                )
            )

    def _get_view_sql_hash(self, spark, view_name: str) -> Optional[str]:
        """Read the SQL hash stored in TBLPROPERTIES by _apply_sql_entity."""
        try:
            rows = spark.sql(f"SHOW TBLPROPERTIES {view_name}").collect()
            for row in rows:
                if row["key"] == "kindling.sql_hash":
                    return row["value"]
            return None
        except Exception as e:
            self._logger.debug(f"Could not read TBLPROPERTIES for view {view_name}: {e}")
            return None

    def _table_target(self, table_ref: DeltaTableReference) -> str:
        if self._provider._is_for_path_mode(table_ref.access_mode):
            escaped = table_ref.table_path.replace("`", "``")
            return f"delta.`{escaped}`"
        return self._provider._quote_table_identifier(table_ref.table_name)


def _catalog_object_exists(spark, name: str) -> bool:
    """Check whether a catalog table or view exists using SQL only.

    Uses ``DESCRIBE`` rather than ``spark.catalog.tableExists()`` because the
    Catalog Python API is blocked by Databricks's Py4J security whitelist in
    some cluster configurations.
    """
    try:
        spark.sql(f"DESCRIBE {name}")
        return True
    except Exception:
        return False


def _normalize_sql(sql: str) -> str:
    """Normalize SQL for comparison: collapse whitespace and lowercase."""
    return " ".join(sql.lower().split())


def _sql_hash(sql: str) -> str:
    """Return a short stable hash of SQL for TBLPROPERTIES storage."""
    return hashlib.sha256(sql.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Applier
# ---------------------------------------------------------------------------


@GlobalInjector.singleton_autobind()
class MigrationApplier:
    """
    Applies a MigrationPlan.

    Safe changes are always applied. Destructive changes require
    allow_destructive=True. Type changes / partition changes that require a
    full table rewrite use:
      - CATALOG mode: blue-green (write green, validate, swap, archive blue)
      - STORAGE mode: in-place overwrite with overwriteSchema=true (Delta ACID
        guarantees atomicity), with an optional CLONE snapshot beforehand.
    """

    @inject
    def __init__(self, provider: DeltaEntityProvider, tp: PythonLoggerProvider):
        self._provider = provider
        self._logger = tp.get_logger("MigrationApplier")

    def apply(
        self,
        plan: MigrationPlan,
        allow_destructive: bool = False,
        backup: BackupStrategy = BackupStrategy.NONE,
        user_transforms: Optional[Dict[str, object]] = None,
    ) -> None:
        """
        Apply the migration plan.

        Args:
            plan: The plan produced by MigrationPlanner.plan().
            allow_destructive: If False, destructive changes raise an error.
            backup: Whether to snapshot before applying destructive changes.
            user_transforms: Optional dict of {column_name: Column} for type
                changes that cannot be auto-cast.
        """
        user_transforms = user_transforms or {}

        for status in plan.statuses:
            if status.error:
                self._logger.warning(
                    f"Skipping {status.entity.entityid}: plan error — {status.error}"
                )
                continue
            if status.is_up_to_date:
                self._logger.info(f"{status.entity.entityid}: up to date, nothing to do")
                continue
            if status.entity.is_sql_entity:
                self._apply_sql_entity(status)
            else:
                self._apply_delta_entity(status, allow_destructive, backup, user_transforms)

    def _apply_sql_entity(self, status: EntityMigrationStatus) -> None:
        """Apply CREATE_VIEW or UPDATE_VIEW — always safe, always idempotent."""
        from kindling.spark_session import get_or_create_spark_session

        spark = get_or_create_spark_session()
        entity = status.entity
        view_name = entity.tags.get("provider.table_name") or entity.name

        for change in status.changes:
            if change.kind in (ChangeKind.CREATE_VIEW, ChangeKind.UPDATE_VIEW):
                # Ensure the schema/database exists before creating the view.
                # Qualified view names (schema.view) require the schema to exist first.
                if "." in view_name:
                    schema = view_name.rsplit(".", 1)[0]
                    try:
                        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                        self._logger.info(f"Ensured schema exists: {schema}")
                    except Exception as e:
                        self._logger.warning(
                            f"Could not auto-create schema '{schema}' (continuing): {e}"
                        )
                self._logger.info(f"{change.kind.value}: {view_name}")
                spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS {entity.sql}")

                # Record SQL hash in TBLPROPERTIES so future plan() comparisons
                # are stable across SQL reformatting by different engines.
                try:
                    sql_hash = _sql_hash(entity.sql)
                    spark.sql(
                        f"ALTER VIEW {view_name} SET TBLPROPERTIES "
                        f"('kindling.sql_hash' = '{sql_hash}')"
                    )
                except Exception as e:
                    self._logger.warning(
                        f"Could not store SQL hash for '{view_name}' (continuing): {e}"
                    )

    def _apply_delta_entity(
        self,
        status: EntityMigrationStatus,
        allow_destructive: bool,
        backup: BackupStrategy,
        user_transforms: Dict[str, object],
    ) -> None:
        entity = status.entity
        table_ref = self._provider._get_table_reference(entity)

        if status.has_destructive_changes and not allow_destructive:
            kinds = [
                c.kind.value
                for c in status.changes
                if c.destructiveness == ChangeDestructiveness.DESTRUCTIVE
            ]
            raise RuntimeError(
                f"Entity '{entity.entityid}' has destructive changes ({', '.join(kinds)}). "
                "Pass allow_destructive=True to apply."
            )

        # Apply safe changes first (they don't need a rewrite)
        for change in status.changes:
            if change.destructiveness == ChangeDestructiveness.SAFE:
                self._apply_safe_delta_change(entity, table_ref, change)

        # Collect destructive changes and apply via rewrite if needed
        destructive = [
            c for c in status.changes if c.destructiveness == ChangeDestructiveness.DESTRUCTIVE
        ]
        if destructive:
            self._apply_destructive_changes(entity, table_ref, destructive, backup, user_transforms)

    def _apply_safe_delta_change(
        self, entity: EntityMetadata, table_ref: DeltaTableReference, change: EntityMigrationChange
    ) -> None:
        from kindling.spark_session import get_or_create_spark_session

        spark = get_or_create_spark_session()
        target = self._table_target(table_ref)

        if change.kind == ChangeKind.CREATE_TABLE:
            self._logger.info(f"Creating table for {entity.entityid}")
            self._provider.ensure_entity_table(entity)

        elif change.kind == ChangeKind.ADD_COLUMNS:
            self._logger.info(f"Adding columns to {entity.entityid}: {change.columns_to_add}")
            from pyspark.sql.types import StructType

            desired_schema = (
                entity.schema
                if isinstance(entity.schema, StructType)
                else StructType(entity.schema)
            )
            desired = {f.name: f for f in desired_schema.fields}
            col_defs = ", ".join(
                f"`{name}` {desired[name].dataType.simpleString()}"
                for name in change.columns_to_add
            )
            spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({col_defs})")

        elif change.kind == ChangeKind.CLUSTER_CHANGE:
            self._logger.info(
                f"Updating cluster columns for {entity.entityid}: {change.new_clusters}"
            )
            if change.new_clusters:
                cols = ", ".join(f"`{c}`" for c in change.new_clusters)
                try:
                    spark.sql(f"ALTER TABLE {target} CLUSTER BY ({cols})")
                except Exception as e:
                    self._logger.warning(f"Could not apply CLUSTER BY for {entity.entityid}: {e}")

        elif change.kind == ChangeKind.TAG_UPDATE:
            self._logger.info(f"Updating tags for {entity.entityid}: {change.tags_to_set}")
            props = ", ".join(f"'{k}' = '{v}'" for k, v in change.tags_to_set.items())
            spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ({props})")

    def _apply_destructive_changes(
        self,
        entity: EntityMetadata,
        table_ref: DeltaTableReference,
        changes: List[EntityMigrationChange],
        backup: BackupStrategy,
        user_transforms: Dict[str, object],
    ) -> None:
        is_catalog = self._provider._is_for_name_mode(table_ref.access_mode)

        if backup == BackupStrategy.SNAPSHOT:
            self._snapshot(entity, table_ref)

        if is_catalog:
            self._blue_green_rewrite(entity, table_ref, changes, user_transforms)
        else:
            self._inplace_rewrite(entity, table_ref, changes, user_transforms)

    # ------------------------------------------------------------------
    # Blue-green (CATALOG mode)
    # ------------------------------------------------------------------

    def _blue_green_rewrite(
        self,
        entity: EntityMetadata,
        table_ref: DeltaTableReference,
        changes: List[EntityMigrationChange],
        user_transforms: Dict[str, object],
    ) -> None:
        from kindling.spark_session import get_or_create_spark_session

        spark = get_or_create_spark_session()

        original_name = table_ref.table_name
        green_name = f"{original_name}_migration_green"
        blue_archive_name = f"{original_name}_migration_blue"
        quoted_original = self._provider._quote_table_identifier(original_name)
        quoted_green = self._provider._quote_table_identifier(green_name)
        quoted_blue = self._provider._quote_table_identifier(blue_archive_name)

        self._logger.info(
            f"Blue-green rewrite for {entity.entityid}: building green table {green_name}"
        )

        # Read existing data and apply transforms
        old_df = spark.read.table(original_name)
        new_df = self._apply_transforms(old_df, entity, changes, user_transforms)

        # Write green
        writer = new_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if entity.partition_columns:
            writer = writer.partitionBy(*entity.partition_columns)
        writer.saveAsTable(green_name)

        # Validate
        old_count = old_df.count()
        new_count = spark.read.table(green_name).count()
        if new_count != old_count:
            self._logger.warning(
                f"Row count mismatch after rewrite for {entity.entityid}: "
                f"old={old_count}, new={new_count}. Green table preserved at {green_name}."
            )
            raise RuntimeError(
                f"Migration validation failed for '{entity.entityid}': "
                f"row count changed from {old_count} to {new_count}."
            )

        # Swap: archive blue, promote green
        self._logger.info(f"Swapping tables: {original_name} -> blue archive, {green_name} -> live")
        spark.sql(f"ALTER TABLE {quoted_original} RENAME TO {quoted_blue}")
        spark.sql(f"ALTER TABLE {quoted_green} RENAME TO {quoted_original}")
        self._logger.info(
            f"Rewrite complete for {entity.entityid}. "
            f"Old data archived at {blue_archive_name}. "
            f"Run cleanup to drop it."
        )

    # ------------------------------------------------------------------
    # In-place overwrite (STORAGE mode)
    # ------------------------------------------------------------------

    def _inplace_rewrite(
        self,
        entity: EntityMetadata,
        table_ref: DeltaTableReference,
        changes: List[EntityMigrationChange],
        user_transforms: Dict[str, object],
    ) -> None:
        from kindling.spark_session import get_or_create_spark_session

        spark = get_or_create_spark_session()

        self._logger.info(f"In-place rewrite for {entity.entityid} at {table_ref.table_path}")
        old_df = spark.read.format("delta").load(table_ref.table_path)
        new_df = self._apply_transforms(old_df, entity, changes, user_transforms)

        writer = new_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if entity.partition_columns:
            writer = writer.partitionBy(*entity.partition_columns)
        writer.save(table_ref.table_path)
        self._logger.info(f"In-place rewrite complete for {entity.entityid}")

    # ------------------------------------------------------------------
    # Snapshot (optional pre-destructive backup)
    # ------------------------------------------------------------------

    def _snapshot(self, entity: EntityMetadata, table_ref: DeltaTableReference) -> None:
        from kindling.spark_session import get_or_create_spark_session

        spark = get_or_create_spark_session()

        stamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        target = self._table_target(table_ref)

        if self._provider._is_for_name_mode(table_ref.access_mode):
            snapshot_name = f"{table_ref.table_name}_snapshot_{stamp}"
            quoted_snap = self._provider._quote_table_identifier(snapshot_name)
            self._logger.info(f"Snapshotting {entity.entityid} -> {snapshot_name}")
            spark.sql(f"CREATE TABLE {quoted_snap} CLONE {target}")
        else:
            snapshot_path = f"{table_ref.table_path}/_snapshots/{stamp}"
            self._logger.info(f"Snapshotting {entity.entityid} -> {snapshot_path}")
            spark.sql(f"CREATE TABLE delta.`{snapshot_path}` CLONE {target}")

    # ------------------------------------------------------------------
    # Transform application
    # ------------------------------------------------------------------

    def _apply_transforms(
        self,
        df,
        entity: EntityMetadata,
        changes: List[EntityMigrationChange],
        user_transforms: Dict[str, object],
    ):
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType

        desired_schema = (
            entity.schema if isinstance(entity.schema, StructType) else StructType(entity.schema)
        )
        result = df

        for change in changes:
            if change.kind == ChangeKind.TYPE_CHANGE:
                desired = {f.name: f for f in desired_schema.fields}
                for tc in change.type_changes:
                    if tc.column_name in user_transforms:
                        result = result.withColumn(tc.column_name, user_transforms[tc.column_name])
                    elif tc.auto_castable:
                        result = result.withColumn(
                            tc.column_name,
                            F.col(tc.column_name).cast(desired[tc.column_name].dataType),
                        )
                    else:
                        raise RuntimeError(
                            f"Cannot auto-cast column '{tc.column_name}' from {tc.old_type} to {tc.new_type} "
                            f"for entity '{entity.entityid}'. Provide a user_transform for this column."
                        )

            elif change.kind == ChangeKind.REMOVE_COLUMNS:
                for col_name in change.columns_to_remove:
                    result = result.drop(col_name)

        return result

    def _table_target(self, table_ref: DeltaTableReference) -> str:
        if self._provider._is_for_path_mode(table_ref.access_mode):
            escaped = table_ref.table_path.replace("`", "``")
            return f"delta.`{escaped}`"
        return self._provider._quote_table_identifier(table_ref.table_name)


# ---------------------------------------------------------------------------
# Rollback and cleanup
# ---------------------------------------------------------------------------


@GlobalInjector.singleton_autobind()
class MigrationManager:
    """Handles rollback and cleanup of blue-green artifacts."""

    @inject
    def __init__(self, provider: DeltaEntityProvider, tp: PythonLoggerProvider):
        self._provider = provider
        self._logger = tp.get_logger("MigrationManager")

    def rollback(self, entity: EntityMetadata) -> None:
        """
        Promote the blue archive back to live. Only valid for CATALOG mode
        after a blue-green apply that has not yet been cleaned up.
        """
        from kindling.spark_session import get_or_create_spark_session

        spark = get_or_create_spark_session()

        table_ref = self._provider._get_table_reference(entity)
        if not self._provider._is_for_name_mode(table_ref.access_mode):
            raise RuntimeError(
                f"Rollback is only supported for CATALOG mode entities. "
                f"'{entity.entityid}' uses STORAGE mode."
            )

        original_name = table_ref.table_name
        blue_name = f"{original_name}_migration_blue"
        green_name = f"{original_name}_migration_green"

        quoted_original = self._provider._quote_table_identifier(original_name)
        quoted_blue = self._provider._quote_table_identifier(blue_name)
        quoted_green = self._provider._quote_table_identifier(green_name)

        self._logger.info(f"Rolling back {entity.entityid}: restoring from {blue_name}")
        spark.sql(f"ALTER TABLE {quoted_original} RENAME TO {quoted_green}")
        spark.sql(f"ALTER TABLE {quoted_blue} RENAME TO {quoted_original}")
        self._logger.info(
            f"Rollback complete. Failed green table is now at {green_name}. Run cleanup to drop it."
        )

    def cleanup(self, entity: EntityMetadata) -> None:
        """
        Drop the blue archive table left behind after a successful blue-green apply.
        Also drops any failed green table if present.
        """
        from kindling.spark_session import get_or_create_spark_session

        spark = get_or_create_spark_session()

        table_ref = self._provider._get_table_reference(entity)
        if not self._provider._is_for_name_mode(table_ref.access_mode):
            raise RuntimeError(
                f"Cleanup is only supported for CATALOG mode entities. "
                f"'{entity.entityid}' uses STORAGE mode."
            )

        original_name = table_ref.table_name
        for suffix in ("_migration_blue", "_migration_green"):
            candidate = f"{original_name}{suffix}"
            try:
                quoted = self._provider._quote_table_identifier(candidate)
                spark.sql(f"DROP TABLE IF EXISTS {quoted}")
                self._logger.info(f"Dropped {candidate}")
            except Exception as e:
                self._logger.warning(f"Could not drop {candidate}: {e}")


# ---------------------------------------------------------------------------
# Service facade
# ---------------------------------------------------------------------------


@GlobalInjector.singleton_autobind()
class MigrationService:
    """
    Main entry point for migration operations.

    Example::

        svc = GlobalInjector.get(MigrationService)
        plan = svc.plan()
        plan.print_summary()
        svc.apply(plan, allow_destructive=True, backup=BackupStrategy.SNAPSHOT)
        svc.cleanup(my_entity)
    """

    @inject
    def __init__(
        self, planner: MigrationPlanner, applier: MigrationApplier, manager: MigrationManager
    ):
        self._planner = planner
        self._applier = applier
        self._manager = manager

    def plan(self) -> MigrationPlan:
        """Inspect all registered entities and return a MigrationPlan."""
        return self._planner.plan()

    def apply(
        self,
        plan: MigrationPlan,
        allow_destructive: bool = False,
        backup: BackupStrategy = BackupStrategy.NONE,
        user_transforms: Optional[Dict[str, object]] = None,
    ) -> None:
        """Apply a MigrationPlan."""
        self._applier.apply(
            plan,
            allow_destructive=allow_destructive,
            backup=backup,
            user_transforms=user_transforms,
        )

    def rollback(self, entity: EntityMetadata) -> None:
        """Restore the pre-migration blue archive to live (CATALOG mode only)."""
        self._manager.rollback(entity)

    def cleanup(self, entity: EntityMetadata) -> None:
        """Drop blue-green artifacts after a confirmed successful migration."""
        self._manager.cleanup(entity)
