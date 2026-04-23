"""
Unit tests for kindling.migration module.

Tests cover:
- MigrationPlan / EntityMigrationStatus helpers
- Cast inference (_is_auto_castable)
- MigrationPlanner schema diff logic
- MigrationPlanner view planning logic
- MigrationApplier transform application
- MigrationApplier view apply logic
- MigrationService facade delegation
"""

from unittest.mock import MagicMock, call, patch

import pytest
from kindling.data_entities import EntityMetadata, SqlSource
from kindling.migration import (
    BackupStrategy,
    ChangeDestructiveness,
    ChangeKind,
    ColumnTypeChange,
    EntityMigrationChange,
    EntityMigrationStatus,
    MigrationApplier,
    MigrationPlan,
    MigrationPlanner,
    MigrationService,
    _is_auto_castable,
    _normalize_sql,
    _sql_hash,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _entity(entityid="test.entity", partition_columns=None, cluster_columns=None, schema=None):
    return EntityMetadata(
        entityid=entityid,
        name=entityid.split(".")[-1],
        merge_columns=["id"],
        tags={},
        schema=schema,
        partition_columns=partition_columns or [],
        cluster_columns=cluster_columns or [],
    )


def _make_struct(*fields):
    """Build a StructType from (name, DataType) pairs."""
    from pyspark.sql.types import StructField, StructType

    return StructType([StructField(name, dtype, True) for name, dtype in fields])


# ---------------------------------------------------------------------------
# Cast inference
# ---------------------------------------------------------------------------


class TestIsAutoCastable:
    def test_same_type_is_castable(self):
        from pyspark.sql.types import IntegerType

        t = IntegerType()
        assert _is_auto_castable(t, t)

    def test_integer_to_long_is_castable(self):
        from pyspark.sql.types import IntegerType, LongType

        assert _is_auto_castable(IntegerType(), LongType())

    def test_long_to_integer_is_not_castable(self):
        from pyspark.sql.types import IntegerType, LongType

        assert not _is_auto_castable(LongType(), IntegerType())

    def test_float_to_double_is_castable(self):
        from pyspark.sql.types import DoubleType, FloatType

        assert _is_auto_castable(FloatType(), DoubleType())

    def test_double_to_float_is_not_castable(self):
        from pyspark.sql.types import DoubleType, FloatType

        assert not _is_auto_castable(DoubleType(), FloatType())

    def test_date_to_timestamp_is_castable(self):
        from pyspark.sql.types import DateType, TimestampType

        assert _is_auto_castable(DateType(), TimestampType())

    def test_string_to_long_is_not_castable(self):
        from pyspark.sql.types import LongType, StringType

        assert not _is_auto_castable(StringType(), LongType())

    def test_long_to_string_is_not_castable(self):
        from pyspark.sql.types import LongType, StringType

        assert not _is_auto_castable(LongType(), StringType())

    def test_byte_to_double_is_castable(self):
        from pyspark.sql.types import ByteType, DoubleType

        assert _is_auto_castable(ByteType(), DoubleType())


# ---------------------------------------------------------------------------
# EntityMigrationStatus / MigrationPlan helpers
# ---------------------------------------------------------------------------


class TestEntityMigrationStatus:
    def test_is_up_to_date_when_no_changes(self):
        status = EntityMigrationStatus(entity=_entity())
        assert status.is_up_to_date

    def test_not_up_to_date_when_changes_present(self):
        status = EntityMigrationStatus(
            entity=_entity(),
            changes=[
                EntityMigrationChange(
                    entity=_entity(),
                    kind=ChangeKind.ADD_COLUMNS,
                    destructiveness=ChangeDestructiveness.SAFE,
                    detail="add columns: foo",
                    columns_to_add=["foo"],
                )
            ],
        )
        assert not status.is_up_to_date

    def test_not_up_to_date_when_error(self):
        status = EntityMigrationStatus(entity=_entity(), error="boom")
        assert not status.is_up_to_date

    def test_has_destructive_changes(self):
        status = EntityMigrationStatus(
            entity=_entity(),
            changes=[
                EntityMigrationChange(
                    entity=_entity(),
                    kind=ChangeKind.REMOVE_COLUMNS,
                    destructiveness=ChangeDestructiveness.DESTRUCTIVE,
                    detail="remove columns: old",
                )
            ],
        )
        assert status.has_destructive_changes

    def test_has_no_destructive_changes_when_all_safe(self):
        status = EntityMigrationStatus(
            entity=_entity(),
            changes=[
                EntityMigrationChange(
                    entity=_entity(),
                    kind=ChangeKind.ADD_COLUMNS,
                    destructiveness=ChangeDestructiveness.SAFE,
                    detail="add columns: new",
                )
            ],
        )
        assert not status.has_destructive_changes


class TestMigrationPlan:
    def test_has_no_changes_when_all_up_to_date(self):
        plan = MigrationPlan(
            statuses=[
                EntityMigrationStatus(entity=_entity("a")),
                EntityMigrationStatus(entity=_entity("b")),
            ]
        )
        assert not plan.has_changes

    def test_has_changes_when_any_status_has_change(self):
        plan = MigrationPlan(
            statuses=[
                EntityMigrationStatus(entity=_entity("a")),
                EntityMigrationStatus(
                    entity=_entity("b"),
                    changes=[
                        EntityMigrationChange(
                            entity=_entity("b"),
                            kind=ChangeKind.ADD_COLUMNS,
                            destructiveness=ChangeDestructiveness.SAFE,
                            detail="add columns: x",
                        )
                    ],
                ),
            ]
        )
        assert plan.has_changes

    def test_errors_returns_errored_statuses(self):
        plan = MigrationPlan(
            statuses=[
                EntityMigrationStatus(entity=_entity("a"), error="oops"),
                EntityMigrationStatus(entity=_entity("b")),
            ]
        )
        assert len(plan.errors) == 1
        assert plan.errors[0].entity.entityid == "a"


# ---------------------------------------------------------------------------
# MigrationPlanner schema diff
# ---------------------------------------------------------------------------


class TestMigrationPlannerSchemaDiff:
    """Tests for _diff_schema via direct method invocation (no Spark needed for logic)."""

    def _make_planner(self):
        registry = MagicMock()
        provider = MagicMock()
        provider._is_for_name_mode.return_value = True
        provider._is_for_path_mode.return_value = False
        tp = MagicMock()
        tp.get_logger.return_value = MagicMock()
        planner = MigrationPlanner.__new__(MigrationPlanner)
        planner._registry = registry
        planner._provider = provider
        planner._logger = tp.get_logger("test")
        return planner

    def _make_table_ref(self, table_name="test.entity"):
        from kindling.entity_provider_delta import DeltaAccessMode, DeltaTableReference

        ref = MagicMock(spec=DeltaTableReference)
        ref.table_name = table_name
        ref.table_path = None
        ref.access_mode = DeltaAccessMode.CATALOG
        return ref

    def test_detects_columns_to_add(self):
        from pyspark.sql.types import StringType

        planner = self._make_planner()

        desired_schema = _make_struct(
            ("id", __import__("pyspark.sql.types", fromlist=["LongType"]).LongType()),
            ("new_col", StringType()),
        )
        live_schema = _make_struct(
            ("id", __import__("pyspark.sql.types", fromlist=["LongType"]).LongType())
        )

        entity = _entity(schema=desired_schema)
        table_ref = self._make_table_ref()
        status = EntityMigrationStatus(entity=entity)

        spark_mock = MagicMock()
        spark_mock.read.table.return_value.schema = live_schema

        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            planner._diff_schema(entity, table_ref, status)

        assert len(status.changes) == 1
        assert status.changes[0].kind == ChangeKind.ADD_COLUMNS
        assert status.changes[0].destructiveness == ChangeDestructiveness.SAFE
        assert "new_col" in status.changes[0].columns_to_add

    def test_detects_columns_to_remove(self):
        from pyspark.sql.types import LongType, StringType

        planner = self._make_planner()

        desired_schema = _make_struct(("id", LongType()))
        live_schema = _make_struct(("id", LongType()), ("old_col", StringType()))

        entity = _entity(schema=desired_schema)
        table_ref = self._make_table_ref()
        status = EntityMigrationStatus(entity=entity)

        spark_mock = MagicMock()
        spark_mock.read.table.return_value.schema = live_schema

        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            planner._diff_schema(entity, table_ref, status)

        assert len(status.changes) == 1
        assert status.changes[0].kind == ChangeKind.REMOVE_COLUMNS
        assert status.changes[0].destructiveness == ChangeDestructiveness.DESTRUCTIVE
        assert "old_col" in status.changes[0].columns_to_remove

    def test_detects_type_change(self):
        from pyspark.sql.types import IntegerType, LongType

        planner = self._make_planner()

        desired_schema = _make_struct(("id", LongType()))
        live_schema = _make_struct(("id", IntegerType()))

        entity = _entity(schema=desired_schema)
        table_ref = self._make_table_ref()
        status = EntityMigrationStatus(entity=entity)

        spark_mock = MagicMock()
        spark_mock.read.table.return_value.schema = live_schema

        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            planner._diff_schema(entity, table_ref, status)

        assert len(status.changes) == 1
        change = status.changes[0]
        assert change.kind == ChangeKind.TYPE_CHANGE
        assert change.destructiveness == ChangeDestructiveness.DESTRUCTIVE
        assert change.type_changes[0].column_name == "id"
        assert change.type_changes[0].auto_castable is True  # int -> long is safe widening

    def test_no_changes_when_schema_matches(self):
        from pyspark.sql.types import LongType, StringType

        planner = self._make_planner()

        schema = _make_struct(("id", LongType()), ("name", StringType()))
        entity = _entity(schema=schema)
        table_ref = self._make_table_ref()
        status = EntityMigrationStatus(entity=entity)

        spark_mock = MagicMock()
        spark_mock.read.table.return_value.schema = schema

        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            planner._diff_schema(entity, table_ref, status)

        assert len(status.changes) == 0

    def test_skips_diff_when_no_entity_schema(self):
        planner = self._make_planner()
        entity = _entity(schema=None)
        table_ref = self._make_table_ref()
        status = EntityMigrationStatus(entity=entity)
        planner._diff_schema(entity, table_ref, status)
        assert len(status.changes) == 0


# ---------------------------------------------------------------------------
# MigrationApplier transform logic
# ---------------------------------------------------------------------------


class TestMigrationApplierTransforms:
    def _make_applier(self):
        provider = MagicMock()
        tp = MagicMock()
        tp.get_logger.return_value = MagicMock()
        applier = MigrationApplier.__new__(MigrationApplier)
        applier._provider = provider
        applier._logger = tp.get_logger("test")
        return applier

    def test_apply_transforms_auto_casts_widening_type(self):
        from pyspark.sql.types import IntegerType, LongType, StringType

        applier = self._make_applier()

        desired_schema = _make_struct(("id", LongType()), ("name", StringType()))
        entity = _entity(schema=desired_schema)

        tc = ColumnTypeChange(
            column_name="id", old_type="integer", new_type="long", auto_castable=True
        )
        change = EntityMigrationChange(
            entity=entity,
            kind=ChangeKind.TYPE_CHANGE,
            destructiveness=ChangeDestructiveness.DESTRUCTIVE,
            detail="id: integer -> long",
            type_changes=[tc],
        )

        # Mock DataFrame with withColumn that returns self
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df

        result = applier._apply_transforms(mock_df, entity, [change], {})

        mock_df.withColumn.assert_called_once()
        call_args = mock_df.withColumn.call_args
        assert call_args[0][0] == "id"

    def test_apply_transforms_uses_user_transform_over_auto_cast(self):
        from pyspark.sql import functions as F
        from pyspark.sql.types import LongType, StringType

        applier = self._make_applier()

        desired_schema = _make_struct(("id", StringType()))
        entity = _entity(schema=desired_schema)

        tc = ColumnTypeChange(
            column_name="id", old_type="long", new_type="string", auto_castable=False
        )
        change = EntityMigrationChange(
            entity=entity,
            kind=ChangeKind.TYPE_CHANGE,
            destructiveness=ChangeDestructiveness.DESTRUCTIVE,
            detail="id: long -> string",
            type_changes=[tc],
        )

        user_expr = F.col("id").cast(StringType())
        mock_df = MagicMock()
        mock_df.withColumn.return_value = mock_df

        result = applier._apply_transforms(mock_df, entity, [change], {"id": user_expr})
        mock_df.withColumn.assert_called_once_with("id", user_expr)

    def test_apply_transforms_raises_when_not_castable_and_no_user_transform(self):
        from pyspark.sql.types import LongType, StringType

        applier = self._make_applier()

        desired_schema = _make_struct(("id", StringType()))
        entity = _entity(schema=desired_schema)

        tc = ColumnTypeChange(
            column_name="id", old_type="long", new_type="string", auto_castable=False
        )
        change = EntityMigrationChange(
            entity=entity,
            kind=ChangeKind.TYPE_CHANGE,
            destructiveness=ChangeDestructiveness.DESTRUCTIVE,
            detail="id: long -> string",
            type_changes=[tc],
        )

        mock_df = MagicMock()
        with pytest.raises(RuntimeError, match="user_transform"):
            applier._apply_transforms(mock_df, entity, [change], {})

    def test_apply_transforms_drops_removed_columns(self):
        from pyspark.sql.types import LongType

        applier = self._make_applier()
        entity = _entity(schema=_make_struct(("id", LongType())))

        change = EntityMigrationChange(
            entity=entity,
            kind=ChangeKind.REMOVE_COLUMNS,
            destructiveness=ChangeDestructiveness.DESTRUCTIVE,
            detail="remove columns: old_col",
            columns_to_remove=["old_col"],
        )

        mock_df = MagicMock()
        mock_df.drop.return_value = mock_df

        applier._apply_transforms(mock_df, entity, [change], {})
        mock_df.drop.assert_called_once_with("old_col")


# ---------------------------------------------------------------------------
# MigrationApplier.apply — destructive guard
# ---------------------------------------------------------------------------


class TestMigrationApplierDestructiveGuard:
    def _make_applier(self):
        provider = MagicMock()
        provider._get_table_reference.return_value = MagicMock()
        provider._is_for_name_mode.return_value = True
        tp = MagicMock()
        tp.get_logger.return_value = MagicMock()
        applier = MigrationApplier.__new__(MigrationApplier)
        applier._provider = provider
        applier._logger = tp.get_logger("test")
        return applier

    def test_raises_when_destructive_not_allowed(self):
        applier = self._make_applier()
        entity = _entity()
        status = EntityMigrationStatus(
            entity=entity,
            changes=[
                EntityMigrationChange(
                    entity=entity,
                    kind=ChangeKind.REMOVE_COLUMNS,
                    destructiveness=ChangeDestructiveness.DESTRUCTIVE,
                    detail="remove columns: old",
                    columns_to_remove=["old"],
                )
            ],
        )
        plan = MigrationPlan(statuses=[status])

        with pytest.raises(RuntimeError, match="allow_destructive"):
            applier.apply(plan, allow_destructive=False)

    def test_skips_entity_with_plan_error(self):
        applier = self._make_applier()
        entity = _entity()
        status = EntityMigrationStatus(entity=entity, error="some error")
        plan = MigrationPlan(statuses=[status])

        # Should not raise, just log and skip
        applier.apply(plan, allow_destructive=False)

    def test_skips_up_to_date_entity(self):
        applier = self._make_applier()
        entity = _entity()
        status = EntityMigrationStatus(entity=entity)  # no changes
        plan = MigrationPlan(statuses=[status])

        applier.apply(plan, allow_destructive=False)
        applier._provider._get_table_reference.assert_not_called()


# ---------------------------------------------------------------------------
# MigrationService — facade delegation
# ---------------------------------------------------------------------------


class TestMigrationService:
    def _make_service(self):
        planner = MagicMock()
        applier = MagicMock()
        manager = MagicMock()
        svc = MigrationService.__new__(MigrationService)
        svc._planner = planner
        svc._applier = applier
        svc._manager = manager
        return svc, planner, applier, manager

    def test_plan_delegates_to_planner(self):
        svc, planner, _, _ = self._make_service()
        mock_plan = MagicMock()
        planner.plan.return_value = mock_plan

        result = svc.plan()

        planner.plan.assert_called_once()
        assert result is mock_plan

    def test_apply_delegates_to_applier(self):
        svc, _, applier, _ = self._make_service()
        mock_plan = MagicMock()

        svc.apply(mock_plan, allow_destructive=True, backup=BackupStrategy.SNAPSHOT)

        applier.apply.assert_called_once_with(
            mock_plan,
            allow_destructive=True,
            backup=BackupStrategy.SNAPSHOT,
            user_transforms=None,
        )

    def test_rollback_delegates_to_manager(self):
        svc, _, _, manager = self._make_service()
        entity = _entity()
        svc.rollback(entity)
        manager.rollback.assert_called_once_with(entity)

    def test_cleanup_delegates_to_manager(self):
        svc, _, _, manager = self._make_service()
        entity = _entity()
        svc.cleanup(entity)
        manager.cleanup.assert_called_once_with(entity)


# ---------------------------------------------------------------------------
# SQL normalization
# ---------------------------------------------------------------------------


class TestNormalizeSql:
    def test_collapses_whitespace(self):
        assert _normalize_sql("SELECT  a,  b  FROM  t") == _normalize_sql("SELECT a, b FROM t")

    def test_lowercases(self):
        assert _normalize_sql("SELECT A FROM T") == _normalize_sql("select a from t")

    def test_strips_leading_trailing(self):
        assert _normalize_sql("  SELECT 1  ") == "select 1"


# ---------------------------------------------------------------------------
# SqlSource
# ---------------------------------------------------------------------------


class TestSqlSource:
    def test_inline_loads_directly(self):
        src = SqlSource(inline="SELECT 1")
        assert src.load() == "SELECT 1"

    def test_file_loads_from_path(self, tmp_path):
        f = tmp_path / "q.sql"
        f.write_text("SELECT * FROM t", encoding="utf-8")
        src = SqlSource(file=str(f))
        assert src.load() == "SELECT * FROM t"

    def test_resource_loads_via_importlib(self):
        mock_files = MagicMock()
        mock_files.return_value.joinpath.return_value.read_text.return_value = "SELECT 2"
        with patch("importlib.resources.files", mock_files):
            src = SqlSource(resource="my_app:sql/q.sql")
            result = src.load()
        mock_files.assert_called_once_with("my_app")
        assert result == "SELECT 2"

    def test_raises_when_none_provided(self):
        with pytest.raises(ValueError, match="exactly one"):
            SqlSource()

    def test_raises_when_multiple_provided(self):
        with pytest.raises(ValueError, match="exactly one"):
            SqlSource(inline="SELECT 1", file="/tmp/q.sql")

    def test_resource_raises_on_bad_format(self):
        src = SqlSource(resource="no-colon-here")
        with pytest.raises(ValueError, match="package:path"):
            src.load()


# ---------------------------------------------------------------------------
# SQL entity planning (_plan_sql_entity)
# ---------------------------------------------------------------------------


class TestMigrationPlannerSqlEntity:
    def _make_planner(self):
        registry = MagicMock()
        registry.get_entity_ids.return_value = []
        provider = MagicMock()
        tp = MagicMock()
        tp.get_logger.return_value = MagicMock()
        planner = MigrationPlanner.__new__(MigrationPlanner)
        planner._registry = registry
        planner._provider = provider
        planner._logger = tp.get_logger("test")
        return planner

    def _sql_entity(self, sql="SELECT 1", name="test.v"):
        return EntityMetadata(
            entityid=name,
            name=name,
            merge_columns=[],
            tags={},
            schema=None,
            sql=sql,
        )

    def test_create_view_when_not_exists(self):
        planner = self._make_planner()
        entity = self._sql_entity()
        spark_mock = MagicMock()
        spark_mock.sql.side_effect = Exception("no such table")  # DESCRIBE raises → not exists

        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            status = planner._plan_sql_entity(entity)

        assert len(status.changes) == 1
        assert status.changes[0].kind == ChangeKind.CREATE_VIEW
        assert status.changes[0].destructiveness == ChangeDestructiveness.SAFE

    def test_up_to_date_when_sql_hash_matches(self):
        planner = self._make_planner()
        sql = "SELECT a, b FROM t"
        entity = self._sql_entity(sql=sql)
        spark_mock = MagicMock()
        spark_mock.sql.return_value = MagicMock()  # DESCRIBE succeeds → exists
        # SHOW TBLPROPERTIES returns matching hash → up to date
        spark_mock.sql.return_value.collect.return_value = [
            MagicMock(
                **{
                    "__getitem__": lambda s, k: (
                        _sql_hash(sql) if k == "value" else "kindling.sql_hash"
                    )
                }
            )
        ]
        # Simplify: mock _get_view_sql_hash directly
        planner._get_view_sql_hash = MagicMock(return_value=_sql_hash(sql))

        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            status = planner._plan_sql_entity(entity)

        assert status.is_up_to_date

    def test_update_view_when_sql_hash_differs(self):
        planner = self._make_planner()
        entity = self._sql_entity(sql="SELECT a, b, c FROM t")
        spark_mock = MagicMock()
        spark_mock.sql.return_value = MagicMock()  # DESCRIBE succeeds → exists
        # Stored hash corresponds to different SQL → UPDATE_VIEW
        planner._get_view_sql_hash = MagicMock(return_value=_sql_hash("SELECT a, b FROM t"))

        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            status = planner._plan_sql_entity(entity)

        assert len(status.changes) == 1
        assert status.changes[0].kind == ChangeKind.UPDATE_VIEW
        assert status.changes[0].destructiveness == ChangeDestructiveness.SAFE

    def test_update_view_when_no_hash_stored(self):
        planner = self._make_planner()
        entity = self._sql_entity(sql="SELECT 1")
        spark_mock = MagicMock()
        spark_mock.sql.return_value = MagicMock()  # DESCRIBE succeeds → exists
        # No hash stored (view predates hash tracking) → treat as UPDATE_VIEW
        planner._get_view_sql_hash = MagicMock(return_value=None)

        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            status = planner._plan_sql_entity(entity)

        assert len(status.changes) == 1
        assert status.changes[0].kind == ChangeKind.UPDATE_VIEW


# ---------------------------------------------------------------------------
# SQL entity applying (_apply_sql_entity)
# ---------------------------------------------------------------------------


class TestMigrationApplierSqlEntity:
    def _make_applier(self):
        provider = MagicMock()
        tp = MagicMock()
        tp.get_logger.return_value = MagicMock()
        applier = MigrationApplier.__new__(MigrationApplier)
        applier._provider = provider
        applier._logger = tp.get_logger("test")
        return applier

    def _sql_entity(self, sql="SELECT 1", name="test.v"):
        return EntityMetadata(
            entityid=name,
            name=name,
            merge_columns=[],
            tags={},
            schema=None,
            sql=sql,
        )

    def test_create_view_issues_create_or_replace(self):
        applier = self._make_applier()
        entity = self._sql_entity(sql="SELECT 1", name="test.v")
        status = EntityMigrationStatus(
            entity=entity,
            changes=[
                EntityMigrationChange(
                    entity=entity,
                    kind=ChangeKind.CREATE_VIEW,
                    destructiveness=ChangeDestructiveness.SAFE,
                    detail="",
                ),
            ],
        )
        spark_mock = MagicMock()
        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            applier._apply_sql_entity(status)
        # Qualified name → schema created first, then view
        calls = [c.args[0] for c in spark_mock.sql.call_args_list]
        assert "CREATE SCHEMA IF NOT EXISTS test" in calls
        assert "CREATE OR REPLACE VIEW test.v AS SELECT 1" in calls

    def test_update_view_issues_create_or_replace(self):
        applier = self._make_applier()
        entity = self._sql_entity(sql="SELECT 2", name="test.v")
        status = EntityMigrationStatus(
            entity=entity,
            changes=[
                EntityMigrationChange(
                    entity=entity,
                    kind=ChangeKind.UPDATE_VIEW,
                    destructiveness=ChangeDestructiveness.SAFE,
                    detail="",
                ),
            ],
        )
        spark_mock = MagicMock()
        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            applier._apply_sql_entity(status)
        calls = [c.args[0] for c in spark_mock.sql.call_args_list]
        assert "CREATE SCHEMA IF NOT EXISTS test" in calls
        assert "CREATE OR REPLACE VIEW test.v AS SELECT 2" in calls

    def test_unqualified_view_name_skips_schema_creation(self):
        applier = self._make_applier()
        entity = EntityMetadata(
            entityid="simple_view",
            name="simple_view",
            merge_columns=[],
            tags={},
            schema=None,
            sql="SELECT 1",
        )
        status = EntityMigrationStatus(
            entity=entity,
            changes=[
                EntityMigrationChange(
                    entity=entity,
                    kind=ChangeKind.CREATE_VIEW,
                    destructiveness=ChangeDestructiveness.SAFE,
                    detail="",
                ),
            ],
        )
        spark_mock = MagicMock()
        with patch("kindling.spark_session.get_or_create_spark_session", return_value=spark_mock):
            applier._apply_sql_entity(status)
        # No dot in name → schema creation skipped; calls are CREATE OR REPLACE VIEW + ALTER VIEW hash
        calls = [c.args[0] for c in spark_mock.sql.call_args_list]
        assert any("CREATE OR REPLACE VIEW simple_view AS SELECT 1" in c for c in calls)
        assert not any("CREATE SCHEMA" in c for c in calls)
