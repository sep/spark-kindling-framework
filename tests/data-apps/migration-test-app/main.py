#!/usr/bin/env python3
"""
Migration Test App

Tests the MigrationService end-to-end on a real Spark/Delta platform:

  Scenario A — Create from scratch
    1. Plan: detects CREATE_TABLE + CREATE_VIEW (neither exists)
    2. Apply: creates the Delta table and catalog view
    3. Plan again: both up to date
    4. Write data → read from view (proves view resolves correctly)

  Scenario B — Schema evolution (add columns)
    1. Create a Delta table with a partial schema (id, name only)
    2. Plan: detects ADD_COLUMNS (amount is in registered schema but not in table)
    3. Apply: adds the missing column
    4. Verify the column is present

Each test step emits a TEST_ID= marker so the system test harness can validate
individual outcomes without parsing free-form log output.
"""

import sys

from kindling.data_entities import DataEntities
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.migration import BackupStrategy, MigrationService
from kindling.platform_provider import PlatformServiceProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

logger_provider = get_kindling_service(SparkLoggerProvider)
logger = logger_provider.get_logger("migration-test-app")

config_service = get_kindling_service(ConfigService)
test_id = str(config_service.get("test_id") or "unknown").replace("-", "_")

platform_service = get_kindling_service(PlatformServiceProvider).get_service()
platform_name = platform_service.get_platform_name() if platform_service else "unknown"

msg = f"TEST_ID={test_id} status=STARTED platform={platform_name}"
logger.info(msg)
print(msg, flush=True)

spark = get_or_create_spark_session()

# ---------------------------------------------------------------------------
# Platform-specific storage paths / schema names
#
# table_root:   base path for Delta table files (STORAGE mode)
# test_schema:  Spark catalog database/schema to use for CATALOG mode entities
#               Using a unique schema per test run prevents collisions.
# ---------------------------------------------------------------------------

table_root = config_service.get("kindling.storage.table_root") or "Tables"

# On Databricks Unity Catalog, view names must be catalog-qualified
# (catalog.schema.view). On Fabric and Synapse a bare schema name is fine.
# We detect this by inspecting the table_root: Databricks UC uses a
# /Volumes/catalog/... path, so the catalog name is encoded there.
if str(table_root).startswith("/Volumes/"):
    # /Volumes/<catalog>/<schema>/<volume>/...
    parts = str(table_root).strip("/").split("/")
    catalog_prefix = parts[1] + "." if len(parts) > 1 else ""
else:
    catalog_prefix = ""

test_schema = f"{catalog_prefix}migration_test_{test_id}"

items_table_path = f"{table_root}/migration_test/{test_id}/items"
items_evolve_path = f"{table_root}/migration_test/{test_id}/items_evolve"
items_table_name = f"{test_schema}.items"
items_evolve_name = f"{test_schema}.items_evolve"

# On Databricks UC the test runner may not have CREATE SCHEMA permission in
# the main catalog. Instead, reuse the existing schema that holds the test
# volumes (/Volumes/<catalog>/<schema>/...) and suffix the view name with the
# test_id to avoid collisions across parallel runs.
if str(table_root).startswith("/Volumes/"):
    parts = str(table_root).strip("/").split("/")
    if len(parts) >= 3:
        uc_catalog, uc_schema = parts[1], parts[2]
        items_view_name = f"{uc_catalog}.{uc_schema}.recent_items_{test_id}"
    else:
        items_view_name = f"recent_items_{test_id}"
else:
    # Fabric / Synapse: create a per-test schema, create the view inside it.
    items_view_name = f"{test_schema}.recent_items"
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {test_schema}")
    except Exception as e:
        logger.warning(f"Could not pre-create test schema {test_schema}: {e}")

# ---------------------------------------------------------------------------
# Entity registration
#
# Entities use STORAGE mode (path-based) for cross-platform compatibility.
# The SQL entity view is created in a test-scoped schema.
# ---------------------------------------------------------------------------

ITEMS_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)

# items_evolve is NOT registered here — scenario B registers it dynamically
# after creating the table with a partial schema, so scenario A's apply does
# not pre-create it with the full schema and mask the ADD_COLUMNS drift.
ITEMS_EVOLVE_SCHEMA = StructType(
    [
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),  # absent initially; migration adds it
    ]
)

DataEntities.entity(
    entityid=f"migration_test_{test_id}.items",
    name="items",
    merge_columns=["id"],
    tags={
        "provider_type": "delta",
        "provider.path": items_table_path,
        "provider.access_mode": "storage",
    },
    schema=ITEMS_SCHEMA,
)

# SQL entity: a permanent view over the items table.
# The SQL uses a fully-qualified name so it resolves regardless of current database.
DataEntities.sql_entity(
    entityid=f"migration_test_{test_id}.recent_items",
    name=items_view_name,
    tags={"provider.table_name": items_view_name},
    sql=f"SELECT * FROM delta.`{items_table_path}` WHERE amount > 0",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

test_results = {}


def _pass(test_name: str) -> None:
    test_results[test_name] = True
    msg = f"TEST_ID={test_id} test={test_name} status=PASSED"
    logger.info(msg)
    print(msg, flush=True)


def _fail(test_name: str, reason: str) -> None:
    test_results[test_name] = False
    msg = f"TEST_ID={test_id} test={test_name} status=FAILED reason={reason}"
    logger.error(msg)
    print(msg, flush=True)


def _check(test_name: str, condition: bool, reason: str = "") -> None:
    if condition:
        _pass(test_name)
    else:
        _fail(test_name, reason or "condition was False")


# ---------------------------------------------------------------------------
# Scenario A — Create from scratch
# ---------------------------------------------------------------------------


def test_scenario_a():
    migration_svc = GlobalInjector.get(MigrationService)

    # Step 1: plan — should detect CREATE_TABLE (x2) + CREATE_VIEW
    try:
        plan = migration_svc.plan()
        entity_ids_with_changes = {s.entity.entityid for s in plan.statuses if s.changes}
        items_eid = f"migration_test_{test_id}.items"
        view_eid = f"migration_test_{test_id}.recent_items"

        _check(
            "scenario_a_plan_detects_create_table",
            items_eid in entity_ids_with_changes,
            f"items entity not in changes; all: {entity_ids_with_changes}",
        )
        _check(
            "scenario_a_plan_detects_create_view",
            view_eid in entity_ids_with_changes,
            f"recent_items entity not in changes; all: {entity_ids_with_changes}",
        )
        _check("scenario_a_plan_no_errors", not plan.errors, str(plan.errors))
    except Exception as e:
        _fail("scenario_a_plan_detects_create_table", str(e))
        _fail("scenario_a_plan_detects_create_view", str(e))
        return

    # Step 2: apply — creates the Delta table and catalog view
    try:
        migration_svc.apply(plan)
        _pass("scenario_a_apply_succeeds")
    except Exception as e:
        _fail("scenario_a_apply_succeeds", str(e))
        return

    # Step 3: plan again — should be up to date
    try:
        plan2 = migration_svc.plan()
        items_status = next(
            (s for s in plan2.statuses if s.entity.entityid == f"migration_test_{test_id}.items"),
            None,
        )
        view_status = next(
            (
                s
                for s in plan2.statuses
                if s.entity.entityid == f"migration_test_{test_id}.recent_items"
            ),
            None,
        )
        _check(
            "scenario_a_items_up_to_date",
            items_status is not None and items_status.is_up_to_date,
            f"items not up to date: {[c.kind.value for c in (items_status.changes if items_status else [])]}",
        )
        _check(
            "scenario_a_view_up_to_date",
            view_status is not None and view_status.is_up_to_date,
            f"view not up to date: {[c.kind.value for c in (view_status.changes if view_status else [])]}",
        )
    except Exception as e:
        _fail("scenario_a_items_up_to_date", str(e))
        _fail("scenario_a_view_up_to_date", str(e))
        return

    # Step 4: write data to items table, read through view
    try:
        data = [(1, "alpha", 10.0), (2, "beta", -5.0), (3, "gamma", 20.0)]
        df = spark.createDataFrame(data, ["id", "name", "amount"])
        df.write.format("delta").mode("append").save(items_table_path)

        view_df = spark.read.table(items_view_name)
        view_count = view_df.count()
        # Only rows with amount > 0 should appear (id=1 and id=3)
        _check(
            "scenario_a_view_filters_correctly",
            view_count == 2,
            f"expected 2 rows from view, got {view_count}",
        )
    except Exception as e:
        _fail("scenario_a_view_filters_correctly", str(e))


# ---------------------------------------------------------------------------
# Scenario B — Schema evolution (add columns)
# ---------------------------------------------------------------------------


def test_scenario_b():
    migration_svc = GlobalInjector.get(MigrationService)
    items_evolve_eid = f"migration_test_{test_id}.items_evolve"

    # Step 1: create the table with a PARTIAL schema (id, name only — no amount),
    # then register the entity. Registration is deferred to here so scenario A's
    # apply does not pre-create items_evolve with the full 3-column schema.
    try:
        partial_schema = StructType(
            [
                StructField("id", LongType(), False),
                StructField("name", StringType(), True),
            ]
        )
        empty_df = spark.createDataFrame([], schema=partial_schema)
        empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            items_evolve_path
        )

        # Register the entity AFTER the table exists so the planner sees drift.
        DataEntities.entity(
            entityid=items_evolve_eid,
            name="items_evolve",
            merge_columns=["id"],
            tags={
                "provider_type": "delta",
                "provider.path": items_evolve_path,
                "provider.access_mode": "storage",
            },
            schema=ITEMS_EVOLVE_SCHEMA,
        )
        _pass("scenario_b_partial_table_created")
    except Exception as e:
        _fail("scenario_b_partial_table_created", str(e))
        return

    # Step 2: plan — registered entity has 3 columns, table has 2 → should detect ADD_COLUMNS
    try:
        plan = migration_svc.plan()
        evolve_status = next(
            (s for s in plan.statuses if s.entity.entityid == items_evolve_eid), None
        )
        add_col_changes = [
            c
            for c in (evolve_status.changes if evolve_status else [])
            if c.kind.value == "add_columns"
        ]
        plan_error = evolve_status.error if evolve_status else "status not found in plan"
        _check(
            "scenario_b_plan_detects_add_columns",
            len(add_col_changes) == 1 and "amount" in add_col_changes[0].columns_to_add,
            f"no add_columns change detected; "
            f"changes: {[c.kind.value for c in (evolve_status.changes if evolve_status else [])]}; "
            f"plan_error: {plan_error}; "
            f"items_evolve_path: {items_evolve_path}",
        )
    except Exception as e:
        _fail("scenario_b_plan_detects_add_columns", str(e))
        return

    # Step 3: apply — should add the amount column
    try:
        migration_svc.apply(plan)
        _pass("scenario_b_apply_add_columns_succeeds")
    except Exception as e:
        _fail("scenario_b_apply_add_columns_succeeds", str(e))
        return

    # Step 4: verify the column exists
    try:
        live_schema = spark.read.format("delta").load(items_evolve_path).schema
        live_col_names = {f.name for f in live_schema.fields}
        _check(
            "scenario_b_amount_column_present",
            "amount" in live_col_names,
            f"amount not in schema; columns: {live_col_names}",
        )
    except Exception as e:
        _fail("scenario_b_amount_column_present", str(e))


# ---------------------------------------------------------------------------
# Run tests
# ---------------------------------------------------------------------------

try:
    test_scenario_a()
except Exception as e:
    _fail("scenario_a_overall", str(e))

try:
    test_scenario_b()
except Exception as e:
    _fail("scenario_b_overall", str(e))

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

try:
    spark.sql(f"DROP VIEW IF EXISTS {items_view_name}")
except Exception:
    pass
try:
    # Only drop the per-test schema where we created one (non-Databricks UC path).
    if not str(table_root).startswith("/Volumes/"):
        spark.sql(f"DROP DATABASE IF EXISTS {test_schema} CASCADE")
except Exception:
    pass

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

overall = all(test_results.values()) if test_results else False
overall_status = "PASSED" if overall else "FAILED"
failed = [k for k, v in test_results.items() if not v]

msg = f"TEST_ID={test_id} status=COMPLETED result={overall_status}"
if failed:
    msg += f" failed_tests={','.join(failed)}"
logger.info(msg)
print(msg, flush=True)

sys.exit(0 if overall else 1)
