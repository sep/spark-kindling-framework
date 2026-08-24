"""
Platform system test for the general Lakeflow SDP execution path.

Complements the SCD (AUTO CDC) and temporal (stratified lowering) Lakeflow
platform tests with coverage of the general path: application selection,
configuration overlays, dependency inference, materialized views, dataset
metadata, and Lakeflow expectations.

Deploys lakeflow-engine-test-app (bronze.orders -> silver.orders, both
materialized views, SDP inferring the dependency from input_entity_ids)
into a real serverless pipeline, configured with:

  - kindling.data_app / kindling.lakeflow.allowed_apps (app selection)
  - datapipes.lakeflow.silver_orders.engine.sdp.table_properties.* (the
    portable common-engine config block)
  - datapipes.lakeflow.silver_orders.engine.databricks_sdp.expectations.* /
    .expectations_drop.* (the Databricks-specific block, layered over sdp)

and validates:

  - bronze.orders / silver.orders both materialize as Lakeflow-managed
    tables, with SDP inferring the bronze -> silver dependency edge (the
    update succeeds with no explicit wiring beyond input_entity_ids).
  - the configured table property lands on the silver.orders table
    (SHOW TBLPROPERTIES).
  - a row violating the warning expectation (order_id IS NULL) is present
    in the output (expect_all: counted, not dropped).
  - a row violating the drop expectation (quantity > 0) is absent from the
    output (expect_all_or_drop: removed).
  - valid rows carry the pipe's own transformation (total_amount =
    quantity * amount).

Limitation: bronze_orders/silver_orders are pipeline-produced datasets with
fixed names in the target schema (Lakeflow's single-part dataset naming has
no per-run isolation the way Kindling's table_name_prefix bridges external
entities) -- same limitation test_temporal_lakeflow.py documents for its
own pipeline-produced tables. This test must not run concurrently with
itself; cleanup drops both tables in `finally` so sequential runs don't
collide.

Prerequisites: spark_kindling, spark_kindling_ext_sdp,
spark_kindling_ext_databricks and lakeflow_engine_test_app wheels uploaded
to the UC artifacts volume packages/ path at the versions in this checkout
(serverless caches environments by requirement set -- bump versions on
change).

Usage:
    poe test-extension --extension databricks --platform databricks
"""

import base64
import os
import uuid

import pytest

from tests.system.extensions.databricks.lakeflow_test_helpers import (
    PACKAGES_VOLUME,
    WORKSPACE_ROOT,
    execute_statement,
    pipeline_notebook,
    print_error_events,
    select_warehouse_id,
    wait_for_update,
    wheel_version,
)

EXPECTED_VALID_ROWS = {
    ("o1", "c1", 5, 100.0, 500.0),
    ("o2", "c2", 3, 60.0, 180.0),
    (None, "c3", 2, 40.0, 80.0),  # warning-expectation violation: kept
}
DROPPED_ORDER_ID = "o4"  # drop-expectation violation (quantity <= 0): removed


def _pipeline_notebook(pkg_root: str) -> str:
    repo = WORKSPACE_ROOT.parent
    return pipeline_notebook(
        [
            f"{pkg_root}/spark_kindling-{wheel_version(repo / 'pyproject.toml')}-py3-none-any.whl",
            f"{pkg_root}/spark_kindling_ext_sdp-"
            f"{wheel_version(repo / 'packages/extensions/kindling_ext_sdp/pyproject.toml')}-py3-none-any.whl",
            f"{pkg_root}/spark_kindling_ext_databricks-"
            f"{wheel_version(repo / 'packages/extensions/kindling_ext_databricks/pyproject.toml')}-py3-none-any.whl",
            f"{pkg_root}/lakeflow_engine_test_app-"
            f"{wheel_version(WORKSPACE_ROOT / 'data-apps/lakeflow-engine-test-app/pyproject.toml')}-py3-none-any.whl",
        ]
    )


@pytest.mark.system
@pytest.mark.slow
class TestLakeflowEnginePlatform:
    """General Lakeflow SDP execution path on a real pipeline."""

    def test_pipes_declared_and_executed_with_overlay_config(self, platform_client):
        client, platform = platform_client
        if platform != "databricks":
            pytest.skip("Lakeflow SDP coverage is Databricks-only.")

        w = client.client
        catalog = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "medallion")
        schema = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "default")
        pkg_root = PACKAGES_VOLUME.format(catalog=catalog, schema=schema)

        warehouse_id = select_warehouse_id(w, os.getenv("SYSTEM_TEST_SQL_WAREHOUSE_ID"))
        if not warehouse_id:
            pytest.skip("No SQL warehouse available to verify pipeline outputs.")

        test_id = str(uuid.uuid4())[:8]
        pipeline_name = f"systest-lakeflow-engine-{test_id}"
        notebook_path = f"/Shared/systest-lakeflow/{pipeline_name}"
        bronze_table = f"{catalog}.{schema}.bronze_orders"
        silver_table = f"{catalog}.{schema}.silver_orders"

        from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary
        from databricks.sdk.service.workspace import ImportFormat, Language

        w.workspace.mkdirs("/Shared/systest-lakeflow")
        w.workspace.import_(
            path=notebook_path,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=base64.b64encode(_pipeline_notebook(pkg_root).encode()).decode(),
            overwrite=True,
        )

        config_keys = ",".join(
            [
                "datapipes.lakeflow.silver_orders.engine.sdp.table_properties.test_layer",
                "datapipes.lakeflow.silver_orders.engine.databricks_sdp.expectations.valid_order_id",
                "datapipes.lakeflow.silver_orders.engine.databricks_sdp."
                "expectations_drop.positive_quantity",
            ]
        )
        configuration = {
            "kindling.data_app": "lakeflow_engine",
            "kindling.lakeflow.allowed_apps": "lakeflow_engine",
            "kindling.lakeflow.config_keys": config_keys,
            "datapipes.lakeflow.silver_orders.engine.sdp.table_properties.test_layer": "silver",
            "datapipes.lakeflow.silver_orders.engine.databricks_sdp.expectations.valid_order_id": (
                "order_id IS NOT NULL"
            ),
            "datapipes.lakeflow.silver_orders.engine.databricks_sdp."
            "expectations_drop.positive_quantity": "quantity > 0",
        }

        created = w.pipelines.create(
            name=pipeline_name,
            catalog=catalog,
            target=schema,
            development=True,
            continuous=False,
            channel="CURRENT",
            serverless=True,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=notebook_path))],
            configuration=configuration,
        )
        pipeline_id = created.pipeline_id
        print(f"🚀 Pipeline created: {pipeline_id} ({pipeline_name})")

        try:
            update = w.pipelines.start_update(pipeline_id)
            state = wait_for_update(w, pipeline_id, update.update_id)
            if state != "COMPLETED":
                print_error_events(w, pipeline_id)
            assert state == "COMPLETED", f"update ended {state}"
            print("✅ pipeline update completed: bronze_orders -> silver_orders declared")

            # --- dependency inference: both datasets materialized --------
            bronze_count = execute_statement(
                w, warehouse_id, f"SELECT COUNT(*) FROM {bronze_table}"
            )
            assert bronze_count and int(bronze_count[0][0]) == 4, bronze_count
            print("✅ bronze.orders materialized (4 source rows)")

            # --- overlay: portable sdp table_properties block ------------
            properties = execute_statement(w, warehouse_id, f"SHOW TBLPROPERTIES {silver_table}")
            props = {row[0]: row[1] for row in properties}
            assert props.get("test_layer") == "silver", properties
            print("✅ sdp.table_properties overlay applied (test_layer=silver)")

            # --- overlay: databricks_sdp expectations --------------------
            rows = execute_statement(
                w,
                warehouse_id,
                f"SELECT order_id, customer_id, quantity, amount, total_amount "
                f"FROM {silver_table} ORDER BY customer_id",
            )
            # Statement Execution API's default JSON result format returns
            # every column as a string (or None for SQL NULL) regardless of
            # its SQL type -- normalize numeric columns explicitly rather
            # than comparing against raw API values.
            result_rows = {
                (row[0], row[1], int(row[2]), float(row[3]), float(row[4])) for row in rows
            }
            order_ids = {row[0] for row in rows}

            assert result_rows == EXPECTED_VALID_ROWS, result_rows
            print("✅ warning-expectation violation kept, valid rows transformed correctly")

            assert DROPPED_ORDER_ID not in order_ids, order_ids
            print("✅ drop-expectation violation removed from silver.orders")
        finally:
            if not os.getenv("SKIP_TEST_CLEANUP"):
                try:
                    w.pipelines.delete(pipeline_id)
                    print(f"🗑️  Deleted pipeline {pipeline_id}")
                except Exception as exc:  # noqa: BLE001
                    print(f"⚠️  Pipeline cleanup warning: {exc}")
                try:
                    w.workspace.delete(notebook_path)
                except Exception as exc:  # noqa: BLE001
                    print(f"⚠️  Notebook cleanup warning: {exc}")
                for table in (silver_table, bronze_table):
                    try:
                        execute_statement(w, warehouse_id, f"DROP TABLE IF EXISTS {table}")
                        print(f"🗑️  Dropped {table}")
                    except Exception as exc:  # noqa: BLE001
                        print(f"⚠️  Table cleanup warning: {exc}")
