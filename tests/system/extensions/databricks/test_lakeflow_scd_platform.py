"""
Platform system test for the Databricks SDP engine (Lakeflow).

Deploys the lakeflow-scd-test-app through the Lakeflow app selector into a
real serverless pipeline and validates the SCD declared-flow -> AUTO CDC
mapping against the live API:

  Update 1 (snapshot v1): the pipeline declares a __scd_source view,
  create_streaming_table target and create_auto_cdc_from_snapshot_flow;
  the target materializes as an SCD2 table with __START_AT/__END_AT.

  Update 2 (snapshot v2, via pipeline configuration change): SCD2 version
  chaining — a changed key closes its old version and opens a new one,
  an unchanged key keeps a single open row, a new key appends.

Prerequisites (deploy once per code change, with version bumps —
serverless environments cache installed wheels by requirement set):

  - spark_kindling, spark_kindling_ext_sdp, spark_kindling_ext_databricks
    and lakeflow_scd_test_app wheels uploaded to the UC artifacts volume
    packages/ path (scripts/deploy_extensions.py convention).

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
    pipeline_notebook,
    print_error_events,
    select_warehouse_id,
    wait_for_update,
    wheel_version,
)

EXPECTED_V1 = {("c1", "bronze", "open"), ("c2", "silver", "open")}
EXPECTED_V2 = {
    ("c1", "bronze", "closed"),
    ("c1", "gold", "open"),
    ("c2", "silver", "open"),
    ("c3", "bronze", "open"),
}


def _pipeline_notebook(pkg_root: str) -> str:
    kindling_version = wheel_version(WORKSPACE_ROOT.parent / "pyproject.toml")
    sdp_version = wheel_version(
        WORKSPACE_ROOT.parent / "packages" / "extensions" / "kindling_ext_sdp" / "pyproject.toml"
    )
    databricks_version = wheel_version(
        WORKSPACE_ROOT.parent
        / "packages"
        / "extensions"
        / "kindling_ext_databricks"
        / "pyproject.toml"
    )
    app_version = wheel_version(
        WORKSPACE_ROOT / "data-apps" / "lakeflow-scd-test-app" / "pyproject.toml"
    )
    return pipeline_notebook(
        [
            f"{pkg_root}/spark_kindling-{kindling_version}-py3-none-any.whl",
            f"{pkg_root}/spark_kindling_ext_sdp-{sdp_version}-py3-none-any.whl",
            f"{pkg_root}/spark_kindling_ext_databricks-{databricks_version}-py3-none-any.whl",
            f"{pkg_root}/lakeflow_scd_test_app-{app_version}-py3-none-any.whl",
        ]
    )


def _query_scd_rows(w, warehouse_id: str, table: str):
    result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=(
            "SELECT customer_id, tier, "
            "CASE WHEN __END_AT IS NULL THEN 'open' ELSE 'closed' END AS row_state "
            f"FROM {table} ORDER BY customer_id, __START_AT"
        ),
        wait_timeout="50s",
    )
    assert (
        result.status and result.status.state.value == "SUCCEEDED"
    ), f"query failed: {result.status.error.message if result.status and result.status.error else result.status}"
    return {tuple(row) for row in (result.result.data_array or [])}


@pytest.mark.system
@pytest.mark.slow
class TestLakeflowSdpPlatform:
    """SCD declared flow -> AUTO CDC on a real Lakeflow pipeline."""

    def test_scd_declared_flow_maps_to_auto_cdc(self, platform_client):
        client, platform = platform_client
        if platform != "databricks":
            pytest.skip("Lakeflow SDP coverage is Databricks-only.")

        w = client.client  # underlying databricks.sdk WorkspaceClient
        catalog = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "medallion")
        schema = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "default")
        pkg_root = PACKAGES_VOLUME.format(catalog=catalog, schema=schema)

        warehouse_id = select_warehouse_id(w, os.getenv("SYSTEM_TEST_SQL_WAREHOUSE_ID"))
        if not warehouse_id:
            pytest.skip("No SQL warehouse available to verify pipeline outputs.")

        test_id = str(uuid.uuid4())[:8]
        pipeline_name = f"systest-lakeflow-scd-{test_id}"
        notebook_path = f"/Shared/systest-lakeflow/{pipeline_name}"
        # The app emits dataset silver.customers -> table silver_customers
        # in the pipeline target schema (single-part dataset naming).
        table = f"{catalog}.{schema}.silver_customers"

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

        def _configuration(snapshot: str) -> dict:
            return {
                "kindling.data_app": "lakeflow_scd",
                "kindling.lakeflow.allowed_apps": "lakeflow_scd",
                "lakeflow_scd.snapshot": snapshot,
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
            configuration=_configuration("v1"),
        )
        pipeline_id = created.pipeline_id
        print(f"🚀 Pipeline created: {pipeline_id} ({pipeline_name})")

        try:
            update = w.pipelines.start_update(pipeline_id)
            state = wait_for_update(w, pipeline_id, update.update_id)
            if state != "COMPLETED":
                print_error_events(w, pipeline_id)
            assert state == "COMPLETED", f"v1 update ended {state}"

            rows = _query_scd_rows(w, warehouse_id, table)
            assert rows == EXPECTED_V1, f"v1 rows: {rows}"
            print("✅ v1 snapshot materialized as SCD2 (2 open rows)")

            spec = w.pipelines.get(pipeline_id).spec
            w.pipelines.update(
                pipeline_id=pipeline_id,
                name=spec.name,
                catalog=spec.catalog,
                target=spec.target,
                development=spec.development,
                continuous=spec.continuous,
                channel=spec.channel,
                serverless=spec.serverless,
                libraries=spec.libraries,
                configuration=_configuration("v2"),
            )
            update = w.pipelines.start_update(pipeline_id)
            state = wait_for_update(w, pipeline_id, update.update_id)
            if state != "COMPLETED":
                print_error_events(w, pipeline_id)
            assert state == "COMPLETED", f"v2 update ended {state}"

            rows = _query_scd_rows(w, warehouse_id, table)
            assert rows == EXPECTED_V2, f"v2 rows: {rows}"
            print("✅ v2 snapshot chained SCD2 versions (close + new + append)")
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
                try:
                    w.statement_execution.execute_statement(
                        warehouse_id=warehouse_id,
                        statement=f"DROP TABLE IF EXISTS {table}",
                        wait_timeout="50s",
                    )
                    print(f"🗑️  Dropped {table}")
                except Exception as exc:  # noqa: BLE001
                    print(f"⚠️  Table cleanup warning: {exc}")
