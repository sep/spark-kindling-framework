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
import re
import sys
import time
import uuid
from pathlib import Path

import pytest

WORKSPACE_ROOT = Path(__file__).parent.parent.parent.parent
PACKAGES_VOLUME = "/Volumes/{catalog}/{schema}/artifacts/packages"

EXPECTED_V1 = {("c1", "bronze", "open"), ("c2", "silver", "open")}
EXPECTED_V2 = {
    ("c1", "bronze", "closed"),
    ("c1", "gold", "open"),
    ("c2", "silver", "open"),
    ("c3", "bronze", "open"),
}


def _wheel_version(pyproject: Path) -> str:
    match = re.search(r'^version = "([^"]+)"', pyproject.read_text(), re.MULTILINE)
    assert match, f"no version in {pyproject}"
    return match.group(1)


def _pipeline_notebook(pkg_root: str) -> str:
    kindling_version = _wheel_version(WORKSPACE_ROOT.parent / "pyproject.toml")
    sdp_version = _wheel_version(
        WORKSPACE_ROOT.parent / "packages" / "extensions" / "kindling_ext_sdp" / "pyproject.toml"
    )
    databricks_version = _wheel_version(
        WORKSPACE_ROOT.parent
        / "packages"
        / "extensions"
        / "kindling_ext_databricks"
        / "pyproject.toml"
    )
    app_version = _wheel_version(
        WORKSPACE_ROOT / "data-apps" / "lakeflow-scd-test-app" / "pyproject.toml"
    )
    wheels = " ".join(
        [
            f"{pkg_root}/spark_kindling-{kindling_version}-py3-none-any.whl",
            f"{pkg_root}/spark_kindling_ext_sdp-{sdp_version}-py3-none-any.whl",
            f"{pkg_root}/spark_kindling_ext_databricks-{databricks_version}-py3-none-any.whl",
            f"{pkg_root}/lakeflow_scd_test_app-{app_version}-py3-none-any.whl",
        ]
    )
    return f"""# Databricks notebook source
# MAGIC %pip install {wheels}

# COMMAND ----------

from kindling_ext_databricks.lakeflow_app_selector import declare_from_pipeline_config

declare_from_pipeline_config()
"""


def _wait_for_update(w, pipeline_id: str, update_id: str, max_wait: float = 1800.0) -> str:
    deadline = time.time() + max_wait
    state = None
    while time.time() < deadline:
        info = w.pipelines.get_update(pipeline_id, update_id)
        state = info.update.state.value if info.update and info.update.state else None
        if state in {"COMPLETED", "FAILED", "CANCELED"}:
            return state
        time.sleep(15)
    return state or "TIMEOUT"


def _print_error_events(w, pipeline_id: str) -> None:
    for event in list(w.pipelines.list_pipeline_events(pipeline_id, max_results=50)):
        if event.level and event.level.value == "ERROR":
            print(f"[ERROR] {event.event_type}: {(event.message or '').strip()[:2000]}")
            error = getattr(event, "error", None)
            if error and getattr(error, "exceptions", None):
                for exc in error.exceptions:
                    print("  EXC:", (exc.message or "").strip()[:2000])
    sys.stdout.flush()


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

        # Prefer RUNNING warehouses; a STOPPED serverless warehouse
        # auto-starts on statement execution but adds startup latency.
        warehouses = sorted(
            (
                wh
                for wh in w.warehouses.list()
                if wh.state and wh.state.value in ("RUNNING", "STOPPED")
            ),
            key=lambda wh: wh.state.value != "RUNNING",
        )
        if not warehouses:
            pytest.skip("No SQL warehouse available to verify pipeline outputs.")
        warehouse_id = os.getenv("SYSTEM_TEST_SQL_WAREHOUSE_ID") or warehouses[0].id

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
            state = _wait_for_update(w, pipeline_id, update.update_id)
            if state != "COMPLETED":
                _print_error_events(w, pipeline_id)
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
            state = _wait_for_update(w, pipeline_id, update.update_id)
            if state != "COMPLETED":
                _print_error_events(w, pipeline_id)
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
