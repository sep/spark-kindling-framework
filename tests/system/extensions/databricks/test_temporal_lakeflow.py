"""
Platform system test: the temporal chain inside a real Lakeflow pipeline.

Deploys lakeflow-temporal-test-app through the app selector into a
serverless pipeline (the stratified lowering: generation-stratum streaming
tables, episodes as AUTO CDC SCD2, determinations projected from all
episode versions, a union MV as the canonical events surface) and drives
the full lifecycle across two updates:

  Update 1 — machine-hot closes with a real end; machine-late is
  start-only and EXPIRES at update evaluation time; determination events
  and the generation-3 higher-order boundary (condition.thermal_excursion)
  materialize.

  Update 2 — the late real end lands in the external telemetry table:
  machine-late revises as SCD2 version chaining (expired version closed
  out, closed version current, same episode_id) and determination HISTORY
  is preserved — the .expired and .closed events coexist.

External inputs are seeded the way runner jobs would produce them
(telemetry appends; the SCD2 conditions table as ingest_conditions shapes
it), via SQL Statement Execution with typed parameters — rule expressions
must never be inlined into SQL literals (quote mangling).

Prerequisites: spark_kindling, spark_kindling_ext_sdp,
spark_kindling_ext_databricks, kindling_ext_temporal and
lakeflow_temporal_test_app wheels uploaded to the UC artifacts volume
packages/ path at the versions in this checkout (serverless caches
environments by requirement set — bump versions on change).

Limitation: the pipeline-produced tables (silver_events*, silver_episodes)
have fixed names in the target schema, so this test must not run
concurrently with itself.

Usage:
    poe test-extension --extension databricks --platform databricks
"""

import base64
import os
import re
import time
import uuid
from pathlib import Path

import pytest

WORKSPACE_ROOT = Path(__file__).parent.parent.parent.parent
PACKAGES_VOLUME = "/Volumes/{catalog}/{schema}/artifacts/packages"


def _wheel_version(pyproject: Path) -> str:
    match = re.search(r'^version = "([^"]+)"', pyproject.read_text(), re.MULTILINE)
    assert match, f"no version in {pyproject}"
    return match.group(1)


def _pipeline_notebook(pkg_root: str) -> str:
    repo = WORKSPACE_ROOT.parent
    wheels = " ".join(
        [
            f"{pkg_root}/spark_kindling-{_wheel_version(repo / 'pyproject.toml')}-py3-none-any.whl",
            f"{pkg_root}/spark_kindling_ext_sdp-"
            f"{_wheel_version(repo / 'packages/extensions/kindling_ext_sdp/pyproject.toml')}-py3-none-any.whl",
            f"{pkg_root}/spark_kindling_ext_databricks-"
            f"{_wheel_version(repo / 'packages/extensions/kindling_ext_databricks/pyproject.toml')}-py3-none-any.whl",
            f"{pkg_root}/kindling_ext_temporal-"
            f"{_wheel_version(repo / 'packages/extensions/kindling_ext_temporal/pyproject.toml')}-py3-none-any.whl",
            f"{pkg_root}/lakeflow_temporal_test_app-"
            f"{_wheel_version(WORKSPACE_ROOT / 'data-apps/lakeflow-temporal-test-app/pyproject.toml')}-py3-none-any.whl",
        ]
    )
    return f"""# Databricks notebook source
# MAGIC %pip install {wheels}

# COMMAND ----------

from kindling_ext_databricks.lakeflow_app_selector import declare_from_pipeline_config

declare_from_pipeline_config()
"""


def _wait_for_update(w, pipeline_id, update_id, max_wait=1800.0):
    deadline = time.time() + max_wait
    state = None
    while time.time() < deadline:
        info = w.pipelines.get_update(pipeline_id, update_id)
        state = info.update.state.value if info.update and info.update.state else None
        if state in {"COMPLETED", "FAILED", "CANCELED"}:
            return state
        time.sleep(15)
    return state or "TIMEOUT"


def _print_error_events(w, pipeline_id):
    for event in list(w.pipelines.list_pipeline_events(pipeline_id, max_results=50)):
        if event.level and event.level.value == "ERROR":
            print(f"[ERROR] {event.event_type}: {(event.message or '').strip()[:2000]}")
            error = getattr(event, "error", None)
            if error and getattr(error, "exceptions", None):
                for exc in error.exceptions:
                    print("  EXC:", (exc.message or "").strip()[:2000])


@pytest.mark.system
@pytest.mark.slow
class TestTemporalLakeflowPlatform:
    """Full temporal lifecycle through the stratified Lakeflow lowering."""

    def test_temporal_chain_runs_in_a_lakeflow_pipeline(self, platform_client):
        client, platform = platform_client
        if platform != "databricks":
            pytest.skip("Lakeflow coverage is Databricks-only.")

        w = client.client
        catalog = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "medallion")
        schema = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "default")
        pkg_root = PACKAGES_VOLUME.format(catalog=catalog, schema=schema)

        warehouses = sorted(
            (
                wh
                for wh in w.warehouses.list()
                if wh.state and wh.state.value in ("RUNNING", "STOPPED")
            ),
            key=lambda wh: wh.state.value != "RUNNING",
        )
        if not warehouses:
            pytest.skip("No SQL warehouse available to seed/verify pipeline data.")
        warehouse_id = os.getenv("SYSTEM_TEST_SQL_WAREHOUSE_ID") or warehouses[0].id

        def sql(statement, parameters=None):
            from databricks.sdk.service.sql import StatementParameterListItem

            params = None
            if parameters:
                params = [
                    StatementParameterListItem(name=key, value=value, type="STRING")
                    for key, value in parameters.items()
                ]
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=statement,
                wait_timeout="50s",
                parameters=params,
            )
            state = result.status.state.value if result.status else None
            assert state == "SUCCEEDED", (
                f"SQL failed: "
                f"{result.status.error.message if result.status and result.status.error else state}"
            )
            return result.result.data_array if result.result else []

        test_id = str(uuid.uuid4())[:8]
        prefix = f"systest_lftemporal_{test_id}_"
        telemetry = f"{catalog}.{schema}.{prefix}bronze_telemetry"
        conditions = f"{catalog}.{schema}.{prefix}silver_conditions"
        events = f"{catalog}.{schema}.silver_events"
        episodes = f"{catalog}.{schema}.silver_episodes"
        pipeline_name = f"systest-lakeflow-temporal-{test_id}"
        notebook_path = f"/Shared/systest-lakeflow/{pipeline_name}"

        # --- seed external inputs (what runner jobs would produce) --------
        sql(
            f"CREATE TABLE {telemetry} (machine_id STRING, reading_ts TIMESTAMP, temperature DOUBLE)"
        )
        sql(
            f"INSERT INTO {telemetry} VALUES "
            "('machine-hot',  TIMESTAMP'2026-07-14 12:00:00', 95.0),"
            "('machine-hot',  TIMESTAMP'2026-07-14 12:10:00', 80.0),"
            "('machine-late', TIMESTAMP'2026-07-14 12:00:00', 95.0)"
        )
        sql(
            f"CREATE TABLE {conditions} ("
            "condition_id STRING, consumes_event_type ARRAY<STRING>, subject_type STRING,"
            "parameters MAP<STRING,STRING>, enabled BOOLEAN, valid_from TIMESTAMP,"
            "valid_to TIMESTAMP, generation INT, __effective_from TIMESTAMP,"
            "__effective_to TIMESTAMP, __is_current BOOLEAN)"
        )
        sql(
            f"INSERT INTO {conditions} VALUES "
            "('condition.temperature_high', array('telemetry.observed'), 'machine',"
            " map('enter_when', :hot_enter, 'exit_when', :hot_exit),"
            " true, TIMESTAMP'2026-07-14 11:00:00', NULL, 1, current_timestamp(), NULL, true),"
            "('condition.thermal_excursion', array('episode.temperature_high_active.closed'),"
            " 'machine', map('enter_when', :exc_enter, 'exit_when', 'false'),"
            " true, TIMESTAMP'2026-07-14 11:00:00', NULL, 1, current_timestamp(), NULL, true)",
            parameters={
                "hot_enter": "cast(payload['temperature'] as double) > 90",
                "hot_exit": "cast(payload['temperature'] as double) <= 90",
                "exc_enter": "payload['status'] = 'closed'",
            },
        )

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
        created = w.pipelines.create(
            name=pipeline_name,
            catalog=catalog,
            target=schema,
            development=True,
            continuous=False,
            channel="CURRENT",
            serverless=True,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=notebook_path))],
            configuration={
                "kindling.data_app": "lakeflow_temporal",
                "kindling.lakeflow.allowed_apps": "lakeflow_temporal",
                "kindling.lakeflow.pipes": (
                    "temporal.chain.events.default,temporal.chain.episodes.default"
                ),
                "kindling.lakeflow.config_keys": (
                    "kindling.storage.table_catalog,kindling.storage.table_schema,"
                    "kindling.storage.table_name_prefix,kindling.temporal.max_generations"
                ),
                "kindling.storage.table_catalog": catalog,
                "kindling.storage.table_schema": schema,
                "kindling.storage.table_name_prefix": prefix,
                "kindling.temporal.max_generations": "2",
            },
        )
        pipeline_id = created.pipeline_id
        print(f"🚀 Pipeline created: {pipeline_id} ({pipeline_name})")

        try:
            # ---------------- update 1 ------------------------------------
            update = w.pipelines.start_update(pipeline_id)
            state = _wait_for_update(w, pipeline_id, update.update_id)
            if state != "COMPLETED":
                _print_error_events(w, pipeline_id)
            assert state == "COMPLETED", f"update 1 ended {state}"

            current = {
                row[0]: (row[1], row[2], row[3])
                for row in sql(
                    f"SELECT subject_id, status, close_reason, end_event_synthetic "
                    f"FROM {episodes} WHERE __END_AT IS NULL"
                )
            }
            assert current["machine-hot"] == ("closed", "end_event", "false"), current
            assert current["machine-late"] == ("expired", "expiration", "true"), current

            determinations = {
                (row[0], row[1])
                for row in sql(
                    f"SELECT subject_id, event_type FROM {events} WHERE event_class='episode'"
                )
            }
            assert ("machine-hot", "episode.temperature_high_active.closed") in determinations
            assert ("machine-late", "episode.temperature_high_active.expired") in determinations

            higher = {
                row[0]
                for row in sql(
                    f"SELECT DISTINCT subject_id FROM {events} "
                    "WHERE event_type = 'condition.thermal_excursion.entered' AND generation = 3"
                )
            }
            assert "machine-hot" in higher, higher
            print("✅ update 1: closure, expiration, determinations, higher-order boundary")

            # ---------------- update 2: the late end ----------------------
            sql(
                f"INSERT INTO {telemetry} VALUES "
                "('machine-late', TIMESTAMP'2026-07-14 12:10:00', 80.0)"
            )
            update = w.pipelines.start_update(pipeline_id)
            state = _wait_for_update(w, pipeline_id, update.update_id)
            if state != "COMPLETED":
                _print_error_events(w, pipeline_id)
            assert state == "COMPLETED", f"update 2 ended {state}"

            versions = sql(
                f"SELECT status, close_reason, end_event_synthetic, (__END_AT IS NULL), episode_id "
                f"FROM {episodes} WHERE subject_id='machine-late' ORDER BY __START_AT"
            )
            assert len(versions) == 2, versions
            assert versions[0][:4] == ["expired", "expiration", "true", "false"], versions
            assert versions[1][:4] == ["closed", "end_event", "false", "true"], versions
            assert versions[0][4] == versions[1][4], "revision must keep episode identity"

            history = {
                row[0]
                for row in sql(
                    f"SELECT event_type FROM {events} WHERE event_class='episode' "
                    "AND subject_id='machine-late'"
                )
            }
            assert history == {
                "episode.temperature_high_active.closed",
                "episode.temperature_high_active.expired",
            }, history

            higher = {
                row[0]
                for row in sql(
                    f"SELECT DISTINCT subject_id FROM {events} "
                    "WHERE event_type = 'condition.thermal_excursion.entered'"
                )
            }
            assert higher == {"machine-hot", "machine-late"}, higher
            print("✅ update 2: SCD2 revision with identity + determination history preserved")
        finally:
            if not os.getenv("SKIP_TEST_CLEANUP"):
                for cleanup in (
                    lambda: w.pipelines.delete(pipeline_id),
                    lambda: w.workspace.delete(notebook_path),
                    lambda: sql(f"DROP TABLE IF EXISTS {telemetry}"),
                    lambda: sql(f"DROP TABLE IF EXISTS {conditions}"),
                ):
                    try:
                        cleanup()
                    except Exception as exc:  # noqa: BLE001
                        print(f"⚠️  Cleanup warning: {exc}")
                # Pipeline-produced tables outlive pipeline deletion.
                for table in (
                    episodes,
                    events,
                    f"{events}__determinations",
                    f"{events}__g0",
                    f"{events}__g1",
                    f"{events}__g2",
                    f"{events}__ghi",
                ):
                    try:
                        sql(f"DROP TABLE IF EXISTS {table}")
                    except Exception as exc:  # noqa: BLE001
                        print(f"⚠️  Table cleanup warning: {exc}")
