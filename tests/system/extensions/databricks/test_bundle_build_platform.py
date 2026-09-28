"""
Platform system test for ``kindling bundle build`` end to end.

Generates a Databricks bundle from a small Kindling project (shared
``config/`` overlays plus a ``data-apps/lakeflow_engine`` overlay) with the
inline configuration transport, then hands it to the Databricks CLI:
``databricks bundle deploy`` creates a serverless Lakeflow pipeline whose
environment installs the framework, extension and app wheels, ``bundle run``
executes it, and the outputs are verified through SQL exactly like
``test_lakeflow_engine_platform.py`` does for the same app.

What this proves beyond that test:

  - the generated ``databricks.yml`` / ``resources/*.pipeline.yml`` are
    accepted by the pinned Databricks CLI and produce a working pipeline;
  - ``kindling.lakeflow.settings_json`` (the inline transport) reaches the
    declaration engine on a restricted serverless runtime: the
    ``datapipes.lakeflow.silver_orders.engine.*`` overlay block written as
    structured YAML applies table properties and expectations, with no
    workspace-file or volume config reads at declaration time;
  - the declaration-only platform fallback (no workspace id detectable in a
    serverless pipeline) succeeds.

Prerequisites: ``databricks`` CLI and ``poetry`` on PATH (the devcontainer
ships both). The test builds the spark_kindling, spark_kindling_ext_sdp,
spark_kindling_ext_databricks and lakeflow_engine_test_app wheels from this
checkout and stages them with ``--wheel``; ``bundle deploy`` uploads them
under the bundle's own workspace root, so nothing is written to the shared
UC artifacts volume and the pipeline always runs this checkout's code.
Authentication follows the CLI's unified auth: ``DATABRICKS_HOST`` plus
``az login`` locally, or ``ARM_*`` service principal variables in CI
(``AZURE_CLIENT_ID``/``AZURE_CLIENT_SECRET``/``AZURE_TENANT_ID`` are mapped
onto them when ``ARM_*`` are unset).

Limitation: bronze_orders/silver_orders are pipeline-produced datasets with
fixed names in the target schema, shared with test_lakeflow_engine_platform;
neither test may run concurrently with the other. Cleanup destroys the
bundle (pipeline, files, uploaded artifacts) and drops both tables.

Usage:
    poe test-extension --extension databricks --platform databricks
"""

import json
import os
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest
import yaml

from tests.system.extensions.databricks.lakeflow_test_helpers import (
    WORKSPACE_ROOT,
    execute_statement,
    print_error_events,
    select_warehouse_id,
)

EXPECTED_VALID_ROWS = {
    ("o1", "c1", 5, 100.0, 500.0),
    ("o2", "c2", 3, 60.0, 180.0),
    (None, "c3", 2, 40.0, 80.0),  # warning-expectation violation: kept
}
DROPPED_ORDER_ID = "o4"  # drop-expectation violation (quantity <= 0): removed

APP_SETTINGS = """\
datapipes:
  lakeflow:
    silver_orders:
      engine:
        sdp:
          table_properties:
            test_layer: silver
        databricks_sdp:
          expectations:
            valid_order_id: order_id IS NOT NULL
          expectations_drop:
            positive_quantity: quantity > 0
"""


WHEEL_PROJECTS = (
    WORKSPACE_ROOT.parent,
    WORKSPACE_ROOT.parent / "packages" / "extensions" / "kindling_ext_sdp",
    WORKSPACE_ROOT.parent / "packages" / "extensions" / "kindling_ext_databricks",
    WORKSPACE_ROOT / "data-apps" / "lakeflow-engine-test-app",
)


def _build_wheels(output_dir: Path) -> list:
    """Build this checkout's wheels with poetry-core (no virtualenv needed).

    Returned in dependency order (core, SDP extension, Databricks extension,
    app): serverless installs ``environment.dependencies`` one entry at a
    time, and Kindling packages are not on PyPI, so a wheel listed before
    the wheels it requires fails to install.
    """
    env = {**os.environ, "POETRY_VIRTUALENVS_CREATE": "false"}
    wheels = []
    for project in WHEEL_PROJECTS:
        target = output_dir / project.name
        target.mkdir(parents=True, exist_ok=True)
        proc = subprocess.run(
            ["poetry", "build", "-f", "wheel", "-o", str(target)],
            cwd=project,
            env=env,
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert proc.returncode == 0, f"poetry build failed in {project}:\n{proc.stderr[-2000:]}"
        built = list(target.glob("*.whl"))
        assert len(built) == 1, built
        wheels.append(built[0])
    return wheels


def _write_project(root: Path) -> Path:
    (root / "config").mkdir(parents=True)
    (root / "config" / "settings.yaml").write_text(
        "kindling:\n  platform:\n    environment: databricks\n  sdp:\n"
        "    dataset_naming: normalized\n  telemetry:\n    logging:\n      level: INFO\n",
        encoding="utf-8",
    )
    (root / "config" / "settings.dev.yaml").write_text(
        "kindling:\n  telemetry:\n    logging:\n      level: DEBUG\n", encoding="utf-8"
    )
    app = root / "data-apps" / "lakeflow_engine"
    app.mkdir(parents=True)
    (app / "settings.yaml").write_text(APP_SETTINGS, encoding="utf-8")
    return root


def _cli_env() -> dict:
    env = dict(os.environ)
    env.pop("DATABRICKS_CLUSTER_ID", None)  # irrelevant to pipelines; the CLI warns about it
    for arm_key, azure_key in (
        ("ARM_CLIENT_ID", "AZURE_CLIENT_ID"),
        ("ARM_CLIENT_SECRET", "AZURE_CLIENT_SECRET"),
        ("ARM_TENANT_ID", "AZURE_TENANT_ID"),
    ):
        if not env.get(arm_key) and env.get(azure_key):
            env[arm_key] = env[azure_key]
    return env


def _databricks(
    bundle_dir: Path, *args: str, timeout: float = 1800.0
) -> subprocess.CompletedProcess:
    command = ["databricks", "bundle", *args, "-t", "dev"]
    print(f"$ {' '.join(command)}")
    proc = subprocess.run(
        command,
        cwd=bundle_dir,
        env=_cli_env(),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    print(proc.stdout[-4000:])
    if proc.stderr:
        print(proc.stderr[-4000:])
    return proc


@pytest.mark.system
@pytest.mark.slow
class TestBundleBuildPlatform:
    """A generated bundle deploys and runs through the Databricks CLI."""

    def test_generated_bundle_deploys_and_runs_with_inline_config(self, platform_client, tmp_path):
        client, platform = platform_client
        if platform != "databricks":
            pytest.skip("Bundle deployment coverage is Databricks-only.")
        if shutil.which("databricks") is None:
            pytest.skip("databricks CLI not on PATH (the devcontainer image installs it).")
        if shutil.which("poetry") is None:
            pytest.skip("poetry is required to build this checkout's wheels.")
        host = os.getenv("DATABRICKS_HOST")
        if not host:
            pytest.skip("DATABRICKS_HOST is required for bundle deployment.")

        from kindling_cli.bundle import build_bundle, resolve_bundle_inputs

        w = client.client
        catalog = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "medallion")
        schema = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "default")

        warehouse_id = select_warehouse_id(w, os.getenv("SYSTEM_TEST_SQL_WAREHOUSE_ID"))
        if not warehouse_id:
            pytest.skip("No SQL warehouse available to verify pipeline outputs.")

        test_id = str(uuid.uuid4())[:8]
        bundle_name = f"systest-bundle-{test_id}"
        user_name = w.current_user.me().user_name
        workspace_root = f"/Workspace/Users/{user_name}/systest-bundle/{test_id}"
        bronze_table = f"{catalog}.{schema}.bronze_orders"
        silver_table = f"{catalog}.{schema}.silver_orders"

        wheels = _build_wheels(tmp_path / "wheels")
        print("🛞 Built wheels: " + ", ".join(wheel.name for wheel in wheels))
        project_root = _write_project(tmp_path / "project")
        inputs = resolve_bundle_inputs(
            {
                "name": bundle_name,
                "target": "dev",
                "apps": ("lakeflow_engine",),
                "workspace_host": host,
                "workspace_root": workspace_root,
                "catalog": catalog,
                "schema": schema,
                "wheels": tuple(wheels),
            },
            {},
        )
        result = build_bundle(inputs, project_root=project_root, output_dir=tmp_path / "bundle")
        assert result.warnings == [], result.warnings
        resource = yaml.safe_load(
            (result.output_dir / "resources" / "lakeflow_engine.pipeline.yml").read_text(
                encoding="utf-8"
            )
        )
        configuration = resource["resources"]["pipelines"]["lakeflow_engine"]["configuration"]
        assert "kindling.lakeflow.settings_json" in configuration
        assert "spark.kindling.bootstrap.config_files" not in configuration
        dependencies = resource["resources"]["pipelines"]["lakeflow_engine"]["environment"][
            "dependencies"
        ]
        assert dependencies == [f"../wheels/{wheel.name}" for wheel in wheels], dependencies
        print(f"📦 Bundle generated at {result.output_dir} ({len(result.files)} files)")

        deployed = False
        pipeline_id = None
        try:
            validate = _databricks(result.output_dir, "validate", timeout=300)
            assert validate.returncode == 0, "bundle validate failed"

            deploy = _databricks(result.output_dir, "deploy", timeout=900)
            deployed = deploy.returncode == 0
            assert deployed, "bundle deploy failed"

            summary = _databricks(result.output_dir, "summary", "-o", "json", timeout=300)
            assert summary.returncode == 0, "bundle summary failed"
            pipeline_id = json.loads(summary.stdout)["resources"]["pipelines"]["lakeflow_engine"][
                "id"
            ]
            print(f"🚀 Pipeline deployed: {pipeline_id} ({bundle_name})")

            run = _databricks(result.output_dir, "run", "lakeflow_engine", timeout=1800)
            if run.returncode != 0:
                print_error_events(w, pipeline_id)
            assert run.returncode == 0, "bundle run failed"
            print("✅ pipeline update completed through `databricks bundle run`")

            bronze_count = execute_statement(
                w, warehouse_id, f"SELECT COUNT(*) FROM {bronze_table}"
            )
            assert bronze_count and int(bronze_count[0][0]) == 4, bronze_count
            print("✅ bronze.orders materialized (4 source rows)")

            properties = execute_statement(w, warehouse_id, f"SHOW TBLPROPERTIES {silver_table}")
            props = {row[0]: row[1] for row in properties}
            assert props.get("test_layer") == "silver", properties
            print("✅ inline sdp.table_properties overlay applied (test_layer=silver)")

            rows = execute_statement(
                w,
                warehouse_id,
                f"SELECT order_id, customer_id, quantity, amount, total_amount "
                f"FROM {silver_table} ORDER BY customer_id",
            )
            result_rows = {
                (row[0], row[1], int(row[2]), float(row[3]), float(row[4])) for row in rows
            }
            order_ids = {row[0] for row in rows}
            assert result_rows == EXPECTED_VALID_ROWS, result_rows
            assert DROPPED_ORDER_ID not in order_ids, order_ids
            print("✅ inline expectations applied: warning row kept, drop row removed")
        finally:
            if not os.getenv("SKIP_TEST_CLEANUP"):
                if deployed:
                    destroy = _databricks(
                        result.output_dir, "destroy", "--auto-approve", timeout=900
                    )
                    if destroy.returncode == 0:
                        print(f"🗑️  Destroyed bundle {bundle_name}")
                    else:
                        print(f"⚠️  Bundle destroy warning (exit {destroy.returncode})")
                        if pipeline_id:
                            try:
                                w.pipelines.delete(pipeline_id)
                            except Exception as exc:  # noqa: BLE001
                                print(f"⚠️  Pipeline cleanup warning: {exc}")
                for table in (silver_table, bronze_table):
                    try:
                        execute_statement(w, warehouse_id, f"DROP TABLE IF EXISTS {table}")
                        print(f"🗑️  Dropped {table}")
                    except Exception as exc:  # noqa: BLE001
                        print(f"⚠️  Table cleanup warning: {exc}")
