"""
Platform system test for the temporal extension (kindling-ext-temporal).

Deploys tests/data-apps/temporal-test-app once and runs it as TWO separate
job executions sharing one test_id-scoped storage root:

  Run 1 (scenario run1) — Experiment 1: telemetry -> base events ->
  condition engine (rules ingested via ingest_conditions, malformed row
  quarantined) -> episodes -> determination events, all persisted to real
  Delta storage. machine-hot closes with a real end; machine-late is
  start-only and expires at the configured
  kindling.temporal.evaluation_time. Also proves first-run tolerance (the
  engine-owned prior-state read with no episodes table yet).

  Run 2 (scenario run2) — Experiment 2: a separate process ingests only the
  late real end event. The engine reconstructs the persisted expired
  episode across processes and revises it in place (same episode_id,
  status=closed, end_event_synthetic=false) and appends the corrective
  .closed determination event alongside the historical .expired one.
  Experiment 3: a gold interval-join aggregation (avg temperature per
  closed episode window) asserted after revision.

Per-run configuration travels through job-level config_overrides
(kindling.temporal.evaluation_time, temporal_test_scenario) because
Databricks run-time python_params would replace the bootstrap args — two
job definitions over one deployed app is the supported per-run config
mechanism.

The in-app assertions emit TEST_ID markers; this test validates them from
the streamed stdout, mirroring tests/system/core conventions.

Usage:
    poe test-extension --extension temporal --platform databricks
"""

import os
import sys
import uuid
from pathlib import Path

import pytest

from tests.system.test_helpers import (
    cleanup_test_storage,
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
)

EXPECTED_RUN1_TESTS = [
    "run1_conditions_ingested",
    "run1_conditions_quarantined",
    "run1_quarantine_persisted",
    "run1_conditions_current_view",
    "run1_first_run_pipes_executed",
    "run1_hot_episode_closed",
    "run1_late_episode_expired",
    "run1_hot_determination_event",
    "run1_late_expiration_event",
]

EXPECTED_RUN2_TESTS = [
    "run2_sees_persisted_expired_episode",
    "run2_pipes_executed",
    "run2_late_episode_revised_in_place",
    "run2_same_episode_id",
    "run2_corrective_event_appended",
    "run2_hot_episode_untouched",
    "run2_gold_aggregation",
]

TEMPORAL_EXTENSION_SPEC = "kindling-ext-temporal==0.2.3"


def _run_scenario(
    client,
    stdout_validator,
    *,
    job_id: str,
    test_id: str,
    scenario: str,
    platform: str,
    expected_tests,
):
    """Run one job execution and validate its in-app TEST_ID markers."""
    run_id = client.run_job(job_id=job_id)
    print(f"🏃 {scenario} started: run_id={run_id}")
    sys.stdout.flush()

    stdout_validator.stream_with_callback(
        job_id=job_id,
        run_id=run_id,
        print_lines=True,
        poll_interval=get_system_test_poll_interval(10.0),
        max_wait=get_system_test_stream_max_wait(900.0, platform),
    )

    status_info = client.get_job_status(run_id=run_id)
    final_status = str(status_info.get("status") or "").upper()
    print(f"📊 {scenario} final status: {final_status}")

    test_results = stdout_validator.validate_tests(test_id, expected_tests)
    failed = {
        name: result["status"] for name, result in test_results.items() if not result["passed"]
    }
    completion = stdout_validator.validate_completion(test_id)

    assert not failed, f"{scenario} in-app assertions failed or missing: {failed}"
    assert completion["passed"], f"{scenario} did not complete PASSED: {completion['message']}"
    assert final_status not in {
        "FAILED",
        "ERROR",
        "CANCELLED",
    }, f"{scenario} job ended {final_status}"
    print(f"✅ {scenario} validated: {len(expected_tests)} in-app assertions PASSED")
    return run_id


@pytest.mark.system
@pytest.mark.slow
class TestTemporalExtensionPlatform:
    """Temporal extension lifecycle against real platform storage."""

    @pytest.mark.parametrize("execution_mode", ["pipes", "chain"])
    def test_temporal_cross_run_lifecycle(
        self, platform_client, app_packager, stdout_validator, execution_mode
    ):
        client, platform = platform_client
        if platform != "databricks":
            pytest.skip(
                "Temporal platform coverage is currently validated on Databricks only; "
                "the app itself is platform-agnostic."
            )

        workspace_root = Path(__file__).parent.parent.parent.parent
        app_path = workspace_root / "data-apps" / "temporal-test-app"
        assert app_path.exists(), f"Test app not found at {app_path}"

        test_id = str(uuid.uuid4())[:8]
        app_name = f"temporal-test-app-{test_id}"
        print(f"\n🧪 Temporal extension system test on {platform.upper()} (test_id={test_id})")

        app_files = app_packager.prepare_app_files(str(app_path))
        print(f"📦 Prepared {len(app_files)} app files")
        client.deploy_app(app_name, app_files)
        print(f"📂 Deployed app: {app_name}")

        # All app tables are catalog-managed in the UC catalog/schema the
        # system-test volumes live in, isolated per run by a table-name
        # prefix. Delta tables written to /Volumes paths are not durable on
        # UC shared-access clusters, so path-based storage mode is not an
        # option here; the app drops the prefixed tables when run2 passes.
        uc_catalog = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "medallion")
        uc_schema = os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "default")
        table_prefix = f"systest_temporal_{test_id}_"

        def _job_config(scenario: str, evaluation_time: str) -> dict:
            # Both jobs share test_id, so they share the test-scoped tables:
            # run2 revises the episodes run1 persisted.
            return {
                "job_name": f"systest-temporal-{scenario}-{test_id}",
                "app_name": app_name,
                "entry_point": "app.py",
                "test_id": test_id,
                "config_overrides": {
                    "temporal_test_scenario": scenario,
                    "temporal_execution_mode": execution_mode,
                    "kindling": {
                        # settings.yaml also declares the extension, but the
                        # harness's kindling.extensions override replaces that
                        # key wholesale, so declare it here to survive the merge.
                        "extensions": [TEMPORAL_EXTENSION_SPEC],
                        "storage": {
                            "table_catalog": uc_catalog,
                            "table_schema": uc_schema,
                            "table_name_prefix": table_prefix,
                        },
                        "temporal": {
                            "evaluation_time": evaluation_time,
                            "conditions": {"quarantine_entity_id": "silver.conditions.quarantine"},
                        },
                    },
                },
            }

        job_ids = []
        try:
            run1 = client.create_job(
                job_name=f"systest-temporal-run1-{test_id}",
                job_config=_job_config("run1", "2026-07-14 12:30:00"),
            )
            job_ids.append(run1["job_id"])
            _run_scenario(
                client,
                stdout_validator,
                job_id=run1["job_id"],
                test_id=test_id.replace("-", "_"),
                scenario="run1",
                platform=platform,
                expected_tests=EXPECTED_RUN1_TESTS,
            )

            extension_results = stdout_validator.validate_extension_loading("kindling-ext-temporal")
            stdout_validator.print_validation_summary(
                extension_results, "Temporal Extension Loading"
            )
            assert extension_results.get(
                "extension_install"
            ), "kindling-ext-temporal installation not found in stdout"

            run2 = client.create_job(
                job_name=f"systest-temporal-run2-{test_id}",
                job_config=_job_config("run2", "2026-07-14 12:45:00"),
            )
            job_ids.append(run2["job_id"])
            _run_scenario(
                client,
                stdout_validator,
                job_id=run2["job_id"],
                test_id=test_id.replace("-", "_"),
                scenario="run2",
                platform=platform,
                expected_tests=EXPECTED_RUN2_TESTS,
            )

            print("\n📊 Temporal system test summary:")
            print("   ✅ Experiment 1: end-to-end flow persisted on real storage")
            print("   ✅ Experiment 2: cross-run late-end revision (two job executions)")
            print("   ✅ Experiment 3: gold interval-join aggregation after revision")
        finally:
            if not os.getenv("SKIP_TEST_CLEANUP"):
                for job_id in job_ids:
                    try:
                        client.delete_job(job_id)
                        print(f"🗑️  Cleaned up job: {job_id}")
                    except Exception as exc:  # noqa: BLE001
                        print(f"⚠️  Job cleanup warning: {exc}")
                try:
                    client.cleanup_app(app_name)
                    print(f"🗑️  Cleaned up app: {app_name}")
                except Exception as exc:  # noqa: BLE001
                    print(f"⚠️  App cleanup warning: {exc}")
                try:
                    cleanup_test_storage(platform, test_id)
                except Exception as exc:  # noqa: BLE001
                    print(f"⚠️  Storage cleanup warning: {exc}")
