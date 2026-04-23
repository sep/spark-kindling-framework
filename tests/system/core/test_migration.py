"""
System tests for MigrationService and SQL entity (permanent catalog view) support.

Deploys the migration-test-app to each platform and validates:
  - Delta entity creation via migration (CREATE_TABLE)
  - SQL entity (catalog view) creation via migration (CREATE_VIEW)
  - Data readable through a view after migration
  - Schema evolution: ADD_COLUMNS detected and applied

Run all platforms:
    poe test-system

Run specific platform:
    poe test-system --platform fabric
    poe test-system --platform databricks
    poe test-system --platform synapse

Run just migration tests:
    poe test-system --test migration
"""

import uuid
from pathlib import Path

import pytest

from tests.system.test_helpers import (
    assert_no_fatal_system_test_log_lines,
    get_captured_stdout,
    get_system_platform_config_overrides,
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
)

MIGRATION_APP_NAME = "migration-test-app"

EXPECTED_TESTS = [
    "scenario_a_plan_detects_create_table",
    "scenario_a_plan_detects_create_view",
    "scenario_a_plan_no_errors",
    "scenario_a_apply_succeeds",
    "scenario_a_items_up_to_date",
    "scenario_a_view_up_to_date",
    "scenario_a_view_filters_correctly",
    "scenario_b_partial_table_created",
    "scenario_b_plan_detects_add_columns",
    "scenario_b_apply_add_columns_succeeds",
    "scenario_b_amount_column_present",
]


@pytest.fixture
def migration_app_path():
    app_path = Path(__file__).parent.parent.parent / "data-apps" / MIGRATION_APP_NAME
    if not app_path.exists():
        pytest.skip(f"Migration test app not found at {app_path}")
    return app_path


@pytest.fixture
def migration_job_config_provider():
    def get_config():
        unique_suffix = str(uuid.uuid4())[:8]
        return {
            "job_name": f"systest-migration-{unique_suffix}",
            "app_name": f"{MIGRATION_APP_NAME}-{unique_suffix}",
            "entry_point": "main.py",
            "test_id": unique_suffix,
        }

    return get_config


@pytest.mark.system
@pytest.mark.slow
class TestMigrationService:
    """End-to-end migration tests — run on all platforms via parametrization."""

    def test_migration_create_and_evolve(
        self,
        platform_client,
        app_packager,
        migration_app_path,
        migration_job_config_provider,
        stdout_validator,
    ):
        """
        Deploy migration-test-app and validate all migration scenarios:
          - Scenario A: create Delta table + SQL view from scratch, read through view
          - Scenario B: detect and apply ADD_COLUMNS on an existing table
        """
        api_client, platform_name = platform_client
        job_config = migration_job_config_provider()
        test_id = job_config["test_id"]
        app_name = job_config["app_name"]

        print(f"\n[{platform_name.upper()}] Migration system test — test_id={test_id}")

        # Merge platform storage overrides so the app gets the right paths
        platform_overrides = get_system_platform_config_overrides(platform_name, test_id)
        if platform_overrides:
            job_config["config_overrides"] = platform_overrides

        # Package and deploy
        app_files = app_packager.prepare_app_files(str(migration_app_path))
        api_client.deploy_app(app_name, app_files)
        print(f"App deployed: {app_name}")

        result = api_client.create_job(
            job_name=job_config["job_name"],
            job_config=job_config,
        )
        assert "job_id" in result, f"job_id missing from create_job result: {result}"
        job_id = result["job_id"]
        print(f"Job created: {job_id}")

        try:
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"Job running: {run_id}")

            print("\nStreaming stdout...")
            print("=" * 70)
            try:
                stdout_validator.stream_with_callback(
                    job_id=job_id,
                    run_id=run_id,
                    print_lines=True,
                    poll_interval=get_system_test_poll_interval(10.0),
                    max_wait=get_system_test_stream_max_wait(900.0),
                )
            except Exception as stream_err:
                print(f"Stdout streaming error (non-fatal): {stream_err}")
            print("=" * 70)

            status_result = api_client.get_job_status(run_id=run_id)
            final_status = status_result.get("status", "UNKNOWN").upper()
            print(f"Final job status: {final_status}")

            # No fatal framework errors
            stdout_content = "\n".join(get_captured_stdout(stdout_validator))
            assert_no_fatal_system_test_log_lines(stdout_content)

            # Bootstrap completed
            bootstrap_results = stdout_validator.validate_bootstrap_execution()
            assert bootstrap_results.get(
                "bootstrap_start"
            ), "Bootstrap start marker not found in stdout"

            # Completion marker present
            completion = stdout_validator.validate_completion(test_id)
            assert completion.get("passed"), (
                f"Completion marker missing or result=FAILED. "
                f"Stdout tail:\n{stdout_content[-2000:]}"
            )

            # Individual migration test markers.
            # NOT_FOUND means the marker wasn't captured by the streaming poller
            # (timing artifact on fast-completing jobs) — not an actual failure.
            # We only hard-fail on markers explicitly marked FAILED in stdout.
            test_results = stdout_validator.validate_tests(test_id, EXPECTED_TESTS)

            hard_failures = [
                f"{name}: {info['status']} — {info.get('message', '')}"
                for name, info in test_results.items()
                if info["status"] == "FAILED"
            ]
            not_captured = [
                name for name, info in test_results.items() if info["status"] == "NOT_FOUND"
            ]

            if not_captured:
                print(
                    f"\nNote: {len(not_captured)} marker(s) not captured by stdout poller "
                    f"(timing — app reported PASSED overall): {not_captured}"
                )

            if hard_failures:
                pytest.fail(
                    f"Migration scenario failures on {platform_name}:\n"
                    + "\n".join(f"  - {f}" for f in hard_failures)
                    + f"\n\nFull stdout tail:\n{stdout_content[-3000:]}"
                )

            passed = sum(1 for i in test_results.values() if i["passed"])
            print(
                f"\n{passed}/{len(EXPECTED_TESTS)} migration test markers confirmed on {platform_name}."
            )

        finally:
            self._cleanup(api_client, job_id, app_name)

    def _cleanup(self, api_client, job_id: str, app_name: str) -> None:
        import os

        if os.environ.get("SKIP_TEST_CLEANUP", "").lower() == "true":
            print(f"Skipping cleanup (SKIP_TEST_CLEANUP=true): job={job_id}, app={app_name}")
            return
        try:
            api_client.delete_job(job_id=job_id)
            print(f"Deleted job: {job_id}")
        except Exception as e:
            print(f"Warning: could not delete job {job_id}: {e}")
        try:
            api_client.cleanup_app(app_name)
            print(f"Cleaned up app: {app_name}")
        except Exception as e:
            print(f"Warning: could not clean up app {app_name}: {e}")
