"""
System test: provenance stamping and the runtime manifest on each platform.

The provenance-test-app runs a real batch pipe through the executor into an
SCD2 entity with provenance stamping on (see its docstring for the four
in-app checks): the runtime manifest is built by bootstrap, every output row
carries the writing pipe's stamp with the manifest's run_id and the
executor's execution id, a re-run on unchanged data opens no version, and an
SCD2 close keeps the closed version's stamp.

Usage:
    poe test-system --platform databricks --test test_provenance_runtime_manifest
"""

import os
import uuid
from pathlib import Path

import pytest

from tests.system.test_helpers import (
    assert_no_fatal_system_test_log_lines,
    cleanup_test_storage,
    get_captured_stdout,
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
)

APP_NAME = "provenance-test-app"
EXPECTED_TESTS = [
    "runtime_manifest_populated",
    "runtime_is_system_tested",
    "provenance_stamped",
    "provenance_only_difference_no_version",
    "scd2_close_keeps_stamp",
]


@pytest.fixture
def provenance_app_path():
    app_path = Path(__file__).parent.parent.parent / "data-apps" / APP_NAME
    if not app_path.exists():
        pytest.skip(f"Provenance test app not found at {app_path}")
    return app_path


@pytest.mark.system
@pytest.mark.slow
class TestProvenanceRuntimeManifest:
    def test_pipe_output_is_stamped_with_the_manifest_run_id(
        self, platform_client, app_packager, provenance_app_path, stdout_validator
    ):
        api_client, platform_name = platform_client
        test_id = str(uuid.uuid4())[:8]
        app_name = f"{APP_NAME}-{test_id}"
        job_config = {
            "job_name": f"systest-provenance-{test_id}",
            "app_name": app_name,
            "entry_point": "app.py",
            "test_id": test_id,
        }
        if platform_name == "databricks":
            # The platform overrides set access_mode=catalog without a
            # catalog/schema; pin the UC catalog/schema the system-test
            # volumes live in (as test_delta_merge_spark_connect does), so
            # new Delta tables land in a real UC schema.
            job_config["config_overrides"] = {
                "kindling": {
                    "storage": {
                        "table_catalog": os.getenv(
                            "KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "kindling"
                        ),
                        "table_schema": os.getenv(
                            "KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "kindling"
                        ),
                    }
                }
            }

        print(f"\n[{platform_name.upper()}] Provenance + runtime manifest test_id={test_id}")
        app_files = app_packager.prepare_app_files(str(provenance_app_path))
        api_client.deploy_app(app_name, app_files)

        result = api_client.create_job(job_name=job_config["job_name"], job_config=job_config)
        assert "job_id" in result, f"job_id missing from create_job result: {result}"
        job_id = result["job_id"]

        try:
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"

            try:
                stdout_validator.stream_with_callback(
                    job_id=job_id,
                    run_id=run_id,
                    print_lines=True,
                    poll_interval=get_system_test_poll_interval(10.0),
                    max_wait=get_system_test_stream_max_wait(600.0, platform_name),
                )
            except Exception as stream_err:
                print(f"Stdout streaming error (non-fatal): {stream_err}")

            status = str(api_client.get_job_status(run_id=run_id).get("status", "UNKNOWN"))
            stdout_content = "\n".join(get_captured_stdout(stdout_validator))
            assert_no_fatal_system_test_log_lines(stdout_content)

            test_results = stdout_validator.validate_tests(test_id, EXPECTED_TESTS)
            failures = [
                f"{name}: {info['status']} {info.get('message', '')}"
                for name, info in test_results.items()
                if not info["passed"]
            ]
            assert not failures, (
                f"Provenance checks failed on {platform_name}:\n"
                + "\n".join(f"  - {failure}" for failure in failures)
                + f"\n\nStdout tail:\n{stdout_content[-3000:]}"
            )
            completion = stdout_validator.validate_completion(test_id)
            assert completion.get("passed"), f"Completion failed: {completion}"
            assert status.upper() not in {"FAILED", "ERROR", "CANCELLED"}, status
        finally:
            if os.environ.get("SKIP_TEST_CLEANUP", "").lower() != "true":
                try:
                    api_client.delete_job(job_id=job_id)
                except Exception as exc:
                    print(f"Warning: could not delete job {job_id}: {exc}")
                try:
                    api_client.cleanup_app(app_name)
                except Exception as exc:
                    print(f"Warning: could not clean up app {app_name}: {exc}")
                cleanup_test_storage(platform_name, test_id)
