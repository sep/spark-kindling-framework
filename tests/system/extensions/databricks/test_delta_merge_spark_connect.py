"""
Regression system test: keyed Delta merge on a Spark-Connect-backed session.

Background (see docs/contributing/databricks_execution_contract.md):
kindling.trace_ops._entity_id_from_args used to call
getattr(value, "entityid", None) on every argument passed to a traced
provider op, including the DataFrame argument to merge_to_entity(df, entity).
On a classic PySpark DataFrame that's a harmless no-op (a plain
AttributeError getattr() swallows). On a genuine Spark Connect DataFrame,
an unrecognized attribute name is treated as a possible column reference
and can trigger a schema-resolution RPC that fails outside an active
session/API-URL context (observed as "No api url found in local command
context"). The fix reads the argument's __dict__ directly, which never
invokes __getattr__ regardless of the argument's type.

Starting with DBR 14.0, clusters running Shared/Standard (user-isolation)
access mode use Spark Connect for the Python driver session by default --
this test relies on the shared CI cluster (DATABRICKS_CLUSTER_ID) already
being configured that way (confirmed: DBR 17.3 LTS, Photon, Standard
access mode -- matching a real client cluster observed hitting this bug)
rather than forcing a fresh cluster itself: the CI service principal is not
authorized to create clusters (PERMISSION_DENIED), so existing_cluster_id
is the only viable path here. If the shared cluster's access mode ever
reverts to Single User/Dedicated, this test stops exercising the bug it
guards against without warning -- see databricks_execution_contract.md.

Usage:
    poe test-extension --extension databricks --platform databricks
"""

import os
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

APP_NAME = "delta-merge-spark-connect-test"
EXPECTED_TESTS = [
    "initial_merge",
    "keyed_merge_updates_matched_rows",
]


@pytest.fixture
def delta_merge_app_path():
    app_path = Path(__file__).parent.parent.parent.parent / "data-apps" / APP_NAME
    if not app_path.exists():
        pytest.skip(f"Delta merge / Spark Connect test app not found at {app_path}")
    return app_path


@pytest.mark.system
@pytest.mark.slow
class TestDeltaMergeSparkConnect:
    """Keyed Delta merge must succeed on a real Spark-Connect-backed session."""

    def test_keyed_merge_succeeds_on_shared_mode_cluster(
        self, platform_client, app_packager, delta_merge_app_path, stdout_validator
    ):
        api_client, platform_name = platform_client
        if platform_name != "databricks":
            pytest.skip("Spark Connect access-mode coverage is Databricks-only.")

        test_id = str(uuid.uuid4())[:8]
        app_name = f"{APP_NAME}-{test_id}"
        job_config = {
            "job_name": f"systest-delta-merge-connect-{test_id}",
            "app_name": app_name,
            "entry_point": "app.py",
            "test_id": test_id,
        }

        platform_overrides = get_system_platform_config_overrides(platform_name, test_id)
        if platform_name == "databricks":
            # get_system_platform_config_overrides sets access_mode=catalog but
            # never a table_catalog/table_schema, so entity resolution falls back
            # to whatever the session's ambient default catalog happens to be --
            # on this cluster that's the legacy hive_metastore, where creating a
            # genuinely new Delta table from a UC-governed Standard-mode session
            # doesn't behave like a real UC catalog. Pin the real UC catalog/schema
            # explicitly, same env vars the UC-volume path itself is derived from.
            platform_overrides.setdefault("kindling", {}).setdefault("storage", {}).update(
                {
                    "table_catalog": os.getenv(
                        "KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "kindling"
                    ),
                    "table_schema": os.getenv(
                        "KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "kindling"
                    ),
                }
            )
        if platform_overrides:
            job_config["config_overrides"] = platform_overrides

        print(
            f"\n[{platform_name.upper()}] Delta merge / Spark Connect regression test_id={test_id}"
        )

        app_files = app_packager.prepare_app_files(str(delta_merge_app_path))
        api_client.deploy_app(app_name, app_files)
        print(f"App deployed: {app_name}")

        result = api_client.create_job(job_name=job_config["job_name"], job_config=job_config)
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
                    max_wait=get_system_test_stream_max_wait(600.0, platform_name),
                )
            except Exception as stream_err:
                print(f"Stdout streaming error (non-fatal): {stream_err}")
            print("=" * 70)

            status_result = api_client.get_job_status(run_id=run_id)
            final_status = status_result.get("status", "UNKNOWN").upper()
            print(f"Final job status: {final_status}")

            stdout_content = "\n".join(get_captured_stdout(stdout_validator))

            # The exact failure this test guards against: if this string shows up,
            # the fix regressed and a provider-op argument was getattr()-introspected
            # again on a Spark Connect DataFrame.
            assert "No api url found in local command context" not in stdout_content, (
                "Spark-Connect-unsafe attribute introspection regressed — provider "
                "tracing accessed an attribute on a DataFrame argument.\n"
                f"Stdout tail:\n{stdout_content[-3000:]}"
            )

            assert_no_fatal_system_test_log_lines(stdout_content)

            bootstrap_results = stdout_validator.validate_bootstrap_execution()
            assert bootstrap_results.get(
                "bootstrap_start"
            ), "Bootstrap start marker not found in stdout"

            completion = stdout_validator.validate_completion(test_id)
            assert completion.get("passed"), (
                f"Completion marker missing or result=FAILED. "
                f"Stdout tail:\n{stdout_content[-3000:]}"
            )

            test_results = stdout_validator.validate_tests(test_id, EXPECTED_TESTS)
            hard_failures = [
                f"{name}: {info['status']} — {info.get('message', '')}"
                for name, info in test_results.items()
                if info["status"] == "FAILED"
            ]
            if hard_failures:
                pytest.fail(
                    f"Delta merge / Spark Connect regression failures on {platform_name}:\n"
                    + "\n".join(f"  - {f}" for f in hard_failures)
                    + f"\n\nFull stdout tail:\n{stdout_content[-3000:]}"
                )

            passed = sum(1 for info in test_results.values() if info["passed"])
            print(f"\n{passed}/{len(EXPECTED_TESTS)} merge test markers confirmed.")

        finally:
            self._cleanup(
                api_client, job_id, app_name, platform_name=platform_name, test_id=test_id
            )

    def _cleanup(
        self, api_client, job_id: str, app_name: str, platform_name: str = None, test_id: str = None
    ) -> None:
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

        from tests.system.test_helpers import cleanup_test_storage

        cleanup_test_storage(platform_name, test_id)
