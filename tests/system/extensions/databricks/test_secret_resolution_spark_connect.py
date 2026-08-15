"""
Regression system test: @secret resolution on a Spark-Connect-backed session.

Reproduces a real client failure without needing them to re-test after every
release: on DBR 14.0+, Standard/Shared (user-isolation) access mode clusters
back the Python driver session with Spark Connect, and __main__.dbutils is
never injected there the way it is in a classic attached session -- so
DatabricksService.get_secret()'s dbutils lookup used to fail silently,
leaving a live "@secret:..." literal in resolved config, which a downstream
consumer (e.g. the EventHub provider parsing it as a connection string) then
failed on with a confusing, unrelated KeyError.

This uses create_job()/run_job() against existing_cluster_id (the CI
DATABRICKS_CLUSTER_ID secret) -- the same job shape
`kindling app run --platform databricks` submits -- and relies on that
shared cluster already being configured Standard/Shared access mode
(confirmed: DBR 17.3 LTS, Photon, matching a real client cluster observed
hitting this bug). It does not force a fresh cluster itself: the CI service
principal is not authorized to create clusters (PERMISSION_DENIED). If the
shared cluster's access mode ever reverts to Single User/Dedicated, this
test stops exercising the bug it guards against without warning -- see
databricks_execution_contract.md.

Usage:
    poe test-extension --extension databricks --platform databricks
"""

import os
import uuid

import pytest

from tests.system.test_helpers import (
    apply_env_config_overrides,
    create_stdout_validator,
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
    wait_for_job_not_pending,
)

APP_NAME = "config-secrets-test-app"
EXPECTED_TESTS = [
    "config_secret_raw_service_token",
    "config_secret_raw_database_password",
    "config_secret_raw_integration_key",
    "config_secret_yaml_ref_service_token",
    "config_secret_yaml_ref_database_password",
    "config_secret_yaml_ref_integration_key",
    "config_secret_yaml_template_auth_header",
]


def _extract_secret_name(secret_ref: str) -> str:
    if secret_ref.startswith("@secret "):
        return secret_ref[len("@secret ") :].strip()
    if secret_ref.startswith("@secret:"):
        return secret_ref[len("@secret:") :].strip()
    return secret_ref


@pytest.fixture
def config_secrets_app_path():
    from pathlib import Path

    app_path = Path(__file__).parent.parent.parent.parent / "data-apps" / APP_NAME
    if not app_path.exists():
        pytest.skip(f"Config secrets test app not found at {app_path}")
    return app_path


@pytest.mark.system
@pytest.mark.slow
class TestSecretResolutionSparkConnect:
    """@secret references (Databricks-backed scope) must resolve on Standard/Shared compute."""

    def test_secret_round_trip_on_shared_mode_cluster(
        self, platform_client, app_packager, config_secrets_app_path
    ):
        api_client, platform_name = platform_client
        if platform_name != "databricks":
            pytest.skip("Spark Connect dbutils-bridge coverage is Databricks-only.")

        test_id = str(uuid.uuid4())[:8]
        app_name = f"{APP_NAME}-{test_id}"

        secrets_config = {"secret_scope": "kindling-system-tests"}
        platform_secrets_config = {
            "service": {"api_token": f"@secret:kindling-systest-{test_id}-service-token"},
            "database": {"password": f"@secret:kindling-systest-{test_id}-db-password"},
            "integration": {"webhook_key": f"@secret:kindling-systest-{test_id}-webhook-key"},
            "secret_scope": secrets_config["secret_scope"],
        }

        job_config = {
            "job_name": f"systest-secret-connect-{test_id}",
            "app_name": app_name,
            "entry_point": "app.py",
            "test_id": test_id,
            "config_overrides": {"kindling": {"secrets": platform_secrets_config}},
        }
        job_config = apply_env_config_overrides(job_config, platform_name)

        secret_refs = {
            "service_token": _extract_secret_name(
                job_config["config_overrides"]["kindling"]["secrets"]["service"]["api_token"]
            ),
            "database_password": _extract_secret_name(
                job_config["config_overrides"]["kindling"]["secrets"]["database"]["password"]
            ),
            "integration_key": _extract_secret_name(
                job_config["config_overrides"]["kindling"]["secrets"]["integration"]["webhook_key"]
            ),
        }
        expected_values = {
            "service_token": f"svc-{test_id}-A1",
            "database_password": f"db-{test_id}-P9",
            "integration_key": f"wh-{test_id}-K7",
        }

        for key in ("service_token", "database_password", "integration_key"):
            api_client.set_secret(
                secret_name=secret_refs[key],
                secret_value=expected_values[key],
                secret_config=secrets_config,
            )

        validator = create_stdout_validator(api_client)
        job_id = None

        print(f"\n[{platform_name.upper()}] @secret resolution on Standard-mode cluster: {test_id}")

        app_files = app_packager.prepare_app_files(str(config_secrets_app_path))
        api_client.deploy_app(app_name, app_files)
        print(f"App deployed: {app_name}")

        try:
            result = api_client.create_job(job_name=job_config["job_name"], job_config=job_config)
            job_id = result["job_id"]
            print(f"Job created: {job_id}")

            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"Job running: {run_id}")

            validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=get_system_test_poll_interval(10.0),
                max_wait=get_system_test_stream_max_wait(600.0, platform_name),
            )

            final_status = wait_for_job_not_pending(api_client, run_id)
            stdout_content = "\n".join(validator.captured_lines)

            if final_status not in ["TERMINATED", "COMPLETED", "SUCCESS"]:
                pytest.fail(
                    f"Unexpected final status: {final_status}.\n"
                    f"Stdout tail:\n{stdout_content[-3000:]}"
                )

            # The exact failure this test guards against: an unresolved
            # @secret literal reaching downstream code looks like this in
            # stdout (from a Kafka/EventHub-style consumer) or, before the
            # dbutils bridge fix, an explicit resolution-failure log line.
            assert "No platform secret provider available" not in stdout_content, (
                "Secret resolution regressed on a Spark-Connect-backed session.\n"
                f"Stdout tail:\n{stdout_content[-3000:]}"
            )

            validation_results = validator.validate_tests(test_id, EXPECTED_TESTS)
            hard_failures = [
                f"{name}: {info['status']} — {info.get('message', '')}"
                for name, info in validation_results.items()
                if info["status"] == "FAILED"
            ]
            if hard_failures:
                pytest.fail(
                    "Secret resolution failures on Standard-mode cluster:\n"
                    + "\n".join(f"  - {f}" for f in hard_failures)
                    + f"\n\nFull stdout tail:\n{stdout_content[-3000:]}"
                )

            completion_result = validator.validate_completion(test_id)
            assert completion_result["passed"], (
                f"Secret resolution test did not complete successfully: {completion_result}\n"
                f"Stdout tail:\n{stdout_content[-3000:]}"
            )

            passed = sum(1 for info in validation_results.values() if info["passed"])
            print(f"\n{passed}/{len(EXPECTED_TESTS)} secret resolution markers confirmed.")

        finally:
            for key in ("service_token", "database_password", "integration_key"):
                try:
                    api_client.delete_secret(secret_refs[key], secret_config=secrets_config)
                except Exception as exc:
                    print(f"Warning: failed to delete secret {secret_refs[key]}: {exc}")

            if os.environ.get("SKIP_TEST_CLEANUP", "").lower() == "true":
                print(f"Skipping cleanup (SKIP_TEST_CLEANUP=true): job={job_id}, app={app_name}")
                return

            if job_id:
                try:
                    api_client.delete_job(job_id)
                    print(f"Deleted job: {job_id}")
                except Exception as exc:
                    print(f"Warning: could not delete job {job_id}: {exc}")
            try:
                api_client.cleanup_app(app_name)
                print(f"Cleaned up app: {app_name}")
            except Exception as exc:
                print(f"Warning: could not clean up app {app_name}: {exc}")

            from tests.system.test_helpers import cleanup_test_storage

            cleanup_test_storage(platform_name, test_id)
