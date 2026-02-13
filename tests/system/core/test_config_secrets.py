"""
Databricks system test for config secret-like values.

This test validates:
1. Secret-like values injected via job_config config_overrides
2. Dynaconf YAML reference interpolation for keys that point at those secrets
"""

import os
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def config_secrets_app_path():
    """Fixture providing path to config secrets test app."""
    app_path = Path(__file__).parent.parent.parent / "data-apps" / "config-secrets-test-app"
    if not app_path.exists():
        pytest.skip(f"Config secrets test app not found at {app_path}")
    return app_path


@pytest.fixture
def config_secrets_job_config():
    """Build job config with nested secret-like overrides."""
    test_id = str(uuid.uuid4())[:8]
    secret_values = {
        "service": {"api_token": f"svc-{test_id}-A1"},
        "database": {"password": f"db-{test_id}-P9"},
        "integration": {"webhook_key": f"wh-{test_id}-K7"},
    }

    return {
        "job_name": f"systest-config-secrets-{test_id}",
        "app_name": f"config-secrets-test-app-{test_id}",
        "entry_point": "main.py",
        "test_id": test_id,
        "config_overrides": {
            "kindling": {
                "secrets": secret_values,
            }
        },
    }


@pytest.mark.system
@pytest.mark.slow
class TestConfigSecrets:
    """System test for config secret injection and YAML reference resolution."""

    def test_config_secrets_round_trip(
        self,
        platform_client,
        app_packager,
        config_secrets_app_path,
        config_secrets_job_config,
        stdout_validator,
    ):
        api_client, platform_name = platform_client
        if platform_name != "databricks":
            pytest.skip("Config secrets system test is Databricks-only")

        validator = stdout_validator
        job_config = config_secrets_job_config
        job_id = None

        print(f"\n🎯 [{platform_name.upper()}] Testing config secrets round-trip")

        app_files = app_packager.prepare_app_files(str(config_secrets_app_path))
        app_name = job_config["app_name"]
        storage_path = api_client.deploy_app(app_name, app_files)
        print(f"📂 App deployed to: {storage_path}")

        result = api_client.create_job(
            job_name=job_config["job_name"],
            job_config=job_config,
        )
        job_id = result["job_id"]
        print(f"📦 Job created: {job_id}")

        try:
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"🏃 Job started: {run_id}")

            validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=10.0,
                max_wait=600.0,
            )

            status = api_client.get_job_status(run_id=run_id)
            final_status = str(status.get("status", "UNKNOWN")).upper()
            assert final_status in [
                "TERMINATED",
                "COMPLETED",
                "SUCCESS",
            ], f"Unexpected final status: {final_status}"

            test_id = job_config["test_id"]
            expected_tests = [
                "config_secret_raw_service_token",
                "config_secret_raw_database_password",
                "config_secret_raw_integration_key",
                "config_secret_yaml_ref_service_token",
                "config_secret_yaml_ref_database_password",
                "config_secret_yaml_ref_integration_key",
                "config_secret_yaml_template_auth_header",
            ]

            validation_results = validator.validate_tests(test_id, expected_tests)
            completion_result = validator.validate_completion(test_id)

            for test_name, result in validation_results.items():
                icon = "✅" if result["passed"] else "❌"
                print(f"{icon} {test_name}: {result['status']}")

            assert all(r["passed"] for r in validation_results.values()), (
                "Config secrets checks failed. " f"Validation results: {validation_results}"
            )
            assert completion_result[
                "passed"
            ], f"Config secrets test did not complete successfully: {completion_result}"

            print("✅ Config secrets round-trip validated")

        finally:
            if os.environ.get("SKIP_TEST_CLEANUP", "").lower() == "true":
                print(
                    f"⏸️  Skipping cleanup (SKIP_TEST_CLEANUP=true) - job: {job_id}, app: {app_name}"
                )
            else:
                if job_id:
                    try:
                        api_client.delete_job(job_id)
                        print(f"✅ Deleted job: {job_id}")
                    except Exception as e:
                        print(f"⚠️  Failed to delete job {job_id}: {e}")

                try:
                    api_client.cleanup_app(app_name)
                    print(f"🗑️  Cleaned up app: {app_name}")
                except Exception as e:
                    print(f"⚠️  Failed to cleanup app {app_name}: {e}")
