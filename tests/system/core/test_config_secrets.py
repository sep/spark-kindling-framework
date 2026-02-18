"""
System test for config override substitution.

This test validates:
1. Values injected via job_config config_overrides
2. Dynaconf YAML reference interpolation for keys that point at those values

Note:
The config override test below does NOT validate platform secret manager retrieval.
The platform secret-provider test in this module does validate real secret retrieval.
"""

import os
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def config_overrides_app_path():
    """Fixture providing path to config override substitution test app."""
    app_path = Path(__file__).parent.parent.parent / "data-apps" / "config-secrets-test-app"
    assert app_path.exists(), f"Config overrides test app not found at {app_path}"
    return app_path


@pytest.fixture
def config_overrides_job_config():
    """Build job config with nested override values."""
    test_id = str(uuid.uuid4())[:8]
    secret_values = {
        "service": {"api_token": f"svc-{test_id}-A1"},
        "database": {"password": f"db-{test_id}-P9"},
        "integration": {"webhook_key": f"wh-{test_id}-K7"},
    }

    return {
        "job_name": f"systest-config-overrides-{test_id}",
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
class TestConfigOverrides:
    """System test for config override injection and YAML reference resolution."""

    def test_config_overrides_round_trip(
        self,
        platform_client,
        app_packager,
        config_overrides_app_path,
        config_overrides_job_config,
        stdout_validator,
    ):
        api_client, platform_name = platform_client

        validator = stdout_validator
        job_config = config_overrides_job_config
        from tests.system.test_helpers import apply_env_config_overrides

        job_config = apply_env_config_overrides(job_config, platform_name)
        job_id = None

        print(f"\n🎯 [{platform_name.upper()}] Testing config overrides round-trip")

        app_files = app_packager.prepare_app_files(str(config_overrides_app_path))
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
                "Config override substitution checks failed. "
                f"Validation results: {validation_results}"
            )
            assert completion_result[
                "passed"
            ], f"Config override substitution test did not complete successfully: {completion_result}"

            print("✅ Config overrides round-trip validated")

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


@pytest.mark.system
@pytest.mark.slow
class TestPlatformSecretProvider:
    """System test for platform secret resolution via @secret references."""

    def test_platform_secret_provider_round_trip(
        self,
        platform_client,
        app_packager,
        config_overrides_app_path,
    ):
        api_client, platform_name = platform_client

        test_id = str(uuid.uuid4())[:8]
        app_name = f"config-secrets-test-app-{test_id}"

        def _extract_secret_name(secret_ref: str) -> str:
            if secret_ref.startswith("@secret "):
                return secret_ref[len("@secret ") :].strip()
            if secret_ref.startswith("@secret:"):
                return secret_ref[len("@secret:") :].strip()
            return secret_ref

        platform_secrets_config = {
            "service": {
                "api_token": f"@secret:kindling-systest-{test_id}-service-token",
            },
            "database": {
                "password": f"@secret:kindling-systest-{test_id}-db-password",
            },
            "integration": {
                "webhook_key": f"@secret:kindling-systest-{test_id}-webhook-key",
            },
        }
        if platform_name == "databricks":
            platform_secrets_config["secret_scope"] = "kindling-system-tests"

        job_config = {
            "job_name": f"systest-platform-secrets-{test_id}",
            "app_name": app_name,
            "entry_point": "main.py",
            "test_id": test_id,
            "config_overrides": {
                "kindling": {
                    "secrets": platform_secrets_config
                }
            },
        }

        from tests.system.test_helpers import apply_env_config_overrides

        job_config = apply_env_config_overrides(job_config, platform_name)

        secrets_config = ((job_config.get("config_overrides") or {}).get("kindling") or {}).get(
            "secrets", {}
        )
        if platform_name in {"fabric", "synapse"}:
            has_runtime_provider = bool(
                secrets_config.get("linked_service") or secrets_config.get("key_vault_url")
            )
            assert has_runtime_provider, (
                f"Missing runtime secret provider config for {platform_name}. "
                f"Set CONFIG__platform_{platform_name}__kindling__secrets__linked_service "
                f"or CONFIG__platform_{platform_name}__kindling__secrets__key_vault_url."
            )

        secret_refs = {
            "service_token": job_config["config_overrides"]["kindling"]["secrets"]["service"][
                "api_token"
            ],
            "database_password": job_config["config_overrides"]["kindling"]["secrets"]["database"][
                "password"
            ],
            "integration_key": job_config["config_overrides"]["kindling"]["secrets"]["integration"][
                "webhook_key"
            ],
        }
        secret_refs = {key: _extract_secret_name(value) for key, value in secret_refs.items()}
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

        from tests.system.test_helpers import create_stdout_validator

        validator = create_stdout_validator(api_client)
        job_id = None

        print(f"\n🎯 [{platform_name.upper()}] Testing platform @secret provider round-trip")

        app_files = app_packager.prepare_app_files(str(config_overrides_app_path))
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
            if final_status not in ["TERMINATED", "COMPLETED", "SUCCESS"]:
                failure_detail = (
                    status.get("failureReason")
                    or status.get("error")
                    or status.get("message")
                    or status
                )
                stdout_tail = "\n".join(validator.captured_lines[-80:])
                pytest.fail(
                    f"Unexpected final status: {final_status}. "
                    f"failure_detail={failure_detail}\n"
                    f"--- stdout tail ---\n{stdout_tail}"
                )

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
                "Platform secret-provider checks failed. "
                f"Validation results: {validation_results}"
            )
            assert completion_result[
                "passed"
            ], f"Platform secret-provider test did not complete successfully: {completion_result}"

            print("✅ Platform secret-provider round-trip validated")
        finally:
            for key in ("service_token", "database_password", "integration_key"):
                try:
                    api_client.delete_secret(secret_refs[key], secret_config=secrets_config)
                except Exception as e:
                    print(f"⚠️  Failed to delete secret {secret_refs[key]}: {e}")

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
