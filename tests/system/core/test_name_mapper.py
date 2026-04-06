"""
System test for default EntityNameMapper behavior across platforms.

Run on specific platform:
    poe test-system --platform fabric --test name_mapper
    poe test-system --platform databricks --test name_mapper
    poe test-system --platform synapse --test name_mapper

Run on all platforms:
    poe test-system --test name_mapper
"""

import uuid
from pathlib import Path

import pytest

from tests.system.test_helpers import (
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
)


@pytest.fixture
def name_mapper_app_path():
    app_path = Path(__file__).parent.parent.parent / "data-apps" / "name-mapper-test-app"
    if not app_path.exists():
        pytest.skip(f"Name mapper test app not found at {app_path}")
    return app_path


@pytest.fixture
def name_mapper_job_config():
    unique_suffix = f"t{str(uuid.uuid4())[:7]}"
    return {
        "job_name": f"systest-name-mapper-{unique_suffix}",
        "app_name": f"name-mapper-test-app-{unique_suffix}",
        "entry_point": "main.py",
        "test_id": unique_suffix,
    }


@pytest.mark.system
@pytest.mark.slow
class TestNameMapper:
    def test_default_name_mapper(
        self,
        platform_client,
        app_packager,
        name_mapper_app_path,
        name_mapper_job_config,
        stdout_validator,
    ):
        api_client, platform_name = platform_client
        test_id = name_mapper_job_config["test_id"]
        app_name = name_mapper_job_config["app_name"]

        print(f"\n🎯 [{platform_name.upper()}] Testing default EntityNameMapper")

        app_files = app_packager.prepare_app_files(str(name_mapper_app_path))
        storage_path = api_client.deploy_app(app_name, app_files)
        print(f"📂 App deployed to: {storage_path}")

        result = api_client.create_job(
            job_name=name_mapper_job_config["job_name"],
            job_config=name_mapper_job_config,
        )
        job_id = result["job_id"]
        print(f"📦 Job created: {job_id}")

        try:
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"🏃 Job started: {run_id}")

            try:
                stdout_validator.stream_with_callback(
                    job_id=job_id,
                    run_id=run_id,
                    print_lines=True,
                    poll_interval=get_system_test_poll_interval(10.0),
                    max_wait=get_system_test_stream_max_wait(600.0),
                )
            except Exception as e:
                print(f"⚠️  Stdout streaming error: {e}")

            status_result = api_client.get_job_status(run_id=run_id)
            final_status = str(status_result.get("status", "UNKNOWN")).upper()
            print(f"\n✅ Job completed with status: {final_status}")

            expected_tests = [
                "namespace_resolution",
                "two_part_name_mapping",
                "two_part_name_write",
            ]
            if platform_name in {"fabric", "databricks"}:
                expected_tests.extend(
                    [
                        "three_part_name_mapping",
                        "three_part_name_write",
                    ]
                )

            validation_results = stdout_validator.validate_tests(test_id, expected_tests)
            completion_result = stdout_validator.validate_completion(test_id)

            all_passed = all(r["passed"] for r in validation_results.values())
            assert all_passed, f"Validation failed: {validation_results}"
            assert completion_result["passed"], f"Completion failed: {completion_result}"
            assert final_status in [
                "TERMINATED",
                "COMPLETED",
                "SUCCESS",
            ], f"Unexpected final status: {final_status}. Validation: {validation_results}"
        finally:
            try:
                api_client.delete_job(job_id)
                print(f"✅ Deleted job: {job_id}")
            except Exception as e:
                print(f"⚠️  Failed to delete job {job_id}: {e}")

            try:
                api_client.cleanup_app(app_name)
                print(f"✅ Cleaned up app: {app_name}")
            except Exception as e:
                print(f"⚠️  Failed to cleanup app {app_name}: {e}")
