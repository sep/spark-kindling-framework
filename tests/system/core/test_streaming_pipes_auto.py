"""
System test: streaming pipes with auto clustering override.

Uses the same data-app as the baseline streaming pipes test, but injects config
to set Delta `cluster_columns` to "auto" for the clustering entities.

Run:
  poe test-system --platform fabric --test streaming_pipes_auto
  poe test-system --platform databricks --test streaming_pipes_auto
  poe test-system --platform synapse --test streaming_pipes_auto
"""

import uuid
from pathlib import Path

import pytest


def _deep_merge(base: dict, override: dict) -> dict:
    result = dict(base or {})
    for key, value in (override or {}).items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


@pytest.fixture
def streaming_pipes_app_path():
    app_path = Path(__file__).parent.parent.parent / "data-apps" / "streaming-pipes-test-app"
    if not app_path.exists():
        pytest.skip(f"Streaming pipes test app not found at {app_path}")
    return app_path


@pytest.fixture
def streaming_pipes_auto_job_config():
    unique_suffix = f"t{str(uuid.uuid4())[:7]}"
    return {
        "job_name": f"systest-streaming-pipes-auto-{unique_suffix}",
        "app_name": f"streaming-pipes-test-app-auto-{unique_suffix}",
        "entry_point": "main.py",
        "test_id": unique_suffix,
    }


@pytest.mark.system
@pytest.mark.slow
class TestStreamingPipesOrchestratorAuto:
    def test_streaming_pipes_auto(
        self,
        platform_client,
        app_packager,
        streaming_pipes_app_path,
        streaming_pipes_auto_job_config,
        stdout_validator,
    ):
        api_client, platform_name = platform_client
        test_id = streaming_pipes_auto_job_config["test_id"]
        app_name = streaming_pipes_auto_job_config["app_name"]

        print(f"\n🎯 [{platform_name.upper()}] Testing streaming pipes (auto clustering override)")

        from tests.system.conftest import inject_platform_config

        app_files = app_packager.prepare_app_files(str(streaming_pipes_app_path))
        app_files = inject_platform_config(app_files, platform_name, test_id)

        # Override clustering columns via app config.
        # The app reads these and sets cluster_columns accordingly.
        import yaml

        existing_config = yaml.safe_load(app_files.get("settings.yaml") or "") or {}
        override_config = {
            "kindling": {
                "system_tests": {
                    "streaming_pipes": {
                        "cluster_columns": "auto",
                        "cluster_columns_v2": "auto",
                    }
                }
            }
        }
        merged = _deep_merge(existing_config, override_config)
        app_files["settings.yaml"] = yaml.dump(merged, default_flow_style=False, sort_keys=False)
        print("✅ Injected auto clustering override into app settings.yaml")

        storage_path = api_client.deploy_app(app_name, app_files)
        print(f"📂 App deployed to: {storage_path}")

        result = api_client.create_job(
            job_name=streaming_pipes_auto_job_config["job_name"],
            job_config=streaming_pipes_auto_job_config,
        )
        job_id = result["job_id"]
        print(f"📦 Job created: {job_id}")

        try:
            print("▶️  Starting streaming pipes auto test job...")
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"🏃 Job started: {run_id}")

            print("\n📡 Streaming stdout in real-time...")
            print(f"{'='*80}\n")

            try:
                stdout_validator.stream_with_callback(
                    job_id=job_id,
                    run_id=run_id,
                    print_lines=True,
                    poll_interval=10.0,
                    max_wait=600.0,
                )
            except Exception as e:
                print(f"⚠️  Stdout streaming error: {e}")

            print(f"\n{'='*80}")

            status_result = api_client.get_job_status(run_id=run_id)
            final_status = status_result.get("status", "UNKNOWN").upper()
            print(f"\n✅ Job completed with status: {final_status}")

            print("\n🔍 Validating test results from stdout...")
            expected_tests = [
                "entity_definitions",
                "pipe_definitions",
                "delta_partitioning",
                "delta_clustering",
                "delta_clustering_update",
                "lookup_data",
                "executor_streaming",
                "bronze_data",
                "silver_data",
                "gold_data",
                "multi_input_lookup_join",
                "queries_stopped",
            ]

            validation_results = stdout_validator.validate_tests(test_id, expected_tests)

            print("\n📊 Validation Results:")
            print(f"{'='*80}")
            for test_name, result in validation_results.items():
                status_icon = "✅" if result["passed"] else "❌"
                print(f"{status_icon} {test_name}: {result['status']}")
                if result.get("message"):
                    print(f"   └─ {result['message']}")
            print(f"{'='*80}\n")

            completion_result = stdout_validator.validate_completion(test_id)
            print(f"\n🎯 Overall Test Result: {completion_result['status']}")

            all_passed = all(r["passed"] for r in validation_results.values())
            assert (
                all_passed
            ), f"Some tests failed. Check validation results above. Completion: {completion_result}"
            assert completion_result[
                "passed"
            ], f"Test did not complete successfully: {completion_result}"

            print("\n✅ All streaming pipes auto-clustering tests PASSED!")

        finally:
            print(f"\n🧹 Cleaning up test resources...")
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

