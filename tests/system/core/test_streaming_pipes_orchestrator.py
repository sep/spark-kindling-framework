"""
System tests for streaming pipeline with GenerationExecutor.

Tests the complete streaming data pipeline with the Unified DAG Orchestrator:
- Entity definitions and registration
- Pipe definitions with transformations
- GenerationExecutor for streaming execution
- Data flow through bronze → silver → gold layers

These tests deploy and run the streaming-pipes-test-app on actual platforms
to verify end-to-end streaming pipeline functionality.

Run on specific platform:
    poe test-system --platform fabric --test streaming_pipes
    poe test-system --platform databricks --test streaming_pipes
    poe test-system --platform synapse --test streaming_pipes

Run on all platforms:
    poe test-system --test streaming_pipes
"""

import time
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def streaming_pipes_app_path():
    """Fixture providing path to streaming pipes test app"""
    app_path = Path(__file__).parent.parent.parent / "data-apps" / "streaming-pipes-test-app"
    if not app_path.exists():
        pytest.skip(f"Streaming pipes test app not found at {app_path}")
    return app_path


@pytest.fixture
def streaming_pipes_job_config():
    """Provides job config for streaming pipes tests"""
    unique_suffix = str(uuid.uuid4())[:8]

    config = {
        "job_name": f"systest-streaming-pipes-{unique_suffix}",
        "app_name": f"streaming-pipes-test-app-{unique_suffix}",
        "entry_point": "main.py",
        "test_id": unique_suffix,
    }

    return config


@pytest.mark.system
@pytest.mark.slow
class TestStreamingPipesOrchestrator:
    """
    System tests for streaming pipeline with GenerationExecutor.

    Deploys and runs the streaming-pipes-test-app to verify:
    1. Entity definitions work in streaming context
    2. Pipe definitions process streams correctly
    3. GenerationExecutor manages streaming query lifecycle
    4. Data flows through bronze → silver → gold layers
    """

    def test_streaming_pipeline(
        self,
        platform_client,
        app_packager,
        streaming_pipes_app_path,
        streaming_pipes_job_config,
        stdout_validator,
    ):
        """Test complete streaming pipeline with GenerationExecutor."""
        api_client, platform_name = platform_client
        test_id = streaming_pipes_job_config["test_id"]
        app_name = streaming_pipes_job_config["app_name"]

        print(f"\n🎯 [{platform_name.upper()}] Testing streaming pipes orchestrator")

        # Step 1: Prepare app files and inject platform-specific config
        from tests.system.conftest import inject_platform_config

        app_files = app_packager.prepare_app_files(str(streaming_pipes_app_path))
        app_files = inject_platform_config(app_files, platform_name, test_id)
        print(f"✅ Injected {platform_name} config into app settings.yaml")

        # Step 2: Deploy app to storage with platform config baked in
        storage_path = api_client.deploy_app(app_name, app_files)
        print(f"📂 App deployed to: {storage_path}")

        # Step 2: Create job definition (independent of app)
        result = api_client.create_job(
            job_name=streaming_pipes_job_config["job_name"],
            job_config=streaming_pipes_job_config,
        )
        job_id = result["job_id"]
        print(f"📦 Job created: {job_id}")

        try:
            # Run job
            print("▶️  Starting streaming pipes test job...")
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"🏃 Job started: {run_id}")

            # Stream stdout in real-time
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

            # Check final job status
            status_result = api_client.get_job_status(run_id=run_id)
            final_status = status_result.get("status", "UNKNOWN").upper()
            print(f"\n✅ Job completed with status: {final_status}")

            # Validate results via stdout markers
            print("\n🔍 Validating test results from stdout...")

            expected_tests = [
                "entity_definitions",
                "pipe_definitions",
                "lookup_data",
                "executor_streaming",
                "bronze_data",
                "silver_data",
                "gold_data",
                "multi_input_lookup_join",
                "queries_stopped",
            ]

            validation_results = stdout_validator.validate_tests(test_id, expected_tests)

            # Print validation summary
            print("\n📊 Validation Results:")
            print(f"{'='*80}")
            for test_name, result in validation_results.items():
                status_icon = "✅" if result["passed"] else "❌"
                print(f"{status_icon} {test_name}: {result['status']}")
                if result.get("message"):
                    print(f"   └─ {result['message']}")
            print(f"{'='*80}\n")

            # Check overall completion
            completion_result = stdout_validator.validate_completion(test_id)
            print(f"\n🎯 Overall Test Result: {completion_result['status']}")

            # Assert
            all_passed = all(r["passed"] for r in validation_results.values())
            assert (
                all_passed
            ), f"Some tests failed. Check validation results above. Completion: {completion_result}"
            assert completion_result[
                "passed"
            ], f"Test did not complete successfully: {completion_result}"

            print("\n✅ All streaming pipes orchestrator tests PASSED!")

        finally:
            # Cleanup using platform API
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
