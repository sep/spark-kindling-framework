"""System test for StreamingOrchestrator as the primary streaming entrypoint."""

import os
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def streaming_orchestrator_test_app_path():
    app_path = Path(__file__).parent.parent.parent / "data-apps" / "streaming-orchestrator-test-app"
    if not app_path.exists():
        pytest.skip(f"Streaming orchestrator test app not found at {app_path}")
    return app_path


@pytest.fixture
def streaming_orchestrator_job_config():
    unique_suffix = str(uuid.uuid4())[:8]
    return {
        "job_name": f"systest-streaming-orchestrator-{unique_suffix}",
        "app_name": f"streaming-orchestrator-test-app-{unique_suffix}",
        "entry_point": "main.py",
        "test_id": unique_suffix,
    }


@pytest.mark.system
@pytest.mark.slow
class TestStreamingOrchestratorIntegration:
    def test_orchestrator_run_owns_stream_lifecycle(
        self,
        platform_client,
        app_packager,
        streaming_orchestrator_test_app_path,
        streaming_orchestrator_job_config,
        stdout_validator,
    ):
        api_client, platform_name = platform_client
        test_id = streaming_orchestrator_job_config["test_id"]
        app_name = streaming_orchestrator_job_config["app_name"]

        print(f"\n🎼 [{platform_name.upper()}] Testing StreamingOrchestrator.run lifecycle")

        app_files = app_packager.prepare_app_files(str(streaming_orchestrator_test_app_path))
        api_client.deploy_app(app_name, app_files)
        result = api_client.create_job(
            job_name=streaming_orchestrator_job_config["job_name"],
            job_config=streaming_orchestrator_job_config,
        )
        job_id = result["job_id"]
        print(f"📦 App deployed and job created: {job_id}")

        try:
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"🏃 Job started: {run_id}")

            stdout_validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=10.0,
                max_wait=600.0,
            )

            status_result = api_client.get_job_status(run_id=run_id)
            final_status = status_result.get("status", "UNKNOWN")
            print(f"\n📊 Final Status: {final_status}")

            from tests.system.test_helpers import get_captured_stdout

            stdout_content = "\n".join(get_captured_stdout(stdout_validator))
            expected_markers = [
                f"TEST_ID={test_id} status=STARTED component=streaming_orchestrator",
                f"TEST_ID={test_id} test=spark_session status=PASSED",
                f"TEST_ID={test_id} test=entity_definitions status=PASSED",
                f"TEST_ID={test_id} test=pipe_definitions status=PASSED",
                f"TEST_ID={test_id} test=entity_bootstrap status=PASSED",
                f"TEST_ID={test_id} test=plan_generation status=PASSED",
                f"TEST_ID={test_id} signal=streaming.orchestrator_started received=true",
                f"TEST_ID={test_id} signal=streaming.query_started received=true",
                f"TEST_ID={test_id} test=controller_stop status=PASSED",
                f"TEST_ID={test_id} test=source_batches status=PASSED",
                f"TEST_ID={test_id} test=orchestrator_run status=PASSED",
                f"TEST_ID={test_id} test=orchestrator_signals status=PASSED",
                f"TEST_ID={test_id} status=COMPLETED result=PASSED",
            ]

            missing = [marker for marker in expected_markers if marker not in stdout_content]
            for marker in expected_markers:
                print(f"   {'✅' if marker not in missing else '❌'} {marker}")

            assert not missing, f"Missing orchestrator markers: {missing}"

        finally:
            self._cleanup_test(api_client, job_id, app_name)

    def _cleanup_test(self, api_client, job_id: str, app_name: str):
        if os.environ.get("SKIP_TEST_CLEANUP", "").lower() == "true":
            print(f"⏸️  Skipping cleanup (SKIP_TEST_CLEANUP=true) - job: {job_id}, app: {app_name}")
            return

        try:
            success = api_client.delete_job(job_id=job_id)
            if success:
                print(f"✅ Deleted job: {job_id}")
        except Exception as e:
            print(f"⚠️  Error deleting job: {e}")

        try:
            success = api_client.cleanup_app(app_name)
            if success:
                print(f"🗑️  Cleaned up app: {app_name}")
        except Exception as e:
            print(f"⚠️  Error cleaning up app: {e}")
