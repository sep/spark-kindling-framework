"""
System tests for streaming components.

Tests the entire streaming stack working together by deploying and running
a test app on actual platforms (Fabric, Databricks, Synapse):
- KindlingStreamingListener (event capture)
- StreamingQueryManager (lifecycle management)
- StreamingHealthMonitor (health detection)
- StreamingRecoveryManager (auto-recovery)

These tests use the same pattern as test_platform_job_deployment.py:
- Deploy test app as a Spark job
- Run job and stream stdout
- Validate execution from stdout markers
- Clean up resources

Run on specific platform:
    pytest -m fabric tests/system/core/test_streaming_system.py
    pytest -m databricks tests/system/core/test_streaming_system.py
    pytest -m synapse tests/system/core/test_streaming_system.py
"""

import os
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def streaming_test_app_path():
    """Fixture providing path to streaming test app"""
    app_path = Path(__file__).parent.parent.parent / "data-apps" / "streaming-test-app"
    if not app_path.exists():
        pytest.skip(f"Streaming test app not found at {app_path}")
    return app_path


@pytest.fixture
def streaming_job_config():
    """Provides job config for streaming tests (platform APIs handle their own required fields)"""
    unique_suffix = str(uuid.uuid4())[:8]

    config = {
        "job_name": f"systest-streaming-{unique_suffix}",
        "app_name": f"streaming-test-app-{unique_suffix}",
        "entry_point": "main.py",
        "test_id": unique_suffix,
    }

    return config


@pytest.mark.system
@pytest.mark.slow
@pytest.mark.parametrize("platform", ["fabric", "databricks", "synapse"])
class TestStreamingSystemIntegration:
    """
    System tests for integrated streaming components.

    These tests deploy and run a streaming test app on actual platforms
    to verify end-to-end functionality.
    """

    def test_complete_streaming_lifecycle(
        self,
        platform_client,
        job_packager,
        streaming_test_app_path,
        streaming_job_config,
        stdout_validator,
    ):
        """
        Test complete streaming lifecycle with all components.

        This test verifies:
        1. StreamingListener captures events from Spark
        2. StreamingQueryManager manages query lifecycle
        3. StreamingHealthMonitor tracks query health
        4. Signals flow correctly between components
        """
        api_client, platform_name = platform_client
        test_id = streaming_job_config["test_id"]

        print(f"\n🎯 [{platform_name.upper()}] Testing streaming system lifecycle")

        # Deploy job
        app_files = job_packager.prepare_app_files(str(streaming_test_app_path))
        result = api_client.deploy_spark_job(app_files, streaming_job_config)
        job_id = result["job_id"]
        app_name = streaming_job_config["app_name"]
        print(f"📦 Job deployed: {job_id}")

        try:
            # Run job
            print("▶️  Starting streaming test job...")
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"🏃 Job started: {run_id}")

            # Stream stdout in real-time
            print("\n📡 Streaming stdout in real-time...")
            print("=" * 80)

            try:
                stdout_validator.stream_with_callback(
                    job_id=job_id,
                    run_id=run_id,
                    print_lines=True,
                    poll_interval=10.0,
                    max_wait=600.0,
                )
                print("=" * 80)
            except Exception as e:
                print(f"⚠️  Stdout streaming error: {e}")
                import traceback

                traceback.print_exc()

            # Check final job status
            status_result = api_client.get_job_status(run_id=run_id)
            final_status = status_result.get("status", "UNKNOWN")
            print(f"\n📊 Final Status: {final_status}")

            # Validate streaming execution from stdout
            print("\n📋 Validating streaming execution from stdout...")

            # Get captured stdout
            from tests.system.test_helpers import get_captured_stdout

            stdout_lines = get_captured_stdout(stdout_validator)
            stdout_content = "\n".join(stdout_lines)

            # Define expected markers for streaming tests
            expected_markers = [
                f"TEST_ID={test_id} status=STARTED",
                f"TEST_ID={test_id} test=spark_session status=PASSED",
                f"TEST_ID={test_id} test=di_services status=PASSED",
                f"TEST_ID={test_id} test=streaming_listener status=STARTED",
                f"TEST_ID={test_id} test=query_manager status=CREATED",
                f"TEST_ID={test_id} test=health_monitor status=STARTED",
                f"TEST_ID={test_id} test=recovery_manager status=STARTED",
                f"TEST_ID={test_id} test=components_started status=PASSED",
                f"TEST_ID={test_id} test=query_registration status=PASSED",
                f"TEST_ID={test_id} test=query_start status=PASSED",
                f"TEST_ID={test_id} test=listener_events status=PASSED",
                f"TEST_ID={test_id} test=query_stop status=PASSED",
                f"TEST_ID={test_id} status=COMPLETED result=PASSED",
            ]

            missing_markers = []
            found_markers = []

            for marker in expected_markers:
                if marker in stdout_content:
                    found_markers.append(marker)
                    print(f"   ✅ Found: {marker}")
                else:
                    missing_markers.append(marker)
                    print(f"   ❌ Missing: {marker}")

            # Check for signal emissions
            signal_markers = [
                "signal=streaming.query_started",
                "signal=streaming.spark_query_started",
            ]

            signal_found = []
            for marker in signal_markers:
                if marker in stdout_content:
                    signal_found.append(marker)
                    print(f"   ✅ Signal: {marker}")

            # Assert critical validations
            assert (
                f"TEST_ID={test_id} status=STARTED" in stdout_content
            ), "Streaming test did not start"
            assert (
                f"TEST_ID={test_id} test=components_started status=PASSED" in stdout_content
            ), "Streaming components did not start"
            assert (
                f"TEST_ID={test_id} test=query_start status=PASSED" in stdout_content
            ), "Streaming query did not start"
            assert (
                f"TEST_ID={test_id} test=listener_events status=PASSED" in stdout_content
            ), "Streaming listener did not process events"
            assert (
                f"TEST_ID={test_id} status=COMPLETED result=PASSED" in stdout_content
            ), "Streaming test did not complete successfully"

            # Verify at least one signal was emitted
            assert len(signal_found) > 0, "No streaming signals were emitted"

            print(f"\n✅ All streaming validations passed!")
            print(f"   - Found {len(found_markers)}/{len(expected_markers)} markers")
            print(f"   - Found {len(signal_found)}/{len(signal_markers)} signals")

        finally:
            self._cleanup_test(api_client, job_id, app_name)

    def test_streaming_health_monitoring(
        self,
        platform_client,
        job_packager,
        streaming_test_app_path,
        streaming_job_config,
        stdout_validator,
    ):
        """
        Test health monitoring integration.

        This test verifies:
        1. Health monitor tracks query health
        2. Health status updates based on progress
        """
        api_client, platform_name = platform_client
        test_id = streaming_job_config["test_id"]

        print(f"\n🏥 [{platform_name.upper()}] Testing streaming health monitoring")

        # Deploy and run job
        app_files = job_packager.prepare_app_files(str(streaming_test_app_path))
        result = api_client.deploy_spark_job(app_files, streaming_job_config)
        job_id = result["job_id"]
        app_name = streaming_job_config["app_name"]

        try:
            run_id = api_client.run_job(job_id=job_id)
            print(f"🏃 Job started: {run_id}")

            # Stream stdout
            stdout_validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=10.0,
                max_wait=600.0,
            )

            # Validate health monitoring from stdout
            from tests.system.test_helpers import get_captured_stdout

            stdout_content = "\n".join(get_captured_stdout(stdout_validator))

            assert (
                f"TEST_ID={test_id} test=health_monitor status=STARTED" in stdout_content
            ), "Health monitor did not start"
            assert (
                f"TEST_ID={test_id} test=health_monitoring status=PASSED" in stdout_content
            ), "Health monitoring did not track query"

            print(f"\n✅ Health monitoring validated!")

        finally:
            self._cleanup_test(api_client, job_id, app_name)

    def test_streaming_signal_flow(
        self,
        platform_client,
        job_packager,
        streaming_test_app_path,
        streaming_job_config,
        stdout_validator,
    ):
        """
        Test signal flow through the streaming stack.

        This test verifies:
        1. Signals originate from StreamingListener
        2. Signals propagate through components
        3. Signal handlers receive events
        """
        api_client, platform_name = platform_client
        test_id = streaming_job_config["test_id"]

        print(f"\n📡 [{platform_name.upper()}] Testing streaming signal flow")

        # Deploy and run job
        app_files = job_packager.prepare_app_files(str(streaming_test_app_path))
        result = api_client.deploy_spark_job(app_files, streaming_job_config)
        job_id = result["job_id"]
        app_name = streaming_job_config["app_name"]

        try:
            run_id = api_client.run_job(job_id=job_id)
            print(f"🏃 Job started: {run_id}")

            # Stream stdout
            stdout_validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=10.0,
                max_wait=600.0,
            )

            # Validate signal flow from stdout
            from tests.system.test_helpers import get_captured_stdout

            stdout_content = "\n".join(get_captured_stdout(stdout_validator))

            # Check for signal emissions
            expected_signals = [
                "signal=streaming.query_started",
                "signal=streaming.spark_query_started",
            ]

            found_signals = []
            for signal in expected_signals:
                if signal in stdout_content:
                    found_signals.append(signal)
                    print(f"   ✅ Signal emitted: {signal}")

            assert (
                len(found_signals) >= 1
            ), f"Expected at least 1 signal, found {len(found_signals)}"

            print(f"\n✅ Signal flow validated! ({len(found_signals)} signals found)")

        finally:
            self._cleanup_test(api_client, job_id, app_name)

    def _cleanup_test(self, api_client, job_id: str, app_name: str):
        """Clean up job and data-app files"""
        # Skip cleanup if environment variable is set
        if os.environ.get("SKIP_TEST_CLEANUP", "").lower() == "true":
            print(f"⏸️  Skipping cleanup (SKIP_TEST_CLEANUP=true) - job: {job_id}, app: {app_name}")
            return

        # Clean up job
        try:
            success = api_client.delete_job(job_id=job_id)
            if success:
                print(f"✅ Deleted job: {job_id}")
        except Exception as e:
            print(f"⚠️  Error deleting job: {e}")

        # Clean up app files from ABFSS storage
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            storage_account = getattr(api_client, "storage_account", None)
            container = getattr(api_client, "container", None)
            base_path = getattr(api_client, "base_path", None)

            if storage_account and container:
                account_url = f"https://{storage_account}.dfs.core.windows.net"
                credential = getattr(api_client, "credential", None)
                if credential is None:
                    credential = DefaultAzureCredential()

                storage_client = DataLakeServiceClient(
                    account_url=account_url, credential=credential
                )
                file_system_client = storage_client.get_file_system_client(file_system=container)

                target_path = f"data-apps/{app_name}"
                full_path = f"{base_path}/{target_path}" if base_path else target_path

                directory_client = file_system_client.get_directory_client(full_path)
                directory_client.delete_directory()
                print(f"🗑️  Deleted data-app from ABFSS: {full_path}")
        except Exception as e:
            print(f"⚠️  Error deleting data-app: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
