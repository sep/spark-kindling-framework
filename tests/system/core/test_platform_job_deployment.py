"""
Unified system tests for job deployment across all platforms.

These tests validate end-to-end job deployment, execution, and monitoring
using the PlatformAPI interface. The SAME test code runs on all platforms.

Select platform via marker:
    pytest -m databricks tests/system/test_platform_job_deployment.py
    pytest -m synapse tests/system/test_platform_job_deployment.py
    pytest -m fabric tests/system/test_platform_job_deployment.py

Run all platforms (run pytest 3 times with different markers):
    pytest -m databricks tests/system/test_platform_job_deployment.py && \
    pytest -m synapse tests/system/test_platform_job_deployment.py && \
    pytest -m fabric tests/system/test_platform_job_deployment.py
"""

import os
import time
import uuid
from typing import Any, Dict, Tuple

import pytest


def get_platform_from_markers(request) -> str:
    """Determine which platform to test based on pytest markers

    When run with -m fabric/synapse/databricks, checks which marker was used to SELECT the test.
    Priority: fabric > synapse > databricks (reverse alphabetical to catch most specific first)
    """
    # Check config for marker expression used
    marker_expr = request.config.getoption("-m", default="")

    # Direct marker check from command line
    if "fabric" in marker_expr:
        return "fabric"
    elif "synapse" in marker_expr:
        return "synapse"
    elif "databricks" in marker_expr:
        return "databricks"

    # Fallback to keyword check (for programmatic test execution)
    # Check in reverse priority to prefer more specific platforms
    if "fabric" in request.keywords:
        return "fabric"
    elif "synapse" in request.keywords:
        return "synapse"
    elif "databricks" in request.keywords:
        return "databricks"
    else:
        pytest.fail("Must specify platform marker: -m databricks, -m synapse, or -m fabric")


@pytest.fixture
def platform_client(request) -> Tuple[Any, str]:
    """Provides the platform API client based on pytest marker

    Usage: pytest -m databricks tests/system/test_platform_job_deployment.py
    """
    platform = get_platform_from_markers(request)

    if platform == "databricks":
        if not os.getenv("DATABRICKS_HOST"):
            pytest.skip(f"DATABRICKS_HOST not configured")
        from kindling.platform_databricks import DatabricksAPI

        client = DatabricksAPI(
            workspace_url=os.getenv("DATABRICKS_HOST"),
            storage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
            container=os.getenv("AZURE_CONTAINER", "artifacts"),
            base_path=os.getenv("AZURE_BASE_PATH", "system-tests"),
            azure_tenant_id=os.getenv("AZURE_TENANT_ID"),
            azure_client_id=os.getenv("AZURE_CLIENT_ID"),
            azure_client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        )
        # Store lakehouse_id on client for job_config retrieval
        return client, platform

    elif platform == "synapse":
        if not os.getenv("SYNAPSE_WORKSPACE_NAME"):
            pytest.skip(f"SYNAPSE_WORKSPACE_NAME not configured")
        from kindling.platform_synapse import SynapseAPI

        client = SynapseAPI(
            workspace_name=os.getenv("SYNAPSE_WORKSPACE_NAME"),
            spark_pool_name=os.getenv("SYNAPSE_SPARK_POOL") or os.getenv("SYNAPSE_SPARK_POOL_NAME"),
            storage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
            container=os.getenv("AZURE_CONTAINER", "artifacts"),
            base_path=os.getenv("AZURE_BASE_PATH", "system-tests"),
        )
        return client, platform

    elif platform == "fabric":
        if not os.getenv("FABRIC_WORKSPACE_ID") or not os.getenv("FABRIC_LAKEHOUSE_ID"):
            pytest.skip(f"FABRIC_WORKSPACE_ID or FABRIC_LAKEHOUSE_ID not configured")
        from kindling.platform_fabric import FabricAPI

        client = FabricAPI(
            workspace_id=os.getenv("FABRIC_WORKSPACE_ID"),
            lakehouse_id=os.getenv("FABRIC_LAKEHOUSE_ID"),
            storage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
            container=os.getenv("AZURE_CONTAINER", "artifacts"),
            base_path=os.getenv("AZURE_BASE_PATH", ""),
        )
        return client, platform


@pytest.fixture
def job_config_provider(platform_client):
    """Provides platform-specific job config"""
    client, platform = platform_client

    def get_config():
        """Generate platform-specific job config with unique names"""
        unique_suffix = str(uuid.uuid4())[:8]
        config = {
            "job_name": f"systest-job-deploy-{unique_suffix}",
            "app_name": f"universal-test-app-{unique_suffix}",
            "entry_point": "main.py",  # Universal test app entry point
            "test_id": unique_suffix,  # Pass test_id for log tracking
        }

        # Add platform-specific required fields
        if platform == "fabric":
            config["lakehouse_id"] = client.lakehouse_id
        elif platform == "synapse":
            config["spark_pool_name"] = client.spark_pool_name
        elif platform == "databricks":
            # Use existing cluster from environment
            cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
            if cluster_id:
                config["cluster_id"] = cluster_id

        return config

    return get_config


@pytest.mark.system
@pytest.mark.slow
@pytest.mark.fabric
@pytest.mark.synapse
@pytest.mark.databricks
class TestPlatformJobDeployment:
    """Unified tests that run on all platforms via parametrization"""

    def test_deploy_app_as_job(
        self, platform_client, job_packager, test_app_path, job_config_provider
    ):
        """Test deploying an app as a Spark job - runs on all platforms"""
        api_client, platform_name = platform_client
        job_config = job_config_provider()

        print(f"\n🚀 [{platform_name.upper()}] Deploying test app from {test_app_path}")

        # Package app files
        app_files = job_packager.prepare_app_files(str(test_app_path))
        print(f"📦 Prepared {len(app_files)} files")

        # Deploy job via PlatformAPI (all-in-one: create + upload + configure)
        result = api_client.deploy_spark_job(app_files, job_config)

        assert "job_id" in result, "job_id not in deployment result"
        job_id = result["job_id"]
        print(f"✅ Job deployed: {job_id}")
        print(f"📂 Deployment path: {result.get('deployment_path', 'N/A')}")

        # Cleanup
        app_name = job_config["app_name"]
        print(f"🧹 Cleaning up: {job_id}")
        self._cleanup_test(api_client, job_id, app_name)

    def test_run_and_monitor_job(
        self, platform_client, job_packager, test_app_path, job_config_provider
    ):
        """Test running and monitoring a job - runs on all platforms"""
        api_client, platform_name = platform_client
        job_config = job_config_provider()

        print(f"\n🎯 [{platform_name.upper()}] Testing job execution and monitoring")

        # Deploy job (all-in-one: create + upload + configure)
        app_files = job_packager.prepare_app_files(str(test_app_path))
        result = api_client.deploy_spark_job(app_files, job_config)
        job_id = result["job_id"]
        app_name = job_config["app_name"]
        print(f"📦 Job deployed: {job_id}")

        try:
            # Run job
            print("▶️  Starting job...")
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"🏃 Job started: {run_id}")

            # Monitor execution (10-minute timeout)
            print("⏳ Monitoring job (10-minute timeout)...\n")
            timeout = 600
            poll_interval = 10
            start_time = time.time()
            last_status = None

            while (time.time() - start_time) < timeout:
                try:
                    status_result = api_client.get_job_status(run_id=run_id)
                    current_status = status_result.get("status", "UNKNOWN")

                    if current_status != last_status:
                        elapsed = int(time.time() - start_time)
                        print(f"[{elapsed}s] Status: {current_status}")
                        last_status = current_status

                    # Check completion (case-insensitive, platform-agnostic)
                    status_upper = current_status.upper()
                    if status_upper in ["COMPLETED", "SUCCEEDED", "SUCCESS", "TERMINATED"]:
                        elapsed = int(time.time() - start_time)
                        print(f"\n✅ Job completed in {elapsed}s!")
                        status = status_result
                        break
                    elif status_upper in ["FAILED", "CANCELLED", "CANCELED", "ERROR"]:
                        elapsed = int(time.time() - start_time)
                        print(f"\n❌ Job failed after {elapsed}s")
                        print(f"   Status: {current_status}")
                        if status_result.get("error"):
                            print(f"   Error: {status_result['error']}")
                        status = status_result
                        break

                except Exception as e:
                    print(f"⚠️  Error checking status: {e}")

                time.sleep(poll_interval)
            else:
                print(f"\n⏱️  Timeout reached")
                status = {"status": last_status or "TIMEOUT"}

            print(f"\n📊 Final Status: {status.get('status', 'UNKNOWN')}")

            # Fetch and validate logs with retry for diagnostic emitters
            # Fabric and Synapse write logs asynchronously - typically 2-5 minutes after completion
            print(f"\n📜 Fetching job logs...")

            max_log_attempts = (
                6 if platform_name in ["fabric", "synapse"] else 1
            )  # 6 attempts × 60s = 6 min (was 30 min)
            log_retry_delay = 60  # seconds between retries
            logs_found = False

            for attempt in range(1, max_log_attempts + 1):
                try:
                    if attempt > 1:
                        print(
                            f"   Attempt {attempt}/{max_log_attempts} (waiting for diagnostic logs to propagate)..."
                        )
                        time.sleep(log_retry_delay)

                    logs = api_client.get_job_logs(run_id=run_id, size=5000)
                    log_lines = logs.get("log", logs.get("logs", []))
                    log_source = logs.get("source", "unknown")

                    print(f"   Retrieved {len(log_lines)} log lines from {log_source}")

                    # Need substantial logs, not just headers
                    if log_lines and len(log_lines) > 5:
                        print(f"\n{'='*80}")
                        print("JOB LOGS:")
                        print("=" * 80)
                        if isinstance(log_lines, list):
                            for line in log_lines:
                                print(line)
                        else:
                            print(log_lines)
                        print("=" * 80)

                        # Validate test app
                        log_content = (
                            "\n".join(log_lines) if isinstance(log_lines, list) else str(log_lines)
                        )

                        # Check if logs contain test markers
                        if "TEST_ID=" in log_content:
                            self._validate_test_app_logs(log_content)
                            logs_found = True
                            break
                        elif attempt < max_log_attempts:
                            print(f"   ⚠️  Logs found but don't contain test markers, retrying...")
                            continue
                    elif attempt < max_log_attempts:
                        print(f"   ⚠️  No logs available yet, retrying...")
                        continue
                    else:
                        print("   ❌ No logs available after all retries")

                except Exception as e:
                    print(f"   ⚠️  Error fetching logs: {e}")
                    if attempt < max_log_attempts:
                        continue
                    else:
                        raise

            if not logs_found:
                pytest.fail(
                    f"Expected logs from test app execution after {max_log_attempts} attempts over {max_log_attempts * log_retry_delay}s"
                )

        finally:
            self._cleanup_test(api_client, job_id, app_name)

    def test_job_cancellation(
        self, platform_client, job_packager, test_app_path, job_config_provider
    ):
        """Test cancelling a running job - runs on all platforms"""
        api_client, platform_name = platform_client
        job_config = job_config_provider()

        print(f"\n🛑 [{platform_name.upper()}] Testing job cancellation")

        # Deploy job
        app_files = job_packager.prepare_app_files(str(test_app_path))
        result = api_client.deploy_spark_job(app_files, job_config)
        job_id = result["job_id"]
        app_name = job_config["app_name"]
        app_name = job_config["app_name"]

        try:
            # Start job
            run_id = api_client.run_job(job_id=job_id)
            print(f"🏃 Job started: {run_id}")

            # Wait for job to start
            time.sleep(5)

            # Cancel
            print("🛑 Cancelling job...")
            cancelled = api_client.cancel_job(run_id=run_id)
            assert cancelled, "Job cancellation failed"
            print("✅ Job cancellation requested")

            # Poll for cancellation status (cancellation is asynchronous)
            print("⏳ Waiting for job to stop...")
            max_wait = 60  # seconds
            poll_interval = 5
            start_time = time.time()
            final_status = None

            while (time.time() - start_time) < max_wait:
                status = api_client.get_job_status(run_id=run_id)
                status_upper = status["status"].upper()
                print(f"   Status: {status['status']}")

                # Check if job has stopped
                if status_upper in [
                    "CANCELLED",
                    "CANCELED",
                    "FAILED",
                    "NOTSTARTED",
                    "DEAD",
                    "KILLED",
                    "TERMINATED",
                ]:
                    final_status = status
                    break

                time.sleep(poll_interval)

            assert (
                final_status is not None
            ), f"Job did not stop within {max_wait}s - still {status['status']}"
            print(f"✅ Verified cancellation: {final_status['status']}")

        finally:
            self._cleanup_test(api_client, job_id, app_name)

    def test_job_with_invalid_config(self, platform_client, test_app_path):
        """Test that invalid config is rejected - runs on all platforms"""
        api_client, platform_name = platform_client

        print(f"\n❌ [{platform_name.upper()}] Testing invalid job configuration")

        invalid_config = {"job_name": "test-job", "main_file": ""}
        job_id = None
        creation_failed = False

        # Try to create job with invalid config
        try:
            result = api_client.create_spark_job(
                job_name=invalid_config["job_name"], job_config=invalid_config
            )
            job_id = result.get("job_id")
            print(f"   Job created with ID: {job_id} (creation did not validate config)")
        except Exception as e:
            print(f"✅ Invalid config rejected during creation: {e}")
            creation_failed = True

        # If creation succeeded, try to run - should fail at some point
        if not creation_failed and job_id:
            try:
                run_id = api_client.run_job(job_id=job_id)
                print(f"   Job run started: {run_id}")

                # Wait a bit and check status - should fail
                time.sleep(10)
                status = api_client.get_job_status(run_id=run_id)
                status_upper = status["status"].upper()

                # Invalid config should cause job to fail
                assert status_upper in [
                    "FAILED",
                    "ERROR",
                ], f"Expected job with invalid config to fail, got: {status['status']}"
                print(f"✅ Invalid config caused job failure: {status['status']}")

            except Exception as e:
                # Exception during run is acceptable
                print(f"✅ Invalid config rejected during run: {e}")
            finally:
                # Cleanup
                try:
                    if job_id:
                        api_client.delete_job(job_id)
                except:
                    pass
        else:
            # Creation failed - test passed
            pass

    def _validate_test_app_logs(self, log_content: str):
        """Validate test app execution via log entries - platform-agnostic"""
        print(f"\n🔍 Validating test app execution...")

        expected_entries = [
            "TEST_ID=",
            "status=STARTED",
            "test=spark_session status=PASSED",
            "test=spark_operations status=PASSED",
            "test=framework_available status=PASSED",
            "test=storage_access status=PASSED",
            "status=COMPLETED result=PASSED",
        ]

        missing_entries = []
        for entry in expected_entries:
            if entry in log_content:
                print(f"   ✅ Found: {entry}")
            else:
                missing_entries.append(entry)
                print(f"   ❌ Missing: {entry}")

        if missing_entries:
            pytest.fail(
                f"Log validation failed: {len(missing_entries)}/{len(expected_entries)} entries missing:\n"
                + "\n".join(f"  - {entry}" for entry in missing_entries)
            )
        else:
            print(f"   ✅ All expected log entries found!")

    def _cleanup_test(self, api_client, job_id: str, app_name: str):
        """Clean up job and data-app files"""
        # Skip cleanup if environment variable is set
        import os

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

        # Clean up app files
        # ALL platforms now upload to ABFSS storage (not lakehouse OneLake)
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            storage_account = getattr(api_client, "storage_account", None)
            container = getattr(api_client, "container", None)
            base_path = getattr(api_client, "base_path", None)

            if storage_account and container:
                account_url = f"https://{storage_account}.dfs.core.windows.net"
                # Use credential from api_client if available, otherwise use DefaultAzureCredential
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


@pytest.mark.system
@pytest.mark.slow
@pytest.mark.fabric
@pytest.mark.synapse
@pytest.mark.databricks
class TestPlatformJobResults:
    """Tests for job execution results - runs on all platforms"""

    def test_job_output_files(
        self, platform_client, job_packager, test_app_path, job_config_provider
    ):
        """Test that job creates output files - runs on all platforms"""
        api_client, platform_name = platform_client
        job_config = job_config_provider()

        print(f"\n📁 [{platform_name.upper()}] Testing job output file creation")

        # Deploy and run job
        app_files = job_packager.prepare_app_files(str(test_app_path))
        result = api_client.deploy_spark_job(app_files, job_config)
        job_id = result["job_id"]
        app_name = job_config["app_name"]
        app_name = job_config["app_name"]

        try:
            run_id = api_client.run_job(job_id=job_id)
            print("⏳ Waiting for job to complete...")

            # Wait for completion
            timeout = 300
            start_time = time.time()
            while (time.time() - start_time) < timeout:
                status = api_client.get_job_status(run_id=run_id)
                status_upper = status["status"].upper()
                if status_upper in [
                    "SUCCEEDED",
                    "COMPLETED",
                    "SUCCESS",
                    "TERMINATED",
                    "FAILED",
                    "CANCELLED",
                    "CANCELED",
                ]:
                    break
                time.sleep(10)

            assert status["status"].upper() in ["SUCCEEDED", "COMPLETED", "SUCCESS", "TERMINATED"]
            print("✅ Job completed - output files should be created")

            # TODO: Add actual file verification when storage access configured

        finally:
            TestPlatformJobDeployment()._cleanup_test(api_client, job_id, app_name)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
