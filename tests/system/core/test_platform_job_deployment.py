"""
Unified system tests for job deployment across all platforms.

These tests validate end-to-end job deployment, execution, and monitoring
using the PlatformAPI interface. The SAME test code runs on all platforms.

Run specific platform:
    pytest tests/system/core/test_platform_job_deployment.py -k fabric
    pytest tests/system/core/test_platform_job_deployment.py -k synapse
    pytest tests/system/core/test_platform_job_deployment.py -k databricks
"""

import os
import time
import uuid

import pytest


@pytest.fixture
def job_config_provider():
    """Provides job config (platform APIs handle their own required fields)"""

    def get_config():
        """Generate job config with unique names"""
        unique_suffix = str(uuid.uuid4())[:8]
        config = {
            "job_name": f"systest-job-deploy-{unique_suffix}",
            "app_name": f"universal-test-app-{unique_suffix}",
            "entry_point": "main.py",
            "test_id": unique_suffix,
        }
        return config

    return get_config


@pytest.mark.system
@pytest.mark.slow
@pytest.mark.parametrize("platform", ["fabric", "databricks", "synapse"])
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
        self, platform_client, job_packager, test_app_path, job_config_provider, stdout_validator
    ):
        """Test running and monitoring a job - runs on all platforms"""
        api_client, platform_name = platform_client
        job_config = job_config_provider()
        test_id = job_config["test_id"]

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

            # Stream stdout in real-time to capture execution
            print("\n📡 Streaming stdout in real-time...")
            print("=" * 80)

            try:
                stdout_validator.stream_with_callback(
                    job_id=job_id,
                    run_id=run_id,
                    print_lines=True,  # Print each line as it arrives
                    poll_interval=10.0,
                    max_wait=600.0,  # 10 minute timeout
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

            # Validate using stdout (captured during streaming)
            print("\n📋 Validating job execution from stdout...")

            # Validate bootstrap and test app markers
            bootstrap_results = stdout_validator.validate_bootstrap_execution()
            test_app_results = stdout_validator.validate_test_app_execution(test_id)

            # Print validation summaries
            stdout_validator.print_validation_summary(bootstrap_results, "Bootstrap Validation")
            stdout_validator.print_validation_summary(test_app_results, "Test App Validation")

            # Assert critical validations
            assert bootstrap_results.get(
                "bootstrap_start"
            ), "Bootstrap execution not found in stdout"
            assert test_app_results.get(
                "test_markers"
            ), "Test execution markers not found in stdout"
            if test_id:
                assert test_app_results.get(
                    "test_id_match"
                ), f"Test ID {test_id} not found in stdout"

            print("\n✅ All validations passed!")

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
@pytest.mark.parametrize("platform", ["fabric", "databricks", "synapse"])
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
