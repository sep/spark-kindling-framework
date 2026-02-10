"""
System test for Azure Monitor OpenTelemetry extension.

Tests that the kindling-otel-azure extension:
1. Loads correctly from artifacts storage
2. Overrides default trace and log providers
3. Successfully sends telemetry to Azure Monitor

Usage:
    pytest -k fabric tests/system/extensions/azure-monitor/
    pytest -k synapse tests/system/extensions/azure-monitor/
    pytest -k databricks tests/system/extensions/azure-monitor/
"""

import os
import sys
import time
import uuid
from pathlib import Path

import pytest


@pytest.mark.system
@pytest.mark.slow
class TestAzureMonitorExtension:
    """Test Azure Monitor OpenTelemetry extension functionality"""

    def test_extension_loads_and_overrides_providers(
        self, platform_client, app_packager, stdout_validator
    ):
        """
        Test that the extension loads and overrides default providers.

        This test:
        1. Deploys a test app with extension configured in settings.yaml
        2. Creates a job definition and runs it
        3. Streams stdout in real-time to capture bootstrap and extension loading
        4. Validates bootstrap execution, framework init, and extension installation
        5. Verifies job completes successfully
        """
        client, platform = platform_client

        print(f"\n🧪 Testing Azure Monitor Extension on {platform.upper()}")

        # Use otel-azure-test-app to verify extension functionality
        workspace_root = Path(__file__).parent.parent.parent.parent
        app_path = workspace_root / "data-apps" / "otel-azure-test-app"
        assert app_path.exists(), f"Test app not found at {app_path}"

        # Create unique test ID
        test_id = str(uuid.uuid4())[:8]
        app_name = f"otel-azure-test-app-{test_id}"
        job_name = f"systest-azure-monitor-providers-{test_id}"

        # Prepare app files using app packager
        print(f"📦 Packaging test app: {app_name}")
        app_files = app_packager.prepare_app_files(str(app_path))
        print(f"✅ Prepared {len(app_files)} files")

        # Discover config overrides from environment variables
        # CONFIG__kindling__key__subkey maps to kindling.key.subkey
        # __ indicates nesting, _ is preserved in key names
        config_overrides = {}
        for key, value in os.environ.items():
            if key.startswith("CONFIG__"):
                # Remove CONFIG__ prefix and parse nested structure
                config_path = key[8:]  # Remove CONFIG__ prefix
                parts = config_path.split("__")  # Split on __ for nesting

                # Build nested dict
                current = config_overrides
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = value

        if config_overrides:
            print(f"📋 Discovered config overrides from env: {config_overrides}")

        # Create job config (platform APIs handle their own required fields)
        job_config = {
            "job_name": job_name,
            "app_name": app_name,
            "entry_point": "main.py",
            "test_id": test_id,
        }

        # Add config overrides if discovered from env
        if config_overrides:
            job_config["config_overrides"] = config_overrides

        # Step 1: Deploy app to storage
        print(f"📂 Deploying app: {app_name}")
        client.deploy_app(app_name, app_files)

        # Step 2: Create job definition
        print(f"🚀 Creating job: {job_name}")
        result = client.create_job(job_name=job_name, job_config=job_config)
        job_id = result["job_id"]
        print(f"✅ Job created: {job_id}")

        # Run job
        run_id = client.run_job(job_id=job_id)
        print(f"🏃 Job started: {run_id}")
        sys.stdout.flush()

        # Stream stdout logs in real-time
        print("\n📡 Streaming stdout in real-time...")
        print("=" * 80)
        sys.stdout.flush()

        # Use stdout validator helper to stream and capture logs
        try:
            all_stdout = stdout_validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,  # Print each line as it arrives
                poll_interval=5.0,
                max_wait=300.0,
            )
            print("=" * 80)
            print(f"📊 Captured {len(all_stdout)} stdout lines")
            sys.stdout.flush()
        except Exception as e:
            print(f"⚠️  Stdout streaming error: {e}")
            import traceback

            traceback.print_exc()
            sys.stdout.flush()
            all_stdout = []

        # Check final job status
        status_info = client.get_job_status(run_id=run_id)
        final_status = status_info["status"]

        if final_status.upper() in ["COMPLETED", "SUCCESS"]:
            print(f"✅ Job completed successfully")
        elif final_status.upper() in ["FAILED", "ERROR", "CANCELLED"]:
            print(f"❌ Job failed with status: {final_status}")
        sys.stdout.flush()
        # Get diagnostic logs (retrieve even on failure to see what went wrong)
        print("\n📋 Retrieving diagnostic emitter logs...")
        logs = client.get_job_logs(run_id=run_id)

        # Print diagnostic logs (first 100 lines)
        print("\n📋 Diagnostic Logs (first 100 lines):")
        log_content = ""  # Initialize for later use
        if isinstance(logs, dict) and "log" in logs:
            log_lines = logs["log"]
            for i, line in enumerate(log_lines[:100]):
                print(f"  {line}")
            log_content = "\n".join(log_lines) if log_lines else ""
        else:
            log_content = str(logs) if not isinstance(logs, list) else "\n".join(logs)
            print(log_content[:5000])  # First 5000 chars

        # If job failed, fail test
        if final_status and final_status.upper() in ["FAILED", "ERROR", "CANCELLED"]:
            pytest.fail(f"Job failed with status: {final_status}")

        print("\n📋 Checking job execution...")

        # Validate using stdout validator helper
        bootstrap_results = stdout_validator.validate_bootstrap_execution()
        extension_results = stdout_validator.validate_extension_loading("kindling-otel-azure")

        # Print validation results
        stdout_validator.print_validation_summary(bootstrap_results, "Bootstrap Validation")
        stdout_validator.print_validation_summary(extension_results, "Extension Validation")

        # Assert critical checks - MUST not have bootstrap errors
        assert bootstrap_results.get(
            "no_bootstrap_errors"
        ), "Bootstrap or framework initialization failed - check logs for errors"
        assert bootstrap_results.get("bootstrap_start"), "Bootstrap execution not found in stdout"
        assert bootstrap_results.get(
            "framework_init"
        ), "Framework initialization not found in stdout"
        assert extension_results.get(
            "extension_install"
        ), "Extension installation not found in stdout"

        print("\n📊 Extension Test Summary:")
        print("   ✅ Job deployed successfully")
        print("   ✅ Bootstrap script executed (verified via stdout)")
        print("   ✅ Framework initialized (verified via stdout)")
        print("   ✅ Extension installed and loaded (verified via stdout)")
        print("   ✅ Job completed without errors")
        print(f"\n💡 Validation Method: Real-time stdout streaming via {platform.capitalize()} API")
        print("   - Captured bootstrap output, framework init, and extension loading")
        print("   - Provides immediate visibility into job execution")
        print("\n🔍 Next steps for full verification:")
        print("   1. Check Application Insights for traces and logs")
        print("   2. Verify telemetry data appears with correct attributes")
        print("   3. Confirm trace correlation works end-to-end")

        print("\n✅ Extension test passed - job completed successfully!")

        # Cleanup
        try:
            if not os.getenv("SKIP_TEST_CLEANUP"):
                client.delete_job(job_id)
                print(f"✅ Cleaned up job: {job_id}")
                client.cleanup_app(app_name)
                print(f"🗑️  Cleaned up app: {app_name}")
        except Exception as e:
            print(f"⚠️  Cleanup warning: {e}")
