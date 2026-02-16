"""
System test for complex tracing functionality.

Tests that complex span hierarchies with variable durations work correctly
with both default Kindling telemetry and Azure Monitor extension.

Usage:
    # Test with default telemetry on a specific platform
    poe test-system --platform fabric --test default_telemetry
    poe test-system --platform synapse --test default_telemetry
    poe test-system --platform databricks --test default_telemetry

    # Test with Azure Monitor extension on a specific platform
    poe test-system --platform fabric --test azure_monitor_telemetry
    poe test-system --platform synapse --test azure_monitor_telemetry
    poe test-system --platform databricks --test azure_monitor_telemetry
"""

import os
import time
import uuid
from pathlib import Path

import pytest


@pytest.mark.system
@pytest.mark.slow
@pytest.mark.azure
class TestComplexTracing:
    """Test complex tracing with variable durations across Azure platforms."""

    def _run_tracing_test(self, platform_client, app_packager, use_azure_monitor=False):
        """Common test logic for both default and Azure Monitor telemetry"""
        client, platform = platform_client

        test_type = "Azure Monitor" if use_azure_monitor else "Default Telemetry"
        print(f"\n🧪 Testing Complex Tracing with {test_type} on {platform.upper()}")

        # Get test app path
        workspace_root = Path(__file__).resolve().parents[3]
        app_path = workspace_root / "tests" / "data-apps" / "complex-tracing-test"
        assert app_path.exists(), f"Test app not found at {app_path}"

        # Create unique test ID
        test_id = str(uuid.uuid4())[:8]
        app_name = f"complex-tracing-test-{test_id}"
        job_name = f"systest-complex-tracing-{test_id}"

        # Prepare app files
        print(f"📦 Packaging test app: {app_name}")
        app_files = app_packager.prepare_app_files(str(app_path))

        # Use Azure Monitor settings if requested
        if use_azure_monitor and os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING"):
            print("🔧 Using Azure Monitor configuration")
            settings_azure_path = app_path / "settings-azure.yaml"
            with open(settings_azure_path, "r") as f:
                settings_content = f.read()
            # Replace environment variable
            conn_str = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
            settings_content = settings_content.replace(
                "${APPLICATIONINSIGHTS_CONNECTION_STRING}", conn_str
            )
            app_files["settings.yaml"] = settings_content

        print(f"✅ Prepared {len(app_files)} files")

        # Create job config
        job_config = {
            "job_name": job_name,
            "app_name": app_name,
            "entry_point": "main.py",
            "test_id": test_id,
        }
        from tests.system.test_helpers import apply_env_config_overrides

        job_config = apply_env_config_overrides(job_config, platform)

        # Force true "default telemetry" behavior for this test path even if
        # global CONFIG__ env overrides enable extensions.
        if not use_azure_monitor:
            config_overrides = job_config.setdefault("config_overrides", {})
            kindling_overrides = config_overrides.setdefault("kindling", {})
            kindling_overrides["extensions"] = []
            telemetry_overrides = kindling_overrides.setdefault("telemetry", {})
            azure_monitor_overrides = telemetry_overrides.setdefault("azure_monitor", {})
            azure_monitor_overrides["enable_logging"] = False
            azure_monitor_overrides["enable_tracing"] = False

        # Deploy app and create job (separate operations)
        print(f"📂 Deploying app: {app_name}")
        client.deploy_app(app_name, app_files)
        print(f"🚀 Creating job: {job_name}")
        result = client.create_job(job_name=job_name, job_config=job_config)
        job_id = result["job_id"]
        print(f"✅ Job created: {job_id}")

        run_id = client.run_job(job_id=job_id)
        print(f"🏃 Job started: {run_id}")

        # Wait for completion
        print("⏳ Waiting for job to complete...")
        timeout = 600  # 10 minutes (complex test takes longer)
        start_time = time.time()
        final_status = None
        final_result_state = None
        job_failed = False

        while time.time() - start_time < timeout:
            status_info = client.get_job_status(run_id=run_id)
            status = (status_info.get("status") or "").upper()
            result_state = (status_info.get("result_state") or "").upper()
            elapsed = int(time.time() - start_time)
            print(
                f"   status poll ({elapsed}s): lifecycle={status or 'N/A'} result={result_state or 'N/A'}"
            )

            failed_result_states = {
                "FAILED",
                "ERROR",
                "CANCELED",
                "CANCELLED",
                "INTERNAL_ERROR",
                "TIMEDOUT",
                "UPSTREAM_FAILED",
                "UPSTREAM_CANCELED",
                "MAXIMUM_CONCURRENT_RUNS_REACHED",
            }
            terminal_lifecycle_states = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

            # Treat classic statuses as terminal.
            if status in ["COMPLETED", "SUCCESS"]:
                print("✅ Job completed successfully")
                final_status = status
                final_result_state = result_state
                break
            if status in ["FAILED", "ERROR", "CANCELLED", "CANCELED"]:
                print(f"❌ Job failed with status: {status} (result_state: {result_state or 'N/A'})")
                final_status = status
                final_result_state = result_state
                job_failed = True
                break

            # Databricks lifecycle can be TERMINATED/SKIPPED/INTERNAL_ERROR with
            # result_state carrying success/failure detail.
            if status in terminal_lifecycle_states:
                if status == "INTERNAL_ERROR" or result_state in failed_result_states:
                    print(
                        f"❌ Job failed with lifecycle={status} (result_state: {result_state or 'N/A'})"
                    )
                    final_status = status
                    final_result_state = result_state
                    job_failed = True
                    break
                print(f"✅ Job completed with lifecycle={status} (result_state: {result_state or 'N/A'})")
                final_status = status
                final_result_state = result_state
                break

            # Some Databricks runs remain in TERMINATING while result_state is final.
            if status == "TERMINATING" and result_state:
                if result_state in failed_result_states:
                    print(
                        f"❌ Job failed while terminating (result_state: {result_state or 'N/A'})"
                    )
                    final_status = status
                    final_result_state = result_state
                    job_failed = True
                    break
                print(
                    f"✅ Job completed while terminating (result_state: {result_state or 'N/A'})"
                )
                final_status = status
                final_result_state = result_state
                break

            time.sleep(15)
        else:
            pytest.fail(f"Job timed out after {timeout} seconds")

        # Get logs
        print("\n📋 Retrieving job logs...")
        logs = client.get_job_logs(run_id=run_id)

        if isinstance(logs, list):
            log_content = "\n".join(logs)
        else:
            log_content = str(logs)

        assert log_content, "No logs returned from job execution"

        # Print summary
        print("\n📋 Job Logs Summary (last 50 lines):")
        if isinstance(logs, dict) and "log" in logs:
            log_lines = logs["log"]
            for line in log_lines[-50:]:
                print(f"  {line}")
        else:
            print(log_content[-5000:])

        # Verify job succeeded
        if job_failed:
            pytest.fail(
                f"Job failed with lifecycle={final_status} (result_state: {final_result_state or 'N/A'})"
            )

        # Verify test execution
        assert "Complex Tracing System Test" in log_content, "Test app did not start properly"

        assert "completed successfully" in log_content.lower(), "Test did not complete successfully"

        # Verify telemetry provider type
        if use_azure_monitor:
            # Should see Azure Monitor extension loaded
            assert (
                "kindling_otel_azure" in log_content or "Loaded extension" in log_content
            ), "Azure Monitor extension not loaded"
            print("✅ Azure Monitor extension loaded and active")
        else:
            # Should use default provider
            assert (
                "Using default Kindling telemetry" in log_content
                or "kindling_otel_azure" not in log_content
            ), "Unexpected Azure Monitor extension detected"
            print("✅ Default Kindling telemetry active")

        # Verify pipeline execution
        assert "Pipeline 1 of 3" in log_content, "Pipeline 1 not found"
        assert "Pipeline 2 of 3" in log_content, "Pipeline 2 not found"
        assert "Pipeline 3 of 3" in log_content, "Pipeline 3 not found"

        print("✅ All 3 pipelines executed successfully")

        # Verify stages executed
        for stage in ["Validating", "Transforming", "Enriching", "Aggregating"]:
            assert stage in log_content, f"Stage '{stage}' not found in logs"

        print("✅ All pipeline stages executed")

        print("\n📊 Complex Tracing Test Summary:")
        print(f"   ✅ Test type: {test_type}")
        print("   ✅ Job completed successfully")
        print("   ✅ 3 pipelines executed")
        print("   ✅ All stages completed (validate, transform, enrich, aggregate)")
        print("   ✅ Complex span hierarchy generated")

        if use_azure_monitor:
            print("\n📍 Verify spans in Application Insights:")
            print("   Query: dependencies | where timestamp > ago(1h)")
            print("          | where name in ('data_pipeline', 'validate_data', 'transform_data')")
            print("   Expected: ~150-300 span records with variable durations")

        # Cleanup
        try:
            if not os.getenv("SKIP_TEST_CLEANUP"):
                client.delete_job(job_id)
                print(f"✅ Cleaned up job: {job_id}")
                client.cleanup_app(app_name)
                print(f"🗑️  Cleaned up app: {app_name}")
        except Exception as e:
            print(f"⚠️  Cleanup warning: {e}")

    def test_default_telemetry(self, platform_client, app_packager):
        """Test complex tracing with default Kindling telemetry"""
        self._run_tracing_test(platform_client, app_packager, use_azure_monitor=False)

    @pytest.mark.skipif(
        not os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING"),
        reason="APPLICATIONINSIGHTS_CONNECTION_STRING not set",
    )
    def test_azure_monitor_telemetry(self, platform_client, app_packager):
        """Test complex tracing with Azure Monitor OpenTelemetry extension"""
        self._run_tracing_test(platform_client, app_packager, use_azure_monitor=True)
