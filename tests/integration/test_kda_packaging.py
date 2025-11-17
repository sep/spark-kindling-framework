#!/usr/bin/env python3
"""
Integration test for KDA packaging workflow.

This test demonstrates the core KDA functionality without complex dependency injection.
Tests are platform-agnostic and validate KDA structure and content.
"""

import json
import os
import shutil
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import pytest
from kindling.data_apps import KDAManifest

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

# Mark all tests in this module as integration tests
pytestmark = [pytest.mark.integration]


@pytest.fixture(scope="class")
def temp_test_dir():
    """Create temporary directory for tests"""
    temp_dir = tempfile.mkdtemp(prefix="simple_kda_test_")
    print(f"ℹ️  Test environment created: {temp_dir}")
    yield temp_dir
    # Cleanup
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print("ℹ️  Test environment cleaned up")


class TestKDAPackaging:
    """Integration tests for KDA packaging functionality"""

    def test_manual_kda_creation(self, temp_test_dir):
        """Test manual KDA creation with platform config"""
        print("🧪 Testing manual KDA creation...")

        # Use shared test app from tests/data-apps/
        test_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        app_path = os.path.join(test_dir, "data-apps", "kda-test-app")

        if not os.path.exists(app_path):
            pytest.skip(f"Test app not found: {app_path}")

        # Create output directory
        output_dir = os.path.join(temp_test_dir, "packages")
        os.makedirs(output_dir, exist_ok=True)

        # Manually create KDA package
        kda_path = self._create_kda_manually(app_path, output_dir)

        # Verify the KDA file was created
        assert os.path.exists(kda_path), f"KDA package not created: {kda_path}"

        print(f"✅ KDA package created: {os.path.basename(kda_path)}")

        # Validate the KDA contents
        self._validate_kda(kda_path)

    def test_kda_deployment_simulation(self, temp_test_dir):
        """Test KDA deployment simulation"""
        print("🚀 Testing KDA deployment simulation...")

        # Use shared test app from tests/data-apps/
        test_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        app_path = os.path.join(test_dir, "data-apps", "kda-test-app")

        if not os.path.exists(app_path):
            pytest.skip(f"Test app not found: {app_path}")

        output_dir = os.path.join(temp_test_dir, "packages")
        os.makedirs(output_dir, exist_ok=True)
        kda_path = self._create_kda_manually(app_path, output_dir)

        # Simulate deployment by extracting and setting up the app
        deployment_dir = os.path.join(temp_test_dir, "deployed")
        os.makedirs(deployment_dir, exist_ok=True)

        app_name = self._extract_kda_for_deployment(kda_path, deployment_dir)

        # Simulate creating a deployment service config
        deployment_config = self._create_deployment_config(app_name, deployment_dir)

        print(f"✅ Deployment configuration created for: {app_name}")
        print(f"Deployment config: {json.dumps(deployment_config, indent=2)}")

        assert deployment_config is not None
        assert "app_name" in deployment_config

    def test_job_simulation(self, temp_test_dir):
        """Test job execution simulation"""
        print("⚡ Testing job execution simulation...")

        # Create a test script
        script_path = os.path.join(temp_test_dir, "test_script.py")
        with open(script_path, "w") as f:
            f.write("print('Hello from Synapse job simulation')\n")

        # Create a minimal deployment config for testing
        deployment_config = {
            "app_name": "test-app",
            "platform": "test",
            "job_type": "spark",
            "script_path": script_path,
        }

        # Simulate job execution
        job_result = self._simulate_job_execution(deployment_config)

        print(f"✅ Job simulation completed: {job_result['status']}")
        print(f"Job details: {json.dumps(job_result, indent=2)}")

        assert job_result is not None
        assert "status" in job_result

    def _create_kda_manually(self, app_path, output_dir):
        """Manually create a KDA package (simulating DataAppManager.package_app)"""

        # Read the base app config
        app_config_path = os.path.join(app_path, "app.yaml")
        with open(app_config_path, "r") as f:
            import yaml

            base_config = yaml.safe_load(f)

        # Read and merge platform-specific config (use synapse as example)
        platform_config_path = os.path.join(app_path, "app.synapse.yaml")
        if os.path.exists(platform_config_path):
            with open(platform_config_path, "r") as f:
                platform_config = yaml.safe_load(f)
            # Simple merge (in real implementation, this would be more sophisticated)
            merged_config = {**base_config, **platform_config}
        else:
            merged_config = base_config

        # Create KDA manifest
        app_name = merged_config.get("name", "unknown-app")
        manifest = KDAManifest(
            name=app_name,
            version="1.0",
            description=merged_config.get("description", "Test app"),
            entry_point="main.py",
            dependencies=merged_config.get("dependencies", []),
            lake_requirements=merged_config.get("lake_requirements", []),
            environment="test",
            metadata={
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source_path": app_path,
                "target_platform": "test",
            },
            created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

        # Create KDA package
        kda_filename = f"{app_name}-test-v{manifest.version}.kda"
        kda_path = os.path.join(output_dir, kda_filename)

        with zipfile.ZipFile(kda_path, "w", zipfile.ZIP_DEFLATED) as kda:
            # Add manifest
            kda.writestr("manifest.json", json.dumps(manifest.__dict__, indent=2))

            # Add merged app config (single-platform mode)
            kda.writestr("app.yaml", yaml.dump(merged_config, default_flow_style=False))

            # Add all other files except platform-specific configs AND base app.yaml (already added)
            for root, dirs, files in os.walk(app_path):
                for file in files:
                    # Skip ALL app.*.yaml files (including app.yaml which we already added)
                    if file.startswith("app.") and file.endswith(".yaml"):
                        continue

                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, app_path)
                    kda.write(file_path, arcname)

        return kda_path

    def _validate_kda(self, kda_path):
        """Validate KDA package contents"""
        with zipfile.ZipFile(kda_path, "r") as kda:
            files = kda.namelist()

            # Check for required files
            required_files = ["manifest.json", "main.py", "app.yaml"]
            for required_file in required_files:
                if required_file not in files:
                    raise ValueError(f"Required file missing from KDA: {required_file}")

            # Validate manifest
            manifest_content = kda.read("manifest.json").decode("utf-8")
            manifest = json.loads(manifest_content)

            if not manifest.get("environment"):
                raise ValueError("Manifest missing environment field")

            # Check that platform-specific config was merged
            if "app.synapse.yaml" in files:
                raise ValueError("app.synapse.yaml should not exist in single-platform package")

            print("✅ KDA validation passed - all required files present")

    def _extract_kda_for_deployment(self, kda_path, deployment_dir):
        """Extract KDA for deployment simulation"""
        with zipfile.ZipFile(kda_path, "r") as kda:
            # Extract all files
            kda.extractall(deployment_dir)

            # Read manifest to get app name
            manifest_path = os.path.join(deployment_dir, "manifest.json")
            with open(manifest_path, "r") as f:
                manifest = json.load(f)

            app_name = manifest.get("name", "unknown-app")

            print(f"KDA extracted to: {deployment_dir}")
            return app_name

    def _create_deployment_config(self, app_name, deployment_dir):
        """Create deployment configuration"""
        main_script = os.path.join(deployment_dir, "main.py")

        config = {
            "app_name": app_name,
            "script_path": main_script,
            "environment_vars": {
                "AZURE_STORAGE_ACCOUNT": "testaccount",
                "AZURE_STORAGE_KEY": "fake-key-for-testing",
            },
            "platform": "test",
            "execution_mode": "notebook",
            "deployment_time": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        return config

    def _simulate_job_execution(self, deployment_config):
        """Simulate job execution"""

        # Generate job ID
        job_id = f"test-{deployment_config['app_name']}-{int(time.time())}"

        script_path = deployment_config["script_path"]

        try:
            # Validate script exists and is syntactically correct
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"Script not found: {script_path}")

            with open(script_path, "r") as f:
                script_content = f.read()

            # Basic Python syntax validation
            compile(script_content, script_path, "exec")

            print(f"Script content preview:")
            print(f"```python\\n{script_content[:300]}...\\n```")

            # Simulate successful execution
            result = {
                "job_id": job_id,
                "status": "SUCCEEDED",
                "message": f"Job {job_id} completed successfully (simulated)",
                "submission_time": time.time(),
                "execution_time": 0.5,
                "platform": "test",
                "script_validated": True,
            }

        except SyntaxError as e:
            result = {
                "job_id": job_id,
                "status": "FAILED",
                "message": f"Script syntax error: {str(e)}",
                "submission_time": time.time(),
                "platform": "test",
            }
        except Exception as e:
            result = {
                "job_id": job_id,
                "status": "FAILED",
                "message": f"Job execution failed: {str(e)}",
                "submission_time": time.time(),
                "platform": "test",
            }

        return result


#  ═══════════════════════════════════════════════════════════════════════════
#  How to run these tests:
#  ═══════════════════════════════════════════════════════════════════════════
#
#  Run all integration tests:
#    pytest tests/integration/ -v
#
#  Run only this test file:
#    pytest tests/integration/test_kda_packaging.py -v
#
#  Run specific test:
#    pytest tests/integration/test_kda_packaging.py::TestKDAPackaging::test_manual_kda_creation -v
#
#  ═══════════════════════════════════════════════════════════════════════════
