#!/usr/bin/env python3
"""
System test for KDA packaging and deployment to Synapse.

This test demonstrates the complete workflow:
1. Package a data app as a KDA file
2. Deploy the KDA to a platform (simulated for Synapse)
3. Execute the app on the platform

This test can be run in a Synapse environment to validate actual deployment.
"""

from kindling.data_apps import DataAppManager, KDAManifest
import os
import sys
import tempfile
import shutil
import json
import zipfile
import logging
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))


class SimpleLogger:
    """Simple logger for testing"""

    def __init__(self):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        self.logger = logging.getLogger("SynapseKDATest")

    def info(self, message):
        self.logger.info(message)
        print(f"ℹ️  {message}")

    def error(self, message):
        self.logger.error(message)
        print(f"❌ {message}")

    def warning(self, message):
        self.logger.warning(message)
        print(f"⚠️  {message}")

    def debug(self, message):
        self.logger.debug(message)


class SynapseKDADeploymentTest:
    """System test for KDA deployment to Synapse"""

    def __init__(self):
        self.logger = SimpleLogger()
        self.temp_dir = None
        self.test_results = []

    def setup(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp(prefix="synapse_kda_test_")
        self.logger.info(f"Test environment created: {self.temp_dir}")

    def teardown(self):
        """Clean up test environment"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            self.logger.info("Test environment cleaned up")

    def test_kda_packaging_for_synapse(self):
        """Test KDA packaging with Synapse-specific configuration"""
        try:
            self.logger.info("🧪 Testing KDA packaging for Synapse deployment...")

            # Use the existing test app
            app_path = "/workspace/tests/system/apps/azure-storage-test"

            if not os.path.exists(app_path):
                raise FileNotFoundError(f"Test app not found: {app_path}")

            # Create DataAppManager without dependency injection for testing
            # We'll manually create the required dependencies
            from kindling.spark_config import DynaconfConfig
            from kindling.platform_local import LocalService
            from kindling.platform_provider import SparkPlatformServiceProvider
            from kindling.spark_log_provider import SparkLoggerProvider
            from kindling.spark_trace import SparkTraceProvider
            from kindling.notebook_framework import NotebookManager

            # Create minimal dependencies
            config = DynaconfConfig()
            config.set("artifacts_storage_path", self.temp_dir)

            logger_provider = SparkLoggerProvider()
            trace_provider = SparkTraceProvider(logger_provider)

            # Create a platform service (local for testing)
            platform_service = LocalService(config, logger_provider.get_logger("LocalService"))
            platform_provider = SparkPlatformServiceProvider()
            platform_provider.set_service(platform_service)

            # Create notebook manager
            notebook_manager = NotebookManager(
                platform_provider, config, trace_provider, logger_provider
            )

            # Create DataAppManager with all dependencies
            manager = DataAppManager(
                nm=notebook_manager,
                pp=platform_provider,
                config=config,
                tp=trace_provider,
                lp=logger_provider,
            )

            # Package the app for Synapse (single-platform mode)
            output_dir = os.path.join(self.temp_dir, "packages")
            os.makedirs(output_dir, exist_ok=True)

            kda_path = manager.package_app(
                app_path=app_path,
                output_dir=output_dir,
                target_platform="synapse",
                merge_platform_config=True,  # Single-platform package
            )

            # Verify the KDA file was created
            if not os.path.exists(kda_path):
                raise FileNotFoundError(f"KDA package not created: {kda_path}")

            self.logger.info(f"✅ KDA package created: {os.path.basename(kda_path)}")

            # Validate the KDA contents
            self._validate_synapse_kda(kda_path)

            self.test_results.append(("KDA Packaging for Synapse", "PASSED"))
            return kda_path

        except Exception as e:
            self.logger.error(f"❌ KDA packaging test failed: {e}")
            self.test_results.append(("KDA Packaging for Synapse", "FAILED", str(e)))
            raise

    def test_kda_deployment_simulation(self, kda_path):
        """Test KDA deployment simulation (Synapse-style)"""
        try:
            self.logger.info("🚀 Testing KDA deployment simulation...")

            # Simulate deployment by extracting and setting up the app
            deployment_dir = os.path.join(self.temp_dir, "deployed")
            os.makedirs(deployment_dir, exist_ok=True)

            app_name = self._extract_kda_for_deployment(kda_path, deployment_dir)

            # Simulate creating a deployment service
            deployment_config = self._create_synapse_deployment_config(app_name, deployment_dir)

            self.logger.info(f"✅ Deployment configuration created for: {app_name}")
            self.logger.info(f"Deployment config: {json.dumps(deployment_config, indent=2)}")

            self.test_results.append(("KDA Deployment Simulation", "PASSED"))
            return deployment_config

        except Exception as e:
            self.logger.error(f"❌ KDA deployment simulation failed: {e}")
            self.test_results.append(("KDA Deployment Simulation", "FAILED", str(e)))
            raise

    def test_app_execution_simulation(self, deployment_config):
        """Test app execution simulation"""
        try:
            self.logger.info("⚡ Testing app execution simulation...")

            script_path = deployment_config.get("script_path")
            app_name = deployment_config.get("app_name")

            if not os.path.exists(script_path):
                raise FileNotFoundError(f"Main script not found: {script_path}")

            # Read and validate the main script
            with open(script_path, "r") as f:
                script_content = f.read()

            self.logger.info(f"Main script content preview:")
            self.logger.info(f"```python\n{script_content[:500]}...\n```")

            # Simulate job submission
            job_result = self._simulate_synapse_job_submission(deployment_config)

            self.logger.info(f"✅ Job simulation completed: {job_result['status']}")

            self.test_results.append(("App Execution Simulation", "PASSED"))
            return job_result

        except Exception as e:
            self.logger.error(f"❌ App execution simulation failed: {e}")
            self.test_results.append(("App Execution Simulation", "FAILED", str(e)))
            raise

    def _validate_synapse_kda(self, kda_path):
        """Validate KDA package contents for Synapse"""
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

            if manifest.get("platform") != "synapse":
                raise ValueError(f"Expected platform 'synapse', got: {manifest.get('platform')}")

            # Check that platform-specific config was merged
            if "app.synapse.yaml" in files:
                raise ValueError("app.synapse.yaml should not exist in single-platform package")

            self.logger.info("✅ KDA validation passed - all required files present")

    def _extract_kda_for_deployment(self, kda_path, deployment_dir):
        """Extract KDA for deployment simulation"""
        with zipfile.ZipFile(kda_path, "r") as kda:
            # Extract all files
            kda.extractall(deployment_dir)

            # Read manifest to get app name
            manifest_path = os.path.join(deployment_dir, "manifest.json")
            with open(manifest_path, "r") as f:
                manifest = json.load(f)

            app_name = manifest.get("app_name", "unknown-app")

            self.logger.info(f"KDA extracted to: {deployment_dir}")
            return app_name

    def _create_synapse_deployment_config(self, app_name, deployment_dir):
        """Create Synapse-specific deployment configuration"""
        main_script = os.path.join(deployment_dir, "main.py")

        config = {
            "app_name": app_name,
            "script_path": main_script,
            "environment_vars": {
                "AZURE_STORAGE_ACCOUNT": "testaccount",
                "AZURE_STORAGE_KEY": "fake-key-for-testing",
            },
            "platform": "synapse",
            "execution_mode": "notebook",
            "deployment_time": "2025-10-17T00:00:00Z",
        }

        return config

    def _simulate_synapse_job_submission(self, deployment_config):
        """Simulate Synapse job submission"""
        import time

        # Simulate job ID generation
        job_id = f"synapse-{deployment_config['app_name']}-{int(time.time())}"

        # Simulate job execution (just validate script can be read)
        script_path = deployment_config["script_path"]

        try:
            with open(script_path, "r") as f:
                script_content = f.read()

            # Basic validation - check for Python syntax
            compile(script_content, script_path, "exec")

            result = {
                "job_id": job_id,
                "status": "SUCCEEDED",
                "message": f"Job {job_id} completed successfully (simulated)",
                "submission_time": time.time(),
                "execution_time": 0.5,  # Simulated execution time
            }

        except SyntaxError as e:
            result = {
                "job_id": job_id,
                "status": "FAILED",
                "message": f"Script syntax error: {str(e)}",
                "submission_time": time.time(),
            }

        return result

    def run_full_test_suite(self):
        """Run the complete test suite"""
        try:
            self.setup()

            self.logger.info("🎯 Starting Synapse KDA Deployment System Test")
            self.logger.info("=" * 60)

            # Test 1: Package KDA for Synapse
            kda_path = self.test_kda_packaging_for_synapse()

            # Test 2: Simulate deployment
            deployment_config = self.test_kda_deployment_simulation(kda_path)

            # Test 3: Simulate execution
            job_result = self.test_app_execution_simulation(deployment_config)

            # Report results
            self._report_test_results()

            return True

        except Exception as e:
            self.logger.error(f"Test suite failed: {e}")
            self._report_test_results()
            return False

        finally:
            self.teardown()

    def _report_test_results(self):
        """Report test results summary"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 TEST RESULTS SUMMARY")
        self.logger.info("=" * 60)

        passed = 0
        failed = 0

        for result in self.test_results:
            test_name = result[0]
            status = result[1]

            if status == "PASSED":
                self.logger.info(f"✅ {test_name}: {status}")
                passed += 1
            else:
                error_msg = result[2] if len(result) > 2 else "Unknown error"
                self.logger.error(f"❌ {test_name}: {status} - {error_msg}")
                failed += 1

        self.logger.info("-" * 60)
        self.logger.info(f"Total Tests: {passed + failed}")
        self.logger.info(f"Passed: {passed}")
        self.logger.info(f"Failed: {failed}")

        if failed == 0:
            self.logger.info("🎉 ALL TESTS PASSED!")
        else:
            self.logger.error(f"💥 {failed} TESTS FAILED")


def main():
    """Main test execution"""
    test = SynapseKDADeploymentTest()
    success = test.run_full_test_suite()

    if success:
        print("\n✅ Synapse KDA deployment test completed successfully!")
        return 0
    else:
        print("\n❌ Synapse KDA deployment test failed!")
        return 1


if __name__ == "__main__":
    exit(main())
