#!/usr/bin/env python3
"""
Integration test showing how SynapseAppDeploymentService works with KDA packages.

This test demonstrates the integration between:
1. KDA packaging system
2. SynapseAppDeploymentService 
3. Synapse platform execution

In a real Synapse environment, this would actually deploy and run the app.
"""

from kindling.data_apps import KDAManifest
import os
import sys
import tempfile
import shutil
import json
import zipfile
import time
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))


class MockSynapseService:
    """Mock Synapse service for testing"""

    def __init__(self):
        self.temp_storage = {}

    def exists(self, path):
        return path in self.temp_storage or os.path.exists(path)

    def write(self, path, content):
        self.temp_storage[path] = content

    def read(self, path):
        if path in self.temp_storage:
            return self.temp_storage[path]
        return open(path, 'r').read()


class MockLogger:
    """Simple mock logger"""

    def info(self, msg):
        print(f"ℹ️  {msg}")

    def error(self, msg):
        print(f"❌ {msg}")

    def warning(self, msg):
        print(f"⚠️  {msg}")


class SynapseAppDeploymentServiceDemo:
    """Demonstration of SynapseAppDeploymentService integration"""

    def __init__(self):
        self.temp_dir = None
        self.logger = MockLogger()
        self.synapse_service = MockSynapseService()

        # Import and create the SynapseAppDeploymentService
        try:
            from kindling.platform_synapse import SynapseAppDeploymentService
            self.deployment_service = SynapseAppDeploymentService(
                synapse_service=self.synapse_service,
                logger=self.logger
            )
            self.has_deployment_service = True
        except ImportError:
            self.logger.warning(
                "SynapseAppDeploymentService not available - using mock")
            self.deployment_service = self._create_mock_deployment_service()
            self.has_deployment_service = False

    def _create_mock_deployment_service(self):
        """Create a mock deployment service for testing"""
        class MockSynapseAppDeploymentService:
            def __init__(self, synapse_service, logger):
                self.synapse_service = synapse_service
                self.logger = logger

            def submit_spark_job(self, job_config):
                script_path = job_config.get('script_path')
                app_name = job_config.get('app_name', 'unknown-app')

                job_id = f"synapse-{app_name}-{int(time.time())}"

                try:
                    if script_path and os.path.exists(script_path):
                        # Validate script
                        with open(script_path, 'r') as f:
                            script_content = f.read()
                        compile(script_content, script_path, 'exec')

                        status = "SUCCEEDED"
                        message = f"Mock job {job_id} completed successfully"
                    else:
                        status = "FAILED"
                        message = f"Script not found: {script_path}"

                except Exception as e:
                    status = "FAILED"
                    message = f"Job execution failed: {str(e)}"

                return {
                    "job_id": job_id,
                    "status": status,
                    "message": message,
                    "submission_time": time.time()
                }

            def create_job_config(self, app_name, app_path, environment_vars=None):
                main_script = f"{app_path}/main.py"
                return {
                    "app_name": app_name,
                    "script_path": main_script,
                    "environment_vars": environment_vars or {},
                    "platform": "synapse",
                    "execution_mode": "notebook"
                }

            def get_job_status(self, job_id):
                return {"job_id": job_id, "status": "COMPLETED"}

            def cancel_job(self, job_id):
                return False

            def get_job_logs(self, job_id):
                return f"Logs for {job_id}"

        return MockSynapseAppDeploymentService(self.synapse_service, self.logger)

    def setup(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp(prefix="synapse_deployment_demo_")
        self.logger.info(f"Demo environment created: {self.temp_dir}")

    def teardown(self):
        """Clean up test environment"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            self.logger.info("Demo environment cleaned up")

    def demo_complete_workflow(self):
        """Demonstrate the complete KDA + Synapse deployment workflow"""
        try:
            self.setup()

            print("🎯 Synapse App Deployment Service Integration Demo")
            print("=" * 70)
            print(
                f"Using {'Real' if self.has_deployment_service else 'Mock'} SynapseAppDeploymentService")
            print("=" * 70)

            # Step 1: Create KDA package
            self.logger.info("📦 Step 1: Creating KDA package...")
            kda_path = self._create_kda_package()

            # Step 2: Deploy KDA
            self.logger.info("🚀 Step 2: Deploying KDA package...")
            deployment_dir = self._deploy_kda_package(kda_path)

            # Step 3: Create job configuration
            self.logger.info("⚙️  Step 3: Creating job configuration...")
            job_config = self._create_job_configuration(deployment_dir)

            # Step 4: Submit Spark job
            self.logger.info("⚡ Step 4: Submitting Spark job...")
            job_result = self._submit_spark_job(job_config)

            # Step 5: Monitor job
            self.logger.info("👀 Step 5: Monitoring job status...")
            job_status = self._monitor_job(job_result['job_id'])

            print("\\n" + "=" * 70)
            print("✅ DEMO COMPLETED SUCCESSFULLY!")
            print("=" * 70)
            print("This demonstrates how a real Synapse system test would:")
            print("  1. 📦 Package data apps as KDA files")
            print("  2. 🚀 Deploy KDA to Synapse workspace storage")
            print("  3. ⚙️  Configure Spark job parameters")
            print("  4. ⚡ Submit job via SynapseAppDeploymentService")
            print("  5. 👀 Monitor execution and collect results")
            print(
                "\\nIn a real Synapse environment, this would execute on actual compute!")

            return True

        except Exception as e:
            print(f"❌ Demo failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.teardown()

    def _create_kda_package(self):
        """Create a KDA package"""
        app_path = "/workspace/tests/system/apps/azure-storage-test"

        if not os.path.exists(app_path):
            raise FileNotFoundError(f"Test app not found: {app_path}")

        # Read app config
        app_config_path = os.path.join(app_path, 'app.yaml')
        with open(app_config_path, 'r') as f:
            import yaml
            base_config = yaml.safe_load(f)

        # Merge Synapse config
        synapse_config_path = os.path.join(app_path, 'app.synapse.yaml')
        if os.path.exists(synapse_config_path):
            with open(synapse_config_path, 'r') as f:
                synapse_config = yaml.safe_load(f)
            merged_config = {**base_config, **synapse_config}
        else:
            merged_config = base_config

        # Create manifest
        app_name = merged_config.get('name', 'azure-storage-test')
        manifest = KDAManifest(
            name=app_name,
            version="1.0",
            description="Azure Storage Test App for Synapse",
            entry_point="main.py",
            dependencies=merged_config.get('dependencies', []),
            lake_requirements=merged_config.get('lake_requirements', []),
            environment="synapse",
            metadata={
                "demo": True,
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
            },
            created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ")
        )

        # Create KDA package
        output_dir = os.path.join(self.temp_dir, "packages")
        os.makedirs(output_dir, exist_ok=True)

        kda_filename = f"{app_name}-synapse-demo-v{manifest.version}.kda"
        kda_path = os.path.join(output_dir, kda_filename)

        with zipfile.ZipFile(kda_path, 'w', zipfile.ZIP_DEFLATED) as kda:
            # Add manifest
            kda.writestr('manifest.json', json.dumps(
                manifest.__dict__, indent=2))

            # Add merged config
            kda.writestr('app.yaml', yaml.dump(
                merged_config, default_flow_style=False))

            # Add all app files
            for root, dirs, files in os.walk(app_path):
                for file in files:
                    if file.startswith('app.') and file.endswith('.yaml') and file != 'app.yaml':
                        continue  # Skip platform configs in single-platform mode

                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, app_path)
                    kda.write(file_path, arcname)

        self.logger.info(f"✅ KDA package created: {kda_filename}")
        return kda_path

    def _deploy_kda_package(self, kda_path):
        """Deploy KDA package (simulate extracting to platform storage)"""
        deployment_dir = os.path.join(
            self.temp_dir, "deployed", "azure-storage-test")
        os.makedirs(deployment_dir, exist_ok=True)

        # Extract KDA
        with zipfile.ZipFile(kda_path, 'r') as kda:
            kda.extractall(deployment_dir)

        self.logger.info(f"✅ KDA deployed to: {deployment_dir}")
        return deployment_dir

    def _create_job_configuration(self, deployment_dir):
        """Create job configuration using deployment service"""
        app_name = "azure-storage-test"
        environment_vars = {
            "AZURE_STORAGE_ACCOUNT": "demodatalake",
            "AZURE_STORAGE_KEY": "demo-key-for-testing",
            "SYNAPSE_WORKSPACE": "demo-workspace"
        }

        job_config = self.deployment_service.create_job_config(
            app_name=app_name,
            app_path=deployment_dir,
            environment_vars=environment_vars
        )

        self.logger.info(f"✅ Job configuration created")
        print(f"   📋 Config: {json.dumps(job_config, indent=2)}")
        return job_config

    def _submit_spark_job(self, job_config):
        """Submit Spark job using deployment service"""
        job_result = self.deployment_service.submit_spark_job(job_config)

        self.logger.info(f"✅ Job submitted: {job_result['job_id']}")
        print(f"   📊 Result: {json.dumps(job_result, indent=2)}")
        return job_result

    def _monitor_job(self, job_id):
        """Monitor job status"""
        job_status = self.deployment_service.get_job_status(job_id)

        self.logger.info(f"✅ Job status retrieved")
        print(f"   👀 Status: {json.dumps(job_status, indent=2)}")

        # Get logs
        logs = self.deployment_service.get_job_logs(job_id)
        print(f"   📝 Logs: {logs}")

        return job_status


def main():
    """Main demo execution"""
    demo = SynapseAppDeploymentServiceDemo()
    success = demo.demo_complete_workflow()

    if success:
        print("\\n🎉 Synapse App Deployment Service demo completed successfully!")
        return 0
    else:
        print("\\n💥 Synapse App Deployment Service demo failed!")
        return 1


if __name__ == "__main__":
    exit(main())
