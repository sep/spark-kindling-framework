#!/usr/bin/env python3
"""
Synapse Analytics System Test Runner

Deploys and executes system test apps on Azure Synapse Analytics from local environment.
Integrates with the Kindling App Framework to run tests as Spark jobs on Synapse.
"""

import os
import sys
import json
import time
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

try:
    from azure.identity import DefaultAzureCredential, ClientSecretCredential
    from azure.mgmt.synapse import SynapseManagementClient
    from azure.mgmt.synapse.models import LibraryInfo, SparkBatchJob
    from azure.storage.blob import BlobServiceClient
except ImportError as e:
    print(f"❌ Missing required Azure libraries: {e}")
    print("Install with: pip install azure-identity azure-mgmt-synapse azure-storage-blob")
    sys.exit(1)


class SynapseTestRunner:
    """Runner for deploying and executing system tests on Synapse Analytics"""

    def __init__(self, workspace_name: str, spark_pool_name: str,
                 subscription_id: str, resource_group: str):
        self.workspace_name = workspace_name
        self.spark_pool_name = spark_pool_name
        self.subscription_id = subscription_id
        self.resource_group = resource_group

        # Initialize Azure clients
        self.credential = self._get_azure_credential()
        self.synapse_client = SynapseManagementClient(
            credential=self.credential,
            subscription_id=self.subscription_id
        )

        # Synapse workspace endpoint
        self.workspace_endpoint = f"https://{workspace_name}.dev.azuresynapse.net"

        print(f"🔧 Initialized Synapse runner for workspace: {workspace_name}")
        print(f"🎯 Spark pool: {spark_pool_name}")

    def _get_azure_credential(self):
        """Get Azure credential for authentication"""
        # Try different credential methods
        try:
            # Try managed identity first (for Azure VMs, App Service, etc.)
            credential = DefaultAzureCredential()
            return credential
        except Exception as e:
            print(f"⚠️ DefaultAzureCredential failed: {e}")

            # Fallback to service principal if available
            client_id = os.getenv('AZURE_CLIENT_ID')
            client_secret = os.getenv('AZURE_CLIENT_SECRET')
            tenant_id = os.getenv('AZURE_TENANT_ID')

            if client_id and client_secret and tenant_id:
                print("🔑 Using service principal authentication")
                return ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret
                )

            raise Exception(
                "No valid Azure credentials found. Set up managed identity or service principal.")

    def deploy_test_app(self, app_name: str, app_directory: str) -> bool:
        """Deploy test app to Synapse workspace"""
        try:
            print(f"📦 Deploying test app '{app_name}' to Synapse...")

            # Upload app files to Synapse workspace storage
            app_path = Path(app_directory)

            if not app_path.exists():
                raise FileNotFoundError(
                    f"App directory not found: {app_directory}")

            # Create deployment package
            deployment_path = self._create_deployment_package(
                app_name, app_path)

            # Upload to Synapse workspace file system
            upload_success = self._upload_to_synapse(app_name, deployment_path)

            if upload_success:
                print(f"✅ Successfully deployed '{app_name}' to Synapse")
                return True
            else:
                print(f"❌ Failed to deploy '{app_name}' to Synapse")
                return False

        except Exception as e:
            print(f"❌ Deployment failed: {str(e)}")
            return False

    def _create_deployment_package(self, app_name: str, app_path: Path) -> Path:
        """Create deployment package with app files and framework"""
        import tempfile
        import shutil

        # Create temporary deployment directory
        temp_dir = Path(tempfile.mkdtemp(prefix=f"synapse_deploy_{app_name}_"))

        try:
            print(f"📁 Creating deployment package in: {temp_dir}")

            # Copy app files
            app_deploy_dir = temp_dir / "app"
            shutil.copytree(app_path, app_deploy_dir)

            # Copy Kindling framework (from src/)
            framework_src = Path(
                __file__).parent.parent.parent.parent / "src" / "kindling"
            if framework_src.exists():
                framework_deploy_dir = temp_dir / "kindling"
                shutil.copytree(framework_src, framework_deploy_dir)
                print(f"📚 Copied Kindling framework from: {framework_src}")
            else:
                print(f"⚠️ Kindling framework not found at: {framework_src}")

            # Copy test utilities
            test_utils_src = Path(
                __file__).parent.parent.parent / "spark_test_helper.py"
            if test_utils_src.exists():
                test_utils_dir = temp_dir / "tests"
                test_utils_dir.mkdir()
                shutil.copy2(test_utils_src, test_utils_dir /
                             "spark_test_helper.py")
                print("🧪 Copied test utilities")

            return temp_dir

        except Exception as e:
            # Cleanup on error
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise Exception(f"Failed to create deployment package: {str(e)}")

    def _upload_to_synapse(self, app_name: str, deployment_path: Path) -> bool:
        """Upload deployment package to Synapse workspace file system"""
        try:
            # Get storage account for the Synapse workspace
            # Default naming convention
            storage_account = f"{self.workspace_name}storage"

            # Create blob service client
            blob_service_client = BlobServiceClient(
                account_url=f"https://{storage_account}.blob.core.windows.net",
                credential=self.credential
            )

            # Upload files to workspace file system container
            container_name = "workspace"  # Default Synapse workspace container

            for file_path in deployment_path.rglob("*"):
                if file_path.is_file():
                    # Calculate relative path for blob name
                    relative_path = file_path.relative_to(deployment_path)
                    blob_name = f"system-tests/{app_name}/{relative_path}".replace(
                        "\\", "/")

                    # Upload file
                    blob_client = blob_service_client.get_blob_client(
                        container=container_name,
                        blob=blob_name
                    )

                    with open(file_path, 'rb') as data:
                        blob_client.upload_blob(data, overwrite=True)

                    print(f"📤 Uploaded: {blob_name}")

            return True

        except Exception as e:
            print(f"❌ Upload failed: {str(e)}")
            return False

    def run_test_app(self, app_name: str, environment_vars: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Execute test app as Spark job on Synapse"""
        try:
            print(f"🚀 Starting test app '{app_name}' on Synapse...")

            # Prepare Spark job configuration
            job_config = self._create_spark_job_config(
                app_name, environment_vars)

            # Submit Spark job
            job_result = self._submit_spark_job(job_config)

            if job_result["success"]:
                print(f"✅ Test app '{app_name}' completed successfully")

                # Collect results
                results = self._collect_test_results(
                    app_name, job_result["job_id"])
                return {
                    "success": True,
                    "job_id": job_result["job_id"],
                    "results": results
                }
            else:
                print(f"❌ Test app '{app_name}' failed")
                return {
                    "success": False,
                    "error": job_result.get("error", "Unknown error"),
                    "job_id": job_result.get("job_id")
                }

        except Exception as e:
            print(f"❌ Test execution failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }

    def _create_spark_job_config(self, app_name: str, environment_vars: Optional[Dict[str, str]]) -> Dict[str, Any]:
        """Create Synapse Spark job configuration"""

        # Default environment variables
        default_env_vars = {
            "AZURE_STORAGE_ACCOUNT": os.getenv("AZURE_STORAGE_ACCOUNT"),
            "AZURE_CONTAINER": os.getenv("AZURE_CONTAINER"),
            "AZURE_CLOUD": os.getenv("AZURE_CLOUD", "AZURE_PUBLIC_CLOUD"),
            "SYNAPSE_WORKSPACE_NAME": self.workspace_name,
            "AZURE_SUBSCRIPTION_ID": self.subscription_id,
            "AZURE_RESOURCE_GROUP": self.resource_group
        }

        # Merge with provided environment variables
        if environment_vars:
            default_env_vars.update(environment_vars)

        # Spark job configuration
        job_config = {
            "name": f"system-test-{app_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            "file": f"abfss://workspace@{self.workspace_name}storage.dfs.core.windows.net/system-tests/{app_name}/app/main.py",
            "className": "main",
            "args": [],
            "jars": [],
            "pyFiles": [
                f"abfss://workspace@{self.workspace_name}storage.dfs.core.windows.net/system-tests/{app_name}/kindling.zip",
                f"abfss://workspace@{self.workspace_name}storage.dfs.core.windows.net/system-tests/{app_name}/tests.zip"
            ],
            "files": [],
            "driverMemory": "4g",
            "driverCores": 2,
            "executorMemory": "4g",
            "executorCores": 2,
            "numExecutors": 3,
            "conf": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.synapse.linkedService.useDefaultCredential": "true"
            }
        }

        # Add environment variables
        if default_env_vars:
            job_config["conf"].update({
                f"spark.kubernetes.driverEnv.{k}": v
                for k, v in default_env_vars.items() if v is not None
            })

        return job_config

    def _submit_spark_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Submit Spark job to Synapse and wait for completion"""
        try:
            # Submit job using REST API (easier than management SDK for Spark jobs)
            submit_url = f"{self.workspace_endpoint}/sparkJobs"

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._get_access_token()}"
            }

            print(f"🎯 Submitting Spark job: {job_config['name']}")

            response = requests.post(
                submit_url, headers=headers, json=job_config)

            if response.status_code not in [200, 201]:
                return {
                    "success": False,
                    "error": f"Job submission failed: {response.status_code} {response.text}"
                }

            job_info = response.json()
            job_id = job_info.get("id")

            print(f"📋 Job submitted with ID: {job_id}")

            # Wait for job completion
            return self._wait_for_job_completion(job_id)

        except Exception as e:
            return {
                "success": False,
                "error": f"Job submission failed: {str(e)}"
            }

    def _get_access_token(self) -> str:
        """Get access token for Synapse REST API"""
        token = self.credential.get_token(
            "https://dev.azuresynapse.net/.default")
        return token.token

    def _wait_for_job_completion(self, job_id: str, timeout_minutes: int = 30) -> Dict[str, Any]:
        """Wait for Spark job to complete"""
        try:
            start_time = time.time()
            timeout_seconds = timeout_minutes * 60

            status_url = f"{self.workspace_endpoint}/sparkJobs/{job_id}"
            headers = {
                "Authorization": f"Bearer {self._get_access_token()}"
            }

            while time.time() - start_time < timeout_seconds:
                response = requests.get(status_url, headers=headers)

                if response.status_code != 200:
                    return {
                        "success": False,
                        "job_id": job_id,
                        "error": f"Failed to get job status: {response.status_code}"
                    }

                job_info = response.json()
                state = job_info.get("state", "unknown")

                print(f"🔄 Job {job_id} state: {state}")

                if state in ["success", "succeeded"]:
                    return {
                        "success": True,
                        "job_id": job_id,
                        "job_info": job_info
                    }
                elif state in ["failed", "error", "cancelled"]:
                    return {
                        "success": False,
                        "job_id": job_id,
                        "error": f"Job failed with state: {state}",
                        "job_info": job_info
                    }

                # Wait before next status check
                time.sleep(30)

            # Timeout
            return {
                "success": False,
                "job_id": job_id,
                "error": f"Job timeout after {timeout_minutes} minutes"
            }

        except Exception as e:
            return {
                "success": False,
                "job_id": job_id,
                "error": f"Job monitoring failed: {str(e)}"
            }

    def _collect_test_results(self, app_name: str, job_id: str) -> Optional[Dict[str, Any]]:
        """Collect test results from Azure storage"""
        try:
            print(f"📊 Collecting test results for job {job_id}...")

            # Results are saved by the test app to storage
            storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
            container = os.getenv("AZURE_CONTAINER")

            if not storage_account or not container:
                print("⚠️ Storage configuration not available for result collection")
                return None

            # Create blob service client
            blob_service_client = BlobServiceClient(
                account_url=f"https://{storage_account}.blob.core.windows.net",
                credential=self.credential
            )

            # Look for results files (app saves with timestamp)
            container_client = blob_service_client.get_container_client(
                container)

            results_prefix = "system-test-results/azure-storage-test/synapse_"

            # Find the most recent results file
            recent_blob = None
            recent_time = None

            for blob in container_client.list_blobs(name_starts_with=results_prefix):
                if recent_time is None or blob.last_modified > recent_time:
                    recent_time = blob.last_modified
                    recent_blob = blob

            if recent_blob:
                # Download and parse results
                blob_client = container_client.get_blob_client(
                    recent_blob.name)
                results_content = blob_client.download_blob().readall()
                results_data = json.loads(results_content.decode())

                print(f"✅ Test results collected from: {recent_blob.name}")
                return results_data
            else:
                print("⚠️ No test results found in storage")
                return None

        except Exception as e:
            print(f"❌ Failed to collect test results: {str(e)}")
            return None

    def run_system_test(self, app_name: str, app_directory: str,
                        environment_vars: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Complete system test workflow: deploy and run"""
        try:
            print(f"🚀 Running complete system test: {app_name}")

            # Step 1: Deploy app
            if not self.deploy_test_app(app_name, app_directory):
                return {
                    "success": False,
                    "error": "Deployment failed"
                }

            # Step 2: Run test
            result = self.run_test_app(app_name, environment_vars)

            return result

        except Exception as e:
            return {
                "success": False,
                "error": f"System test failed: {str(e)}"
            }


def main():
    """CLI entry point for Synapse test runner"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run system tests on Azure Synapse Analytics")
    parser.add_argument("app_name", help="Name of the test app to run")
    parser.add_argument("--workspace", required=True,
                        help="Synapse workspace name")
    parser.add_argument("--pool", required=True, help="Spark pool name")
    parser.add_argument("--subscription", required=True,
                        help="Azure subscription ID")
    parser.add_argument("--resource-group", required=True,
                        help="Azure resource group")
    parser.add_argument("--app-dir", help="Test app directory",
                        default=lambda: str(Path(__file__).parent.parent / "apps"))

    args = parser.parse_args()

    # Initialize runner
    runner = SynapseTestRunner(
        workspace_name=args.workspace,
        spark_pool_name=args.pool,
        subscription_id=args.subscription,
        resource_group=args.resource_group
    )

    # Run test
    app_directory = Path(args.app_dir) / args.app_name
    result = runner.run_system_test(args.app_name, str(app_directory))

    # Print results
    if result["success"]:
        print(f"\n🎉 System test '{args.app_name}' completed successfully!")
        if "results" in result and result["results"]:
            test_results = result["results"]
            print(f"📊 Test Status: {test_results.get('status')}")
            print(f"⏱️ Duration: {test_results.get('total_duration', 0):.1f}s")

            if "test_steps" in test_results:
                print("\n📋 Test Steps:")
                for step in test_results["test_steps"]:
                    status_emoji = "✅" if step["status"] == "passed" else "❌"
                    print(
                        f"  {status_emoji} {step['step']}: {step['status']} ({step['duration']}s)")

        sys.exit(0)
    else:
        print(f"\n❌ System test '{args.app_name}' failed!")
        print(f"Error: {result.get('error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
