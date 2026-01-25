#!/usr/bin/env python3
"""
Cleanup orphaned test resources (jobs and data-apps) from system tests

Usage:
    python scripts/cleanup_test_resources.py --platform fabric
    python scripts/cleanup_test_resources.py --platform synapse
    python scripts/cleanup_test_resources.py --platform databricks
    python scripts/cleanup_test_resources.py --all  # Clean all platforms
"""

import argparse
import os
import sys
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def cleanup_fabric(storage_account: str, container: str, base_path: str = ""):
    """Clean up Fabric test resources"""
    from kindling.platform_fabric import FabricAPI

    print("🧹 Cleaning up Fabric test resources...")

    # Initialize Fabric client
    workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
    lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID")

    if not workspace_id or not lakehouse_id:
        print("⚠️  Skipping Fabric: Missing FABRIC_WORKSPACE_ID or FABRIC_LAKEHOUSE_ID")
        return 0, 0

    client = FabricAPI(
        workspace_id=workspace_id,
        lakehouse_id=lakehouse_id,
        storage_account=storage_account,
        container=container,
        base_path=base_path,
    )

    # Delete all system test jobs (systest-*)
    jobs_deleted = 0
    try:
        jobs = client.list_jobs()
        for job in jobs:
            job_name = job.get("displayName", "")
            if job_name.startswith("systest-"):
                job_id = job.get("id")
                try:
                    if client.delete_job(job_id):
                        print(f"  🗑️  Deleted job: {job_name}")
                        jobs_deleted += 1
                except Exception as e:
                    print(f"  ⚠️  Failed to delete job {job_name}: {e}")
    except Exception as e:
        print(f"⚠️  Error listing Fabric jobs: {e}")

    # Delete test data-apps from storage
    apps_deleted = cleanup_storage_apps(storage_account, container, base_path)

    return jobs_deleted, apps_deleted


def cleanup_synapse(storage_account: str, container: str, base_path: str = ""):
    """Clean up Synapse test resources"""
    from kindling.platform_synapse import SynapseAPI

    print("🧹 Cleaning up Synapse test resources...")

    workspace_name = os.getenv("SYNAPSE_WORKSPACE_NAME")
    spark_pool = os.getenv("SYNAPSE_SPARK_POOL") or os.getenv("SYNAPSE_SPARK_POOL_NAME")

    if not workspace_name or not spark_pool:
        print("⚠️  Skipping Synapse: Missing SYNAPSE_WORKSPACE_NAME or SYNAPSE_SPARK_POOL")
        return 0, 0

    client = SynapseAPI(
        workspace_name=workspace_name,
        spark_pool_name=spark_pool,
        storage_account=storage_account,
        container=container,
        base_path=base_path,
    )

    # Delete all system test jobs (systest-*)
    jobs_deleted = 0
    try:
        jobs = client.list_jobs()
        for job in jobs:
            job_name = job.get("name", "")
            if job_name.startswith("systest-"):
                job_id = job.get("id")
                try:
                    if client.delete_job(job_id):
                        print(f"  🗑️  Deleted job: {job_name}")
                        jobs_deleted += 1
                except Exception as e:
                    print(f"  ⚠️  Failed to delete job {job_name}: {e}")
    except Exception as e:
        print(f"⚠️  Error listing Synapse jobs: {e}")

    # Delete test data-apps from storage
    apps_deleted = cleanup_storage_apps(storage_account, container, base_path)

    return jobs_deleted, apps_deleted


def cleanup_databricks(storage_account: str, container: str, base_path: str = ""):
    """Clean up Databricks test resources"""
    from kindling.platform_databricks import DatabricksAPI

    print("🧹 Cleaning up Databricks test resources...")

    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")

    if not host or not token:
        print("⚠️  Skipping Databricks: Missing DATABRICKS_HOST or DATABRICKS_TOKEN")
        return 0, 0

    client = DatabricksAPI(
        host=host,
        token=token,
        cluster_id=cluster_id,
        storage_account=storage_account,
        container=container,
        base_path=base_path,
    )

    # Delete all system test jobs (systest-*)
    jobs_deleted = 0
    try:
        jobs = client.list_jobs()
        for job in jobs:
            job_name = job.get("settings", {}).get("name", "")
            if job_name.startswith("systest-"):
                job_id = job.get("job_id")
                try:
                    if client.delete_job(job_id):
                        print(f"  🗑️  Deleted job: {job_name}")
                        jobs_deleted += 1
                except Exception as e:
                    print(f"  ⚠️  Failed to delete job {job_name}: {e}")
    except Exception as e:
        print(f"⚠️  Error listing Databricks jobs: {e}")

    # Delete test data-apps from storage
    apps_deleted = cleanup_storage_apps(storage_account, container, base_path)

    return jobs_deleted, apps_deleted


def cleanup_storage_apps(storage_account: str, container: str, base_path: str = ""):
    """Clean up test data-apps from Azure storage"""
    apps_deleted = 0

    try:
        account_url = f"https://{storage_account}.dfs.core.windows.net"
        credential = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url, credential=credential)
        file_system_client = service_client.get_file_system_client(container)

        data_apps_path = f"{base_path}/data-apps" if base_path else "data-apps"

        # List and delete test apps
        paths = file_system_client.get_paths(path=data_apps_path)
        for path in paths:
            if path.is_directory:
                app_name = path.name.split("/")[-1]
                if app_name.startswith("universal-test-app-") or "test" in app_name.lower():
                    try:
                        directory_client = file_system_client.get_directory_client(path.name)
                        directory_client.delete_directory()
                        print(f"  🗑️  Deleted data-app: {app_name}")
                        apps_deleted += 1
                    except Exception as e:
                        print(f"  ⚠️  Failed to delete data-app {app_name}: {e}")
    except Exception as e:
        print(f"⚠️  Error cleaning up storage apps: {e}")

    return apps_deleted


def main():
    parser = argparse.ArgumentParser(description="Clean up test resources from platforms")
    parser.add_argument(
        "--platform",
        choices=["fabric", "synapse", "databricks"],
        help="Platform to clean up (fabric, synapse, or databricks)",
    )
    parser.add_argument("--all", action="store_true", help="Clean up all platforms")

    args = parser.parse_args()

    if not args.platform and not args.all:
        parser.error("Must specify either --platform or --all")

    # Get storage configuration
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container = os.getenv("AZURE_CONTAINER", "artifacts")
    base_path = os.getenv("AZURE_BASE_PATH", "")

    if not storage_account:
        print("❌ Error: AZURE_STORAGE_ACCOUNT environment variable not set")
        sys.exit(1)

    print("🧹 Kindling Test Resource Cleanup")
    print("=" * 50)
    print(f"Storage: {storage_account}/{container}")
    print()

    total_jobs = 0
    total_apps = 0

    platforms = []
    if args.all:
        platforms = ["fabric", "synapse", "databricks"]
    else:
        platforms = [args.platform]

    for platform in platforms:
        try:
            if platform == "fabric":
                jobs, apps = cleanup_fabric(storage_account, container, base_path)
            elif platform == "synapse":
                jobs, apps = cleanup_synapse(storage_account, container, base_path)
            elif platform == "databricks":
                jobs, apps = cleanup_databricks(storage_account, container, base_path)

            total_jobs += jobs
            total_apps += apps
            print()
        except Exception as e:
            print(f"❌ Error cleaning up {platform}: {e}")
            print()

    print("=" * 50)
    print(f"✅ Cleanup complete!")
    print(f"   Jobs deleted: {total_jobs}")
    print(f"   Data-apps deleted: {total_apps}")
    print(f"   Total: {total_jobs + total_apps}")


if __name__ == "__main__":
    main()
