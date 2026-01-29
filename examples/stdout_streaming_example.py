"""
Example: Real-time Stdout Log Streaming for Spark Jobs (All Platforms)

This example demonstrates how to stream stdout logs from a running
Spark job in real-time using platform APIs. Works with:
- Microsoft Fabric (FabricAPI)
- Azure Synapse Analytics (SynapseAPI)
- Databricks (DatabricksAPI)

Requirements:
    Platform-specific environment variables:

    Fabric:
        - FABRIC_WORKSPACE_ID
        - FABRIC_LAKEHOUSE_ID
        - AZURE_STORAGE_ACCOUNT (optional)

    Synapse:
        - SYNAPSE_WORKSPACE_NAME
        - SYNAPSE_SPARK_POOL
        - AZURE_STORAGE_ACCOUNT

    Databricks:
        - DATABRICKS_HOST
        - DATABRICKS_TOKEN (or use Azure CLI auth)

    - Azure credentials (az login or service principal)

Usage:
    # Fabric
    python examples/stdout_streaming_example.py fabric <job_id> <run_id>

    # Synapse
    python examples/stdout_streaming_example.py synapse <job_name> <batch_id>

    # Databricks
    python examples/stdout_streaming_example.py databricks <job_id> <run_id>
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# Add packages to path
sys.path.insert(0, str(Path(__file__).parent.parent / "packages"))


def print_with_timestamp(line: str):
    """Callback function to print logs with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {line}")


def create_fabric_client():
    """Create Fabric API client"""
    from kindling.platform_fabric import FabricAPI

    workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
    lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID")
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container = os.getenv("AZURE_CONTAINER", "artifacts")

    if not workspace_id or not lakehouse_id:
        print("❌ FABRIC_WORKSPACE_ID and FABRIC_LAKEHOUSE_ID must be set")
        sys.exit(1)

    print(f"📡 Connecting to Fabric workspace: {workspace_id}")
    return FabricAPI(
        workspace_id=workspace_id,
        lakehouse_id=lakehouse_id,
        storage_account=storage_account,
        container=container,
    )


def create_synapse_client():
    """Create Synapse API client"""
    from kindling.platform_synapse import SynapseAPI

    workspace_name = os.getenv("SYNAPSE_WORKSPACE_NAME")
    spark_pool_name = os.getenv("SYNAPSE_SPARK_POOL")
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container = os.getenv("AZURE_CONTAINER", "artifacts")

    if not workspace_name or not spark_pool_name:
        print("❌ SYNAPSE_WORKSPACE_NAME and SYNAPSE_SPARK_POOL must be set")
        sys.exit(1)

    print(f"📡 Connecting to Synapse workspace: {workspace_name}")
    return SynapseAPI(
        workspace_name=workspace_name,
        spark_pool_name=spark_pool_name,
        storage_account=storage_account,
        container=container,
    )


def create_databricks_client():
    """Create Databricks API client"""
    from kindling.platform_databricks import DatabricksAPI

    workspace_url = os.getenv("DATABRICKS_HOST")
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    container = os.getenv("AZURE_CONTAINER", "artifacts")

    if not workspace_url:
        print("❌ DATABRICKS_HOST must be set")
        sys.exit(1)

    print(f"📡 Connecting to Databricks workspace: {workspace_url}")
    return DatabricksAPI(
        workspace_url=workspace_url,
        storage_account=storage_account,
        container=container,
    )


def main():
    if len(sys.argv) < 4:
        print("Usage: python stdout_streaming_example.py <platform> <job_id> <run_id>")
        print("  platform: fabric, synapse, or databricks")
        print("  job_id: Job/batch identifier")
        print("  run_id: Run/instance identifier")
        sys.exit(1)

    platform = sys.argv[1].lower()
    job_id = sys.argv[2]
    run_id = sys.argv[3]

    # Create platform-specific client
    if platform == "fabric":
        api = create_fabric_client()
    elif platform == "synapse":
        api = create_synapse_client()
    elif platform == "databricks":
        api = create_databricks_client()
    else:
        print(f"❌ Unknown platform: {platform}")
        print("   Supported: fabric, synapse, databricks")
        sys.exit(1)

    print(f"🔍 Streaming stdout for {platform.upper()} job: {job_id}")
    print(f"   Run ID: {run_id}")
    print("=" * 80)

    # Stream stdout with callback (same API across all platforms!)
    logs = api.stream_stdout_logs(
        job_id=job_id,
        run_id=run_id,
        callback=print_with_timestamp,
        poll_interval=5.0,  # Poll every 5 seconds
        max_wait=300.0,  # Wait up to 5 minutes for job to start
    )

    print("=" * 80)
    print(f"✅ Streaming complete - captured {len(logs)} lines")


if __name__ == "__main__":
    main()
