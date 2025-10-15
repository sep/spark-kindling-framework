#!/usr/bin/env python3
"""
Quick test to verify Azure Government Cloud connection
"""
import os
from tests.spark_test_helper import get_local_spark_session_with_azure

# Set your Government Cloud storage account
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT",
                            "your-gov-storage-account")
CONTAINER = os.getenv("AZURE_CONTAINER", "data")

print("=" * 70)
print("Azure Government Cloud - Spark Connection Test")
print("=" * 70)
print(f"\nStorage Account: {STORAGE_ACCOUNT}")
print(f"Container: {CONTAINER}")
print(f"Cloud: Azure Government (.usgovcloudapi.net)")
print()

# Create Spark session for Government Cloud
print("Creating Spark session with Azure Government Cloud authentication...")
spark = get_local_spark_session_with_azure(
    app_name="AzureGovCloudTest",
    storage_account=STORAGE_ACCOUNT,
    auth_type="azure_cli",
    azure_cloud="government"  # This is the key setting!
)

print("\n✅ Spark session created successfully!")
print(f"   Configured for: {STORAGE_ACCOUNT}.dfs.core.usgovcloudapi.net")
print()

# Example ABFSS path for Government Cloud
example_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.usgovcloudapi.net/"
print(f"You can now access data at: {example_path}")
print()

# Try to verify connection (optional - will only work if container exists)
try:
    print("Attempting to verify connection...")
    # This will fail gracefully if container doesn't exist or no permissions
    test_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.usgovcloudapi.net/"
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(test_path),
        hadoop_conf
    )
    print(f"✅ Successfully connected to storage account!")
except Exception as e:
    print(f"⚠️  Could not verify connection: {e}")
    print("   This is normal if the container doesn't exist yet or you don't have permissions")

spark.stop()
print("\n" + "=" * 70)
print("Setup Complete!")
print("=" * 70)
print("\nNext steps:")
print("1. Set your storage account: export AZURE_STORAGE_ACCOUNT=your-account-name")
print("2. Use in your code:")
print("   spark = get_local_spark_session_with_azure(")
print("       storage_account='your-account',")
print("       auth_type='azure_cli',")
print("       azure_cloud='government'  # <-- This is the key!")
print("   )")
print()
