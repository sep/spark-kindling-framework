"""
Test Azure storage access using storage account key.

Works with any Azure cloud environment (Commercial, Government, China).
Cloud configuration is detected automatically from environment variables.

This is the recommended approach for local development.
For production, use managed identities in Synapse/Fabric/Databricks.
"""
from tests.spark_test_helper import get_local_spark_session_with_azure
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


def test_azure_storage_with_key():
    # Read configuration from environment (set by init script or .env file)
    storage_account = os.getenv(
        'AZURE_STORAGE_ACCOUNT', 'your-storage-account')
    storage_container = os.getenv(
        'AZURE_CONTAINER', 'your-container')
    # 'public', 'government', or 'china'
    azure_cloud = os.getenv('AZURE_CLOUD', 'public')
    endpoint_suffix = os.getenv(
        'AZURE_STORAGE_ENDPOINT_SUFFIX', 'dfs.core.windows.net')
    account_key = os.getenv('AZURE_STORAGE_KEY')

    if not account_key:
        print("❌ AZURE_STORAGE_KEY environment variable not set")
        print("\nPlease run the initialization script:")
        print("  source scripts/init_azure_dev.sh")
        print("\nOr manually set the key:")
        print(
            f"  az storage account keys list --account-name {storage_account} --query \"[0].value\" -o tsv")
        print("  export AZURE_STORAGE_KEY=\"<your-key>\"")
        return False

    # Display cloud info
    cloud_names = {
        'public': 'Azure Commercial Cloud',
        'government': 'Azure Government Cloud',
        'china': 'Azure China Cloud'
    }
    cloud_display = cloud_names.get(azure_cloud, f'Azure ({azure_cloud})')

    print(f"Testing Azure storage access...")
    print(f"Cloud: {cloud_display} ({endpoint_suffix})")
    print(f"Storage account: {storage_account}")
    print(f"Container: {storage_container}")

    # Create Spark session with cloud configuration from environment
    spark = get_local_spark_session_with_azure(
        storage_account=storage_account,
        auth_type='key',
        azure_cloud=azure_cloud,
        account_key=account_key
    )

    print(f"\n✓ Spark session created (version {spark.version})")

    # Test write
    test_path = f"abfss://{storage_container}@{storage_account}.{endpoint_suffix}/test/test_file.txt"
    print(f"\nTesting write to: {test_path}")

    try:
        df = spark.createDataFrame([
            (f"Hello from {cloud_display}!",),
            ("Local Spark development with storage keys works!",),
            ("Ready for production with managed identities.",)
        ], ["message"])

        df.coalesce(1).write.mode("overwrite").text(test_path)
        print("✓ Write successful!")

        # Test read
        print(f"\nTesting read from: {test_path}")
        df_read = spark.read.text(test_path)

        print("✓ Read successful!")
        print("\nData retrieved:")
        for row in df_read.collect():
            print(f"  - {row[0]}")

        print(f"\n🎉 SUCCESS! {cloud_display} storage is working!")
        print("\nYou can now:")
        print(
            f"  - Access: abfss://{storage_container}@{storage_account}.{endpoint_suffix}/path")
        print("  - Use Delta Lake tables")
        print("  - Process data locally")
        print("\nFor production:")
        print(f"  - Deploy to Synapse/Fabric/Databricks in {cloud_display}")
        print("  - Use managed identities (no keys needed)")

        return True

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return False

    finally:
        spark.stop()


if __name__ == "__main__":
    success = test_azure_storage_with_key()
    sys.exit(0 if success else 1)
