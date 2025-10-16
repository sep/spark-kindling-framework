"""
Example: Using Azure Storage with Azure CLI authentication
"""
import os
import pytest
from tests.spark_test_helper import get_local_spark_session_with_azure, _get_azure_cloud_config


# Configure your storage account
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "mystorageacct")
CONTAINER = os.getenv("AZURE_CONTAINER", "data")

# Get the appropriate endpoint suffix for the Azure cloud
AZURE_CLOUD = os.getenv("AZURE_CLOUD", "AZURE_PUBLIC_CLOUD")
CLOUD_CONFIG = _get_azure_cloud_config(AZURE_CLOUD)
DFS_SUFFIX = CLOUD_CONFIG['dfs_suffix']


@pytest.fixture(scope="module")
def spark_azure():
    """
    Spark session with Azure storage configured via storage account key.

    Requires:
        1. AZURE_STORAGE_ACCOUNT environment variable
        2. AZURE_STORAGE_KEY environment variable  
        3. AZURE_CONTAINER environment variable
        4. Storage account permissions (Storage Blob Data Contributor or Reader)
    """
    # Check if required environment variables are set
    if not STORAGE_ACCOUNT or STORAGE_ACCOUNT == "mystorageacct":
        pytest.skip(
            "AZURE_STORAGE_ACCOUNT environment variable not configured for integration tests")

    if not CONTAINER or CONTAINER == "data":
        pytest.skip(
            "AZURE_CONTAINER environment variable not configured for integration tests")

    # Skip environment checking during tests to avoid prompting for input
    try:
        spark = get_local_spark_session_with_azure(
            app_name="AzureStorageExample",
            storage_account=STORAGE_ACCOUNT,
            auth_type="key",  # Use storage account key from environment
            azure_cloud=os.getenv('AZURE_CLOUD', 'AZURE_US_GOVERNMENT'),
            check_env=False,  # Don't prompt for input during tests
            account_key=os.getenv('AZURE_STORAGE_KEY')
        )
    except Exception as e:
        pytest.skip(f"Failed to create Azure Spark session: {e}")

    yield spark
    spark.stop()


def test_write_and_read_parquet_roundtrip(spark_azure):
    """Example: Write and read parquet file to/from Azure storage (round-trip test)"""
    # Create test data
    test_data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (3, "Charlie", 35)
    ]
    original_df = spark_azure.createDataFrame(test_data, ["id", "name", "age"])

    # Construct ABFSS path for our test
    test_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.{DFS_SUFFIX}/test/roundtrip_test/"

    try:
        # Write data
        original_df.write.mode("overwrite").parquet(test_path)
        print(f"✓ Wrote {original_df.count()} rows to {test_path}")

        # Read data back
        read_df = spark_azure.read.parquet(test_path)

        # Verify the data matches
        assert read_df.count() == original_df.count()
        assert read_df.schema == original_df.schema

        # Verify actual data content
        original_data = original_df.collect()
        read_data = read_df.collect()
        assert sorted(original_data) == sorted(read_data)

        print(
            f"✓ Successfully read back {read_df.count()} rows with matching data")

    finally:
        # Clean up test data
        try:
            from tests.spark_test_helper import cleanup_test_data
            cleanup_test_data(spark_azure, test_path)
            print(f"✓ Cleaned up test data at {test_path}")
        except Exception as e:
            print(f"⚠ Warning: Could not clean up {test_path}: {e}")


def test_write_and_read_delta_roundtrip(spark_azure):
    """Example: Write and read Delta table to/from Azure storage (round-trip test)"""
    # Create test data
    test_data = [
        (1, "Product A", 100.0),
        (2, "Product B", 200.0),
        (3, "Product C", 150.0)
    ]
    original_df = spark_azure.createDataFrame(
        test_data, ["id", "product", "price"])

    # Construct ABFSS path for our Delta test
    delta_test_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.{DFS_SUFFIX}/delta/roundtrip_test"

    try:
        # Write as Delta table
        original_df.write.format("delta").mode(
            "overwrite").save(delta_test_path)
        print(
            f"✓ Wrote Delta table with {original_df.count()} rows to {delta_test_path}")

        # Read Delta table back
        read_df = spark_azure.read.format("delta").load(delta_test_path)

        # Verify the data matches
        assert read_df.count() == original_df.count()
        assert read_df.schema == original_df.schema

        # Verify actual data content
        original_data = original_df.collect()
        read_data = read_df.collect()
        assert sorted(original_data) == sorted(read_data)

        print(
            f"✓ Successfully read back Delta table with {read_df.count()} rows and matching data")

    finally:
        # Clean up test data
        try:
            from tests.spark_test_helper import cleanup_test_data
            cleanup_test_data(spark_azure, delta_test_path)
            print(f"✓ Cleaned up test Delta table at {delta_test_path}")
        except Exception as e:
            print(f"⚠ Warning: Could not clean up {delta_test_path}: {e}")


if __name__ == "__main__":
    """
    Run this example directly (not as pytest)

    Usage:
        # Set your environment variables
        export AZURE_STORAGE_ACCOUNT=mystorageacct
        export AZURE_STORAGE_KEY=your-storage-key
        export AZURE_CONTAINER=data
        export AZURE_CLOUD=AZURE_US_GOVERNMENT  # For Government Cloud

        # Run
        python tests/integration/test_azure_storage_example.py
    """
    print("Setting up Spark with Azure storage...")
    spark = get_local_spark_session_with_azure(
        app_name="AzureStorageExample",
        storage_account=STORAGE_ACCOUNT,
        auth_type="key",  # Use storage account key
        azure_cloud=os.getenv('AZURE_CLOUD', 'AZURE_US_GOVERNMENT'),
        account_key=os.getenv('AZURE_STORAGE_KEY')
    )

    print("\n" + "="*60)
    print("Spark session ready with Azure storage access!")
    print(f"Storage Account: {STORAGE_ACCOUNT}")
    print(f"Container: {CONTAINER}")
    print("="*60 + "\n")

    # Example: List files (if supported)
    try:
        path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.{DFS_SUFFIX}/"
        print(f"Listing files in {path}:")
        files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jsc.hadoopConfiguration()
        ).listStatus(
            spark._jvm.org.apache.hadoop.fs.Path(path)
        )
        for f in files:
            print(f"  - {f.getPath().getName()}")
    except Exception as e:
        print(f"Could not list files: {e}")

    spark.stop()
    print("\n✓ Example complete!")
