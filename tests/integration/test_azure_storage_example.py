"""
Example: Using Azure Storage with Azure CLI authentication
"""
import os
import pytest
from tests.spark_test_helper import get_local_spark_session_with_azure


# Configure your storage account
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "mystorageacct")
CONTAINER = os.getenv("AZURE_CONTAINER", "data")


@pytest.fixture(scope="module")
def spark_azure():
    """
    Spark session with Azure storage configured via Azure CLI.

    Requires:
        1. az login (to authenticate)
        2. pip install azure-identity
        3. Storage account permissions (Storage Blob Data Contributor or Reader)
    """
    spark = get_local_spark_session_with_azure(
        app_name="AzureStorageExample",
        storage_account=STORAGE_ACCOUNT,
        auth_type="azure_cli"  # Uses your 'az login' credentials
    )
    yield spark
    spark.stop()


def test_read_parquet_from_abfss(spark_azure):
    """Example: Read parquet file from Azure storage"""
    # Construct ABFSS path
    path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/test/sample.parquet"

    # Read data
    df = spark_azure.read.parquet(path)

    # Verify
    assert df.count() > 0
    print(f"✓ Read {df.count()} rows from {path}")


def test_write_parquet_to_abfss(spark_azure):
    """Example: Write parquet file to Azure storage"""
    # Create test data
    test_data = [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (3, "Charlie", 35)
    ]
    df = spark_azure.createDataFrame(test_data, ["id", "name", "age"])

    # Construct ABFSS path
    output_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/test/output/"

    # Write data
    df.write.mode("overwrite").parquet(output_path)

    # Verify by reading back
    result_df = spark_azure.read.parquet(output_path)
    assert result_df.count() == 3
    print(f"✓ Wrote {result_df.count()} rows to {output_path}")


def test_read_delta_from_abfss(spark_azure):
    """Example: Read Delta table from Azure storage"""
    # Construct ABFSS path to Delta table
    delta_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/delta/my_table"

    # Read Delta table
    df = spark_azure.read.format("delta").load(delta_path)

    # Verify
    assert df.count() > 0
    print(f"✓ Read {df.count()} rows from Delta table at {delta_path}")


def test_write_delta_to_abfss(spark_azure):
    """Example: Write Delta table to Azure storage"""
    # Create test data
    test_data = [
        (1, "Product A", 100.0),
        (2, "Product B", 200.0)
    ]
    df = spark_azure.createDataFrame(test_data, ["id", "product", "price"])

    # Construct ABFSS path for Delta table
    delta_output = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/delta/test_output"

    # Write as Delta table
    df.write.format("delta").mode("overwrite").save(delta_output)

    # Verify by reading back
    result_df = spark_azure.read.format("delta").load(delta_output)
    assert result_df.count() == 2
    print(
        f"✓ Wrote Delta table with {result_df.count()} rows to {delta_output}")


if __name__ == "__main__":
    """
    Run this example directly (not as pytest)

    Usage:
        # Make sure you're logged in
        az login

        # Set your storage account
        export AZURE_STORAGE_ACCOUNT=mystorageacct
        export AZURE_CONTAINER=data

        # Run
        python tests/integration/test_azure_storage_example.py
    """
    print("Setting up Spark with Azure storage...")
    spark = get_local_spark_session_with_azure(
        app_name="AzureStorageExample",
        storage_account=STORAGE_ACCOUNT,
        auth_type="azure_cli"
    )

    print("\n" + "="*60)
    print("Spark session ready with Azure storage access!")
    print(f"Storage Account: {STORAGE_ACCOUNT}")
    print(f"Container: {CONTAINER}")
    print("="*60 + "\n")

    # Example: List files (if supported)
    try:
        path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
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
