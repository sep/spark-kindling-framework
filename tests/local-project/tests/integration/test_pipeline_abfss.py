"""Integration tests — reads and writes against real ABFSS storage.

Two auth paths are covered:

  SP OAuth (requires_azure)    — CI path; uses service principal creds.
    AZURE_STORAGE_ACCOUNT, AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
    ABFSS_BRONZE_ORDERS_PATH, ABFSS_SILVER_ORDERS_PATH

  Azure CLI (requires_az_login) — local dev path; uses az login session.
    AZURE_STORAGE_ACCOUNT
    ABFSS_BRONZE_ORDERS_PATH, ABFSS_SILVER_ORDERS_PATH

Each class is skipped automatically when its prerequisites are absent.
"""

import os

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _abfss_path(env_var: str) -> str:
    value = os.environ.get(env_var)
    if not value:
        pytest.skip(f"{env_var} not set")
    return value


@pytest.fixture
def sample_orders(spark_abfss):
    """Small representative DataFrame matching the bronze.orders schema."""
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("order_date", StringType(), True),
            StructField("status", StringType(), True),
        ]
    )
    data = [
        ("ORD-001", "CUST-A", "PROD-X", 3, 12.50, "2024-01-15", "confirmed"),
        ("ORD-002", "CUST-B", "PROD-Y", 1, 8.00, "2024-01-15", "pending"),
        ("ORD-003", None, "PROD-Z", 2, 5.00, "2024-01-16", "confirmed"),  # null customer
        ("ORD-004", "CUST-D", "PROD-X", 4, 12.50, "2024-01-16", "confirmed"),
    ]
    return spark_abfss.createDataFrame(data, schema)


@pytest.mark.integration
@pytest.mark.requires_azure
class TestABFSSWriteRead:
    def test_write_and_read_bronze_orders(self, spark_abfss, sample_orders):
        """Write sample orders to ABFSS and read them back — verifies path and credentials."""
        bronze_path = _abfss_path("ABFSS_BRONZE_ORDERS_PATH")

        (
            sample_orders.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(bronze_path)
        )

        read_back = spark_abfss.read.format("delta").load(bronze_path)
        assert read_back.count() == sample_orders.count()

    def test_clean_orders_written_to_silver(self, spark_abfss, sample_orders):
        """Apply clean_orders transform and write result to silver ABFSS path."""
        from sales_ops.transforms.quality import clean_orders

        bronze_path = _abfss_path("ABFSS_BRONZE_ORDERS_PATH")
        silver_path = _abfss_path("ABFSS_SILVER_ORDERS_PATH")

        # Seed bronze
        (
            sample_orders.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(bronze_path)
        )

        # Read, transform, write silver
        bronze_df = spark_abfss.read.format("delta").load(bronze_path)
        silver_df = clean_orders(bronze_df)

        (
            silver_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )

        result = spark_abfss.read.format("delta").load(silver_path)

        # ORD-003 has null customer_id — should have been dropped
        assert result.count() == 3
        assert "total_value" in result.columns
        assert "ingested_at" in result.columns

    def test_total_values_are_correct(self, spark_abfss, sample_orders):
        """Assert computed total_value matches quantity * unit_price."""
        from sales_ops.transforms.quality import clean_orders

        silver_df = clean_orders(sample_orders)
        rows = {r.order_id: r for r in silver_df.collect()}

        assert rows["ORD-001"].total_value == pytest.approx(37.50)
        assert rows["ORD-002"].total_value == pytest.approx(8.00)
        assert rows["ORD-004"].total_value == pytest.approx(50.00)


@pytest.fixture
def sample_orders_az_cli(spark_abfss_az_cli):
    """Same representative DataFrame as sample_orders, for the az CLI session."""
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("order_date", StringType(), True),
            StructField("status", StringType(), True),
        ]
    )
    data = [
        ("ORD-001", "CUST-A", "PROD-X", 3, 12.50, "2024-01-15", "confirmed"),
        ("ORD-002", "CUST-B", "PROD-Y", 1, 8.00, "2024-01-15", "pending"),
        ("ORD-003", None, "PROD-Z", 2, 5.00, "2024-01-16", "confirmed"),
        ("ORD-004", "CUST-D", "PROD-X", 4, 12.50, "2024-01-16", "confirmed"),
    ]
    return spark_abfss_az_cli.createDataFrame(data, schema)


@pytest.mark.integration
@pytest.mark.requires_az_login
class TestABFSSWriteReadAzCli:
    """Same write/read/transform coverage as TestABFSSWriteRead, using az CLI auth.

    Run locally after `az login`. No service principal credentials required.
    """

    def test_write_and_read_bronze_orders(self, spark_abfss_az_cli, sample_orders_az_cli):
        """Write sample orders to ABFSS and read them back via az CLI auth."""
        bronze_path = _abfss_path("ABFSS_BRONZE_ORDERS_PATH")

        (
            sample_orders_az_cli.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(bronze_path)
        )

        read_back = spark_abfss_az_cli.read.format("delta").load(bronze_path)
        assert read_back.count() == sample_orders_az_cli.count()

    def test_clean_orders_written_to_silver(self, spark_abfss_az_cli, sample_orders_az_cli):
        """Apply clean_orders transform and write result to silver ABFSS path."""
        from sales_ops.transforms.quality import clean_orders

        bronze_path = _abfss_path("ABFSS_BRONZE_ORDERS_PATH")
        silver_path = _abfss_path("ABFSS_SILVER_ORDERS_PATH")

        (
            sample_orders_az_cli.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(bronze_path)
        )

        bronze_df = spark_abfss_az_cli.read.format("delta").load(bronze_path)
        silver_df = clean_orders(bronze_df)

        (
            silver_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )

        result = spark_abfss_az_cli.read.format("delta").load(silver_path)

        assert result.count() == 3
        assert "total_value" in result.columns
        assert "ingested_at" in result.columns

    def test_total_values_are_correct(self, spark_abfss_az_cli, sample_orders_az_cli):
        """Assert computed total_value matches quantity * unit_price."""
        from sales_ops.transforms.quality import clean_orders

        silver_df = clean_orders(sample_orders_az_cli)
        rows = {r.order_id: r for r in silver_df.collect()}

        assert rows["ORD-001"].total_value == pytest.approx(37.50)
        assert rows["ORD-002"].total_value == pytest.approx(8.00)
        assert rows["ORD-004"].total_value == pytest.approx(50.00)


@pytest.mark.integration
@pytest.mark.requires_azure
class TestPipelineViaKindling:
    """End-to-end: initialise framework, register entities/pipes, execute via Kindling.

    These tests rely on the DI graph established by the first initialize() call in
    this process (which may come from the component test suite or the first test in
    this class). initialize_framework() returns early if already initialised, so
    calling initialize() here is idempotent — it either bootstraps fresh or returns
    the existing platform service.
    """

    def test_initialize_standalone_framework_with_local_config(self):
        """Framework initialises against standalone platform using local YAML config."""
        from app import initialize

        svc = initialize(env="local")

        assert svc is not None
        assert svc.get_platform_name() == "standalone"

    def test_entity_paths_resolved_from_env(self):
        """After framework init, entity tags contain the ABFSS paths from env vars."""
        from app import initialize
        from kindling.data_entities import DataEntityRegistry
        from kindling.injection import GlobalInjector

        initialize(env="local")

        registry: DataEntityRegistry = GlobalInjector.get(DataEntityRegistry)
        bronze = registry.get_entity_definition("bronze.orders")

        expected_path = _abfss_path("ABFSS_BRONZE_ORDERS_PATH")
        assert bronze.tags.get("provider.path") == expected_path
