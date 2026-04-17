"""Unit tests for sales_ops.transforms.quality.

No Kindling framework initialisation required. Tests call transform functions
directly with small DataFrames created from a local SparkSession.
"""

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


@pytest.mark.unit
class TestDropNullKeys:
    def test_drops_rows_with_null_key(self, spark_local):
        from sales_ops.transforms.quality import drop_null_keys

        schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("amount", DoubleType(), True),
            ]
        )
        data = [
            ("O1", "C1", 10.0),
            (None, "C2", 20.0),  # null order_id — should be dropped
            ("O3", None, 30.0),  # null customer_id — should be dropped
            ("O4", "C4", 40.0),
        ]
        df = spark_local.createDataFrame(data, schema)

        result = drop_null_keys(df, ["order_id", "customer_id"])

        assert result.count() == 2
        ids = {row.order_id for row in result.collect()}
        assert ids == {"O1", "O4"}

    def test_keeps_all_rows_when_no_nulls(self, spark_local):
        from sales_ops.transforms.quality import drop_null_keys

        schema = StructType([StructField("order_id", StringType(), True)])
        df = spark_local.createDataFrame([("A",), ("B",)], schema)

        assert drop_null_keys(df, ["order_id"]).count() == 2


@pytest.mark.unit
class TestAddTotalValue:
    def test_computes_quantity_times_price(self, spark_local):
        from sales_ops.transforms.quality import add_total_value

        schema = StructType(
            [
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
            ]
        )
        df = spark_local.createDataFrame([(3, 5.0), (2, 10.0)], schema)

        result = add_total_value(df)

        totals = {row.total_value for row in result.collect()}
        assert totals == {15.0, 20.0}

    def test_total_value_column_added(self, spark_local):
        from sales_ops.transforms.quality import add_total_value

        schema = StructType(
            [
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
            ]
        )
        df = spark_local.createDataFrame([(1, 1.0)], schema)
        result = add_total_value(df)

        assert "total_value" in result.columns


@pytest.mark.unit
class TestAddIngestedAt:
    def test_adds_ingested_at_column(self, spark_local):
        from sales_ops.transforms.quality import add_ingested_at

        schema = StructType([StructField("id", StringType(), True)])
        df = spark_local.createDataFrame([("x",)], schema)
        result = add_ingested_at(df)

        assert "ingested_at" in result.columns

    def test_ingested_at_is_iso_string(self, spark_local):
        from datetime import datetime

        from sales_ops.transforms.quality import add_ingested_at

        schema = StructType([StructField("id", StringType(), True)])
        df = spark_local.createDataFrame([("x",)], schema)
        result = add_ingested_at(df).collect()[0]

        # Should parse as an ISO datetime without raising
        datetime.fromisoformat(result.ingested_at)


@pytest.mark.unit
class TestCleanOrders:
    def _make_df(self, spark, rows):
        schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("order_date", StringType(), True),
                StructField("status", StringType(), True),
            ]
        )
        return spark.createDataFrame(rows, schema)

    def test_drops_incomplete_rows(self, spark_local):
        from sales_ops.transforms.quality import clean_orders

        rows = [
            ("O1", "C1", "P1", 2, 5.0, "2024-01-01", "new"),
            (None, "C2", "P2", 1, 3.0, "2024-01-01", "new"),  # dropped
        ]
        df = self._make_df(spark_local, rows)
        result = clean_orders(df)

        assert result.count() == 1

    def test_output_has_all_required_columns(self, spark_local):
        from sales_ops.transforms.quality import clean_orders

        rows = [("O1", "C1", "P1", 2, 5.0, "2024-01-01", "new")]
        df = self._make_df(spark_local, rows)
        result = clean_orders(df)

        assert "total_value" in result.columns
        assert "ingested_at" in result.columns
