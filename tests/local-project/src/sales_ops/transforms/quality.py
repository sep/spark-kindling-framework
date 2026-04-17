"""Pure PySpark transform functions — no Kindling decorators, no registry dependencies.

These can be unit-tested directly with a local SparkSession and small
DataFrames, without initialising the Kindling framework at all.
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def drop_null_keys(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Drop rows where any key column is null."""
    condition = None
    for col in key_columns:
        cond = F.col(col).isNotNull()
        condition = cond if condition is None else condition & cond
    return df.filter(condition)


def add_total_value(df: DataFrame) -> DataFrame:
    """Add total_value = quantity * unit_price."""
    return df.withColumn("total_value", F.col("quantity") * F.col("unit_price"))


def add_ingested_at(df: DataFrame) -> DataFrame:
    """Add ingested_at audit column with current UTC timestamp as ISO string."""
    ts = datetime.now(timezone.utc).isoformat()
    return df.withColumn("ingested_at", F.lit(ts))


def clean_orders(df: DataFrame) -> DataFrame:
    """Full cleaning pipeline for bronze → silver orders.

    Steps:
      1. Drop rows with null key columns
      2. Compute total_value
      3. Stamp ingested_at
    """
    return (
        df.transform(drop_null_keys, ["order_id", "customer_id", "product_id"])
        .transform(add_total_value)
        .transform(add_ingested_at)
    )
