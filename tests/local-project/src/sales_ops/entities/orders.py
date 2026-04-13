from kindling.data_entities import DataEntities
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

orders_schema = StructType(
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

clean_orders_schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("order_date", StringType(), False),
        StructField("status", StringType(), False),
        StructField("total_value", DoubleType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)

DataEntities.entity(
    entityid="bronze.orders",
    name="bronze_orders",
    partition_columns=["order_date"],
    merge_columns=["order_id"],
    tags={
        "provider_type": "delta",
        "layer": "bronze",
    },
    schema=orders_schema,
)

DataEntities.entity(
    entityid="silver.orders",
    name="silver_orders",
    partition_columns=["order_date"],
    merge_columns=["order_id"],
    tags={
        "provider_type": "delta",
        "layer": "silver",
    },
    schema=clean_orders_schema,
)
