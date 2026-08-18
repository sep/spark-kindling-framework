"""Minimal two-pipe declared flow exercising the general Lakeflow SDP
execution path: application selection, configuration overlays, dependency
inference, materialized views, dataset metadata, and Lakeflow expectations.

Deployed through the Lakeflow app selector
(`kindling_ext_databricks.lakeflow_app_selector`): the pipeline's
`kindling.data_app` config selects this package's `spark_kindling.data_apps`
entry point, the selector initializes Kindling with the Databricks SDP
engine, calls :func:`register_all`, and declares the pipeline.

The graph:

- ``bronze.orders`` — a zero-input materialized view (content built by the
  pipe body itself, like ``lakeflow_scd_app``'s snapshot pipe): four
  deterministic rows covering a valid case, a row that violates a warning
  expectation (``order_id IS NULL``), and a row that violates a drop
  expectation (``quantity <= 0``).
- ``silver.orders`` — a materialized view reading ``bronze.orders``
  (SDP infers this dependency from ``input_entity_ids``) that adds a
  computed ``total_amount`` column. ``provider.amqp_headers``-style
  overlay configuration (here: ``sdp``/``databricks_sdp`` engine config
  blocks) applies ``table_properties`` and the two expectations at
  declare time — this app makes no expectation/table-property
  declarations itself; that is exactly what the overlay configuration
  under test is exercising.

Registration only happens inside :func:`register_all`; importing this
module has no side effects (the selector requires declaration-only apps).
"""


def register_all() -> None:
    from pyspark.sql.functions import col
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    from kindling.data_entities import DataEntities
    from kindling.data_pipes import DataPipes

    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("amount", DoubleType(), False),
        ]
    )

    DataEntities.entity(
        entityid="bronze.orders",
        name="orders",
        merge_columns=["order_id"],
        tags={"provider_type": "delta"},
        schema=orders_schema,
        partition_columns=[],
    )

    ORDER_ROWS = [
        ("o1", "c1", 5, 100.0),  # valid
        ("o2", "c2", 3, 60.0),  # valid
        (None, "c3", 2, 40.0),  # violates the warning expectation (kept)
        ("o4", "c4", -1, 20.0),  # violates the drop expectation (removed)
    ]

    @DataPipes.pipe(
        pipeid="lakeflow.bronze_orders",
        name="Bronze Orders",
        input_entity_ids=[],
        output_entity_id="bronze.orders",
        output_type="delta",
        tags={},
        use_watermark=False,
    )
    def bronze_orders():
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        return spark.createDataFrame(ORDER_ROWS, orders_schema)

    DataEntities.entity(
        entityid="silver.orders",
        name="orders",
        merge_columns=["order_id"],
        tags={"provider_type": "delta"},
        schema=None,
        partition_columns=[],
    )

    @DataPipes.pipe(
        pipeid="lakeflow.silver_orders",
        name="Silver Orders",
        input_entity_ids=["bronze.orders"],
        output_entity_id="silver.orders",
        output_type="delta",
        tags={},
        use_watermark=False,
    )
    def silver_orders(bronze_orders):
        return bronze_orders.withColumn("total_amount", col("quantity") * col("amount"))
