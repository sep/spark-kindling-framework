"""Minimal SCD2 declared-flow app for Lakeflow AUTO CDC validation.

Deployed through the Lakeflow app selector
(`kindling_ext_databricks.lakeflow_app_selector`): the pipeline's
`kindling.data_app` config selects this package's `spark_kindling.data_apps`
entry point, the selector initializes Kindling with the Databricks SDP
engine, calls :func:`register_all`, and declares the pipeline.

The graph is the smallest shape that exercises the SCD declared-flow ->
AUTO CDC mapping (#166) against the real API:

- ``lakeflow.customers_seed`` — a zero-input pipe materializing a customer
  snapshot. The snapshot content is selected by the pipeline configuration
  value ``lakeflow_scd.snapshot`` (``v1``/``v2``) so two pipeline updates
  with different configuration demonstrate SCD2 version chaining.
- ``silver.customers`` — the SCD2 target (``scd.type=2``,
  ``scd.source_kind=snapshot``): the Databricks engine emits a
  ``__scd_source`` view, ``create_streaming_table`` and
  ``create_auto_cdc_from_snapshot_flow``.

Registration only happens inside :func:`register_all`; importing this
module has no side effects (the selector requires declaration-only apps).
"""

from datetime import datetime


def register_all() -> None:
    from kindling.data_entities import DataEntities
    from kindling.data_pipes import DataPipes
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    customers_schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("tier", StringType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )

    DataEntities.entity(
        entityid="bronze.customers_feed",
        name="customers_feed",
        merge_columns=["customer_id"],
        tags={"provider_type": "delta"},
        schema=customers_schema,
        partition_columns=[],
    )

    DataEntities.entity(
        entityid="silver.customers",
        name="customers",
        merge_columns=["customer_id"],
        tags={
            "provider_type": "delta",
            "scd.type": "2",
            "scd.source_kind": "snapshot",
        },
        schema=customers_schema,
        partition_columns=[],
    )

    SNAPSHOTS = {
        "v1": [
            ("c1", "Alice", "bronze", datetime(2026, 7, 1, 12, 0, 0)),
            ("c2", "Bob", "silver", datetime(2026, 7, 1, 12, 0, 0)),
        ],
        # v2: c1 changes tier (SCD2 close + new version), c2 unchanged,
        # c3 arrives (new key).
        "v2": [
            ("c1", "Alice", "gold", datetime(2026, 7, 2, 12, 0, 0)),
            ("c2", "Bob", "silver", datetime(2026, 7, 1, 12, 0, 0)),
            ("c3", "Cara", "bronze", datetime(2026, 7, 2, 12, 0, 0)),
        ],
    }

    @DataPipes.pipe(
        pipeid="lakeflow.customers_seed",
        name="Customers snapshot seed",
        input_entity_ids=[],
        output_entity_id="bronze.customers_feed",
        output_type="delta",
        tags={},
        use_watermark=False,
    )
    def customers_seed():
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        snapshot = str(spark.conf.get("lakeflow_scd.snapshot", "v1")).strip().lower()
        return spark.createDataFrame(SNAPSHOTS.get(snapshot, SNAPSHOTS["v1"]), customers_schema)

    @DataPipes.pipe(
        pipeid="lakeflow.customers_scd",
        name="Customers SCD2",
        input_entity_ids=["bronze.customers_feed"],
        output_entity_id="silver.customers",
        output_type="delta",
        tags={},
        use_watermark=False,
    )
    def customers_scd(bronze_customers_feed):
        return bronze_customers_feed
