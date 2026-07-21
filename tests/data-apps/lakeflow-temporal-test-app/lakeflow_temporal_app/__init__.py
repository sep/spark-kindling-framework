"""Temporal chain declared inside a Lakeflow pipeline (W2).

Deployed through the Lakeflow app selector: the pipeline's
``kindling.data_app`` selects this entry point, the selector initializes
Kindling with the Databricks SDP engine, calls :func:`register_all`, and
declares the graph. ``kindling.lakeflow.pipes`` restricts declaration to
the chain pipes; the Databricks engine lowers them as the stratified
dataset graph (generation-stratum streaming tables, episodes as AUTO CDC
SCD2, determination events projected from all episode versions, a union
MV as the canonical events surface).

External inputs — written OUTSIDE the pipeline, read at the physical
names Kindling's EntityNameMapper produces from the bridged storage
config:

- ``bronze.telemetry`` — append-only readings (machine_id, reading_ts,
  temperature), streamed into the g0 stratum.
- ``silver.conditions`` — the SCD2 rules table, normally maintained by
  ``ingest_conditions`` from a runner job/CLI.

Registration only happens inside :func:`register_all`; importing this
module has no side effects.
"""


def register_all() -> None:
    from kindling.data_entities import DataEntities
    from kindling_ext_temporal import DataEpisodes, DataEvents, declare_temporal_chain
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    telemetry_schema = StructType(
        [
            StructField("machine_id", StringType(), False),
            StructField("reading_ts", TimestampType(), False),
            StructField("temperature", DoubleType(), False),
        ]
    )

    DataEntities.entity(
        entityid="bronze.telemetry",
        name="telemetry",
        merge_columns=["machine_id", "reading_ts"],
        tags={"provider_type": "delta"},
        schema=telemetry_schema,
        partition_columns=[],
    )

    @DataEvents.base_event(
        eventid="telemetry.base",
        input_entity_id="bronze.telemetry",
        subject_type="machine",
        subject_keys=["machine_id"],
        time_column="reading_ts",
        event_type="telemetry.observed",
        payload_columns=["temperature"],
        source_system="lakeflow-system-test",
        use_watermark=True,
    )
    def normalize_telemetry(df):
        return df

    DataEvents.condition_engine(engineid="default")

    DataEpisodes.episode(
        episodeid="episode.temperature_high_active",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        subject_type="machine",
        expires_after_seconds=300,
    )

    declare_temporal_chain()
