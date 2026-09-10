"""Real-Spark execution of the batch temporal-chain lowering.

The point of ``kindling.lakeflow.temporal_mode: batch`` is that a base-event
transform may use ordered analytic windows that Structured Streaming rejects.
A mocked-decorator test cannot establish that: these tests evaluate the
recorded query functions against a real local Spark session, force execution,
and check the results.
"""

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

pytestmark = [pytest.mark.integration, pytest.mark.requires_spark]

EXTENSION_ROOTS = [
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_temporal",
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_databricks",
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_sdp",
]


@pytest.fixture(autouse=True)
def _extensions_on_path(monkeypatch):
    for root in EXTENSION_ROOTS:
        monkeypatch.syspath_prepend(str(root))


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("TemporalLakeflowBatchMode")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _logger_provider():
    provider = MagicMock()
    provider.get_logger.return_value = MagicMock()
    return provider


class RecordingDp:
    """Records declarations so the query functions can be evaluated."""

    def __init__(self):
        self.streaming_tables = []
        self.append_flows = []
        self.auto_cdc_snapshot_flows = []
        self.views = {}
        self.materialized_views = {}

    def create_streaming_table(self, name, **_kwargs):
        self.streaming_tables.append(name)

    def append_flow(self, target, name=None, **_kwargs):
        def decorator(fn):
            self.append_flows.append((target, name or fn.__name__))
            return fn

        return decorator

    def create_auto_cdc_from_snapshot_flow(self, **kwargs):
        self.auto_cdc_snapshot_flows.append(kwargs)

    def temporary_view(self, name=None, **_kwargs):
        def decorator(fn):
            self.views[name or fn.__name__] = fn
            return fn

        return decorator

    def materialized_view(self, name=None, **_kwargs):
        def decorator(fn):
            self.materialized_views[name or fn.__name__] = fn
            return fn

        return decorator


T0 = datetime(2026, 7, 14, 12, 0, 0)

READINGS = [
    ("m1", 1, datetime(2026, 7, 14, 12, 0, 0), 40.0),
    ("m1", 2, datetime(2026, 7, 14, 12, 5, 0), 55.0),
    ("m1", 3, datetime(2026, 7, 14, 12, 10, 0), 70.0),
    ("m2", 4, datetime(2026, 7, 14, 12, 1, 0), 20.0),
    ("m2", 5, datetime(2026, 7, 14, 12, 6, 0), 25.0),
]


def _ranked(df):
    """A transform Structured Streaming rejects outright.

    ``row_number`` over an ordered window is the motivating case for batch
    mode. The tie breaker on ``reading_id`` keeps the ranking deterministic
    for equal timestamps.
    """
    ordered = Window.partitionBy("machine_id").orderBy("reading_ts", "reading_id")
    return df.withColumn("reading_rank", F.row_number().over(ordered))


def _declare_batch_chain(spark, source_table, *, transform, max_generations=1):
    from kindling.data_entities import (
        DataEntityManager,
        DataEntityRegistry,
        EntityMetadata,
        EntityNameMapper,
    )
    from kindling.data_pipes import DataPipesManager, DataPipesRegistry
    from kindling_ext_databricks import temporal_lowering
    from kindling_ext_temporal import (
        DataEpisodes,
        DataEvents,
        SimpleTemporalEntityResolver,
        TemporalEntityResolver,
        TemporalEpisodeRegistry,
        TemporalEpisodeRegistryManager,
        TemporalEventRegistry,
        TemporalEventRegistryManager,
        declare_temporal_chain,
    )

    DataEvents.reset()
    DataEpisodes.reset()
    entity_registry = DataEntityManager()
    entity_registry.registry["bronze.readings"] = EntityMetadata(
        entityid="bronze.readings", name="readings", schema=None, merge_columns=[], tags={}
    )
    services = {
        TemporalEntityResolver: SimpleTemporalEntityResolver(),
        TemporalEventRegistry: TemporalEventRegistryManager(_logger_provider()),
        TemporalEpisodeRegistry: TemporalEpisodeRegistryManager(_logger_provider()),
        DataEntityRegistry: entity_registry,
        DataPipesRegistry: DataPipesManager(_logger_provider()),
        # Only the base source resolves; the conditions table deliberately
        # does not exist, which is the first-run path (no rules ingested
        # yet), so every generation above zero stays empty.
        EntityNameMapper: SimpleNamespace(
            get_table_name=lambda entity: (
                source_table if entity.entityid == "bronze.readings" else "conditions_not_ingested"
            )
        ),
    }

    def service_get(dep):
        try:
            return services[dep]
        except KeyError as exc:
            raise AssertionError(f"Unexpected service request: {dep}") from exc

    with patch("kindling.injection.GlobalInjector.get", side_effect=service_get):

        @DataEvents.base_event(
            eventid="reading.base",
            input_entity_id="bronze.readings",
            subject_type="machine",
            subject_keys=["machine_id"],
            time_column="reading_ts",
            event_type="reading.observed",
            payload_columns=["temperature", "reading_rank"],
        )
        def normalize(df):
            return transform(df)

        DataEvents.condition_engine(engineid="default")
        declare_temporal_chain()

        with patch.object(temporal_lowering, "_spark", lambda: spark):
            dp = RecordingDp()
            temporal_lowering.declare_stratified_temporal(
                dp,
                events_name="silver_events",
                episodes_name=None,
                max_generations=max_generations,
                mode="batch",
            )
    return dp


@pytest.fixture(scope="module")
def readings_table(spark):
    df = spark.createDataFrame(
        READINGS, "machine_id string, reading_id int, reading_ts timestamp, temperature double"
    )
    df.createOrReplaceTempView("bronze_readings_src")
    return "bronze_readings_src"


def test_batch_base_stratum_executes_an_ordered_window_transform(spark, readings_table):
    dp = _declare_batch_chain(spark, readings_table, transform=_ranked)

    events = dp.materialized_views["silver_events__g0"]()

    # The whole point: a batch DataFrame, so the window is legal.
    assert events.isStreaming is False

    rows = events.collect()  # forces execution
    assert len(rows) == len(READINGS)

    ranks = {(row["subject_id"], row["payload"]["reading_rank"]): row["event_ts"] for row in rows}
    # m1's three readings rank 1..3 in timestamp order; m2's two rank 1..2.
    assert ranks[("m1", "1")] == datetime(2026, 7, 14, 12, 0, 0)
    assert ranks[("m1", "2")] == datetime(2026, 7, 14, 12, 5, 0)
    assert ranks[("m1", "3")] == datetime(2026, 7, 14, 12, 10, 0)
    assert ranks[("m2", "1")] == datetime(2026, 7, 14, 12, 1, 0)
    assert ranks[("m2", "2")] == datetime(2026, 7, 14, 12, 6, 0)


def test_batch_base_stratum_emits_the_canonical_event_envelope(spark, readings_table):
    dp = _declare_batch_chain(spark, readings_table, transform=_ranked)

    events = dp.materialized_views["silver_events__g0"]()

    for column in (
        "event_id",
        "event_type",
        "generation",
        "event_class",
        "subject_type",
        "subject_id",
        "event_ts",
        "payload",
        "attributes",
        "ingested_at",
    ):
        assert column in events.columns, column

    rows = events.collect()
    assert {row["event_type"] for row in rows} == {"reading.observed"}
    assert {row["subject_type"] for row in rows} == {"machine"}
    assert {row["generation"] for row in rows} == {0}
    # Stable identity, not a mutable row rank.
    assert len({row["event_id"] for row in rows}) == len(READINGS)


def test_batch_empty_generation_preserves_schema_and_is_empty(spark, readings_table):
    dp = _declare_batch_chain(spark, readings_table, transform=_ranked, max_generations=1)

    base = dp.materialized_views["silver_events__g0"]()
    # The stratum depends on __g0 by name, as Lakeflow would have materialized it.
    base.createOrReplaceTempView("silver_events__g0")
    empty = dp.materialized_views["silver_events__g1"]()

    assert empty.isStreaming is False
    assert empty.columns == base.columns
    assert empty.count() == 0


def test_batch_public_events_union_executes_over_the_strata(spark, readings_table):
    dp = _declare_batch_chain(spark, readings_table, transform=_ranked, max_generations=1)

    # The union reads the strata by name, so register them as views the way
    # Lakeflow would materialize them.
    dp.materialized_views["silver_events__g0"]().createOrReplaceTempView("silver_events__g0")
    dp.materialized_views["silver_events__g1"]().createOrReplaceTempView("silver_events__g1")

    union = dp.materialized_views["silver_events"]()
    assert union.isStreaming is False
    assert union.count() == len(READINGS)


def test_batch_mode_reads_no_stream_and_declares_no_flow(spark, readings_table):
    dp = _declare_batch_chain(spark, readings_table, transform=_ranked)

    assert dp.streaming_tables == []
    assert dp.append_flows == []
    assert set(dp.materialized_views) == {
        "silver_events__g0",
        "silver_events__g1",
        "silver_events",
    }
