"""Chained-lowering parity: the two composite pipes vs per-declaration pipes.

The chain must reproduce the per-pipe lowering's semantics exactly (same
events modulo ``ingested_at``, same episodes modulo audit timestamps) while
converging multi-generation graphs in one execution where the per-pipe
path needs one scheduled run per generation hop.
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

pytestmark = [pytest.mark.integration, pytest.mark.requires_spark]

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_temporal"
)


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.appName("TemporalChainParity")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _logger_provider():
    provider = MagicMock()
    provider.get_logger.return_value = MagicMock()
    return provider


def _service_get(services):
    def _get(dep):
        try:
            return services[dep]
        except KeyError as exc:
            raise AssertionError(f"Unexpected service request: {dep}") from exc

    return _get


T0 = datetime(2026, 7, 14, 12, 0, 0)
T_END = datetime(2026, 7, 14, 12, 10, 0)
EVAL_RUN1 = datetime(2026, 7, 14, 12, 30, 0)
EVAL_RUN2 = datetime(2026, 7, 14, 12, 45, 0)

TELEMETRY_COLUMNS = ["machine_id", "reading_ts", "temperature"]


def _telemetry_run1(spark):
    return spark.createDataFrame(
        [
            ("machine-hot", T0, 95.0),
            ("machine-hot", T_END, 80.0),
            ("machine-late", T0, 95.0),
        ],
        TELEMETRY_COLUMNS,
    )


def _telemetry_run2(spark):
    return spark.createDataFrame([("machine-late", T_END, 80.0)], TELEMETRY_COLUMNS)


def _conditions_df(spark):
    from kindling_ext_temporal import conditions_schema

    valid_from = datetime(2026, 7, 14, 11, 0, 0)
    return spark.createDataFrame(
        [
            (
                "condition.temperature_high",
                ["telemetry.observed"],
                "machine",
                {
                    "enter_when": "cast(payload['temperature'] as double) > 90",
                    "exit_when": "cast(payload['temperature'] as double) <= 90",
                },
                True,
                valid_from,
                None,
            ),
            (
                "condition.thermal_excursion",
                ["episode.temperature_high_active.closed"],
                "machine",
                {
                    "enter_when": "payload['status'] = 'closed'",
                    "exit_when": "false",
                },
                True,
                valid_from,
                None,
            ),
        ],
        conditions_schema(),
    )


@pytest.fixture()
def temporal_graph(spark):
    """Register the declarations once and lower BOTH ways."""
    from kindling.data_entities import DataEntityManager, DataEntityRegistry
    from kindling.data_pipes import DataPipesManager, DataPipesRegistry
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
    event_registry = TemporalEventRegistryManager(_logger_provider())
    episode_registry = TemporalEpisodeRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    services = _service_get(
        {
            TemporalEntityResolver: SimpleTemporalEntityResolver(),
            TemporalEventRegistry: event_registry,
            TemporalEpisodeRegistry: episode_registry,
            DataEntityRegistry: entity_registry,
            DataPipesRegistry: pipe_registry,
        }
    )

    with patch("kindling.injection.GlobalInjector.get", side_effect=services):

        @DataEvents.base_event(
            eventid="telemetry.base",
            input_entity_id="bronze.telemetry",
            subject_type="machine",
            subject_keys=["machine_id"],
            time_column="reading_ts",
            event_type="telemetry.observed",
            payload_columns=["temperature"],
            source_system="parity-test",
            use_watermark=True,
        )
        def normalize(df):
            return df

        DataEvents.condition_engine(engineid="default")
        DataEpisodes.episode(
            episodeid="episode.temperature_high_active",
            start_event="condition.temperature_high.entered",
            end_event="condition.temperature_high.exited",
            subject_type="machine",
            expires_after_seconds=300,
        )
        chain_pipe_ids = declare_temporal_chain()

    return pipe_registry, chain_pipe_ids


def _events_key_rows(events_df):
    return {
        (row.event_id, row.event_type, row.generation, row.subject_id, row.event_ts)
        for row in events_df.collect()
    }


def _episode_key_rows(episodes_df):
    return {
        (
            row.episode_id,
            row.episode_type,
            row.subject_id,
            row.status,
            row.close_reason,
            row.end_event_synthetic,
            row.start_time,
            row.end_time,
            row.duration_ms,
        )
        for row in episodes_df.collect()
    }


def test_chain_matches_per_pipe_lowering_and_converges_in_one_run(spark, temporal_graph):
    pipe_registry, chain_pipe_ids = temporal_graph
    assert chain_pipe_ids == ["temporal.chain.events.default", "temporal.chain.episodes.default"]

    telemetry = _telemetry_run1(spark)
    conditions = _conditions_df(spark)

    # --- per-pipe lowering: one generation hop per pass, driven manually ---
    base_pipe = pipe_registry.get_pipe_definition("temporal.event.telemetry.base")
    condition_pipe = pipe_registry.get_pipe_definition("temporal.condition.default")
    episode_pipe = pipe_registry.get_pipe_definition(
        "temporal.episode.episode.temperature_high_active"
    )
    determination_pipe = pipe_registry.get_pipe_definition(
        "temporal.episode_event.episode.temperature_high_active"
    )

    base_events = base_pipe.execute(bronze_telemetry=telemetry)
    boundaries_1 = condition_pipe.execute(
        silver_events=base_events, silver_conditions_current=conditions
    )
    legacy_episodes = episode_pipe.execute(
        silver_events=boundaries_1, temporal_evaluation_time=EVAL_RUN1
    )
    determinations_1 = determination_pipe.execute(
        silver_events=boundaries_1, temporal_evaluation_time=EVAL_RUN1
    )
    # Second scheduled run's condition pass: consumes the determination events.
    boundaries_2 = condition_pipe.execute(
        silver_events=determinations_1, silver_conditions_current=conditions
    )
    legacy_events = (
        base_events.unionByName(boundaries_1)
        .unionByName(determinations_1)
        .unionByName(boundaries_2)
    )

    # --- chained lowering: one execution ---
    chain_events_pipe = pipe_registry.get_pipe_definition(chain_pipe_ids[0])
    chain_episodes_pipe = pipe_registry.get_pipe_definition(chain_pipe_ids[1])

    chain_events = chain_events_pipe.execute(
        bronze_telemetry=telemetry,
        silver_conditions_current=conditions,
        temporal_evaluation_time=EVAL_RUN1,
        silver_episodes=None,
    )
    chain_episodes = chain_episodes_pipe.execute(
        silver_events=chain_events,
        temporal_evaluation_time=EVAL_RUN1,
        silver_episodes=None,
    )

    assert _events_key_rows(chain_events) == _events_key_rows(legacy_events)
    assert _episode_key_rows(chain_episodes) == _episode_key_rows(legacy_episodes)

    # Convergence: the higher-order boundary is present after ONE chain run.
    chain_types = {row.event_type for row in chain_events.collect()}
    assert "condition.thermal_excursion.entered" in chain_types
    generations = {
        row.event_type: row.generation
        for row in chain_events.select("event_type", "generation").distinct().collect()
    }
    assert generations["condition.thermal_excursion.entered"] == 3


def test_chain_cross_run_revision_with_prior_state(spark, temporal_graph):
    pipe_registry, chain_pipe_ids = temporal_graph
    chain_events_pipe = pipe_registry.get_pipe_definition(chain_pipe_ids[0])
    chain_episodes_pipe = pipe_registry.get_pipe_definition(chain_pipe_ids[1])
    conditions = _conditions_df(spark)

    # Run 1: start-only for machine-late -> expired episode persisted.
    events_1 = chain_events_pipe.execute(
        bronze_telemetry=_telemetry_run1(spark),
        silver_conditions_current=conditions,
        temporal_evaluation_time=EVAL_RUN1,
        silver_episodes=None,
    )
    episodes_1 = chain_episodes_pipe.execute(
        silver_events=events_1, temporal_evaluation_time=EVAL_RUN1, silver_episodes=None
    )
    late_1 = [r for r in episodes_1.collect() if r.subject_id == "machine-late"]
    assert len(late_1) == 1 and late_1[0].status == "expired"

    # Run 2: ONLY the late real end arrives; prior state via JIT kwarg.
    events_2 = chain_events_pipe.execute(
        bronze_telemetry=_telemetry_run2(spark),
        silver_conditions_current=conditions,
        temporal_evaluation_time=EVAL_RUN2,
        silver_episodes=episodes_1,
    )
    episodes_2 = chain_episodes_pipe.execute(
        silver_events=events_2, temporal_evaluation_time=EVAL_RUN2, silver_episodes=episodes_1
    )

    late_2 = [r for r in episodes_2.collect() if r.subject_id == "machine-late"]
    assert len(late_2) == 1
    assert late_2[0].status == "closed"
    assert late_2[0].close_reason == "end_event"
    assert late_2[0].end_event_synthetic is False
    assert late_2[0].episode_id == late_1[0].episode_id
    assert late_2[0].start_event_id == late_1[0].start_event_id

    # The corrective .closed determination event is in run 2's event output.
    types_2 = {row.event_type for row in events_2.collect()}
    assert "episode.temperature_high_active.closed" in types_2
