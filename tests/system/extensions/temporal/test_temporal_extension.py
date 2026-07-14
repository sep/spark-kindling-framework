from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

pytestmark = [pytest.mark.system, pytest.mark.standalone, pytest.mark.requires_spark]

EXTENSION_PACKAGE_ROOT = Path(__file__).resolve().parents[4] / "packages" / "kindling_temporal"


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.appName("TemporalExtensionSystemTest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def _logger_provider():
    provider = MagicMock()
    provider.get_logger.return_value = MagicMock()
    return provider


def _temporal_service_get(
    *,
    event_registry,
    episode_registry,
    entity_registry,
    pipe_registry,
):
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling_temporal import (
        SimpleTemporalEntityResolver,
        TemporalEntityResolver,
        TemporalEpisodeRegistry,
        TemporalEventRegistry,
    )

    def _get(dep):
        if dep is TemporalEntityResolver:
            return SimpleTemporalEntityResolver()
        if dep is TemporalEventRegistry:
            return event_registry
        if dep is TemporalEpisodeRegistry:
            return episode_registry
        if dep is DataEntityRegistry:
            return entity_registry
        if dep is DataPipesRegistry:
            return pipe_registry
        raise AssertionError(f"Unexpected service request: {dep}")

    return _get


def _events_df(spark):
    from kindling_temporal import events_schema

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    cooled_at = observed_at + timedelta(minutes=10)
    return spark.createDataFrame(
        [
            (
                "source-hot",
                "telemetry.observed",
                0,
                "base",
                "machine",
                "machine-1",
                observed_at,
                "sensor",
                None,
                {"temperature": "95"},
                None,
                observed_at,
            ),
            (
                "source-cool",
                "telemetry.observed",
                0,
                "base",
                "machine",
                "machine-1",
                cooled_at,
                "sensor",
                None,
                {"temperature": "80"},
                None,
                cooled_at,
            ),
        ],
        events_schema(),
    )


def _unclosed_events_df(spark):
    from kindling_temporal import events_schema

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    return spark.createDataFrame(
        [
            (
                "source-hot",
                "telemetry.observed",
                0,
                "base",
                "machine",
                "machine-2",
                observed_at,
                "sensor",
                None,
                {"temperature": "95"},
                None,
                observed_at,
            ),
        ],
        events_schema(),
    )


def _closed_events_for_machine_2_df(spark):
    from kindling_temporal import events_schema

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    cooled_at = observed_at + timedelta(minutes=10)
    return spark.createDataFrame(
        [
            (
                "source-hot",
                "telemetry.observed",
                0,
                "base",
                "machine",
                "machine-2",
                observed_at,
                "sensor",
                None,
                {"temperature": "95"},
                None,
                observed_at,
            ),
            (
                "source-cool",
                "telemetry.observed",
                0,
                "base",
                "machine",
                "machine-2",
                cooled_at,
                "sensor",
                None,
                {"temperature": "80"},
                None,
                cooled_at,
            ),
        ],
        events_schema(),
    )


def _conditions_df(spark):
    from kindling_temporal import conditions_schema

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
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
                observed_at,
                None,
            )
        ],
        conditions_schema(),
    )


def _episode_conditions_df(spark):
    from kindling_temporal import conditions_schema

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    return spark.createDataFrame(
        [
            (
                "condition.thermal_excursion",
                ["episode.temperature_high_active.closed"],
                "machine",
                {
                    "enter_when": "payload['status'] = 'closed'",
                    "exit_when": "false",
                },
                True,
                observed_at,
                None,
            )
        ],
        conditions_schema(),
    )


def test_temporal_extension_registers_and_executes_condition_episode_flow(spark):
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_temporal import (
        DataEpisodes,
        DataEvents,
        TemporalEpisodeRegistryManager,
        TemporalEventRegistryManager,
    )

    DataEvents.reset()
    DataEpisodes.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    episode_registry = TemporalEpisodeRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            episode_registry=episode_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):
        DataEvents.condition_engine(engineid="condition_engine.default")
        DataEpisodes.episode(
            episodeid="episode.temperature_high_active",
            start_event="condition.temperature_high.entered",
            end_event="condition.temperature_high.exited",
            subject_type="machine",
            expires_after_seconds=300,
            expiration_event="episode.temperature_high_active.expired",
        )

    condition_pipe = pipe_registry.get_pipe_definition(
        "temporal.condition.condition_engine.default"
    )
    episode_pipe = pipe_registry.get_pipe_definition(
        "temporal.episode.episode.temperature_high_active"
    )
    episode_event_pipe = pipe_registry.get_pipe_definition(
        "temporal.episode_event.episode.temperature_high_active"
    )

    boundary_events = condition_pipe.execute(
        silver_events=_events_df(spark),
        silver_conditions_current=_conditions_df(spark),
    )
    boundary_rows = {row.event_type: row for row in boundary_events.collect()}
    assert set(boundary_rows) == {
        "condition.temperature_high.entered",
        "condition.temperature_high.exited",
    }

    episodes = episode_pipe.execute(silver_events=boundary_events)
    rows = episodes.collect()

    assert len(rows) == 1
    assert rows[0].episode_type == "episode.temperature_high_active"
    assert rows[0].condition_id == "condition.temperature_high"
    assert rows[0].status == "closed"
    assert rows[0].duration_ms == 600000

    determination_events = episode_event_pipe.execute(silver_events=boundary_events)
    determination_rows = determination_events.collect()
    assert len(determination_rows) == 1
    assert determination_rows[0].event_type == "episode.temperature_high_active.closed"
    assert determination_rows[0].generation == 2
    assert determination_rows[0].correlation_id == rows[0].episode_id

    recursive_events = boundary_events.unionByName(determination_events)
    recursive_boundary_events = condition_pipe.execute(
        silver_events=recursive_events,
        silver_conditions_current=_episode_conditions_df(spark),
    )
    recursive_rows = {row.event_type: row for row in recursive_boundary_events.collect()}

    assert set(recursive_rows) == {"condition.thermal_excursion.entered"}
    assert recursive_rows["condition.thermal_excursion.entered"].generation == 3
    assert (
        recursive_rows["condition.thermal_excursion.entered"].attributes["source_event_id"]
        == determination_rows[0].event_id
    )

    open_boundary_events = condition_pipe.execute(
        silver_events=_unclosed_events_df(spark),
        silver_conditions_current=_conditions_df(spark),
    )
    open_episodes = episode_pipe.execute(silver_events=open_boundary_events).collect()
    open_determination_events = episode_event_pipe.execute(
        silver_events=open_boundary_events
    ).collect()

    assert len(open_episodes) == 1
    assert open_episodes[0].subject_id == "machine-2"
    assert open_episodes[0].status == "open"
    assert open_episodes[0].end_event_id is None
    assert open_episodes[0].end_time is None
    assert open_episodes[0].duration_ms is None
    assert open_determination_events == []

    expired_episodes = episode_pipe.execute(
        silver_events=open_boundary_events,
        temporal_evaluation_time=datetime(2026, 7, 14, 12, 10, 0),
    ).collect()
    expired_events = episode_event_pipe.execute(
        silver_events=open_boundary_events,
        temporal_evaluation_time=datetime(2026, 7, 14, 12, 10, 0),
    ).collect()

    assert len(expired_episodes) == 1
    assert expired_episodes[0].subject_id == "machine-2"
    assert expired_episodes[0].status == "expired"
    assert expired_episodes[0].episode_id == open_episodes[0].episode_id
    assert expired_episodes[0].start_event_id == open_episodes[0].start_event_id
    assert expired_episodes[0].close_reason == "expiration"
    assert expired_episodes[0].end_event_synthetic is True
    assert expired_episodes[0].end_time == datetime(2026, 7, 14, 12, 5, 0)
    assert expired_episodes[0].duration_ms == 300000

    assert len(expired_events) == 1
    assert expired_events[0].event_type == "episode.temperature_high_active.expired"
    assert expired_events[0].generation == 2
    assert expired_events[0].correlation_id == expired_episodes[0].episode_id
    assert expired_events[0].payload["status"] == "expired"

    closed_machine_2_boundary_events = condition_pipe.execute(
        silver_events=_closed_events_for_machine_2_df(spark),
        silver_conditions_current=_conditions_df(spark),
    )
    closure_for_same_start = episode_pipe.execute(
        silver_events=closed_machine_2_boundary_events,
        temporal_evaluation_time=datetime(2026, 7, 14, 12, 15, 0),
    ).collect()[0]
    closure_event_for_same_start = episode_event_pipe.execute(
        silver_events=closed_machine_2_boundary_events,
        temporal_evaluation_time=datetime(2026, 7, 14, 12, 15, 0),
    ).collect()[0]

    assert closure_for_same_start.status == "closed"
    assert closure_for_same_start.episode_id == open_episodes[0].episode_id
    assert closure_for_same_start.start_event_id == open_episodes[0].start_event_id
    assert closure_for_same_start.end_event_synthetic is False
    assert closure_for_same_start.end_time == datetime(2026, 7, 14, 12, 10, 0)
    assert closure_event_for_same_start.event_type == "episode.temperature_high_active.closed"
    assert closure_event_for_same_start.correlation_id == closure_for_same_start.episode_id
    assert closure_event_for_same_start.payload["status"] == "closed"
    assert closure_event_for_same_start.payload["close_reason"] == "end_event"
