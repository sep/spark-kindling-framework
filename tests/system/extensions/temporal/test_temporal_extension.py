from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

pytestmark = [pytest.mark.system, pytest.mark.standalone, pytest.mark.requires_spark]

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[4] / "packages" / "extensions" / "kindling_ext_temporal"
)


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
    from kindling_ext_temporal import (
        SimpleTemporalEntityResolver,
        TemporalEntityResolver,
        TemporalEpisodeRegistry,
        TemporalEventRegistry,
    )

    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry

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
    from kindling_ext_temporal import events_schema

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


def _conditions_df(spark):
    from kindling_ext_temporal import conditions_schema

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


def test_temporal_extension_registers_and_executes_condition_episode_flow(spark):
    from kindling_ext_temporal import (
        DataEpisodes,
        DataEvents,
        TemporalEpisodeRegistryManager,
        TemporalEventRegistryManager,
    )

    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager

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
        )

    condition_pipe = pipe_registry.get_pipe_definition(
        "temporal.condition.condition_engine.default"
    )
    episode_pipe = pipe_registry.get_pipe_definition(
        "temporal.episode.episode.temperature_high_active"
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
