from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

EXTENSION_PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "packages" / "kindling_temporal"


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.appName("TemporalExtensionIntegrationTest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark

    spark.stop()


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


@pytest.mark.requires_spark
def test_condition_engine_runner_emits_entered_and_exited_events(spark):
    from kindling_temporal import ConditionEngineRunner

    result = ConditionEngineRunner().execute(_events_df(spark), _conditions_df(spark))

    rows = {row.event_type: row for row in result.collect()}
    assert set(rows) == {
        "condition.temperature_high.entered",
        "condition.temperature_high.exited",
    }
    assert rows["condition.temperature_high.entered"].generation == 1
    assert rows["condition.temperature_high.entered"].event_class == "condition"
    assert rows["condition.temperature_high.entered"].correlation_id == (
        "condition.temperature_high"
    )
    assert rows["condition.temperature_high.entered"].attributes["source_event_id"] == (
        "source-hot"
    )
    assert rows["condition.temperature_high.exited"].attributes["source_event_id"] == (
        "source-cool"
    )


@pytest.mark.requires_spark
def test_episode_runner_pairs_entered_and_exited_events(spark):
    from kindling_temporal import ConditionEngineRunner, EpisodeMetadata, EpisodeRunner

    boundary_events = ConditionEngineRunner().execute(_events_df(spark), _conditions_df(spark))
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        subject_type="machine",
    )

    rows = EpisodeRunner().execute(boundary_events, episode).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row.episode_type == "episode.temperature_high_active"
    assert row.condition_id == "condition.temperature_high"
    assert row.subject_type == "machine"
    assert row.subject_id == "machine-1"
    assert row.start_event_id
    assert row.end_event_id
    assert row.status == "closed"
    assert row.close_reason == "end_event"
    assert row.end_event_synthetic is False
    assert row.duration_ms == 600000
    assert row.attributes["start_event_type"] == "condition.temperature_high.entered"
    assert row.attributes["end_event_type"] == "condition.temperature_high.exited"


@pytest.mark.requires_spark
def test_episode_runner_emits_episode_determination_event(spark):
    from kindling_temporal import ConditionEngineRunner, EpisodeMetadata, EpisodeRunner

    boundary_events = ConditionEngineRunner().execute(_events_df(spark), _conditions_df(spark))
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        determination_event="episode.temperature_high_active.closed",
        subject_type="machine",
    )

    runner = EpisodeRunner()
    episode_row = runner.execute(boundary_events, episode).collect()[0]
    event_rows = runner.execute_determination_events(boundary_events, episode).collect()

    assert len(event_rows) == 1
    event = event_rows[0]
    assert event.event_type == "episode.temperature_high_active.closed"
    assert event.generation == 2
    assert event.event_class == "episode"
    assert event.subject_type == "machine"
    assert event.subject_id == "machine-1"
    assert event.event_ts == episode_row.end_time
    assert event.correlation_id == episode_row.episode_id
    assert event.payload["episode_id"] == episode_row.episode_id
    assert event.payload["episode_type"] == "episode.temperature_high_active"
    assert event.payload["condition_id"] == "condition.temperature_high"
    assert event.payload["status"] == "closed"
    assert event.payload["close_reason"] == "end_event"
    assert event.payload["start_event_id"] == episode_row.start_event_id
    assert event.payload["end_event_id"] == episode_row.end_event_id
    assert event.payload["duration_ms"] == "600000"
    assert event.attributes["start_event_type"] == "condition.temperature_high.entered"
    assert event.attributes["end_event_type"] == "condition.temperature_high.exited"
    assert event.attributes["start_generation"] == "1"
    assert event.attributes["end_generation"] == "1"
