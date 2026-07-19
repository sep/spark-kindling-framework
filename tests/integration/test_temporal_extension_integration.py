from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_temporal"
)


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


def _unclosed_events_df(spark):
    from kindling_ext_temporal import events_schema

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
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
        ],
        events_schema(),
    )


def _end_only_events_df(spark, cooled_at):
    from kindling_ext_temporal import events_schema

    return spark.createDataFrame(
        [
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


@pytest.mark.requires_spark
def test_condition_engine_runner_emits_entered_and_exited_events(spark):
    from kindling_ext_temporal import ConditionEngineRunner

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
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

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
    assert row.start_generation == 1
    assert row.end_event_id
    assert row.status == "closed"
    assert row.close_reason == "end_event"
    assert row.end_event_synthetic is False
    assert row.duration_ms == 600000
    assert row.attributes["start_event_type"] == "condition.temperature_high.entered"
    assert row.attributes["end_event_type"] == "condition.temperature_high.exited"


@pytest.mark.requires_spark
def test_episode_runner_materializes_open_episode_without_end_event(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    boundary_events = ConditionEngineRunner().execute(
        _unclosed_events_df(spark), _conditions_df(spark)
    )
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
    rows = runner.execute(boundary_events, episode).collect()
    determination_rows = runner.execute_determination_events(boundary_events, episode).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row.episode_type == "episode.temperature_high_active"
    assert row.subject_id == "machine-1"
    assert row.start_event_id
    assert row.end_event_id is None
    assert row.end_time is None
    assert row.status == "open"
    assert row.close_reason is None
    assert row.end_event_synthetic is False
    assert row.duration_ms is None
    assert determination_rows == []

    still_open = runner.execute(
        boundary_events,
        episode,
        evaluation_time=observed_at + timedelta(minutes=1),
    ).collect()[0]
    assert still_open.status == "open"


@pytest.mark.requires_spark
def test_episode_runner_expires_open_episode_at_batch_evaluation_time(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    boundary_events = ConditionEngineRunner().execute(
        _unclosed_events_df(spark), _conditions_df(spark)
    )
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        determination_event="episode.temperature_high_active.closed",
        expiration_event="episode.temperature_high_active.expired",
        expires_after_seconds=300,
        subject_type="machine",
    )

    runner = EpisodeRunner()
    rows = runner.execute(
        boundary_events,
        episode,
        evaluation_time=observed_at + timedelta(minutes=10),
    ).collect()
    lifecycle_rows = runner.execute_determination_events(
        boundary_events,
        episode,
        evaluation_time=observed_at + timedelta(minutes=10),
    ).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row.episode_type == "episode.temperature_high_active"
    assert row.subject_id == "machine-1"
    assert row.status == "expired"
    assert row.close_reason == "expiration"
    assert row.end_event_synthetic is True
    assert row.end_event_id
    assert row.end_time == observed_at + timedelta(minutes=5)
    assert row.duration_ms == 300000

    assert len(lifecycle_rows) == 1
    event = lifecycle_rows[0]
    assert event.event_type == "episode.temperature_high_active.expired"
    assert event.generation == 2
    assert event.event_class == "episode"
    assert event.event_ts == row.end_time
    assert event.correlation_id == row.episode_id
    assert event.payload["episode_id"] == row.episode_id
    assert event.payload["status"] == "expired"
    assert event.payload["close_reason"] == "expiration"
    assert event.payload["end_event_id"] == row.end_event_id
    assert event.payload["duration_ms"] == "300000"
    assert event.attributes["end_event_synthetic"] == "true"


@pytest.mark.requires_spark
def test_episode_runner_preserves_episode_id_across_lifecycle_views(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    open_boundary_events = ConditionEngineRunner().execute(
        _unclosed_events_df(spark), _conditions_df(spark)
    )
    closed_boundary_events = ConditionEngineRunner().execute(
        _events_df(spark), _conditions_df(spark)
    )
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        expires_after_seconds=300,
        subject_type="machine",
    )
    runner = EpisodeRunner()

    open_episode = runner.execute(
        open_boundary_events,
        episode,
        evaluation_time=observed_at + timedelta(minutes=1),
    ).collect()[0]
    expired_episode = runner.execute(
        open_boundary_events,
        episode,
        evaluation_time=observed_at + timedelta(minutes=10),
    ).collect()[0]
    closed_episode = runner.execute(closed_boundary_events, episode).collect()[0]

    assert open_episode.status == "open"
    assert expired_episode.status == "expired"
    assert closed_episode.status == "closed"
    assert open_episode.start_event_id == expired_episode.start_event_id
    assert open_episode.start_event_id == closed_episode.start_event_id
    assert open_episode.episode_id == expired_episode.episode_id
    assert open_episode.episode_id == closed_episode.episode_id


@pytest.mark.requires_spark
def test_episode_runner_prefers_visible_real_end_over_batch_expiration(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    closed_boundary_events = ConditionEngineRunner().execute(
        _events_df(spark), _conditions_df(spark)
    )
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        determination_event="episode.temperature_high_active.closed",
        expiration_event="episode.temperature_high_active.expired",
        expires_after_seconds=300,
        subject_type="machine",
    )

    runner = EpisodeRunner()
    row = runner.execute(
        closed_boundary_events,
        episode,
        evaluation_time=observed_at + timedelta(minutes=15),
    ).collect()[0]
    event = runner.execute_determination_events(
        closed_boundary_events,
        episode,
        evaluation_time=observed_at + timedelta(minutes=15),
    ).collect()[0]

    assert row.status == "closed"
    assert row.close_reason == "end_event"
    assert row.end_event_synthetic is False
    assert row.end_time == observed_at + timedelta(minutes=10)
    assert row.duration_ms == 600000
    assert event.event_type == "episode.temperature_high_active.closed"
    assert event.payload["status"] == "closed"
    assert event.payload["close_reason"] == "end_event"
    assert event.payload["duration_ms"] == "600000"
    assert event.attributes["end_event_synthetic"] == "false"


@pytest.mark.requires_spark
def test_episode_runner_invalidates_closed_episode_outside_duration_bounds(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    boundary_events = ConditionEngineRunner().execute(_events_df(spark), _conditions_df(spark))
    runner = EpisodeRunner()

    too_short_episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        invalidation_event="episode.temperature_high_active.invalidated",
        min_duration_seconds=900,
        subject_type="machine",
    )
    too_short_row = runner.execute(boundary_events, too_short_episode).collect()[0]
    too_short_event = runner.execute_determination_events(
        boundary_events, too_short_episode
    ).collect()[0]

    assert too_short_row.status == "invalidated"
    assert too_short_row.close_reason == "min_duration"
    assert too_short_row.end_event_synthetic is False
    assert too_short_row.duration_ms == 600000
    assert too_short_event.event_type == "episode.temperature_high_active.invalidated"
    assert too_short_event.payload["status"] == "invalidated"
    assert too_short_event.payload["close_reason"] == "min_duration"
    assert too_short_event.payload["duration_ms"] == "600000"

    too_long_episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        invalidation_event="episode.temperature_high_active.invalidated",
        max_duration_seconds=300,
        subject_type="machine",
    )
    too_long_row = runner.execute(boundary_events, too_long_episode).collect()[0]
    too_long_event = runner.execute_determination_events(
        boundary_events, too_long_episode
    ).collect()[0]

    assert too_long_row.status == "invalidated"
    assert too_long_row.close_reason == "max_duration"
    assert too_long_row.end_event_synthetic is False
    assert too_long_row.duration_ms == 600000
    assert too_long_event.event_type == "episode.temperature_high_active.invalidated"
    assert too_long_event.payload["status"] == "invalidated"
    assert too_long_event.payload["close_reason"] == "max_duration"
    assert too_long_event.payload["duration_ms"] == "600000"


@pytest.mark.requires_spark
def test_episode_runner_earliest_synthetic_boundary_is_terminal(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    boundary_events = ConditionEngineRunner().execute(
        _unclosed_events_df(spark), _conditions_df(spark)
    )
    runner = EpisodeRunner()
    past_both_boundaries = observed_at + timedelta(minutes=20)

    expiration_first = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        expiration_event="episode.temperature_high_active.expired",
        invalidation_event="episode.temperature_high_active.invalidated",
        expires_after_seconds=300,
        max_duration_seconds=600,
        subject_type="machine",
    )
    expired_row = runner.execute(
        boundary_events, expiration_first, evaluation_time=past_both_boundaries
    ).collect()[0]
    expired_event = runner.execute_determination_events(
        boundary_events, expiration_first, evaluation_time=past_both_boundaries
    ).collect()[0]

    assert expired_row.status == "expired"
    assert expired_row.close_reason == "expiration"
    assert expired_row.end_time == observed_at + timedelta(minutes=5)
    assert expired_row.duration_ms == 300000
    assert expired_event.event_type == "episode.temperature_high_active.expired"
    assert expired_event.payload["status"] == "expired"

    max_duration_first = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        expiration_event="episode.temperature_high_active.expired",
        invalidation_event="episode.temperature_high_active.invalidated",
        expires_after_seconds=600,
        max_duration_seconds=300,
        subject_type="machine",
    )
    invalidated_row = runner.execute(
        boundary_events, max_duration_first, evaluation_time=past_both_boundaries
    ).collect()[0]
    invalidated_event = runner.execute_determination_events(
        boundary_events, max_duration_first, evaluation_time=past_both_boundaries
    ).collect()[0]

    assert invalidated_row.status == "invalidated"
    assert invalidated_row.close_reason == "max_duration"
    assert invalidated_row.end_time == observed_at + timedelta(minutes=5)
    assert invalidated_row.duration_ms == 300000
    assert invalidated_event.event_type == "episode.temperature_high_active.invalidated"
    assert invalidated_event.payload["status"] == "invalidated"


@pytest.mark.requires_spark
def test_episode_runner_invalidates_open_episode_past_max_duration(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    boundary_events = ConditionEngineRunner().execute(
        _unclosed_events_df(spark), _conditions_df(spark)
    )
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        invalidation_event="episode.temperature_high_active.invalidated",
        max_duration_seconds=300,
        subject_type="machine",
    )

    runner = EpisodeRunner()
    row = runner.execute(
        boundary_events,
        episode,
        evaluation_time=observed_at + timedelta(minutes=10),
    ).collect()[0]
    event = runner.execute_determination_events(
        boundary_events,
        episode,
        evaluation_time=observed_at + timedelta(minutes=10),
    ).collect()[0]

    assert row.status == "invalidated"
    assert row.close_reason == "max_duration"
    assert row.end_event_synthetic is True
    assert row.end_event_id
    assert row.end_time == observed_at + timedelta(minutes=5)
    assert row.duration_ms == 300000
    assert event.event_type == "episode.temperature_high_active.invalidated"
    assert event.event_ts == row.end_time
    assert event.payload["status"] == "invalidated"
    assert event.payload["close_reason"] == "max_duration"
    assert event.payload["end_event_id"] == row.end_event_id
    assert event.payload["duration_ms"] == "300000"
    assert event.attributes["end_event_synthetic"] == "true"


@pytest.mark.requires_spark
def test_episode_runner_emits_episode_determination_event(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

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


@pytest.mark.requires_spark
def test_episode_runner_revises_persisted_expired_episode_with_late_real_end(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        determination_event="episode.temperature_high_active.closed",
        expiration_event="episode.temperature_high_active.expired",
        expires_after_seconds=300,
        subject_type="machine",
    )
    runner = EpisodeRunner()

    first_batch_boundaries = ConditionEngineRunner().execute(
        _unclosed_events_df(spark), _conditions_df(spark)
    )
    persisted = runner.execute(
        first_batch_boundaries, episode, evaluation_time=observed_at + timedelta(minutes=10)
    )
    persisted_row = persisted.collect()[0]
    assert persisted_row.status == "expired"

    late_end_boundaries = ConditionEngineRunner().execute(
        _end_only_events_df(spark, observed_at + timedelta(minutes=10)), _conditions_df(spark)
    )
    revised = runner.execute(
        late_end_boundaries,
        episode,
        evaluation_time=observed_at + timedelta(minutes=15),
        existing_episodes_df=persisted,
    ).collect()
    revision_events = runner.execute_determination_events(
        late_end_boundaries,
        episode,
        evaluation_time=observed_at + timedelta(minutes=15),
        existing_episodes_df=persisted,
    ).collect()

    assert len(revised) == 1
    row = revised[0]
    assert row.episode_id == persisted_row.episode_id
    assert row.start_event_id == persisted_row.start_event_id
    assert row.status == "closed"
    assert row.close_reason == "end_event"
    assert row.end_event_synthetic is False
    assert row.end_time == observed_at + timedelta(minutes=10)
    assert row.duration_ms == 600000

    assert len(revision_events) == 1
    event = revision_events[0]
    assert event.event_type == "episode.temperature_high_active.closed"
    assert event.generation == 2
    assert event.correlation_id == persisted_row.episode_id
    assert event.payload["status"] == "closed"
    assert event.payload["close_reason"] == "end_event"
    assert event.payload["end_event_id"] == row.end_event_id

    legacy_state = persisted.drop("start_generation")
    legacy_revised = runner.execute(
        late_end_boundaries,
        episode,
        evaluation_time=observed_at + timedelta(minutes=15),
        existing_episodes_df=legacy_state,
    ).collect()
    assert len(legacy_revised) == 1
    assert legacy_revised[0].status == "closed"
    assert legacy_revised[0].episode_id == persisted_row.episode_id
    assert legacy_revised[0].start_generation == 1


@pytest.mark.requires_spark
def test_episode_runner_revises_persisted_invalidated_episode_with_in_bounds_real_end(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        invalidation_event="episode.temperature_high_active.invalidated",
        max_duration_seconds=300,
        subject_type="machine",
    )
    runner = EpisodeRunner()

    first_batch_boundaries = ConditionEngineRunner().execute(
        _unclosed_events_df(spark), _conditions_df(spark)
    )
    persisted = runner.execute(
        first_batch_boundaries, episode, evaluation_time=observed_at + timedelta(minutes=10)
    )
    persisted_row = persisted.collect()[0]
    assert persisted_row.status == "invalidated"
    assert persisted_row.end_event_synthetic is True

    late_end_boundaries = ConditionEngineRunner().execute(
        _end_only_events_df(spark, observed_at + timedelta(minutes=4)), _conditions_df(spark)
    )
    revised = runner.execute(
        late_end_boundaries,
        episode,
        evaluation_time=observed_at + timedelta(minutes=15),
        existing_episodes_df=persisted,
    ).collect()

    assert len(revised) == 1
    row = revised[0]
    assert row.episode_id == persisted_row.episode_id
    assert row.status == "closed"
    assert row.close_reason == "end_event"
    assert row.end_event_synthetic is False
    assert row.end_time == observed_at + timedelta(minutes=4)
    assert row.duration_ms == 240000


@pytest.mark.requires_spark
def test_episode_runner_does_not_regress_persisted_state_without_new_information(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
        events_schema,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        expiration_event="episode.temperature_high_active.expired",
        expires_after_seconds=300,
        subject_type="machine",
    )
    runner = EpisodeRunner()

    first_batch_boundaries = ConditionEngineRunner().execute(
        _unclosed_events_df(spark), _conditions_df(spark)
    )
    persisted = runner.execute(
        first_batch_boundaries, episode, evaluation_time=observed_at + timedelta(minutes=10)
    )
    assert persisted.collect()[0].status == "expired"

    empty_boundaries = spark.createDataFrame([], events_schema())
    no_information = runner.execute(
        empty_boundaries, episode, existing_episodes_df=persisted
    ).collect()
    assert no_information == []

    recomputed = runner.execute(
        empty_boundaries,
        episode,
        evaluation_time=observed_at + timedelta(minutes=20),
        existing_episodes_df=persisted,
    ).collect()
    assert len(recomputed) == 1
    assert recomputed[0].status == "expired"
    assert recomputed[0].episode_id == persisted.collect()[0].episode_id
    assert recomputed[0].end_time == observed_at + timedelta(minutes=5)
    assert recomputed[0].duration_ms == 300000


@pytest.mark.requires_spark
def test_episode_runner_deduplicates_replayed_start_against_persisted_episode(spark):
    from kindling_ext_temporal import (
        ConditionEngineRunner,
        EpisodeMetadata,
        EpisodeRunner,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    episode = EpisodeMetadata(
        episodeid="episode.temperature_high_active",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.temperature_high.entered",
        end_event="condition.temperature_high.exited",
        condition_id="condition.temperature_high",
        expires_after_seconds=300,
        subject_type="machine",
    )
    runner = EpisodeRunner()

    first_batch_boundaries = ConditionEngineRunner().execute(
        _unclosed_events_df(spark), _conditions_df(spark)
    )
    persisted = runner.execute(
        first_batch_boundaries, episode, evaluation_time=observed_at + timedelta(minutes=1)
    )
    persisted_row = persisted.collect()[0]
    assert persisted_row.status == "open"

    replayed = runner.execute(
        first_batch_boundaries,
        episode,
        evaluation_time=observed_at + timedelta(minutes=1),
        existing_episodes_df=persisted,
    ).collect()

    assert len(replayed) == 1
    assert replayed[0].episode_id == persisted_row.episode_id
    assert replayed[0].status == "open"


class _RecordingProvider:
    def __init__(self):
        self.merged = []
        self.appended = []

    def merge_to_entity(self, df, entity):
        self.merged.append((df, entity))

    def append_to_entity(self, df, entity):
        self.appended.append((df, entity))


def _conditions_rows_df(spark, rows):
    from kindling_ext_temporal import conditions_schema

    return spark.createDataFrame(rows, conditions_schema())


@pytest.mark.requires_spark
def test_ingest_conditions_upserts_valid_rows_and_quarantines_invalid(spark):
    from kindling_ext_temporal import SimpleTemporalEntityResolver, ingest_conditions

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    rows = [
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
        ),
        (
            "condition.pressure_high",
            ["telemetry.observed"],
            "machine",
            {
                "enter_when": "cast(payload['pressure'] as double) > 5",
                "exit_when": "cast(payload['pressure'] as double) <= 5",
            },
            False,
            observed_at,
            None,
        ),
        (
            "condition.broken",
            ["telemetry.observed"],
            "machine",
            {"enter_when": "this is ((( not sql", "exit_when": "false"},
            True,
            observed_at,
            None,
        ),
    ]
    provider = _RecordingProvider()

    result = ingest_conditions(
        _conditions_rows_df(spark, rows),
        resolver=SimpleTemporalEntityResolver(),
        provider_factory=lambda entity: provider,
        quarantine_entity_id="silver.conditions_quarantine",
    )

    assert result.ingested_count == 2
    assert result.is_clean is False
    assert [invalid.condition_id for invalid in result.quarantined] == ["condition.broken"]
    assert result.quarantine_entity_id == "silver.conditions_quarantine"

    merged_df, merged_entity = provider.merged[0]
    assert merged_entity.entityid == "silver.conditions"
    merged_ids = sorted(row.condition_id for row in merged_df.collect())
    assert merged_ids == ["condition.pressure_high", "condition.temperature_high"]

    quarantine_df, quarantine_entity = provider.appended[0]
    assert quarantine_entity.entityid == "silver.conditions_quarantine"
    quarantine_rows = quarantine_df.collect()
    assert len(quarantine_rows) == 1
    assert quarantine_rows[0].condition_id == "condition.broken"
    assert quarantine_rows[0].errors
    assert quarantine_rows[0].raw_row
    assert quarantine_rows[0].quarantined_at is not None


@pytest.mark.requires_spark
def test_ingest_conditions_without_quarantine_entity_only_returns_rejects(spark):
    from kindling_ext_temporal import SimpleTemporalEntityResolver, ingest_conditions

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    rows = [
        (
            "condition.broken",
            ["telemetry.observed"],
            "machine",
            {"enter_when": "((( nope", "exit_when": "false"},
            True,
            observed_at,
            None,
        ),
    ]
    provider = _RecordingProvider()

    result = ingest_conditions(
        _conditions_rows_df(spark, rows),
        resolver=SimpleTemporalEntityResolver(),
        provider_factory=lambda entity: provider,
        quarantine_entity_id=None,
    )

    assert result.ingested_count == 0
    assert len(result.quarantined) == 1
    assert result.quarantine_entity_id is None
    assert provider.merged == []
    assert provider.appended == []


@pytest.mark.requires_spark
def test_ingest_conditions_raises_on_event_type_graph_cycle(spark):
    from kindling_ext_temporal import (
        ConditionValidationError,
        SimpleTemporalEntityResolver,
        ingest_conditions,
    )

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    rows = [
        (
            "condition.a",
            ["condition.b.entered"],
            "machine",
            {"enter_when": "true", "exit_when": "false"},
            True,
            observed_at,
            None,
        ),
        (
            "condition.b",
            ["condition.a.entered"],
            "machine",
            {"enter_when": "true", "exit_when": "false"},
            True,
            observed_at,
            None,
        ),
    ]
    provider = _RecordingProvider()

    with pytest.raises(ConditionValidationError, match="not ingestible"):
        ingest_conditions(
            _conditions_rows_df(spark, rows),
            resolver=SimpleTemporalEntityResolver(),
            provider_factory=lambda entity: provider,
        )
    assert provider.merged == []


@pytest.mark.requires_spark
def test_validated_conditions_transform_gates_file_drop(spark):
    from kindling_ext_temporal import (
        ConditionValidationError,
        validated_conditions_transform,
    )

    valid_df = _conditions_df(spark)
    assert validated_conditions_transform(valid_df) is valid_df

    observed_at = datetime(2026, 7, 14, 12, 0, 0)
    invalid_df = _conditions_rows_df(
        spark,
        [
            (
                "condition.broken",
                ["telemetry.observed"],
                "machine",
                {"enter_when": "((( nope", "exit_when": "false"},
                True,
                observed_at,
                None,
            )
        ],
    )
    with pytest.raises(ConditionValidationError):
        validated_conditions_transform(invalid_df)
