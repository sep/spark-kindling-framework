from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_temporal"
)


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


def _logger_provider():
    provider = MagicMock()
    provider.get_logger.return_value = MagicMock()
    return provider


def _temporal_service_get(
    *,
    event_registry=None,
    episode_registry=None,
    condition_registry=None,
    entity_registry=None,
    pipe_registry=None,
):
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling_ext_temporal import (
        SimpleTemporalEntityResolver,
        TemporalConditionRegistry,
        TemporalEntityResolver,
        TemporalEpisodeRegistry,
        TemporalEventRegistry,
    )

    def _get(dep):
        if dep is TemporalEntityResolver:
            return SimpleTemporalEntityResolver()
        if dep is TemporalEventRegistry and event_registry is not None:
            return event_registry
        if dep is TemporalEpisodeRegistry and episode_registry is not None:
            return episode_registry
        if dep is TemporalConditionRegistry and condition_registry is not None:
            return condition_registry
        if dep is DataEntityRegistry and entity_registry is not None:
            return entity_registry
        if dep is DataPipesRegistry and pipe_registry is not None:
            return pipe_registry
        raise AssertionError(f"Unexpected service request: {dep}")

    return _get


def _condition_registry():
    from kindling_ext_temporal import TemporalConditionRegistryManager

    return TemporalConditionRegistryManager(_logger_provider())


def test_default_resolver_returns_canonical_entities():
    from kindling_ext_temporal import SimpleTemporalEntityResolver

    resolver = SimpleTemporalEntityResolver()

    assert resolver.get_events_entity().entityid == "silver.events"
    assert resolver.get_conditions_entity().entityid == "silver.conditions"
    assert resolver.get_episodes_entity().entityid == "silver.episodes"


def test_conditions_entity_is_scd2_tagged():
    from kindling_ext_temporal import SimpleTemporalEntityResolver

    entity = SimpleTemporalEntityResolver().get_conditions_entity()

    assert entity.merge_columns == ["condition_id"]
    assert entity.tags["scd.type"] == "2"
    assert entity.tags["scd.current_entity_id"] == "silver.conditions.current"


def test_conditions_schema_supports_multiple_consumed_event_types():
    from kindling_ext_temporal import conditions_schema
    from pyspark.sql.types import ArrayType, StringType

    consumes_field = conditions_schema()["consumes_event_type"]

    assert isinstance(consumes_field.dataType, ArrayType)
    assert isinstance(consumes_field.dataType.elementType, StringType)


def test_events_schema_matches_proposal_envelope():
    from kindling_ext_temporal import events_schema

    columns = events_schema().fieldNames()

    assert columns == [
        "event_id",
        "event_type",
        "generation",
        "event_class",
        "subject_type",
        "subject_id",
        "event_ts",
        "source_system",
        "correlation_id",
        "payload",
        "attributes",
        "ingested_at",
    ]


def test_base_event_pipe_id_is_namespaced():
    from kindling_ext_temporal import TemporalPipeTranslator

    assert (
        TemporalPipeTranslator.base_event_pipe_id("telemetry.base")
        == "temporal.event.telemetry.base"
    )


def test_base_event_decorator_registers_metadata():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager

    DataEvents.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):

        @DataEvents.base_event(
            eventid="telemetry.base",
            input_entity_id="bronze.telemetry",
            subject_type="machine",
            subject_keys=["machine_id"],
            time_column="event_ts",
            event_type="telemetry.observed",
            payload_columns=["temperature"],
            use_watermark=True,
            tags={"domain": "iot"},
        )
        def normalize(df):
            return df

    metadata = event_registry.get_base_event_definition("telemetry.base")
    assert metadata.input_entity_id == "bronze.telemetry"
    assert metadata.output_entity_id == "silver.events"
    assert metadata.subject_keys == ["machine_id"]
    assert metadata.use_watermark is True
    assert metadata.transform is normalize

    pipe = pipe_registry.get_pipe_definition("temporal.event.telemetry.base")
    assert pipe.input_entity_ids == ["bronze.telemetry"]
    assert pipe.output_entity_id == "silver.events"
    assert pipe.output_type == "delta"
    assert pipe.use_watermark is True
    assert pipe.tags["pipe_type"] == "temporal.base_event"
    assert pipe.tags["temporal.event_type"] == "telemetry.observed"
    assert pipe.tags["domain"] == "iot"
    assert callable(pipe.execute)

    entity = entity_registry.get_entity_definition("silver.events")
    assert entity is not None
    assert entity.merge_columns == ["event_id"]


def test_base_event_registration_accepts_none_tags_and_requires_metadata():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager

    DataEvents.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):

        @DataEvents.base_event(
            eventid="telemetry.null_tags",
            input_entity_id="bronze.telemetry",
            subject_type="machine",
            subject_keys=["machine_id"],
            time_column="event_ts",
            event_type="telemetry.observed",
            tags=None,
        )
        def normalize(df):
            return df

    pipe = pipe_registry.get_pipe_definition("temporal.event.telemetry.null_tags")
    assert pipe.tags["pipe_type"] == "temporal.base_event"

    DataEvents.reset()
    missing_metadata_registry = MagicMock()
    missing_metadata_registry.get_base_event_definition.return_value = None

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=missing_metadata_registry,
            entity_registry=DataEntityManager(),
            pipe_registry=DataPipesManager(_logger_provider()),
        ),
    ):
        with pytest.raises(ValueError, match="Temporal base event 'telemetry.missing'"):

            @DataEvents.base_event(
                eventid="telemetry.missing",
                input_entity_id="bronze.telemetry",
                subject_type="machine",
                subject_keys=["machine_id"],
                time_column="event_ts",
                event_type="telemetry.observed",
            )
            def missing(df):
                return df


def test_condition_engine_registration_is_not_condition_specific():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager

    DataEvents.reset()
    registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):
        DataEvents.condition_engine(engineid="condition_engine.default")

    metadata = registry.get_condition_engine_definition("condition_engine.default")
    assert metadata.engineid == "condition_engine.default"
    assert metadata.events_entity_id == "silver.events"
    assert metadata.conditions_entity_id == "silver.conditions"
    assert metadata.conditions_current_entity_id == "silver.conditions.current"
    assert entity_registry.get_entity_definition("silver.conditions") is not None
    assert entity_registry.get_entity_definition("silver.conditions.current") is not None

    pipe = pipe_registry.get_pipe_definition("temporal.condition.condition_engine.default")
    assert pipe.input_entity_ids == ["silver.events", "silver.conditions.current"]
    assert pipe.output_entity_id == "silver.events"
    assert pipe.output_type == "delta"
    assert pipe.use_watermark is True
    assert pipe.tags["pipe_type"] == "temporal.condition_engine"
    assert callable(pipe.execute)


def test_episode_registration_uses_canonical_entities():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import DataEpisodes, TemporalEpisodeRegistryManager

    DataEpisodes.reset()
    registry = TemporalEpisodeRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            episode_registry=registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):
        DataEpisodes.episode(
            episodeid="episode.machine_cycle",
            start_event="condition.machine_running.entered",
            end_event="condition.machine_running.exited",
            subject_type="machine",
            expires_after_seconds=28800,
        )

    metadata = registry.get_episode_definition("episode.machine_cycle")
    assert metadata.output_entity_id == "silver.episodes"
    assert metadata.events_entity_id == "silver.events"
    assert metadata.start_event == "condition.machine_running.entered"
    assert metadata.end_event == "condition.machine_running.exited"
    assert metadata.condition_id == "condition.machine_running"
    assert metadata.determination_event == "episode.machine_cycle.closed"
    assert metadata.expiration_event == "episode.machine_cycle.expired"
    assert metadata.invalidation_event == "episode.machine_cycle.invalidated"
    assert metadata.expires_after_seconds == 28800
    assert entity_registry.get_entity_definition("silver.events") is not None
    assert entity_registry.get_entity_definition("silver.episodes") is not None

    pipe = pipe_registry.get_pipe_definition("temporal.episode.episode.machine_cycle")
    assert pipe.input_entity_ids == ["silver.events"]
    assert pipe.output_entity_id == "silver.episodes"
    assert pipe.output_type == "delta"
    assert pipe.use_watermark is True
    assert pipe.tags["pipe_type"] == "temporal.episode"
    assert pipe.tags["temporal.start_event"] == "condition.machine_running.entered"
    assert pipe.tags["temporal.end_event"] == "condition.machine_running.exited"
    assert pipe.tags["temporal.reads_prior_state"] == "true"
    assert callable(pipe.execute)

    event_pipe = pipe_registry.get_pipe_definition("temporal.episode_event.episode.machine_cycle")
    assert event_pipe.input_entity_ids == ["silver.events"]
    assert event_pipe.tags["temporal.reads_prior_state"] == "true"
    assert event_pipe.output_entity_id == "silver.events"
    assert event_pipe.output_type == "delta"
    assert event_pipe.use_watermark is True
    assert event_pipe.tags["pipe_type"] == "temporal.episode_event"
    assert event_pipe.tags["temporal.event_type"] == "episode.machine_cycle.closed"
    assert event_pipe.tags["temporal.expiration_event_type"] == "episode.machine_cycle.expired"
    assert (
        event_pipe.tags["temporal.invalidation_event_type"] == "episode.machine_cycle.invalidated"
    )
    assert event_pipe.tags["temporal.start_event"] == "condition.machine_running.entered"
    assert event_pipe.tags["temporal.end_event"] == "condition.machine_running.exited"
    assert callable(event_pipe.execute)


def test_episode_registration_accepts_explicit_determination_event_and_pipe_id():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import DataEpisodes, TemporalEpisodeRegistryManager

    DataEpisodes.reset()
    registry = TemporalEpisodeRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            episode_registry=registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):
        DataEpisodes.episode(
            episodeid="episode.machine_cycle",
            start_event="condition.machine_running.entered",
            end_event="condition.machine_running.exited",
            determination_event="episode.machine_cycle.completed",
            expiration_event="episode.machine_cycle.timed_out",
            invalidation_event="episode.machine_cycle.rejected",
            determination_pipeid="temporal.episode_event.machine_cycle_completed",
        )

    metadata = registry.get_episode_definition("episode.machine_cycle")
    assert metadata.determination_event == "episode.machine_cycle.completed"
    assert metadata.expiration_event == "episode.machine_cycle.timed_out"
    assert metadata.invalidation_event == "episode.machine_cycle.rejected"

    event_pipe = pipe_registry.get_pipe_definition("temporal.episode_event.machine_cycle_completed")
    assert event_pipe.output_entity_id == "silver.events"
    assert event_pipe.tags["temporal.event_type"] == "episode.machine_cycle.completed"
    assert event_pipe.tags["temporal.expiration_event_type"] == "episode.machine_cycle.timed_out"
    assert event_pipe.tags["temporal.invalidation_event_type"] == "episode.machine_cycle.rejected"


def _episode_metadata_for_resolution():
    from kindling_ext_temporal import EpisodeMetadata

    return EpisodeMetadata(
        episodeid="episode.machine_cycle",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.machine_running.entered",
        end_event="condition.machine_running.exited",
    )


def test_translator_prior_episodes_prefers_execution_parameter():
    from kindling_ext_temporal import TemporalPipeTranslator

    state_df = object()
    with patch("kindling.injection.GlobalInjector.get") as injector_get:
        resolved = TemporalPipeTranslator.resolve_prior_episodes(
            {"silver_events": None, "silver_episodes": state_df},
            _episode_metadata_for_resolution(),
        )

    assert resolved is state_df
    injector_get.assert_not_called()


def test_translator_prior_episodes_reads_existing_entity_through_provider():
    from kindling.data_entities import DataEntityRegistry
    from kindling.entity_provider_registry import EntityProviderRegistry
    from kindling.spark_config import ConfigService
    from kindling_ext_temporal import TemporalPipeTranslator

    state_df = object()
    entity = Mock(entityid="silver.episodes")
    entity_registry = Mock()
    entity_registry.get_entity_definition.return_value = entity
    provider = Mock()
    provider.check_entity_exists.return_value = True
    provider.read_entity.return_value = state_df
    provider_registry = Mock()
    provider_registry.get_provider_for_entity.return_value = provider
    config_service = Mock()
    config_service.get.return_value = True

    def _get(dep):
        if dep is ConfigService:
            return config_service
        if dep is DataEntityRegistry:
            return entity_registry
        if dep is EntityProviderRegistry:
            return provider_registry
        raise AssertionError(f"Unexpected service request: {dep}")

    with patch("kindling.injection.GlobalInjector.get", side_effect=_get):
        resolved = TemporalPipeTranslator.resolve_prior_episodes(
            {"silver_events": None}, _episode_metadata_for_resolution()
        )

    assert resolved is state_df
    config_service.get.assert_called_once_with("kindling.temporal.revise_persisted", True)
    provider.read_entity.assert_called_once_with(entity)


def test_translator_prior_episodes_none_when_entity_missing_or_disabled():
    from kindling.data_entities import DataEntityRegistry
    from kindling.entity_provider_registry import EntityProviderRegistry
    from kindling.spark_config import ConfigService
    from kindling_ext_temporal import TemporalPipeTranslator

    entity_registry = Mock()
    entity_registry.get_entity_definition.return_value = Mock(entityid="silver.episodes")
    provider = Mock()
    provider.check_entity_exists.return_value = False
    provider_registry = Mock()
    provider_registry.get_provider_for_entity.return_value = provider

    def _get_enabled(dep):
        if dep is ConfigService:
            service = Mock()
            service.get.return_value = True
            return service
        if dep is DataEntityRegistry:
            return entity_registry
        if dep is EntityProviderRegistry:
            return provider_registry
        raise AssertionError(f"Unexpected service request: {dep}")

    with patch("kindling.injection.GlobalInjector.get", side_effect=_get_enabled):
        assert (
            TemporalPipeTranslator.resolve_prior_episodes(
                {"silver_events": None}, _episode_metadata_for_resolution()
            )
            is None
        )
    provider.read_entity.assert_not_called()

    def _get_disabled(dep):
        if dep is ConfigService:
            service = Mock()
            service.get.return_value = "false"
            return service
        raise AssertionError(f"Unexpected service request: {dep}")

    with patch("kindling.injection.GlobalInjector.get", side_effect=_get_disabled):
        assert (
            TemporalPipeTranslator.resolve_prior_episodes(
                {"silver_events": None}, _episode_metadata_for_resolution()
            )
            is None
        )


def test_translator_prior_episodes_none_without_bound_services():
    from kindling_ext_temporal import TemporalPipeTranslator

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=RuntimeError("no bindings"),
    ):
        assert (
            TemporalPipeTranslator.resolve_prior_episodes(
                {"silver_events": None}, _episode_metadata_for_resolution()
            )
            is None
        )


def test_translator_evaluation_time_prefers_execution_parameter():
    from kindling_ext_temporal import TemporalPipeTranslator

    explicit = datetime(2026, 7, 14, 12, 10, 0)
    with patch("kindling.injection.GlobalInjector.get") as injector_get:
        resolved = TemporalPipeTranslator.resolve_evaluation_time(
            {"temporal_evaluation_time": explicit}
        )

    assert resolved == explicit
    injector_get.assert_not_called()


def test_translator_evaluation_time_falls_back_to_config():
    from kindling_ext_temporal import TemporalPipeTranslator

    configured = datetime(2026, 7, 14, 12, 10, 0)

    class _ConfigService:
        def __init__(self):
            self.requested = []

        def get(self, key, default=None):
            self.requested.append(key)
            return configured

    config_service = _ConfigService()
    with patch("kindling.injection.GlobalInjector.get", return_value=config_service):
        resolved = TemporalPipeTranslator.resolve_evaluation_time({"silver_events": None})

    assert resolved == configured
    assert config_service.requested == ["kindling.temporal.evaluation_time"]


def test_translator_evaluation_time_defaults_to_none_without_config_service():
    from kindling_ext_temporal import TemporalPipeTranslator

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=RuntimeError("no ConfigService binding"),
    ):
        assert TemporalPipeTranslator.resolve_evaluation_time({"silver_events": None}) is None


def test_translator_handles_none_tags_on_temporal_metadata_and_entities():
    from kindling.data_entities import DataEntityManager, EntityMetadata
    from kindling_ext_temporal import (
        BaseEventMetadata,
        ConditionEngineMetadata,
        EpisodeMetadata,
        TemporalPipeTranslator,
        events_schema,
    )

    base_event = BaseEventMetadata(
        eventid="telemetry.none_tags",
        input_entity_id="bronze.telemetry",
        output_entity_id="silver.events",
        subject_type="machine",
        subject_keys=["machine_id"],
        time_column="event_ts",
        event_type="telemetry.observed",
        tags=None,
    )
    condition_engine = ConditionEngineMetadata(
        engineid="condition_engine.none_tags",
        events_entity_id="silver.events",
        conditions_entity_id="silver.conditions",
        conditions_current_entity_id="silver.conditions.current",
        tags=None,
    )
    episode = EpisodeMetadata(
        episodeid="episode.none_tags",
        output_entity_id="silver.episodes",
        events_entity_id="silver.events",
        start_event="condition.none.entered",
        end_event="condition.none.exited",
        determination_event="episode.none.closed",
        expiration_event="episode.none.expired",
        invalidation_event="episode.none.invalidated",
        tags=None,
    )

    assert (
        TemporalPipeTranslator.base_event_pipe_params(base_event)["tags"]["pipe_type"]
        == "temporal.base_event"
    )
    assert (
        TemporalPipeTranslator.condition_engine_pipe_params(condition_engine)["tags"]["pipe_type"]
        == "temporal.condition_engine"
    )
    assert (
        TemporalPipeTranslator.episode_pipe_params(episode)["tags"]["pipe_type"]
        == "temporal.episode"
    )
    assert (
        TemporalPipeTranslator.episode_determination_event_pipe_params(episode)["tags"]["pipe_type"]
        == "temporal.episode_event"
    )

    registry = DataEntityManager()
    TemporalPipeTranslator.ensure_entity(
        registry,
        EntityMetadata(
            entityid="silver.events",
            name="events",
            merge_columns=["event_id"],
            tags=None,
            schema=events_schema(),
        ),
    )

    assert registry.get_entity_definition("silver.events").tags == {}


class RecordingExpressionParser:
    def __init__(self, invalid_expressions=None):
        self.invalid_expressions = set(invalid_expressions or [])
        self.parsed = []

    def parse(self, expression):
        self.parsed.append(expression)
        if expression in self.invalid_expressions:
            raise ValueError("parse failed")


def _condition_row(**overrides):
    row = {
        "condition_id": "condition.temperature_high",
        "consumes_event_type": ["telemetry.observed"],
        "subject_type": "machine",
        "parameters": {
            "enter_when": "cast(payload['temperature'] as double) > 90",
            "exit_when": "cast(payload['temperature'] as double) <= 90",
        },
        "enabled": True,
    }
    row.update(overrides)
    return row


def test_condition_validator_rejects_bad_expression_per_row():
    from kindling_ext_temporal import TemporalConditionValidator

    parser = RecordingExpressionParser(invalid_expressions={"bad spark sql"})
    validator = TemporalConditionValidator(expression_parser=parser)

    report = validator.validate(
        [
            _condition_row(condition_id="condition.good"),
            _condition_row(
                condition_id="condition.bad",
                parameters={
                    "enter_when": "bad spark sql",
                    "exit_when": "cast(payload['temperature'] as double) <= 90",
                },
            ),
        ]
    )

    assert report.is_valid is False
    assert [rule.condition_id for rule in report.valid_rules] == ["condition.good"]
    assert report.invalid_conditions[0].condition_id == "condition.bad"
    assert "parameters.enter_when is invalid" in report.invalid_conditions[0].errors[0]
    assert "bad spark sql" in parser.parsed


def test_condition_validator_requires_enter_and_exit_expressions():
    from kindling_ext_temporal import TemporalConditionValidator

    report = TemporalConditionValidator(expression_parser=RecordingExpressionParser()).validate(
        [
            _condition_row(
                parameters={
                    "enter_when": "",
                }
            )
        ]
    )

    assert report.is_valid is False
    assert report.invalid_conditions[0].errors == [
        "parameters.enter_when is required",
        "parameters.exit_when is required",
    ]


def test_condition_validator_computes_event_type_generations():
    from kindling_ext_temporal import TemporalConditionValidator

    validator = TemporalConditionValidator(expression_parser=RecordingExpressionParser())
    assert validator.graph_builder.registry is not None

    report = validator.validate(
        [
            _condition_row(condition_id="condition.temperature_high"),
            _condition_row(
                condition_id="condition.thermal_excursion",
                consumes_event_type=["condition.temperature_high.entered"],
            ),
        ]
    )

    assert report.is_valid is True
    assert report.generations == [
        ["telemetry.observed"],
        ["condition.temperature_high.entered", "condition.temperature_high.exited"],
        ["condition.thermal_excursion.entered", "condition.thermal_excursion.exited"],
    ]


def test_condition_validator_rejects_event_type_cycles():
    from kindling_ext_temporal import TemporalConditionValidator

    report = TemporalConditionValidator(expression_parser=RecordingExpressionParser()).validate(
        [
            _condition_row(
                condition_id="condition.first",
                consumes_event_type=["condition.second.entered"],
            ),
            _condition_row(
                condition_id="condition.second",
                consumes_event_type=["condition.first.entered"],
            ),
        ]
    )

    assert report.is_valid is False
    assert "Cycle detected in pipe dependencies" in report.invalid_conditions[0].errors[0]


def test_conditions_ingestion_result_and_config_key():
    from kindling_ext_temporal import (
        QUARANTINE_ENTITY_CONFIG_KEY,
        ConditionsIngestionResult,
    )

    assert QUARANTINE_ENTITY_CONFIG_KEY == "kindling.temporal.conditions.quarantine_entity_id"
    assert ConditionsIngestionResult(ingested_count=3).is_clean is True
    assert ConditionsIngestionResult(ingested_count=0, quarantined=[object()]).is_clean is False


@pytest.fixture(scope="module")
def _memory_spark_session():
    """Plain (non-Delta) SparkSession — see test_entity_provider_memory_scd2.py's
    module docstring for why this avoids conftest.py's shared, Delta-configured
    spark_session fixture (MemoryEntityProvider never needs Delta, and that
    fixture forces Delta config onto whatever SparkSession is already active
    in the process, which breaks if an earlier, unrelated test created a plain
    one first)."""
    from pyspark.sql import SparkSession

    from tests.conftest import _sockets_permitted

    if not _sockets_permitted():
        pytest.skip(
            "Sockets are not permitted in this environment; cannot start a real SparkSession."
        )
    spark = (
        SparkSession.builder.appName("TemporalConditionsIngestTests")
        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark


def test_ingest_conditions_end_to_end_against_memory_provider(_memory_spark_session, monkeypatch):
    """Acceptance criterion #2: ingest_conditions() against a real MemoryEntityProvider
    — validation/quarantine on first ingest, then an SCD2 re-ingest that changes a
    tracked field (enabled) closes the old version and opens exactly one new one."""
    from kindling.entity_provider_memory import MemoryEntityProvider
    from kindling_ext_temporal import (
        SimpleTemporalEntityResolver,
        conditions_schema,
        ingest_conditions,
    )

    spark = _memory_spark_session
    monkeypatch.setattr(
        "kindling.entity_provider_memory.get_or_create_spark_session", lambda: MagicMock()
    )
    memory_provider = MemoryEntityProvider(_logger_provider())
    memory_provider.spark = spark

    resolver = SimpleTemporalEntityResolver()
    valid_from = datetime(2026, 1, 1)

    df = spark.createDataFrame(
        [
            _condition_row(condition_id="condition.temperature_high", valid_from=valid_from),
            _condition_row(
                condition_id="condition.bad",
                valid_from=valid_from,
                parameters={
                    "enter_when": "",
                    "exit_when": "cast(payload['temperature'] as double) <= 90",
                },
            ),
        ],
        conditions_schema(),
    )

    result = ingest_conditions(
        df,
        resolver=resolver,
        provider_factory=lambda entity: memory_provider,
        quarantine_entity_id=None,
    )

    assert result.ingested_count == 1
    assert [invalid.condition_id for invalid in result.quarantined] == ["condition.bad"]

    stored = memory_provider.read_entity(resolver.get_conditions_entity()).collect()
    current = [row for row in stored if row["__is_current"]]
    assert len(current) == 1
    assert current[0]["condition_id"] == "condition.temperature_high"
    assert current[0]["enabled"] is True

    # Re-ingest the same condition with a tracked field (enabled) flipped.
    changed_df = spark.createDataFrame(
        [
            _condition_row(
                condition_id="condition.temperature_high", enabled=False, valid_from=valid_from
            )
        ],
        conditions_schema(),
    )
    ingest_conditions(
        changed_df,
        resolver=resolver,
        provider_factory=lambda entity: memory_provider,
        quarantine_entity_id=None,
    )

    all_rows = memory_provider.read_entity(resolver.get_conditions_entity()).collect()
    current_rows = [row for row in all_rows if row["__is_current"]]
    closed_rows = [row for row in all_rows if not row["__is_current"]]
    assert len(current_rows) == 1
    assert current_rows[0]["enabled"] is False
    assert len(closed_rows) == 1, "exactly one prior version must be closed, not duplicated"
    assert closed_rows[0]["enabled"] is True


# --- gh#222: registry-declared temporal conditions ---------------------------


def test_data_conditions_register_is_retrievable_via_registry():
    from kindling_ext_temporal import DataConditions

    condition_registry = _condition_registry()

    def enter_when(events):
        return events["payload"]["temperature"] > 90

    def exit_when(events):
        return events["payload"]["temperature"] <= 90

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(condition_registry=condition_registry),
    ):
        DataConditions.register(
            condition_id="condition.overheat",
            consumes_event_type=["telemetry.observed"],
            subject_type="machine",
            enter_when=enter_when,
            exit_when=exit_when,
        )

    rule = condition_registry.get_condition_definition("condition.overheat")
    assert rule.condition_id == "condition.overheat"
    assert rule.consumes_event_type == ["telemetry.observed"]
    assert rule.subject_type == "machine"
    assert rule.parameters["enter_when"] is enter_when
    assert rule.parameters["exit_when"] is exit_when
    assert rule.enabled is True
    assert condition_registry.get_condition_ids() == ["condition.overheat"]
    assert condition_registry.get_all_conditions() == [rule]


def test_data_conditions_register_requires_condition_id():
    from kindling_ext_temporal import ConditionValidationError, DataConditions

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(condition_registry=_condition_registry()),
    ):
        with pytest.raises(ConditionValidationError, match="condition_id is required"):
            DataConditions.register(
                condition_id="",
                consumes_event_type=["telemetry.observed"],
                subject_type="machine",
                enter_when=lambda events: events,
                exit_when=lambda events: events,
            )


def test_data_conditions_register_requires_subject_type():
    from kindling_ext_temporal import ConditionValidationError, DataConditions

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(condition_registry=_condition_registry()),
    ):
        with pytest.raises(ConditionValidationError, match="subject_type is required"):
            DataConditions.register(
                condition_id="condition.overheat",
                consumes_event_type=["telemetry.observed"],
                subject_type="",
                enter_when=lambda events: events,
                exit_when=lambda events: events,
            )


def test_data_conditions_register_requires_consumes_event_type():
    from kindling_ext_temporal import ConditionValidationError, DataConditions

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(condition_registry=_condition_registry()),
    ):
        with pytest.raises(
            ConditionValidationError,
            match="consumes_event_type must contain at least one event type",
        ):
            DataConditions.register(
                condition_id="condition.overheat",
                consumes_event_type=[],
                subject_type="machine",
                enter_when=lambda events: events,
                exit_when=lambda events: events,
            )


def test_data_conditions_register_requires_callable_enter_when():
    from kindling_ext_temporal import ConditionValidationError, DataConditions

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(condition_registry=_condition_registry()),
    ):
        with pytest.raises(ConditionValidationError, match="enter_when must be callable"):
            DataConditions.register(
                condition_id="condition.overheat",
                consumes_event_type=["telemetry.observed"],
                subject_type="machine",
                enter_when="not callable",
                exit_when=lambda events: events,
            )


def test_data_conditions_register_requires_callable_exit_when():
    from kindling_ext_temporal import ConditionValidationError, DataConditions

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(condition_registry=_condition_registry()),
    ):
        with pytest.raises(ConditionValidationError, match="exit_when must be callable"):
            DataConditions.register(
                condition_id="condition.overheat",
                consumes_event_type=["telemetry.observed"],
                subject_type="machine",
                enter_when=lambda events: events,
                exit_when="not callable",
            )


def test_data_conditions_register_rejects_duplicate_condition_id():
    from kindling_ext_temporal import ConditionValidationError, DataConditions

    condition_registry = _condition_registry()

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(condition_registry=condition_registry),
    ):
        DataConditions.register(
            condition_id="condition.overheat",
            consumes_event_type=["telemetry.observed"],
            subject_type="machine",
            enter_when=lambda events: events,
            exit_when=lambda events: events,
        )
        with pytest.raises(ConditionValidationError, match="Duplicate condition_id"):
            DataConditions.register(
                condition_id="condition.overheat",
                consumes_event_type=["telemetry.observed"],
                subject_type="machine",
                enter_when=lambda events: events,
                exit_when=lambda events: events,
            )


def test_data_conditions_reset_clears_registered_conditions():
    from kindling_ext_temporal import DataConditions

    condition_registry = _condition_registry()

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(condition_registry=condition_registry),
    ):
        DataConditions.register(
            condition_id="condition.overheat",
            consumes_event_type=["telemetry.observed"],
            subject_type="machine",
            enter_when=lambda events: events,
            exit_when=lambda events: events,
        )
        DataConditions.reset()

    assert condition_registry.get_all_conditions() == []
    assert condition_registry.get_condition_ids() == []


def test_condition_engine_registry_source_has_no_table_entities():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager

    DataEvents.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())
    condition_registry = _condition_registry()

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
            condition_registry=condition_registry,
        ),
    ):
        DataEvents.condition_engine(engineid="static_conditions", condition_source="registry")

    metadata = event_registry.get_condition_engine_definition("static_conditions")
    assert metadata.condition_source == "registry"
    assert metadata.conditions_entity_id is None
    assert metadata.conditions_current_entity_id is None
    assert metadata.events_entity_id == "silver.events"
    assert entity_registry.get_entity_definition("silver.events") is not None
    assert entity_registry.get_entity_definition("silver.conditions") is None
    assert entity_registry.get_entity_definition("silver.conditions.current") is None

    pipe = pipe_registry.get_pipe_definition("temporal.condition.static_conditions")
    assert pipe.input_entity_ids == ["silver.events"]
    assert pipe.output_entity_id == "silver.events"
    assert callable(pipe.execute)


def test_condition_engine_rejects_invalid_condition_source():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager

    DataEvents.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):
        with pytest.raises(ValueError, match="condition_source must be"):
            DataEvents.condition_engine(engineid="bogus", condition_source="both")

    assert event_registry.get_condition_engine_definition("bogus") is None


def test_condition_engine_registry_source_rejects_cycle_at_declaration_time():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import (
        ConditionRule,
        ConditionValidationError,
        DataEvents,
        TemporalEventRegistryManager,
    )

    DataEvents.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())
    condition_registry = _condition_registry()
    # Neither rule is cyclic alone; each consumes the OTHER's produced
    # boundary event type, forming a 2-condition cycle in the registry's
    # current contents -- mirrors test_condition_validator_rejects_event_
    # type_cycles, but through the registry's own declaration-time check
    # (registry.py's condition_engine()) rather than TemporalConditionValidator
    # .validate() directly.
    condition_registry.register_condition(
        ConditionRule(
            condition_id="condition.first",
            consumes_event_type=["condition.second.entered"],
            subject_type="machine",
            parameters={"enter_when": lambda events: events, "exit_when": lambda events: events},
        )
    )
    condition_registry.register_condition(
        ConditionRule(
            condition_id="condition.second",
            consumes_event_type=["condition.first.entered"],
            subject_type="machine",
            parameters={"enter_when": lambda events: events, "exit_when": lambda events: events},
        )
    )

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
            condition_registry=condition_registry,
        ),
    ):
        with pytest.raises(ConditionValidationError, match="Conditions set is not ingestible"):
            DataEvents.condition_engine(engineid="static_conditions", condition_source="registry")

    # The rejected declaration must not have partially registered the engine.
    assert event_registry.get_condition_engine_definition("static_conditions") is None


def test_condition_engine_pipe_params_registry_source_omits_conditions_current():
    from kindling_ext_temporal import ConditionEngineMetadata, TemporalPipeTranslator

    metadata = ConditionEngineMetadata(
        engineid="static_conditions",
        events_entity_id="silver.events",
        condition_source="registry",
    )

    params = TemporalPipeTranslator.condition_engine_pipe_params(metadata)

    assert params["input_entity_ids"] == ["silver.events"]
    assert params["output_entity_id"] == "silver.events"


def test_condition_engine_execute_registry_source_calls_execute_rules_directly():
    from kindling_ext_temporal import (
        ConditionEngineMetadata,
        TemporalConditionRegistry,
        TemporalPipeTranslator,
    )

    metadata = ConditionEngineMetadata(
        engineid="static_conditions",
        events_entity_id="silver.events",
        condition_source="registry",
    )
    events_df = object()
    registry_rules = [object()]
    runner = MagicMock()
    runner.execute_rules.return_value = "boundary_events"

    def _get(dep):
        if dep is TemporalConditionRegistry:
            condition_registry = MagicMock()
            condition_registry.get_all_conditions.return_value = registry_rules
            return condition_registry
        raise AssertionError(f"Unexpected service request: {dep}")

    execute = TemporalPipeTranslator.condition_engine_execute(metadata)
    with (
        patch("kindling.injection.GlobalInjector.get", side_effect=_get),
        patch("kindling_ext_temporal.engine.ConditionEngineRunner", return_value=runner),
    ):
        # Deliberately no "silver_conditions_current" key -- a registry
        # engine must never look for a conditions-current input.
        result = execute(silver_events=events_df)

    assert result == "boundary_events"
    runner.execute_rules.assert_called_once_with(events_df, registry_rules)


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("TemporalExtensionUnit")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _predicate_events_df(spark):
    from kindling_ext_temporal import events_schema

    now = datetime(2026, 7, 14, 12, 0, 0)
    rows = [
        (
            "evt-hot",
            "telemetry.observed",
            0,
            "base",
            "machine",
            "machine-1",
            now,
            "test",
            None,
            {"temperature": "95.0"},
            None,
            now,
        ),
        (
            "evt-cold",
            "telemetry.observed",
            0,
            "base",
            "machine",
            "machine-1",
            now,
            "test",
            None,
            {"temperature": "50.0"},
            None,
            now,
        ),
    ]
    return spark.createDataFrame(rows, events_schema())


@pytest.mark.requires_spark
def test_execute_rules_invokes_callable_predicate_and_filters_on_returned_column(spark):
    from kindling_ext_temporal import ConditionEngineRunner, ConditionRule

    events_df = _predicate_events_df(spark)
    enter_calls = []
    exit_calls = []

    def enter_when(events):
        enter_calls.append(events)
        return events["payload"]["temperature"].cast("double") > 90

    def exit_when(events):
        exit_calls.append(events)
        return events["payload"]["temperature"].cast("double") <= 90

    rule = ConditionRule(
        condition_id="condition.registry_overheat",
        consumes_event_type=["telemetry.observed"],
        subject_type="machine",
        parameters={"enter_when": enter_when, "exit_when": exit_when},
    )

    result = ConditionEngineRunner().execute_rules(events_df, [rule])
    event_types = {row.event_type for row in result.collect()}

    assert len(enter_calls) == 1
    assert len(exit_calls) == 1
    assert "payload" in enter_calls[0].columns
    assert "condition.registry_overheat.entered" in event_types
    assert "condition.registry_overheat.exited" in event_types


@pytest.mark.requires_spark
def test_execute_rules_raises_clearly_when_predicate_builder_returns_non_column(spark):
    from kindling_ext_temporal import ConditionEngineRunner, ConditionRule

    events_df = _predicate_events_df(spark)
    rule = ConditionRule(
        condition_id="condition.bad_builder",
        consumes_event_type=["telemetry.observed"],
        subject_type="machine",
        parameters={
            "enter_when": lambda events: True,  # not a Column
            "exit_when": lambda events: events["payload"]["temperature"].cast("double") <= 90,
        },
    )

    with pytest.raises(TypeError, match="expected a Column"):
        ConditionEngineRunner().execute_rules(events_df, [rule])


@pytest.mark.requires_spark
def test_execute_rules_runs_table_and_registry_rules_side_by_side(spark):
    from kindling_ext_temporal import ConditionEngineRunner, ConditionRule

    events_df = _predicate_events_df(spark)
    table_rule = ConditionRule(
        condition_id="condition.table_overheat",
        consumes_event_type=["telemetry.observed"],
        subject_type="machine",
        parameters={
            "enter_when": "cast(payload['temperature'] as double) > 90",
            "exit_when": "cast(payload['temperature'] as double) <= 90",
        },
    )
    registry_rule = ConditionRule(
        condition_id="condition.registry_overheat",
        consumes_event_type=["telemetry.observed"],
        subject_type="machine",
        parameters={
            "enter_when": lambda events: events["payload"]["temperature"].cast("double") > 90,
            "exit_when": lambda events: events["payload"]["temperature"].cast("double") <= 90,
        },
    )

    result = ConditionEngineRunner().execute_rules(events_df, [table_rule, registry_rule])
    event_types = {row.event_type for row in result.collect()}

    assert "condition.table_overheat.entered" in event_types
    assert "condition.table_overheat.exited" in event_types
    assert "condition.registry_overheat.entered" in event_types
    assert "condition.registry_overheat.exited" in event_types
