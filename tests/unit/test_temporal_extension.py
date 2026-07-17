from pathlib import Path
from unittest.mock import MagicMock, patch

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
    entity_registry=None,
    pipe_registry=None,
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
        if dep is TemporalEventRegistry and event_registry is not None:
            return event_registry
        if dep is TemporalEpisodeRegistry and episode_registry is not None:
            return episode_registry
        if dep is DataEntityRegistry and entity_registry is not None:
            return entity_registry
        if dep is DataPipesRegistry and pipe_registry is not None:
            return pipe_registry
        raise AssertionError(f"Unexpected service request: {dep}")

    return _get


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
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager

    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager

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
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager

    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager

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
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager

    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager

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
    from kindling_ext_temporal import DataEpisodes, TemporalEpisodeRegistryManager

    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager

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
            expiration_event="episode.machine_cycle.expired",
        )

    metadata = registry.get_episode_definition("episode.machine_cycle")
    assert metadata.output_entity_id == "silver.episodes"
    assert metadata.events_entity_id == "silver.events"
    assert metadata.start_event == "condition.machine_running.entered"
    assert metadata.end_event == "condition.machine_running.exited"
    assert metadata.condition_id == "condition.machine_running"
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
    assert callable(pipe.execute)


def test_translator_handles_none_tags_on_temporal_metadata_and_entities():
    from kindling_ext_temporal import (
        BaseEventMetadata,
        ConditionEngineMetadata,
        EpisodeMetadata,
        TemporalPipeTranslator,
        events_schema,
    )

    from kindling.data_entities import DataEntityManager, EntityMetadata

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
