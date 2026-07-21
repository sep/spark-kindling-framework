"""Temporal event, condition, and episode primitives for Kindling."""

from .chain import (
    MAX_GENERATIONS_CONFIG_KEY,
    chain_episodes_pipe_id,
    chain_events_pipe_id,
    declare_temporal_chain,
)
from .conditions import (
    QUARANTINE_ENTITY_CONFIG_KEY,
    ConditionsIngestionResult,
    ingest_conditions,
    validated_conditions_transform,
)
from .engine import ConditionEngineRunner, EpisodeRunner
from .entities import (
    SimpleTemporalEntityResolver,
    TemporalEntityResolver,
    condition_quarantine_schema,
    conditions_entity_schema,
    conditions_schema,
    episodes_schema,
    events_schema,
)
from .registry import (
    BaseEventMetadata,
    ConditionEngineMetadata,
    DataEpisodes,
    DataEvents,
    EpisodeMetadata,
    TemporalEpisodeRegistry,
    TemporalEpisodeRegistryManager,
    TemporalEventRegistry,
    TemporalEventRegistryManager,
)
from .translation import TemporalPipeTranslator
from .validation import (
    ActiveSparkSqlExpressionParser,
    ConditionRule,
    ConditionValidationError,
    ConditionValidationReport,
    InvalidCondition,
    TemporalConditionValidator,
)

__all__ = [
    "ActiveSparkSqlExpressionParser",
    "BaseEventMetadata",
    "MAX_GENERATIONS_CONFIG_KEY",
    "chain_episodes_pipe_id",
    "chain_events_pipe_id",
    "conditions_entity_schema",
    "declare_temporal_chain",
    "ConditionEngineRunner",
    "ConditionEngineMetadata",
    "ConditionRule",
    "ConditionValidationError",
    "ConditionValidationReport",
    "ConditionsIngestionResult",
    "QUARANTINE_ENTITY_CONFIG_KEY",
    "DataEpisodes",
    "DataEvents",
    "EpisodeMetadata",
    "EpisodeRunner",
    "InvalidCondition",
    "SimpleTemporalEntityResolver",
    "TemporalConditionValidator",
    "TemporalEntityResolver",
    "TemporalEpisodeRegistry",
    "TemporalEpisodeRegistryManager",
    "TemporalEventRegistry",
    "TemporalEventRegistryManager",
    "TemporalPipeTranslator",
    "condition_quarantine_schema",
    "conditions_schema",
    "episodes_schema",
    "events_schema",
    "ingest_conditions",
    "validated_conditions_transform",
]

__version__ = "0.1.0"


def _register_services():
    """Register extension services with Kindling's DI container."""
    from injector import singleton
    from kindling.injection import GlobalInjector

    injector = GlobalInjector.get_injector()
    injector.binder.bind(TemporalEventRegistry, to=TemporalEventRegistryManager, scope=singleton)
    injector.binder.bind(
        TemporalEpisodeRegistry, to=TemporalEpisodeRegistryManager, scope=singleton
    )


_register_services()
