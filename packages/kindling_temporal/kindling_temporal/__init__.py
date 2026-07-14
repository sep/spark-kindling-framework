"""Temporal event, condition, and episode primitives for Kindling."""

from .entities import (
    SimpleTemporalEntityResolver,
    TemporalEntityResolver,
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
    "ConditionEngineMetadata",
    "ConditionRule",
    "ConditionValidationError",
    "ConditionValidationReport",
    "DataEpisodes",
    "DataEvents",
    "EpisodeMetadata",
    "InvalidCondition",
    "SimpleTemporalEntityResolver",
    "TemporalConditionValidator",
    "TemporalEntityResolver",
    "TemporalEpisodeRegistry",
    "TemporalEpisodeRegistryManager",
    "TemporalEventRegistry",
    "TemporalEventRegistryManager",
    "TemporalPipeTranslator",
    "conditions_schema",
    "episodes_schema",
    "events_schema",
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
