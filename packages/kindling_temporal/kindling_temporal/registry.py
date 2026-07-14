"""Declarative temporal primitive registration."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from injector import inject
from kindling.data_entities import DataEntityRegistry
from kindling.data_pipes import DataPipesRegistry
from kindling.injection import GlobalInjector
from kindling.spark_log_provider import PythonLoggerProvider

from .entities import TemporalEntityResolver
from .translation import TemporalPipeTranslator


@dataclass
class BaseEventMetadata:
    eventid: str
    input_entity_id: str
    output_entity_id: str
    subject_type: str
    subject_keys: List[str]
    time_column: str
    event_type: str
    name: Optional[str] = None
    pipeid: Optional[str] = None
    payload_columns: List[str] = field(default_factory=list)
    source_system: Optional[str] = None
    output_type: str = "delta"
    use_watermark: bool = False
    tags: Dict[str, str] = field(default_factory=dict)
    transform: Optional[Callable[[Any], Any]] = None


@dataclass
class ConditionEngineMetadata:
    engineid: str
    events_entity_id: str
    conditions_entity_id: str
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class EpisodeMetadata:
    episodeid: str
    output_entity_id: str
    events_entity_id: str
    start_event: str
    end_event: str
    subject_type: Optional[str] = None
    min_duration_seconds: Optional[int] = None
    max_duration_seconds: Optional[int] = None
    late_event_grace_seconds: Optional[int] = None
    expires_after_seconds: Optional[int] = None
    expiration_event: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


class TemporalEventRegistry(ABC):
    @abstractmethod
    def register_base_event(self, eventid: str, **decorator_params) -> None:
        pass

    @abstractmethod
    def register_condition_engine(self, engineid: str, **decorator_params) -> None:
        pass

    @abstractmethod
    def get_base_event_ids(self) -> List[str]:
        pass

    @abstractmethod
    def get_base_event_definition(self, eventid: str) -> Optional[BaseEventMetadata]:
        pass

    @abstractmethod
    def get_condition_engine_ids(self) -> List[str]:
        pass

    @abstractmethod
    def get_condition_engine_definition(self, engineid: str) -> Optional[ConditionEngineMetadata]:
        pass


class TemporalEpisodeRegistry(ABC):
    @abstractmethod
    def register_episode(self, episodeid: str, **decorator_params) -> None:
        pass

    @abstractmethod
    def get_episode_ids(self) -> List[str]:
        pass

    @abstractmethod
    def get_episode_definition(self, episodeid: str) -> Optional[EpisodeMetadata]:
        pass


class DataEvents:
    """Decorator namespace for temporal event primitives."""

    registry = None
    resolver = None
    data_entity_registry = None
    data_pipe_registry = None

    @classmethod
    def reset(cls) -> None:
        cls.registry = None
        cls.resolver = None
        cls.data_entity_registry = None
        cls.data_pipe_registry = None

    @classmethod
    def _registry(cls) -> TemporalEventRegistry:
        if cls.registry is None:
            cls.registry = GlobalInjector.get(TemporalEventRegistry)
        return cls.registry

    @classmethod
    def _resolver(cls) -> TemporalEntityResolver:
        if cls.resolver is None:
            cls.resolver = GlobalInjector.get(TemporalEntityResolver)
        return cls.resolver

    @classmethod
    def _data_entity_registry(cls) -> DataEntityRegistry:
        if cls.data_entity_registry is None:
            cls.data_entity_registry = GlobalInjector.get(DataEntityRegistry)
        return cls.data_entity_registry

    @classmethod
    def _data_pipe_registry(cls) -> DataPipesRegistry:
        if cls.data_pipe_registry is None:
            cls.data_pipe_registry = GlobalInjector.get(DataPipesRegistry)
        return cls.data_pipe_registry

    @classmethod
    def base_event(cls, **decorator_params):
        """Register a base event normalizer.

        The output entity is always the resolver's canonical events entity, and
        the declaration lowers to a normal Kindling pipe.
        """

        def decorator(func):
            params = dict(decorator_params)
            eventid = params.pop("eventid")
            params["transform"] = func
            events_entity = cls._resolver().get_events_entity()
            params["output_entity_id"] = events_entity.entityid
            params.setdefault("output_type", events_entity.tags.get("provider_type", "delta"))
            cls._registry().register_base_event(eventid, **params)
            metadata = cls._registry().get_base_event_definition(eventid)
            TemporalPipeTranslator.register_base_event(
                metadata,
                cls._data_pipe_registry(),
                entity_registry=cls._data_entity_registry(),
                output_entity=events_entity,
            )
            return func

        return decorator

    @classmethod
    def condition_engine(cls, *, engineid: str, tags: Optional[Dict[str, str]] = None):
        """Register the generic rules-as-data condition engine."""
        events_entity = cls._resolver().get_events_entity()
        conditions_entity = cls._resolver().get_conditions_entity()
        entity_registry = cls._data_entity_registry()
        TemporalPipeTranslator.ensure_entity(entity_registry, events_entity)
        TemporalPipeTranslator.ensure_entity(entity_registry, conditions_entity)
        cls._registry().register_condition_engine(
            engineid,
            events_entity_id=events_entity.entityid,
            conditions_entity_id=conditions_entity.entityid,
            tags=tags or {},
        )


class DataEpisodes:
    """Decorator namespace for temporal episode primitives."""

    registry = None
    resolver = None
    data_entity_registry = None

    @classmethod
    def reset(cls) -> None:
        cls.registry = None
        cls.resolver = None
        cls.data_entity_registry = None

    @classmethod
    def _registry(cls) -> TemporalEpisodeRegistry:
        if cls.registry is None:
            cls.registry = GlobalInjector.get(TemporalEpisodeRegistry)
        return cls.registry

    @classmethod
    def _resolver(cls) -> TemporalEntityResolver:
        if cls.resolver is None:
            cls.resolver = GlobalInjector.get(TemporalEntityResolver)
        return cls.resolver

    @classmethod
    def _data_entity_registry(cls) -> DataEntityRegistry:
        if cls.data_entity_registry is None:
            cls.data_entity_registry = GlobalInjector.get(DataEntityRegistry)
        return cls.data_entity_registry

    @classmethod
    def episode(cls, **decorator_params):
        """Register an event-pair episode definition."""
        params = dict(decorator_params)
        episodeid = params.pop("episodeid")
        episodes_entity = cls._resolver().get_episodes_entity()
        events_entity = cls._resolver().get_events_entity()
        entity_registry = cls._data_entity_registry()
        TemporalPipeTranslator.ensure_entity(entity_registry, episodes_entity)
        TemporalPipeTranslator.ensure_entity(entity_registry, events_entity)
        params["output_entity_id"] = episodes_entity.entityid
        params["events_entity_id"] = events_entity.entityid
        cls._registry().register_episode(episodeid, **params)


class TemporalEventRegistryManager(TemporalEventRegistry):
    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.base_events: Dict[str, BaseEventMetadata] = {}
        self.condition_engines: Dict[str, ConditionEngineMetadata] = {}
        self.logger = logger_provider.get_logger("TemporalEventRegistryManager")

    def register_base_event(self, eventid: str, **decorator_params) -> None:
        self.base_events[eventid] = BaseEventMetadata(eventid=eventid, **decorator_params)
        self.logger.debug(f"Temporal base event registered: {eventid}")

    def register_condition_engine(self, engineid: str, **decorator_params) -> None:
        self.condition_engines[engineid] = ConditionEngineMetadata(
            engineid=engineid, **decorator_params
        )
        self.logger.debug(f"Temporal condition engine registered: {engineid}")

    def get_base_event_ids(self) -> List[str]:
        return list(self.base_events.keys())

    def get_base_event_definition(self, eventid: str) -> Optional[BaseEventMetadata]:
        return self.base_events.get(eventid)

    def get_condition_engine_ids(self) -> List[str]:
        return list(self.condition_engines.keys())

    def get_condition_engine_definition(self, engineid: str) -> Optional[ConditionEngineMetadata]:
        return self.condition_engines.get(engineid)


class TemporalEpisodeRegistryManager(TemporalEpisodeRegistry):
    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.episodes: Dict[str, EpisodeMetadata] = {}
        self.logger = logger_provider.get_logger("TemporalEpisodeRegistryManager")

    def register_episode(self, episodeid: str, **decorator_params) -> None:
        self.episodes[episodeid] = EpisodeMetadata(episodeid=episodeid, **decorator_params)
        self.logger.debug(f"Temporal episode registered: {episodeid}")

    def get_episode_ids(self) -> List[str]:
        return list(self.episodes.keys())

    def get_episode_definition(self, episodeid: str) -> Optional[EpisodeMetadata]:
        return self.episodes.get(episodeid)
