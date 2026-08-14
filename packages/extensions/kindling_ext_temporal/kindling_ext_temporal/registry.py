"""Declarative temporal primitive registration."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from injector import inject
from kindling.data_entities import DataEntityRegistry
from kindling.data_pipes import DataPipesRegistry
from kindling.injection import GlobalInjector
from kindling.spark_log_provider import PythonLoggerProvider

from .entities import TemporalEntityResolver
from .translation import TemporalPipeTranslator
from .validation import (
    ConditionRule,
    ConditionValidationError,
    TemporalConditionValidator,
)

if TYPE_CHECKING:
    # Deferred: the rest of kindling_ext_temporal keeps PySpark imports out of
    # module scope so importing the extension in a non-Spark context doesn't
    # hard-require pyspark. DataFrame/Column are only ever used here as
    # annotations on ``DataConditions.register``'s callables (quoted below),
    # never evaluated at runtime.
    from pyspark.sql import Column, DataFrame


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
    condition_source: str = "table"
    conditions_entity_id: Optional[str] = None
    conditions_current_entity_id: Optional[str] = None
    name: Optional[str] = None
    pipeid: Optional[str] = None
    output_type: str = "delta"
    use_watermark: bool = True
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class EpisodeMetadata:
    episodeid: str
    output_entity_id: str
    events_entity_id: str
    start_event: str
    end_event: str
    condition_id: Optional[str] = None
    determination_event: Optional[str] = None
    invalidation_event: Optional[str] = None
    name: Optional[str] = None
    pipeid: Optional[str] = None
    determination_pipeid: Optional[str] = None
    output_type: str = "delta"
    use_watermark: bool = True
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


class TemporalConditionRegistry(ABC):
    """Holds registry-declared Conditions (see ``DataConditions.register``).

    Deliberately stricter than ``TemporalEventRegistry``/
    ``TemporalEpisodeRegistry`` above: those two silently overwrite on a
    duplicate id and do no required-field checking beyond the dataclass
    constructor. Registry conditions are code, not data, so they are
    expected to fail fast -- duplicate ids and missing fields raise instead
    of overwriting or deferring to execution time.
    """

    @abstractmethod
    def register_condition(self, rule: ConditionRule) -> None:
        pass

    @abstractmethod
    def get_condition_ids(self) -> List[str]:
        pass

    @abstractmethod
    def get_condition_definition(self, condition_id: str) -> Optional[ConditionRule]:
        pass

    @abstractmethod
    def get_all_conditions(self) -> List[ConditionRule]:
        pass

    @abstractmethod
    def reset(self) -> None:
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
            events_entity_tags = events_entity.tags or {}
            params.setdefault("output_type", events_entity_tags.get("provider_type", "delta"))
            params["tags"] = params.get("tags") or {}
            cls._registry().register_base_event(eventid, **params)
            metadata = _require_metadata(
                cls._registry().get_base_event_definition(eventid),
                "Temporal base event",
                eventid,
            )
            TemporalPipeTranslator.register_base_event(
                metadata,
                cls._data_pipe_registry(),
                entity_registry=cls._data_entity_registry(),
                output_entity=events_entity,
            )
            _ensure_autocollapse_connected()
            return func

        return decorator

    @classmethod
    def condition_engine(
        cls,
        *,
        engineid: str,
        tags: Optional[Dict[str, str]] = None,
        condition_source: str = "table",
    ):
        """Register the generic rules-as-data condition engine.

        ``condition_source`` selects where this engine's rules come from --
        an engine has exactly one source, never a silent union of both:

        - ``"table"`` (default): unchanged from the original behavior. Rules
          are read from the configured current conditions entity, which is
          resolved/ensured and wired as an input alongside the events
          entity.
        - ``"registry"``: rules are read from the in-process condition
          registry (see ``DataConditions.register``) instead. No conditions
          entity is resolved, ensured, or wired -- zero table/entity
          involvement. Because this is the moment an engine commits to
          consuming the registry, the registry's *current* contents are
          checked for event-type graph cycles right now rather than
          deferred to chain/pipe execution; a condition registered after
          this call is a known, accepted gap (not revalidated later).
        """
        if condition_source not in ("table", "registry"):
            raise ValueError(
                f"Temporal condition engine '{engineid}': condition_source must be "
                f"'table' or 'registry', got {condition_source!r}"
            )

        events_entity = cls._resolver().get_events_entity()
        entity_registry = cls._data_entity_registry()
        TemporalPipeTranslator.ensure_entity(entity_registry, events_entity)

        conditions_entity = None
        conditions_entity_id = None
        conditions_current_entity_id = None
        if condition_source == "table":
            conditions_entity = cls._resolver().get_conditions_entity()
            conditions_entity_id = conditions_entity.entityid
            conditions_current_entity_id = cls._resolver().get_conditions_current_entity_id()
            TemporalPipeTranslator.ensure_entity(entity_registry, conditions_entity)
        else:
            registry_rules = GlobalInjector.get(TemporalConditionRegistry).get_all_conditions()
            if registry_rules:
                validator = TemporalConditionValidator()
                graph = validator.build_event_type_graph(registry_rules)
                cycles = validator.graph_builder.detect_cycles(graph)
                if cycles:
                    raise ConditionValidationError(
                        f"Conditions set is not ingestible:\n{cycles[0]}"
                    )

        cls._registry().register_condition_engine(
            engineid,
            events_entity_id=events_entity.entityid,
            condition_source=condition_source,
            conditions_entity_id=conditions_entity_id,
            conditions_current_entity_id=conditions_current_entity_id,
            tags=tags or {},
        )
        metadata = _require_metadata(
            cls._registry().get_condition_engine_definition(engineid),
            "Temporal condition engine",
            engineid,
        )
        TemporalPipeTranslator.register_condition_engine(
            metadata,
            cls._data_pipe_registry(),
            entity_registry=entity_registry,
            events_entity=events_entity,
            conditions_entity=conditions_entity,
        )
        _ensure_autocollapse_connected()


class DataConditions:
    """Declaration namespace for registry-declared temporal Conditions.

    Unlike ``DataEvents``/``DataEpisodes``, registering a condition here does
    not lower to a Kindling pipe by itself -- it only ever populates the
    in-process condition registry. A condition only takes effect once a
    ``DataEvents.condition_engine(condition_source="registry")`` reads the
    registry's current contents (see registry.py's ``condition_engine``).
    """

    @classmethod
    def reset(cls) -> None:
        cls._registry().reset()

    @classmethod
    def _registry(cls) -> TemporalConditionRegistry:
        return GlobalInjector.get(TemporalConditionRegistry)

    @classmethod
    def register(
        cls,
        *,
        condition_id: str,
        consumes_event_type: List[str],
        subject_type: str,
        enter_when: Callable[["DataFrame"], "Column"],
        exit_when: Callable[["DataFrame"], "Column"],
        enabled: bool = True,
        valid_from: Optional[Any] = None,
        valid_to: Optional[Any] = None,
    ) -> None:
        """Register a static, application-owned condition.

        ``enter_when``/``exit_when`` are predicate builders shaped
        ``Callable[[DataFrame], Column]``. The registry stores the callable
        itself, not a DataFrame-bound ``Column`` -- the builder is invoked
        with the scoped events DataFrame at condition-engine execution time
        (see ``ConditionEngineRunner``), so a registration never captures a
        Spark session or a particular execution plan.

        Required metadata and a duplicate ``condition_id`` both raise
        ``ConditionValidationError`` immediately: registry conditions are
        code, not data, and are expected to fail fast rather than be
        quarantined like a malformed table row.
        """
        if not condition_id or not condition_id.strip():
            raise ConditionValidationError("condition_id is required")
        if not subject_type or not subject_type.strip():
            raise ConditionValidationError("subject_type is required")
        if not consumes_event_type:
            raise ConditionValidationError(
                "consumes_event_type must contain at least one event type"
            )
        if not callable(enter_when):
            raise ConditionValidationError("enter_when must be callable")
        if not callable(exit_when):
            raise ConditionValidationError("exit_when must be callable")

        rule = ConditionRule(
            condition_id=condition_id,
            consumes_event_type=list(consumes_event_type),
            subject_type=subject_type,
            parameters={"enter_when": enter_when, "exit_when": exit_when},
            enabled=enabled,
            valid_from=valid_from,
            valid_to=valid_to,
        )
        cls._registry().register_condition(rule)


class DataEpisodes:
    """Decorator namespace for temporal episode primitives."""

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
    def _data_pipe_registry(cls) -> DataPipesRegistry:
        if cls.data_pipe_registry is None:
            cls.data_pipe_registry = GlobalInjector.get(DataPipesRegistry)
        return cls.data_pipe_registry

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
        params.setdefault("condition_id", _infer_condition_id(params["start_event"]))
        params.setdefault("determination_event", f"{episodeid}.closed")
        params.setdefault("expiration_event", f"{episodeid}.expired")
        params.setdefault("invalidation_event", f"{episodeid}.invalidated")
        params["tags"] = params.get("tags") or {}
        cls._registry().register_episode(episodeid, **params)
        metadata = _require_metadata(
            cls._registry().get_episode_definition(episodeid),
            "Temporal episode",
            episodeid,
        )
        TemporalPipeTranslator.register_episode(
            metadata,
            cls._data_pipe_registry(),
            entity_registry=entity_registry,
            output_entity=episodes_entity,
            events_entity=events_entity,
        )
        TemporalPipeTranslator.register_episode_determination_event(
            metadata,
            cls._data_pipe_registry(),
            entity_registry=entity_registry,
            events_entity=events_entity,
        )
        _ensure_autocollapse_connected()


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


class TemporalConditionRegistryManager(TemporalConditionRegistry):
    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("TemporalConditionRegistryManager")
        self._conditions: Dict[str, ConditionRule] = {}

    def register_condition(self, rule: ConditionRule) -> None:
        if rule.condition_id in self._conditions:
            raise ConditionValidationError(
                f"Duplicate condition_id '{rule.condition_id}' already registered"
            )
        self._conditions[rule.condition_id] = rule
        self.logger.debug(f"Temporal condition registered: {rule.condition_id}")

    def get_condition_ids(self) -> List[str]:
        return list(self._conditions.keys())

    def get_condition_definition(self, condition_id: str) -> Optional[ConditionRule]:
        return self._conditions.get(condition_id)

    def get_all_conditions(self) -> List[ConditionRule]:
        return list(self._conditions.values())

    def reset(self) -> None:
        self._conditions.clear()


def _ensure_autocollapse_connected() -> None:
    """Wire the ``collapse_temporal_chain`` autocollapse hook. Idempotent.

    Deferred import: chain.py imports this module at top level, so this
    module must reach chain.py lazily (call time, not import time) to avoid
    a circular top-level import.
    """
    from .chain import ensure_autocollapse_connected

    ensure_autocollapse_connected()


def _infer_condition_id(start_event: str) -> str:
    if start_event.endswith(".entered"):
        return start_event[: -len(".entered")]
    return start_event


def _require_metadata(metadata: Optional[Any], primitive: str, primitive_id: str) -> Any:
    if metadata is None:
        raise ValueError(f"{primitive} '{primitive_id}' was not available after registration")
    return metadata
