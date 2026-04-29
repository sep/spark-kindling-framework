"""Declarative visualization registration for Kindling apps."""

from abc import ABC, abstractmethod
from dataclasses import MISSING, dataclass, field, fields
from typing import Any, Callable, Dict, List, Optional

from injector import inject
from kindling.injection import GlobalInjector
from kindling.spark_log_provider import PythonLoggerProvider


@dataclass
class VisualizationMetadata:
    """Metadata needed to render a visualization from a Kindling entity."""

    viewid: str
    name: str
    input_entity_id: str
    kind: str
    output_path: str
    x: Optional[str] = None
    y: Optional[str] = None
    title: Optional[str] = None
    group_by: Optional[str] = None
    max_rows: int = 10000
    format: str = "png"
    tags: Dict[str, str] = field(default_factory=dict)
    options: Dict[str, Any] = field(default_factory=dict)
    transform: Optional[Callable[[Any], Any]] = None


class VisualizationRegistry(ABC):
    """Abstract registry for visualization declarations."""

    @abstractmethod
    def register_visualization(self, viewid: str, **decorator_params):
        pass

    @abstractmethod
    def get_visualization_ids(self) -> List[str]:
        pass

    @abstractmethod
    def get_visualization_definition(self, viewid: str) -> Optional[VisualizationMetadata]:
        pass


class Visualizations:
    """Decorator namespace for registering Kindling visualizations."""

    registry = None

    @classmethod
    def figure(cls, **decorator_params):
        """Register a matplotlib-backed visualization declaration."""

        def decorator(func):
            params = dict(decorator_params)
            if cls.registry is None:
                cls.registry = GlobalInjector.get(VisualizationRegistry)

            params["transform"] = func
            required_fields = {
                field_info.name
                for field_info in fields(VisualizationMetadata)
                if field_info.default is MISSING and field_info.default_factory is MISSING
            }
            missing_fields = required_fields - params.keys()
            if missing_fields:
                raise ValueError(
                    "Missing required fields in visualization decorator: "
                    + ", ".join(sorted(missing_fields))
                )

            viewid = params.pop("viewid")
            cls.registry.register_visualization(viewid, **params)
            return func

        return decorator


class VisualizationRegistryManager(VisualizationRegistry):
    """In-memory visualization registry managed by Kindling DI."""

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.registry: Dict[str, VisualizationMetadata] = {}
        self.logger = logger_provider.get_logger("VisualizationRegistryManager")

    def register_visualization(self, viewid: str, **decorator_params):
        self.registry[viewid] = VisualizationMetadata(viewid=viewid, **decorator_params)
        self.logger.debug(f"Visualization registered: {viewid}")

    def get_visualization_ids(self) -> List[str]:
        return list(self.registry.keys())

    def get_visualization_definition(self, viewid: str) -> Optional[VisualizationMetadata]:
        return self.registry.get(viewid)
