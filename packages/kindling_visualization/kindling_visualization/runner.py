"""Execution service for Kindling visualization declarations."""

from abc import ABC, abstractmethod
from typing import List, Optional

from injector import inject
from kindling.data_entities import DataEntityRegistry
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.spark_log_provider import PythonLoggerProvider

from .registry import VisualizationRegistry
from .renderer import VisualizationRenderer


class VisualizationRunner(ABC):
    """Abstract service for rendering registered visualizations."""

    @abstractmethod
    def render_visualization(self, viewid: str) -> str:
        pass

    @abstractmethod
    def render_visualizations(self, viewids: Optional[List[str]] = None) -> List[str]:
        pass


class VisualizationRunnerService(VisualizationRunner):
    """Render Kindling visualizations from registered entity declarations."""

    @inject
    def __init__(
        self,
        registry: VisualizationRegistry,
        entity_registry: DataEntityRegistry,
        provider_registry: EntityProviderRegistry,
        renderer: VisualizationRenderer,
        logger_provider: PythonLoggerProvider,
    ):
        self.registry = registry
        self.entity_registry = entity_registry
        self.provider_registry = provider_registry
        self.renderer = renderer
        self.logger = logger_provider.get_logger("VisualizationRunnerService")

    def render_visualization(self, viewid: str) -> str:
        visualization = self.registry.get_visualization_definition(viewid)
        if visualization is None:
            raise ValueError(f"Unknown visualization: '{viewid}'")

        entity = self.entity_registry.get_entity_definition(visualization.input_entity_id)
        if entity is None:
            raise ValueError(
                f"Visualization '{viewid}' references unknown entity: "
                f"'{visualization.input_entity_id}'"
            )

        provider = self.provider_registry.get_provider_for_entity(entity)
        df = provider.read_entity(entity)
        if visualization.transform:
            df = visualization.transform(df)

        self.logger.info(f"Rendering visualization: {viewid}")
        return self.renderer.render(df, visualization)

    def render_visualizations(self, viewids: Optional[List[str]] = None) -> List[str]:
        selected = viewids if viewids is not None else self.registry.get_visualization_ids()
        return [self.render_visualization(viewid) for viewid in selected]
