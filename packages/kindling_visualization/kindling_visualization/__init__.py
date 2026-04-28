"""Matplotlib visualization extension for Kindling."""

from .registry import (
    VisualizationMetadata,
    VisualizationRegistry,
    VisualizationRegistryManager,
    Visualizations,
)
from .renderer import MatplotlibVisualizationRenderer, VisualizationRenderer
from .runner import VisualizationRunner, VisualizationRunnerService

__all__ = [
    "MatplotlibVisualizationRenderer",
    "VisualizationMetadata",
    "VisualizationRegistry",
    "VisualizationRegistryManager",
    "VisualizationRenderer",
    "VisualizationRunner",
    "VisualizationRunnerService",
    "Visualizations",
]

__version__ = "0.1.0"


def _register_services():
    """Register visualization services with the Kindling DI container."""
    from injector import singleton
    from kindling.injection import GlobalInjector

    injector = GlobalInjector.get_injector()
    injector.binder.bind(
        VisualizationRegistry,
        to=singleton(VisualizationRegistryManager),
        scope=singleton,
    )
    injector.binder.bind(
        VisualizationRegistryManager,
        to=singleton(VisualizationRegistryManager),
        scope=singleton,
    )
    injector.binder.bind(
        VisualizationRenderer,
        to=singleton(MatplotlibVisualizationRenderer),
        scope=singleton,
    )
    injector.binder.bind(
        MatplotlibVisualizationRenderer,
        to=singleton(MatplotlibVisualizationRenderer),
        scope=singleton,
    )
    injector.binder.bind(
        VisualizationRunner,
        to=singleton(VisualizationRunnerService),
        scope=singleton,
    )
    injector.binder.bind(
        VisualizationRunnerService,
        to=singleton(VisualizationRunnerService),
        scope=singleton,
    )


_register_services()
