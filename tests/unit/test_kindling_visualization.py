import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "packages/kindling_visualization"))

from kindling_visualization.registry import (
    VisualizationMetadata,
    VisualizationRegistryManager,
    Visualizations,
)
from kindling_visualization.renderer import MatplotlibVisualizationRenderer
from kindling_visualization.runner import VisualizationRunnerService


class FakeSparkDataFrame:
    def __init__(self, pandas_frame):
        self.pandas_frame = pandas_frame
        self.limit_value = None

    def limit(self, value):
        self.limit_value = value
        return self

    def toPandas(self):
        return self.pandas_frame


def logger_provider():
    provider = MagicMock()
    provider.get_logger.return_value = MagicMock()
    return provider


def visualization(**overrides):
    defaults = {
        "viewid": "ops.example",
        "name": "Example",
        "input_entity_id": "gold.example",
        "kind": "line",
        "x": "day",
        "y": "count",
        "output_path": "unused.png",
    }
    defaults.update(overrides)
    return VisualizationMetadata(**defaults)


def test_registry_registers_visualization():
    registry = VisualizationRegistryManager(logger_provider())

    registry.register_visualization(
        "ops.example",
        name="Example",
        input_entity_id="gold.example",
        kind="bar",
        x="day",
        y="count",
        output_path="example.png",
    )

    assert registry.get_visualization_ids() == ["ops.example"]
    assert registry.get_visualization_definition("ops.example").kind == "bar"


def test_visualization_decorator_can_be_reused_without_mutating_parameters():
    registry = VisualizationRegistryManager(logger_provider())
    original_registry = Visualizations.registry
    Visualizations.registry = registry

    try:
        decorator = Visualizations.figure(
            viewid="ops.example",
            name="Example",
            input_entity_id="gold.example",
            kind="line",
            x="day",
            y="count",
            output_path="example.png",
        )

        @decorator
        def first(df):
            return df

        @decorator
        def second(df):
            return df

        assert registry.get_visualization_definition("ops.example").transform is second
    finally:
        Visualizations.registry = original_registry


def test_renderer_enforces_max_rows():
    import pandas as pd

    renderer = MatplotlibVisualizationRenderer(logger_provider())
    df = FakeSparkDataFrame(pd.DataFrame({"day": [1, 2, 3], "count": [4, 5, 6]}))

    with pytest.raises(ValueError, match="exceeded max_rows=2"):
        renderer.render(df, visualization(max_rows=2))

    assert df.limit_value == 3


def test_renderer_writes_chart(tmp_path):
    pytest.importorskip("matplotlib")
    import pandas as pd

    output_path = tmp_path / "chart.png"
    renderer = MatplotlibVisualizationRenderer(logger_provider())
    df = FakeSparkDataFrame(pd.DataFrame({"day": [1, 2], "count": [4, 5]}))

    result = renderer.render(df, visualization(output_path=str(output_path)))

    assert result == str(output_path)
    assert Path(result).exists()


def test_runner_reads_entity_and_renders():
    registry = MagicMock()
    entity_registry = MagicMock()
    provider_registry = MagicMock()
    renderer = MagicMock()
    entity = MagicMock()
    provider = MagicMock()
    df = MagicMock()
    transformed = MagicMock()

    spec = visualization(transform=lambda source_df: transformed)
    registry.get_visualization_definition.return_value = spec
    entity_registry.get_entity_definition.return_value = entity
    provider_registry.get_provider_for_entity.return_value = provider
    provider.read_entity.return_value = df
    renderer.render.return_value = "chart.png"

    runner = VisualizationRunnerService(
        registry,
        entity_registry,
        provider_registry,
        renderer,
        logger_provider(),
    )

    assert runner.render_visualization("ops.example") == "chart.png"
    entity_registry.get_entity_definition.assert_called_once_with("gold.example")
    provider_registry.get_provider_for_entity.assert_called_once_with(entity)
    provider.read_entity.assert_called_once_with(entity)
    renderer.render.assert_called_once_with(transformed, spec)
