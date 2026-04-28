"""Matplotlib rendering backend for Kindling visualization declarations."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from injector import inject
from kindling.spark_log_provider import PythonLoggerProvider

from .registry import VisualizationMetadata


class VisualizationRenderer(ABC):
    """Abstract visualization renderer."""

    @abstractmethod
    def render(self, df: Any, visualization: VisualizationMetadata) -> str:
        pass


class MatplotlibVisualizationRenderer(VisualizationRenderer):
    """Render small Spark DataFrames to matplotlib artifacts."""

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("MatplotlibVisualizationRenderer")

    def render(self, df: Any, visualization: VisualizationMetadata) -> str:
        pdf = self._to_pandas(df, visualization)
        return self.render_pandas(pdf, visualization)

    def render_pandas(self, pdf: Any, visualization: VisualizationMetadata) -> str:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        output_path = Path(visualization.output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        fig, ax = plt.subplots(
            figsize=visualization.options.get("figsize", (10, 6)),
            dpi=visualization.options.get("dpi", 120),
        )

        kind = visualization.kind.lower()
        if kind == "line":
            self._plot_line(ax, pdf, visualization)
        elif kind == "bar":
            self._plot_bar(ax, pdf, visualization)
        elif kind == "scatter":
            self._plot_scatter(ax, pdf, visualization)
        elif kind == "hist":
            self._plot_hist(ax, pdf, visualization)
        else:
            raise ValueError(
                f"Unsupported visualization kind '{visualization.kind}'. "
                "Supported kinds: line, bar, scatter, hist."
            )

        if visualization.title:
            ax.set_title(visualization.title)
        if visualization.x:
            ax.set_xlabel(visualization.x)
        if visualization.y:
            ax.set_ylabel(visualization.y)
        if visualization.group_by:
            ax.legend()

        fig.tight_layout()
        fig.savefig(output_path, format=visualization.format)
        plt.close(fig)
        self.logger.info(f"Rendered visualization '{visualization.viewid}' to {output_path}")
        return str(output_path)

    def _to_pandas(self, df: Any, visualization: VisualizationMetadata):
        bounded = df.limit(visualization.max_rows + 1)
        pdf = bounded.toPandas()
        if len(pdf.index) > visualization.max_rows:
            raise ValueError(
                f"Visualization '{visualization.viewid}' exceeded max_rows="
                f"{visualization.max_rows}. Add aggregation/filtering before rendering."
            )
        return pdf

    def _plot_line(self, ax, pdf: Any, visualization: VisualizationMetadata) -> None:
        self._require_columns(visualization, x=True, y=True)
        if visualization.group_by:
            for label, group in pdf.groupby(visualization.group_by):
                ax.plot(group[visualization.x], group[visualization.y], label=str(label))
        else:
            ax.plot(pdf[visualization.x], pdf[visualization.y])

    def _plot_bar(self, ax, pdf: Any, visualization: VisualizationMetadata) -> None:
        self._require_columns(visualization, x=True, y=True)
        if visualization.group_by:
            pivoted = pdf.pivot_table(
                index=visualization.x,
                columns=visualization.group_by,
                values=visualization.y,
                aggfunc="sum",
            )
            pivoted.plot(kind="bar", ax=ax)
        else:
            ax.bar(pdf[visualization.x], pdf[visualization.y])

    def _plot_scatter(self, ax, pdf: Any, visualization: VisualizationMetadata) -> None:
        self._require_columns(visualization, x=True, y=True)
        if visualization.group_by:
            for label, group in pdf.groupby(visualization.group_by):
                ax.scatter(group[visualization.x], group[visualization.y], label=str(label))
        else:
            ax.scatter(pdf[visualization.x], pdf[visualization.y])

    def _plot_hist(self, ax, pdf: Any, visualization: VisualizationMetadata) -> None:
        y_column = visualization.y or visualization.x
        if not y_column:
            raise ValueError("Histogram visualizations require either 'x' or 'y'.")
        bins = visualization.options.get("bins", 20)
        ax.hist(pdf[y_column].dropna(), bins=bins)

    def _require_columns(
        self, visualization: VisualizationMetadata, x: bool = False, y: bool = False
    ) -> None:
        missing = []
        if x and not visualization.x:
            missing.append("x")
        if y and not visualization.y:
            missing.append("y")
        if missing:
            raise ValueError(
                f"Visualization '{visualization.viewid}' requires: {', '.join(missing)}"
            )
