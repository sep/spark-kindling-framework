# Kindling Visualization

Matplotlib visualization extension for the Kindling Spark framework.

## Overview

`kindling-visualization` adds declarative chart rendering for Kindling data apps.
Visualizations are registered like Kindling entities and pipes, then rendered from
Kindling entities through the existing entity provider registry.

## Installation

```bash
pip install kindling-visualization
```

In a Kindling app, add the extension to your settings:

```yaml
kindling:
  extensions:
    - kindling-visualization>=0.1.0
```

## Usage

```python
from kindling.injection import get_kindling_service
from kindling_visualization import VisualizationRunner, Visualizations


@Visualizations.figure(
    viewid="ops.order_volume_daily",
    name="Daily order volume",
    input_entity_id="gold.order_daily_metrics",
    kind="line",
    x="order_date",
    y="order_count",
    title="Daily Order Volume",
    output_path="artifacts/visualizations/order_volume_daily.png",
    max_rows=5000,
)
def order_volume_daily(df):
    return df.orderBy("order_date")


runner = get_kindling_service(VisualizationRunner)
runner.render_visualization("ops.order_volume_daily")
```

Supported chart kinds:

- `line`
- `bar`
- `scatter`
- `hist`

## Safety

Rendering collects data into pandas before handing it to matplotlib. Every
visualization has a `max_rows` guard, defaulting to `10000`. Use transforms to
aggregate or filter large Spark DataFrames before rendering.
