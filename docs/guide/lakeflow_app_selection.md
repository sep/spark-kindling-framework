# Lakeflow data-app selection

Kindling's Databricks extension can be used as the declaration entrypoint for
a generic Lakeflow Declarative Pipeline. The pipeline configuration selects a
data app by name:

```hcl
dlt_pipelines = [
  {
    name          = "kindling-orders-lakeflow"
    target_schema = "orders"
    file_glob     = "/Repos/my-org/kindling_lakeflow_pipeline.py"
    development   = true
    continuous    = false
    configuration = {
      "kindling.data_app"              = "orders"
      "kindling.lakeflow.allowed_apps" = "orders,customers"
    }
  }
]
```

The generic source file is:

```python
from kindling_ext_databricks.lakeflow_app_selector import declare_from_pipeline_config

declare_from_pipeline_config()
```

An app distribution advertises a declaration module through the
`spark_kindling.data_apps` entry-point group. Its value names a module with a
callable `register_all()`:

```toml
[tool.poetry.plugins."spark_kindling.data_apps"]
orders = "orders_kindling_app"
```

The selector performs this sequence during Lakeflow source evaluation:

1. Read `kindling.data_app` from Spark configuration.
2. Discover the app entry points and authorize the selected app using the
   comma-separated `kindling.lakeflow.allowed_apps` value. An absent or empty
   allowlist authorizes every discovered app.
3. Bridge `kindling.*` and `datapipes.*` pipeline settings into Kindling's
   configuration service and initialize with `engine="databricks_sdp"`.
4. Import the selected declaration module and call `register_all()`.
5. Call `kindling.declare_pipeline()` last; Lakeflow then owns graph
   orchestration.

`register_all()` must be declaration-only. It must not run jobs, write data,
install dependencies, or invoke `DataAppManager.run_app()`. App code and
dependencies must already be supplied through the pipeline's libraries,
wheel, `root_path`, or environment configuration.

This is a templating mechanism with one pipeline per app. Do not repoint an
existing pipeline ID at another app: streaming tables and checkpoints from the
previous graph can be orphaned, and target tables can collide. Use a new
pipeline ID, or perform an explicitly planned full refresh with target cleanup.
Changing a selected app also requires care around pipeline state, checkpoints,
target-table identity, and concurrent updates.
