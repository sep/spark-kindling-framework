# lakeflow-telemetry example

A minimal Kindling project layout for Databricks Lakeflow pipelines: shared
settings overlays under `config/` and one data app under
`data-apps/telemetry/`. Settings are found by convention; nothing lists files.

`bundle/` is generated output, kept in the repo as a reference and pinned by
`tests/unit/test_lakeflow_config_migration.py`, which regenerates it and fails
on drift. Regenerate it from this directory with:

```bash
kindling bundle build --output bundle \
  --name lakeflow-telemetry --target dev --app telemetry \
  --workspace-host https://adb-lakeflow-telemetry.azuredatabricks.net \
  --workspace-id adb-lakeflow-telemetry \
  --dependency 'spark-kindling-ext-databricks==0.2.0' \
  --app-options-json '{"telemetry": {"pipelines": {
      "bronze": {"catalog": "dev_bronze", "schema": "cwmdp",
                 "pipes": ["bronze.ingest_telemetry"]},
      "silver": {"catalog": "dev_silver", "schema": "cwmdp",
                 "pipes": ["silver.build_telemetry", "silver.derive_events", "silver.derive_episodes"]}}}}'
```

Each generated pipeline resource carries the merged settings for its app
inline (`kindling.lakeflow.settings_json`): base, Databricks platform,
workspace, `dev` environment, then the app overlay, exactly the order the
runtime applies. The `--dependency` pin is illustrative; a real deployment
passes the framework, extension and app wheels with `--wheel` in dependency
order (see the CLI README).
