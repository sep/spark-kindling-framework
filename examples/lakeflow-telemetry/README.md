# lakeflow-telemetry example

A minimal Kindling project layout for Databricks Lakeflow pipelines: shared
settings overlays under `config/`, one data app under `data-apps/telemetry/`,
and a hand-written `databricks.yml` that deploys two pipelines (bronze and
silver) reading those settings through `spark.kindling.bootstrap.config_files`.

The same bundle can be generated instead of maintained. From this directory:

```bash
kindling bundle build \
  --name lakeflow-telemetry --target dev --app telemetry \
  --workspace-host https://adb-lakeflow-telemetry.azuredatabricks.net \
  --workspace-id adb-lakeflow-telemetry \
  --dependency 'spark-kindling-ext-databricks==0.1.15' \
  --app-options-json '{"telemetry": {"pipelines": {
      "bronze": {"catalog": "dev_bronze", "schema": "cwmdp",
                 "pipes": ["bronze.ingest_telemetry"]},
      "silver": {"catalog": "dev_silver", "schema": "cwmdp",
                 "pipes": ["silver.build_telemetry", "silver.derive_events", "silver.derive_episodes"]}}}}'
```

This writes `dist/bundles/databricks/` with `databricks.yml`,
`resources/telemetry_bronze.pipeline.yml`,
`resources/telemetry_silver.pipeline.yml`, `src/kindling_lakeflow.py`, and
`manifest.json`. By default the generated pipelines carry the merged settings
inline (`kindling.lakeflow.settings_json`) rather than referencing the
deployed YAML files; add `--config-transport files` to reproduce the layout of
the checked-in `databricks.yml`. The unit tests in
`tests/unit/test_cli_bundle_build.py` assert that both forms resolve to the
same effective configuration.
