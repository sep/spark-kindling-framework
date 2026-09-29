# Kindling bundle template (Databricks)

This directory is the default template `kindling bundle build` renders. Copy it
into a project with `kindling bundle template init` and edit it as ordinary
Databricks bundle YAML with Jinja placeholders; the generator supplies only what
it uniquely knows.

Files ending in `.j2` are rendered; anything else is copied verbatim. A file
whose name contains `__pipeline__` is rendered once per pipeline with `pipeline`
bound (`key`, `name`, `app`, `catalog`, `schema`, `continuous`, `pipes`), the
name token replaced by the pipeline key.

Context: `bundle` (name, target, runtime_env, workspace_host, workspace_root,
workspace_id, run_as_service_principal, permissions), `apps` (name -> settings,
settings_json, sources), `pipelines`, `dependencies` (relative to
`resources/`), `wheels`, `kindling_version`, `generated_header`.

Helper: `kindling.configuration(app, pipes=None, extra=None)` returns the
complete pipeline `configuration` map: app selection, the inline merged
settings, the pipe subset, any extra flat overrides, and a
`kindling.lakeflow.config_keys` value computed from exactly the keys it emits.
Filters: `to_yaml`, `to_json`. Pair `to_yaml` with Jinja's `indent(n)` under a
block key.
