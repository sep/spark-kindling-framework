"""The checked-in ``examples/lakeflow-telemetry/bundle`` is generated output.

These tests keep it honest: regenerating it from the example project with the
inputs documented in the example README must reproduce it byte for byte, and
the pipeline configuration it carries must load, as written, through the same
selector + configuration-service path a Lakeflow pipeline uses. Settings are
found by convention (``config/`` overlays, then ``data-apps/<app>/``) and
carried inline; no settings-file list appears anywhere in the bundle.
"""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml
from kindling_cli import bundle
from kindling_ext_databricks import lakeflow_app_selector as selector


class FakeConf:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)

    def getAll(self):
        return dict(self.values)


class FakeSpark:
    def __init__(self, values=None):
        self.conf = FakeConf(values)


def _repo_root() -> Path:
    return Path(__file__).parents[2]


def _example_root() -> Path:
    return _repo_root() / "examples" / "lakeflow-telemetry"


def _checked_in_bundle() -> Path:
    return _example_root() / "bundle"


#: Mirrors the command in examples/lakeflow-telemetry/README.md.
EXAMPLE_CLI_INPUTS = {
    "name": "lakeflow-telemetry",
    "target": "dev",
    "apps": ("telemetry",),
    "workspace_host": "https://adb-lakeflow-telemetry.azuredatabricks.net",
    "workspace_id": "adb-lakeflow-telemetry",
    "dependencies": ("spark-kindling-ext-databricks==0.2.0",),
    "app_options_json": json.dumps(
        {
            "telemetry": {
                "pipelines": {
                    "bronze": {
                        "catalog": "dev_bronze",
                        "schema": "cwmdp",
                        "pipes": ["bronze.ingest_telemetry"],
                    },
                    "silver": {
                        "catalog": "dev_silver",
                        "schema": "cwmdp",
                        "pipes": [
                            "silver.build_telemetry",
                            "silver.derive_events",
                            "silver.derive_episodes",
                        ],
                    },
                }
            }
        }
    ),
}


def _regenerate(output_dir: Path) -> bundle.BuildResult:
    inputs = bundle.resolve_bundle_inputs(EXAMPLE_CLI_INPUTS, {})
    return bundle.build_bundle(inputs, project_root=_example_root(), output_dir=output_dir)


def _load_pipeline_configuration(pipeline_key: str) -> dict:
    path = _checked_in_bundle() / "resources" / f"{pipeline_key}.pipeline.yml"
    resource = yaml.safe_load(path.read_text(encoding="utf-8"))
    return resource["resources"]["pipelines"][pipeline_key]["configuration"]


def _load_shared_config(initial_config: dict):
    from kindling.spark_config import DynaconfConfig

    config_service = DynaconfConfig()
    with patch(
        "kindling.spark_config.get_or_create_spark_session",
        return_value=SimpleNamespace(conf=FakeConf()),
    ):
        config_service.initialize(
            config_files=initial_config.get("config_files"),
            initial_config=initial_config,
            environment=initial_config.get("environment", "development"),
        )
    return config_service


def _without_generator_version(manifest_text: str) -> dict:
    manifest = json.loads(manifest_text)
    manifest["generator"].pop("version", None)
    return manifest


def test_checked_in_example_bundle_is_current(tmp_path):
    """Regenerating from the example project reproduces the committed output.

    The generator version is recorded only in manifest.json and differs
    between an installed CLI and a source checkout, so it is the one field
    excluded from the comparison.
    """
    result = _regenerate(tmp_path / "bundle")
    checked_in = _checked_in_bundle()

    committed_files = sorted(
        str(path.relative_to(checked_in).as_posix())
        for path in checked_in.rglob("*")
        if path.is_file()
    )
    assert committed_files == result.files
    for relative in result.files:
        generated = (result.output_dir / relative).read_text(encoding="utf-8")
        committed = (checked_in / relative).read_text(encoding="utf-8")
        if relative == bundle.MANIFEST_FILE:
            assert _without_generator_version(generated) == _without_generator_version(committed)
        else:
            assert generated == committed, relative


def test_example_bundle_carries_settings_inline_and_no_file_lists():
    for key in ("telemetry_bronze", "telemetry_silver"):
        configuration = _load_pipeline_configuration(key)
        assert selector.SETTINGS_JSON_CONFIG_KEY in configuration
        assert "spark.kindling.bootstrap.config_files" not in configuration
        assert "kindling.lakeflow.config_files" not in configuration
        assert all(isinstance(value, str) for value in configuration.values())


def test_example_bundle_loads_as_written():
    lakeflow_config = selector._pipeline_config_for_kindling(
        FakeSpark(_load_pipeline_configuration("telemetry_bronze")), "telemetry"
    )
    assert lakeflow_config.get("config_files") is None
    config_service = _load_shared_config(lakeflow_config)

    assert config_service.get("kindling.platform.environment") == "databricks"
    assert config_service.get("kindling.sdp.dataset_naming") == "leaf"
    assert config_service.get("kindling.telemetry.logging.level") == "CRITICAL"
    assert config_service.get("kindling.storage.table_schema") == "cwmdp"
    assert config_service.get("dataentities")["silver.events"]["tags"]["provider.table_name"] == (
        "dev_silver.cwmdp.events"
    )
    # Workspace overlay wins over base for a key the app overlay leaves alone.
    bytag = config_service.get("dataentities-bytag")
    assert bytag["tier"]["silver"]["tags"]["retention.days"] == "14"
